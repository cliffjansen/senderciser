/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/reconnect_options.hpp>
#include <proton/container.hpp>
#include <proton/message.hpp>
#include <proton/message_id.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/receiver.hpp>
#include <proton/receiver_options.hpp>
#include <proton/sender.hpp>
#include <proton/work_queue.hpp>
#include <proton/transport.hpp>

#include <atomic>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <queue>
#include <list>
#include <list>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <chrono>
#include <cassert>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

std::string tracker_error(proton::tracker *t);

// Senderciser.
//
// Test application to exercise networking re-establishment and the re-sending of in-doubt messages.
// Intended use is to start up networking components (routers/brokers) with healthy networking,
// and then mess with OS connections or restarting individual routers.
// Only the send side is "hardened".  All errors on receive end are considered fatal so those
// connections and last messaging agent must remain up for the duration of the torture session.

// Basic design:
// A sending subsystem, a receiving subsystem, a message generator.
//
// Each runs independently (dedicated threads and data structures) except that the message_generator
// inserts its messages into the sending subsystem using mutexes and proton::work_queues.
//
// The sending subsystem consists of N active proton::connections with multiple proton::senders.  It
// is "driven" by one dedicated proton::container.  Each active proton::connection has its own
// dedicated proton::mesaging_handler.
//
// The receiving subsystem consists of M active proton::connections with multiple proton::receivers
// to drain messages from all the addresses used by the sending side.  It also has one dedicated
// proton::container.  It has a single proton::mesaging_handler used concurrently by multiple threads
// and proton::connections.


// Lock output from threads to avoid scrambling
std::mutex out_lock;
#define OUT(x) do { std::lock_guard<std::mutex> l(out_lock); x; } while (false)

bool file_exists(std::string &name) {
    struct stat sbuf;
    return stat(name.c_str(), &sbuf) == 0;
}

void fatal_error(const std::string &e) {
    OUT(std::cerr << "fatal: " << e << std::endl);
    exit(1);
}

class sending_connection;
class dedup_receiver;
class message_generator;

// Global application data
class app {
  private:
    class message_generator *message_generator_;
    class dedup_receiver *dedup_receiver_;
  public:
    std::mutex lock_;
    proton::container &sending_container_;
    // Data protected by lock
    bool receiver_ready_;
    int send_connections_count_;

    // Static data for the life of the threads that use it.  No lock protection.
    int n_sender_threads_;
    std::vector<std::string> send_urls_;
    proton::connection_options sender_copts_;
    int n_receiver_threads_;
    std::string receive_url_;
    std::string stop_file_;
    proton::duration max_run_duration_;
    proton::timestamp start_time_;
    std::vector<class sending_connection *> sending_connections_;

    app(proton::container &cntnr) : message_generator_(NULL), dedup_receiver_(NULL),
                                    sending_container_(cntnr), receiver_ready_(false),
                                    send_connections_count_(0), n_sender_threads_(0),
                                    n_receiver_threads_(0), max_run_duration_(0), start_time_(0)
    {}

    void message_generator(class message_generator *mg) { message_generator_ = mg; }
    class message_generator &message_generator() {
        assert(message_generator_);
        return *message_generator_;
    }

    void dedup_receiver(class dedup_receiver *recv) { dedup_receiver_ = recv; }
    class dedup_receiver &dedup_receiver() {
        assert(dedup_receiver_);
        return *dedup_receiver_;
    }
            
  private:
    app(const app&) = delete;
    app& operator=(const app&) = delete;
};

class tracked_message {
  public:
    proton::message pmsg_;
    std::string &addr_;
    unsigned resend_count_;
    unsigned released_count_;
    proton::timestamp post_time_;
    proton::timestamp last_send_time_;

    tracked_message(std::string &addr, uint64_t id0) : addr_(addr), resend_count_(0), released_count_(0),
                                                       post_time_(proton::timestamp::now()),
                                                       last_send_time_(0)
    {
        pmsg_.id(id0);
        pmsg_.body(post_time_);
    }

  private:
    tracked_message(const tracked_message&) = delete;
    tracked_message& operator=(const tracked_message&) = delete;
};

class outgoing_q {
  public:
    std::queue<tracked_message*> msgs_;
    std::string addr_;

    outgoing_q() {}
  private:
    outgoing_q(const outgoing_q&) = delete;
    outgoing_q& operator=(const outgoing_q&) = delete;
};


// Connects to url.  Reconnects if connection broken. Has multiple proton::senders.
// Resends messages until peer confirms acceptance.
// Creates a brand new proton::connection when needed.
//
// Arbitrary choice for this example:
//     2 proton::senders per connection,
//     immediate or remote peer accepts messages fairly quickly and in line with credit replenishment,
//     credit is neither huge nor starved,
//     network fails frequently but has plenty of bandwidth to allow re-transmissions to catch up.
//
// Goal: just enough complexity for multi-threading between proton::senders and container threads;
// credit is a good proxy for messages in flight and max number of pending resends; memory
// management simple; performance optimization of the use of data structures not critical to
// runtime behavior.
class sending_connection : private proton::messaging_handler {
    class app &app_;
    proton::container &sending_container_;
    std::string &send_url_;
    int id_;

  public:

    proton::sender sender1_;
    proton::sender sender2_;
    proton::connection connection_;
    bool connected_;
    bool reconnecting_;
    bool shutting_down_;
    class outgoing_q outgoing1_;
    class outgoing_q outgoing2_;
    // A tracked message starts on one of outgoingN_ according to its address.  Then it moves
    // between trackers_ and resends_ until accepted.
    std::list<tracked_message*> resends_;
    std::map<tracked_message*, proton::tracker> trackers_; 

    // Shared by proton and user threads, protected by lock_
    proton::work_queue *work_queue_;
    class outgoing_q staged_;
    std::mutex lock_;
    size_t backlog_;

    // Stats
    size_t fresh_connects_;
    size_t reconnects_;
    size_t total_resends_;
    size_t max_resend_count_;
    proton::duration max_in_doubt_ticks_;

    sending_connection(class app &app0, proton::container &cont, std::string &url, int id)
        : app_(app0), sending_container_(cont), send_url_(url), id_(id), connected_(false),
          reconnecting_(0), shutting_down_(false), work_queue_(NULL), backlog_(0),
          fresh_connects_(0), reconnects_(0), total_resends_(0), max_resend_count_(0),
          max_in_doubt_ticks_(0)
    {
        start_new_connection();
    }

  private:

    void start_new_connection() {
        proton::connection_options copts(app_.sender_copts_);
        sending_container_.connect(send_url_, copts.handler(*this));
        OUT(std::cout << "sender " << id_ << " connect to " << send_url_ << std::endl);
    }

    // Move generated messages from shared location to private.
    void merge_staged_messages() {
       std::unique_lock<std::mutex> l(lock_);
       while(staged_.msgs_.size()) {
           auto msg_ptr = staged_.msgs_.front();
           auto out_q = get_outgoing(msg_ptr->addr_);
           out_q->msgs_.push(msg_ptr);
           staged_.msgs_.pop();
       }
    }

    outgoing_q *get_outgoing(std::string &addr) {
        // Last char determines result.
        if (addr[5] == outgoing1_.addr_[5]) return &outgoing1_;
        if (addr[5] == outgoing2_.addr_[5]) return &outgoing2_;
        throw std::runtime_error("internal sender address logic error");
    }

    proton::sender *get_sender(std::string& addr) {
        if (addr[5] == outgoing1_.addr_[5]) return &sender1_;
        if (addr[5] == outgoing2_.addr_[5]) return &sender2_;
        throw std::runtime_error("internal sender address logic error");
    }

    int total_credit() {
        return sender1_.credit() + sender2_.credit();
    }
    
    bool send_msg(tracked_message *m) {
        auto sender = get_sender(m->addr_);
        if (sender->credit() <= 0)
            return false;
        m->last_send_time_ = proton::timestamp::now();
        proton::tracker t = sender->send(m->pmsg_);
        t.user_data(m);
        trackers_[m] = t;
        return true;
    }

    void update_backlog() {
        std::unique_lock<std::mutex> l(lock_);
        backlog_ = outgoing1_.msgs_.size() + outgoing2_.msgs_.size() + trackers_.size() +
            resends_.size() + staged_.msgs_.size();
    }

    // Called via new credit (on_sendable) or new messages to send (initiate_sending_lh).
    void try_sending() {
        // Only called from proton::connection callbacks including work_queue_.
        // No locks necessary except during merge.
        if (!connected_ || total_credit() == 0) {
            update_backlog();
            return;
        }
        merge_staged_messages();

        // Do resends before more recently staged messages.
        // An arbitrary choice for our messages which have no assigned priority.
        // Sending success is subject to credit, which may differ between proton::senders.
        for (auto m_curr = resends_.begin(); m_curr != resends_.end(); ) {
            if (send_msg(*m_curr)) {
                (*m_curr)->resend_count_++;
                total_resends_++;
                m_curr = resends_.erase(m_curr);
            } else {
                m_curr++;
            }
        }

        while (!outgoing1_.msgs_.empty()) {
            if (send_msg(outgoing1_.msgs_.front()))
                outgoing1_.msgs_.pop();
            else
                break;  // No credit.
        }

        while (!outgoing2_.msgs_.empty()) {
            if (send_msg(outgoing2_.msgs_.front()))
                outgoing2_.msgs_.pop();
            else
                break;
        }
        update_backlog();
    }

    // Work queue call back.
    void initiate_sending_cb () {
        try_sending();
    }

    // Work queue call back.
    void initiate_shutdown_cb() {
        assert(shutting_down_);
        if (connected_) {
            try_sending();
            close_if_finished();
        }
    }

    void thread_safe_cleanup() {
        // Must be called from a callback from connection_ for thread safe object destruction.
        if (connection_)
            connection_ = proton::connection();
        if (sender1_)
            sender1_ = proton::sender();
        if (sender2_)
            sender2_ = proton::sender();
        // trackers_ managed elsewhere and should be gone.
        if (!trackers_.empty())
            fatal_error("Failed to release/repurpose trackers_ before proton::connection object cleanup");
    }

    void cancel_trackers() {
        // Move any old in doubt tracked messages to resends_
        // Must be called from a callback from connection_ for thread safe object destruction.
        for (auto it = trackers_.begin(); it != trackers_.end(); ++it) {
            resends_.push_back(it->first);
        }
        // This will call destructors on the contained proton::tracker objects.
        trackers_.clear();
    }

    void graceful_close() {
        // Be polite to peer (best efforts only).
        // Whether forcing a new connection or shutting down, we no longer need our saved proton objects.
        // This is a good thread-safe place to call destructors.
        if (connection_) {
            if (sender1_) {
                sender1_.close();
            }
            if (sender2_){
                sender2_.close();
            }
            connection_.close();
            cancel_trackers();
        } else {
            if (!trackers_.empty())
                // Sanity check.  Dangling trackers are a thread safety problem.
                fatal_error("Unresolved tracker objects from previous connection");
        }

        reset_connected();
        thread_safe_cleanup();
    }

    bool finished() {
        // True if no more messages to send and no messages in doubt.
        std::unique_lock<std::mutex> l(lock_);
        return shutting_down_ && staged_.msgs_.empty() && outgoing1_.msgs_.empty() && outgoing2_.msgs_.empty() &&
            resends_.empty() && trackers_.empty();
    }

    void close_if_finished() {
        assert(shutting_down_);
        if (finished())
            graceful_close();
    }

    void set_connected() {
        if (!connected_) {
            connected_ = true;
            {
                std::unique_lock<std::mutex> l(lock_);
                work_queue_ = &connection_.work_queue();
            }
            {
                std::unique_lock<std::mutex> l(app_.lock_);
                app_.send_connections_count_++;
            }
        }

    }

    void reset_connected() {
        if (connected_) {
            connected_ = false;
            {
                // The work queue survives on reconnect but not for a brand new connection.
                // The time between now and the next on_connection_open may be very long.
                // For this example, we choose arbitrarily to discard our work when it is not
                // possible to send messages (so it does not pile up as mostly duplicate items), and
                // resume when it is.
                std::unique_lock<std::mutex> l(lock_);
                work_queue_ = NULL;
            }
            {
                std::unique_lock<std::mutex> l(app_.lock_);
                app_.send_connections_count_--;
            }
        }
    }

    void force_new_connection() {
        assert(connected_);
        reset_connected();
        auto old_conn = connection_;
        graceful_close();
        // Most applications are fine to wait for transport_closed on old_conn before starting the
        // next connection.  In our case, we assume frequent network mischief and packet loss.
        // Our new connection is not immediate, but still allows for overlapping callbacks.
        old_conn.user_data(NULL);  // Mark discarded.
        reconnecting_ = false;     // Brand new proton::connection pending.

        OUT(std::cout << "force new connect to " << send_url_ << std::endl);
        // Short delay.  Allow system a chance to normalize without busy looping.
        // First node may immediately accept a connection but fail to establish the link route
        // through subsequent nodes and fail right away.
        app_.sending_container_.schedule(proton::duration(1000), [&]() { this->start_new_connection(); });
    }

    bool discarded(const proton::connection &c) {
        // Discarded means we closed the connection while opening a new one and do not
        // wish to confuse callbacks from the older one.
        // Callbacks from the discarded and active connections can be called simultaneously
        // from 2 threads, so the discarded connection must not touch any shared data.
        return c.user_data() == NULL;
    }

    bool local_closed(const proton::connection &c) {
        // Certain callacks from this connection be ignored (including just to log errors)
        // because we already decided to close this connection (in a previous callback).
        if (discarded(c) || (!c.active())) return true;
        // No thread safety concerns.
        return !connected_;
    }

    void on_connection_open(proton::connection& c) override {
        if (!trackers_.empty()) {
            fatal_error("trackers_ not resolved on reconnect"); // Internal logic error.
        }
        if (!c.reconnected()) {
            OUT(std::cout << "New proton::connection " << id_ << std::endl);
            assert(reconnecting_ == false);
            fresh_connects_++;
            // Set up our senders.
            proton::session ssn = c.default_session();
            std::ostringstream ss;
            ss << "lrq." << id_ << 'a';
            outgoing1_.addr_ = ss.str();
            outgoing2_.addr_ = outgoing1_.addr_;
            outgoing2_.addr_[5] = 'b';
            sender1_ = ssn.open_sender(outgoing1_.addr_);
            sender2_ = ssn.open_sender(outgoing2_.addr_);
            connection_ = c;
            connection_.user_data(this);
        } else {
            OUT(std::cout << "Reconnected proton::connection " << id_ << std::endl);
            assert(reconnecting_ == true);
            reconnecting_ = false;
            reconnects_++;
            assert(c == connection_);
            assert(this == reinterpret_cast<sending_connection *>(c.user_data()));
        }
        set_connected();
        if (shutting_down_) {
            close_if_finished();
            // If not finished, continue until last message is accepted.
        }
    }

    void on_sender_open(proton::sender& s) override {
        if (discarded(s.connection())) return;
        assert((s == sender1_) || (s == sender2_)); // Program logic check.
    }

    void on_sendable(proton::sender& s) override {
        if (local_closed(s.connection())) return;
        try_sending();
    }

    void on_tracker_accept(proton::tracker &t) override {
        if (local_closed(t.connection())) return;
        tracked_message *trmsgp = reinterpret_cast<tracked_message*>(t.user_data());
        auto mapp = trackers_.find(trmsgp);
        if (mapp == trackers_.end()) fatal_error("accepted tracker not found");
        if (t != (*mapp).second) fatal_error("accepted tracker mismatch");
        
        if (trmsgp->resend_count_ > max_resend_count_)
            max_resend_count_ = trmsgp->resend_count_;
        proton::duration in_doubt_ticks = proton::timestamp::now() - trmsgp->post_time_;
        if (in_doubt_ticks > max_in_doubt_ticks_) 
            max_in_doubt_ticks_ = in_doubt_ticks;
        trackers_.erase(mapp);
        delete trmsgp;
        if (shutting_down_)
            close_if_finished();
        update_backlog();
    }

    void on_tracker_reject(proton::tracker &t) override {
        if (local_closed(t.connection())) return;
        tracked_message *trmsgp = reinterpret_cast<tracked_message*>(t.user_data());
        uint64_t id = proton::coerce<uint64_t>(trmsgp->pmsg_.id());
        std::string err = tracker_error(&t);
        OUT(std::cerr << "tracker REJECT: " << trmsgp->addr_ << ' ' << id << " " << err << std::endl);
        // Not expected.  Try a fresh start.
        force_new_connection();
    }

    void on_tracker_release(proton::tracker &t) override {
        if (local_closed(t.connection())) return;
        tracked_message *trmsgp = reinterpret_cast<tracked_message*>(t.user_data());
        // We have no "elsewhere" to send the message.  Try at most twice on the existing connection.
        // Peer may subsequently close the link with an error condition we can log or react to.
        if (++trmsgp->released_count_ > 1) {
            trmsgp->released_count_ = 0;  // reset counter for future connection(s)
            force_new_connection();
        } else {
            auto mapp = trackers_.find(trmsgp);
            if (mapp == trackers_.end()) fatal_error("released tracker not found");
            if (t != (*mapp).second) fatal_error("released tracker mismatch");
            // Move tracked message from trackers_ to resends_
            resends_.push_back(trmsgp);
            trackers_.erase(mapp);
        }
    }

    void on_connection_error(proton::connection& c) override {
        if (local_closed(c)) return;
        OUT(std::cerr << "connection error: " << c.error() << std::endl);
    }

    void on_connection_close(proton::connection& c) override {
        if (local_closed(c)) return;
        // Remote peer unhappy about something other than a problem on a sending link.
        // Not likely to be fixed in a subsequent connection.
        fatal_error("unexpected connection close from remote peer");
    }

    void on_sender_error(proton::sender& s) override {
        if (local_closed(s.connection())) return;
        OUT(std::cout << "sender link error: " << s.error() << std::endl);
    }

    void on_sender_close(proton::sender& s) override {
        if (local_closed(s.connection())) return;
        if (!finished()) {
            // We are not expecting peer to initiate sender close.
            // This will not cause a reconnect unless we do so manually.
            // Edge case: this is still possible between start of shutting_down_
            // and shutdown_complete().
            OUT(std::cout << "sender link closed.  Creating new connection" << std::endl);
            force_new_connection();
        }
    }

    void on_transport_error(proton::transport& t) override {
        if (discarded(t.connection())) return;
        reconnecting_ = true;
        cancel_trackers();
        reset_connected();
        OUT(std::cout << "transport error: " << t.error() << std::endl);
        // reconnect according to connection_options.
    }

    void on_transport_close(proton::transport &t) override {
        if (discarded(t.connection())) return;
        reset_connected();
        OUT(std::cout << "sending_connection transport closed" << std::endl);
    }

    void on_error(const proton::error_condition& e) override {
        OUT(std::cerr << "unexpected sending connection error: " << e << std::endl);
        exit(1);
    }

    void on_container_start(proton::container& cont) override {
        // This should have gone to our default handler.
        fatal_error("sending_connection handler mismatch for container start");
    }

  public:

    // Call with this->lock_ held.
    // Lock is required for work_queue_ validity test
    void initiate_sending_lh() {
        if (work_queue_) {
            work_queue_->add([=]() { this->initiate_sending_cb(); });
        }
    }

    void initiate_shutdown() {
        assert(!shutting_down_);
        shutting_down_ = true;

        std::unique_lock<std::mutex> l(lock_);        
        // If no work queue, on_connection_open() is in charge.
        if (work_queue_) {
            work_queue_->add([=]() { this->initiate_shutdown_cb(); });
        }
    }

  private:
    sending_connection(const sending_connection&) = delete;
    sending_connection& operator=(const sending_connection&) = delete;
};

// Drains messages from destination.  Main job is to exclude duplicate messages
// and gather delivery statistics.  Runs off its own private proton::container and
// threads, i.e. separate from the send side.
class dedup_receiver : private proton::messaging_handler {
    class app &app_;
    std::mutex lock_;
    proton::container recv_container_;
  public:
    std::set<uint64_t> ooo_msgs_;  // out of order messages tracked for de-dup
    uint64_t max_contiguous_id_;
    uint64_t max_seen_id_;
    uint64_t duplicate_count_;
    proton::duration total_transit_span_;
    proton::duration max_transit_;
    int receivers_count_;
    int open_count_;
    uint64_t shutdown_target_id_;
    bool closing_;
    typedef std::pair<proton::connection, proton::work_queue *> conn_data;
    std::list<conn_data> connections_;


    dedup_receiver(class app &app0)
        : app_(app0), recv_container_(*this, "test_receiver"), max_contiguous_id_(0), max_seen_id_(0),
          duplicate_count_(0), total_transit_span_(0), max_transit_(0), receivers_count_(0), open_count_(0),
          shutdown_target_id_(0), closing_(false)
    {
    }

    void run() {
        try {
            recv_container_.run(app_.n_receiver_threads_);
        } catch (const std::exception& e) {
            OUT(std::cerr << "dedup_receiver error in Proton callback processing: " << e.what() << std::endl);
            fatal_error("See previous receiver container error");
        }
    }
    
  private:
    void close_connection_cb(conn_data *cdp) {
        assert(reinterpret_cast<conn_data *>(cdp->first.user_data()) == cdp);
        cdp->first.close();
    }

    void note_inbound_msg(uint64_t id, proton::timestamp msg_start_time) {
        // Mostly updates shared data.  Keep this method lean while holding lock.
        std::lock_guard<std::mutex> l(lock_);

        auto now = proton::timestamp::now();
        bool duplicate = false;
        if (id > max_seen_id_)
            max_seen_id_ = id;
        if (id <= max_contiguous_id_) {
            duplicate = true;
        }
        else if (id == max_contiguous_id_ + 1) {
            max_contiguous_id_ = id;
            while (true) {
                auto ooo_first = ooo_msgs_.cbegin();
                if (ooo_first == ooo_msgs_.cend()) break;
                if (*ooo_first == max_contiguous_id_ + 1) {
                    // Now part of contiguous ids: consolidate.
                    max_contiguous_id_++;
                    ooo_msgs_.erase(ooo_first);
                }
                else break;
            }
        } else if (ooo_msgs_.find(id) != ooo_msgs_.end()) {
            duplicate = true;
        } else {
            ooo_msgs_.insert(id);
        }

        if (duplicate) {
            duplicate_count_++;
        } else {
            if (now < msg_start_time) fatal_error("time statistics failure");
            proton::duration arrival_span = now - msg_start_time;
            total_transit_span_ = total_transit_span_ + arrival_span;
            if (arrival_span > max_transit_)
                max_transit_ = arrival_span;
        }

        // Receiver keeps going until last message has arrived.  Check here.
        check_recv_finished_lh();
    }

    void on_container_start(proton::container& cont) override {
        OUT(std::cout << "(receiver) container start" << std::endl);
        for (int i = 0; i < app_.n_receiver_threads_; i++) {
            // connection_option defaults sufficient for receiver.  No errors expected.
            cont.connect(app_.receive_url_, proton::connection_options().handler(*this));
        }
    }

    void on_connection_open(proton::connection &conn) override {
        // First connection does 'a' addrs, second does 'b' addrs.
        int seq;
        {
            std::lock_guard<std::mutex> l(lock_);
            conn_data cdata(conn, &conn.work_queue());
            connections_.push_back(cdata);
            conn_data *cdp = &connections_.back();
            conn.user_data(cdp);
            seq = open_count_++;
        }
        proton::session s = conn.open_session();
        char suffix = seq == 0 ? 'a' : 'b';
        for (int i = 0 ; i < app_.send_urls_.size(); i++) {
            std::ostringstream ss;
            ss << "lrq." << i << suffix;
            s.open_receiver(ss.str(), proton::receiver_options().credit_window(100));
        }
    }

    void on_receiver_open(proton::receiver& r)  override {
        bool ready = false;
        {
            std::lock_guard<std::mutex> l(lock_);
            if (++receivers_count_ == (2 * app_.send_urls_.size()))
                ready = true;  // Receiving for all addresses.
        }
        if (ready) {
            std::lock_guard<std::mutex> l(app_.lock_);
            app_.receiver_ready_ = true;
        }
    }

    void on_message(proton::delivery &d, proton::message &m) override {
        uint64_t id = proton::coerce<uint64_t>(m.id());
        proton::timestamp start_time = proton::get<proton::timestamp>(m.body());
        note_inbound_msg(id, start_time);
    }

    void on_transport_error(proton::transport &t) override {
        OUT(std::cerr << "transport error: " << t.error().what() << std::endl);
        fatal_error("Receiver connection early failure.");
    }

    void on_transport_close(proton::transport &t) override {
        // We have parked our connection data in a list and kept a pointer to it.
        conn_data *list_cdp = reinterpret_cast<conn_data *>(t.connection().user_data());
        assert(list_cdp->first == t.connection());

        // This callback is the safe place to tidy up Proton objects derived from this connection.
        list_cdp->first = proton::connection(); // list destructor can now safely delete the element from any thread.
        std::lock_guard<std::mutex> l(lock_);
        list_cdp->second = NULL;                // Tell other threads this connection no longer accepts work.
    }

    void on_error(const proton::error_condition& e) override {
        OUT(std::cerr << "unexpected dedup_recever error: " << e << std::endl);
        fatal_error("See previous receiving container error");
    }

    void initiate_close_connection_lh(conn_data *cdp) {
        // Check work_queue valid, protected by receiver lock
        if (!cdp->second)
            return;
        cdp->second->add([=]() { this->close_connection_cb(cdp); });
    }

  public:

    // Call with receiver lock held.
    // This can be called from any thread.  Do not touch Proton objects here.
    void check_recv_finished_lh() {
        if (shutdown_target_id_ && !closing_) {
            if (max_contiguous_id_ == shutdown_target_id_) {
                closing_ = true;
                for (auto it = connections_.begin(); it != connections_.end(); it++) {
                    conn_data *cdp = &(*it);
                    initiate_close_connection_lh(cdp);
                }
            }
        }
    }

    std::pair<uint64_t, uint64_t> receive_stats() {
        // return <highest id seen, number missing < highest id>
        std::lock_guard<std::mutex> l(lock_);
        uint64_t pending;
        if (ooo_msgs_.empty()) {
            assert(max_contiguous_id_ == max_seen_id_);
            pending = 0;
        } else {
            pending = (max_seen_id_ - max_contiguous_id_) - ooo_msgs_.size();
        }
        return std::pair<uint64_t, uint64_t>(max_seen_id_, pending);
    }

    void initiate_shutdown(uint64_t id) {
        std::lock_guard<std::mutex> l(lock_);
        if (!id || shutdown_target_id_) fatal_error("receiver shutdown state error");
        shutdown_target_id_ = id;
        if (open_count_)
            check_recv_finished_lh();
    }

  private:
    dedup_receiver(const dedup_receiver&) = delete;
    dedup_receiver& operator=(const dedup_receiver&) = delete;
};

void run_sending_container(class app &app) {
    try {
        app.sending_container_.run(app.n_sender_threads_);
    } catch (const std::exception& e) {
        OUT(std::cerr << "error in Proton callback processing: " << e.what() << std::endl);
        fatal_error("See previous sending container error");
    }
}

class message_generator {
    class app &app_;
    std::mutex lock_;
    std::thread thread_;
    bool finished_;
  public:
    uint64_t last_generated_id_;
    
    message_generator(class app &app0) : app_(app0), finished_(false), last_generated_id_(0) {}

    uint64_t stop() {
        {
            std::lock_guard<std::mutex> l(lock_);
            finished_ = true;
        }
        thread_.join();
        return last_generated_id_;
    }

    void start() {
        thread_ = std::thread([=]() { this->run(); });
    }

    void run() {
        using namespace std::chrono_literals;
        try {
            generator_loop();
        } catch (const std::exception& e) {
            OUT(std::cerr << "error in message generator: " << e.what() << std::endl);
            fatal_error("message generator failed");
        }
        OUT(std::cout << "message generator finished " << last_generated_id_ << std::endl);
    }

  private:
    void generator_loop();  // defined below
    message_generator(const message_generator&) = delete;
    message_generator& operator=(const message_generator&) = delete;
};


// Housekeeping.  No locking required: connection callbacks go to other handlers.
// Scheduled work is serialized.
class default_handler: public proton::messaging_handler {
    class app *app_;
    bool shutting_down_;

  public:
    default_handler() : app_(0), shutting_down_(0) {}

    void app(class app &app0) { app_ = &app0; }

    void monitor_stop_condition() {
        if (!app_->stop_file_.empty() && file_exists(app_->stop_file_)) {
            initiate_global_shutdown();
            return;
        }
        if (app_->max_run_duration_.milliseconds()) {
            proton::duration runtime = proton::timestamp::now() - app_->start_time_;
            if (app_->max_run_duration_ < runtime) {
                initiate_global_shutdown();
                return;
            }
        }        
        app_->sending_container_.schedule(proton::duration(500), [&]() { this->monitor_stop_condition(); });
    }

  private:
    void on_container_start(proton::container& cont) override {
        OUT(std::cout << "(sender) container start" << std::endl);
        app_->sending_container_.schedule(proton::duration(500), [&]() { this->monitor_stop_condition(); });
    }
    void on_connection_open(proton::connection &conn) override {
        fatal_error("sending connection created without separate handler");
    }

    void initiate_global_shutdown() {
        if (!shutting_down_) {
            shutting_down_ = true;
            uint64_t last_id = app_->message_generator().stop();
            for (int i = 0; i < app_->sending_connections_.size(); i++) {
                sending_connection *scp = app_->sending_connections_[i];
                scp->initiate_shutdown();
            }
            app_->dedup_receiver().initiate_shutdown(last_id);
        }
    }        

  private:
    default_handler(const default_handler&) = delete;
    default_handler& operator=(const default_handler&) = delete;
};

void print_stats(class app &app) {
    dedup_receiver &recv(app.dedup_receiver());
    message_generator &mg(app.message_generator());
    uint64_t backlog = 0;
    for (int i = 0; i < app.sending_connections_.size(); i++) {
        sending_connection *scp = app.sending_connections_[i];
        std::unique_lock<std::mutex> l(scp->lock_);
        backlog += scp->backlog_;
    }
    auto rcv_stats = app.dedup_receiver().receive_stats();
    auto max_seen_id = rcv_stats.first;
    auto missing = rcv_stats.second;

    std::cout << "R: " << max_seen_id << ',' << missing <<
        "   S:" << mg.last_generated_id_ << ',' << backlog << std::endl;
}

int main(int argc, const char **argv) {
    using namespace std::chrono_literals;

    try {
        class default_handler hndlr;
        proton::container sending_container(hndlr, "test_sender");
        class app app(sending_container);
        hndlr.app(app);

        // Configure app:
        app.n_sender_threads_ = 3;  
        app.send_urls_.push_back("127.0.0.1:10300"); // addresses "q0a" "q0b"
        app.send_urls_.push_back("127.0.0.1:10300"); // addresses "q1a" "q1b"
        app.send_urls_.push_back("127.0.0.1:10300"); // ...
        app.sender_copts_.idle_timeout(proton::duration::MILLISECOND * 5000)
            .reconnect(proton::reconnect_options().max_delay(proton::duration::MILLISECOND * 3000));

        app.n_receiver_threads_ = 2;
        app.receive_url_ = "127.0.0.1:10400";

        // App runs until stop condition.  Set one or both.
        app.stop_file_ = "./stop_running_marker";
//        app.max_run_duration_ = proton::duration(60000);

        app.start_time_ = proton::timestamp::now();

        // Stop condition sanity check
        if (file_exists(app.stop_file_))
            fatal_error("Stop file exists  Aborting.");
        if (app.max_run_duration_.milliseconds() == 0 && app.stop_file_.empty())
            fatal_error("Please set stop file name and/or maximum run duration.");

        // Launch the app.  Start receiver and wait for it to initialize.
        // Then the sending components.  Then the message generator.
        
        dedup_receiver recv(app);
        app.dedup_receiver(&recv);
        auto receiver_main_thread = std::thread([&]() { recv.run(); });
        int pause_count = 0;
        while (true) {
            if (pause_count > 50) fatal_error("receiver not ready in 5 seconds");
            std::this_thread::sleep_for(100ms);
            pause_count++;
            std::unique_lock<std::mutex> l(app.lock_);
            if (app.receiver_ready_)
                break;
        }

        auto sender_main_thread = std::thread([&]() { run_sending_container(app); });
        for (int i = 0; i < app.send_urls_.size(); i++) {
            sending_connection *sc = new sending_connection(app, app.sending_container_,
                                                            app.send_urls_[i], i);
            app.sending_connections_.push_back(sc);
        }
        pause_count = 0;
        auto num_sc = app.sending_connections_.size();
        while (true) {
            if (pause_count > 50) fatal_error("sending connections not ready in 5 seconds");
            std::this_thread::sleep_for(100ms);
            pause_count++;
            std::unique_lock<std::mutex> l(app.lock_);
            if (app.send_connections_count_ == num_sc)
                break;
        }

        message_generator generator(app);
        app.message_generator(&generator);
        generator.start();

        OUT(std::cout << argv[0] << " Up and running...  destabilize network now." << std::endl);

        // Application runs until stop condition detected, all connections close, and
        // proton::containers auto_stop.
        receiver_main_thread.join();
        sender_main_thread.join();

        // Now single threaded.
        // Proton objects no longer in use and have been thread-safely finalized by this point.

        size_t fc(0), rc(0), tr(0), mr(0);
        proton::duration mt(0);
        for (int i = 0; i < app.sending_connections_.size(); i++) {
            sending_connection *scp = app.sending_connections_[i];
            fc += scp->fresh_connects_;
            rc += scp->reconnects_;
            tr += scp->total_resends_;
            if (mr < scp->max_resend_count_) mr = scp->max_resend_count_;
            if (mt < scp->max_in_doubt_ticks_) mt = scp->max_in_doubt_ticks_;
            delete scp;
        }

        OUT(std::cout << std::endl);
        OUT(std::cout << argv[0] << " done." << std::endl);
        OUT(std::cout << "  total messages: " << generator.last_generated_id_ << std::endl);
        OUT(std::cout << "  proton::connections: " << fc << std::endl);
        OUT(std::cout << "  reconnects: " << rc << std::endl);
        OUT(std::cout << "  messages resent: " << tr << std::endl);
        OUT(std::cout << "  max resends for 1 message: " << mr << std::endl);
        OUT(std::cout << "  max in doubt millis for one message: " << mt << std::endl);
        OUT(std::cout << "  duplicates received: " << recv.duplicate_count_ << std::endl);
        OUT(std::cout << "  slowest message to receiver (millis): " << recv.max_transit_ << std::endl);

        return 0;
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
    return 1;
}


#include <random>

void message_generator::generator_loop() {
    using namespace std::chrono_literals;
    std::random_device rd;
    uint_fast32_t seed = rd();
    std::ranlux24_base semi_rand(seed);  // Random enough for this purpose.  Light weight.

    uint64_t next_id = 1;
    const uint_fast32_t n_conns = (uint_fast32_t) app_.sending_connections_.size();  // Number of connections (static).
    proton::timestamp start_time = proton::timestamp::now();
    proton::duration stats_interval(5000);
    proton::timestamp next_stats_time = start_time + stats_interval;
    uint64_t ticks = 0;

    while (!finished_) {
        ticks++;
        if (proton::timestamp::now() >= next_stats_time) {
            next_stats_time = next_stats_time + stats_interval;
            last_generated_id_ = next_id - 1;
            print_stats(app_);
        }
                
        // Generate some messages and distribute among connections and associated senders.
        // Logic results in quite lumpy distribution, but fairly even over time.

        uint_fast32_t batch_count = semi_rand() % (4 * n_conns);  // A message per sender per tick on average.
        uint_fast32_t c_idx = semi_rand() % n_conns;              // Where to start round robin.
        bool start_low = (semi_rand() % 2);
        for (int i = 0; i < n_conns; i++) {
            sending_connection *scp = app_.sending_connections_[c_idx];
            {
                int mcount = 0;
                if (i == n_conns -1) {
                    mcount = batch_count;  // Last connection picks up remaining.
                } else {
                    if ((semi_rand() % 2) == 0) { // 50% chance connection is selected
                        if (batch_count > 1)
                            mcount = semi_rand() % batch_count;
                        else
                            mcount = batch_count;
                    }
                }
                batch_count -= mcount;
                if (mcount) {
                    std::unique_lock<std::mutex> l(scp->lock_);
                    int s_idx = start_low ? 0 : 1;
                    // Alternate messages over the connection's senders
                    for (; mcount; mcount--, s_idx++) {
                        std::string *addrp = ((s_idx % 2) == 0) ? &scp->outgoing1_.addr_ : &scp->outgoing2_.addr_;
                        tracked_message *tmsg = new tracked_message(*addrp, next_id++);
                        scp->staged_.msgs_.push(tmsg);
                    }             
                    scp->initiate_sending_lh();
                }
            }
            c_idx++; if (c_idx == n_conns) c_idx = 0;
        }
        assert(batch_count == 0);

        //       std::this_thread::sleep_for(1ms);  // 1 millisec
        //       std::this_thread::sleep_for(100us);  // 100 microsec
        std::this_thread::sleep_for(250us);
        std::unique_lock<std::mutex> l(lock_);
    }
    last_generated_id_ = next_id - 1;
}


#include <proton/delivery.h>
#include <proton/disposition.h>
// Dig out error from Proton C.
// Direct use of underlying Proton C objects not recommended and not supported.
std::string tracker_error(proton::tracker *t) {
    struct fubar {
        pn_delivery_t *ptr_;
    };
    struct fubar *trp = reinterpret_cast<struct fubar *>(t);
    pn_delivery_t *dlv = trp->ptr_;
    pn_disposition_t *disp = pn_delivery_remote(dlv);
    pn_condition_t *cond = pn_disposition_condition(disp);
    if (!pn_condition_is_set(cond)) return "<no error condition>";
    std::string x(pn_condition_get_name(cond));
    x += ": ";
    x += std::string(pn_condition_get_description(cond));
    return x;
}
