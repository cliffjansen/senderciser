# Senderciser

A sample resilient message sending program for Qpid Proton C++

~~~
$ ./senderciser
(receiver) container start
(sender) container start
sender 0 connect to 127.0.0.1:10300
sender 1 connect to 127.0.0.1:10300
New proton::connection 0
sender 2 connect to 127.0.0.1:10300
New proton::connection 1
New proton::connection 2
./senderciser Up and running...  destabilize network now.
R: 7206,10   S:7208,12
R: 10858,13   S:27753,16908
transport error: amqp:resource-limit-exceeded: local-idle-timeout expired
transport error: amqp:resource-limit-exceeded: local-idle-timeout expired
transport error: amqp:resource-limit-exceeded: local-idle-timeout expired
R: 10858,13   S:61991,33609
R: 10858,13   S:97401,33609
R: 10858,13   S:132549,33609
Reconnected proton::connection 2
Reconnected proton::connection 0
Reconnected proton::connection 1
R: 15681,511   S:151669,136497
R: 24439,123   S:154704,130406
R: 33705,238   S:157727,124265
R: 42843,125   S:160795,118052
message generator finished 162177
sending_connection transport closed
sending_connection transport closed
sending_connection transport closed

./senderciser done.
  total messages: 162177
  proton::connections: 3
  reconnects: 3
  messages resent: 30
  max resends for 1 message: 1
  max in doubt millis for one message: 72676
  duplicates received: 30
  slowest message to receiver (millis): 72690
$  

~~~

## Overview

This sample application expects to be run within a network of AMQP intermediaries such as Qpid Dispatch Router with a final target messaging broker.  If the network is stable, the messages flow smoothly from the application to the broker and back again to the application.  The intended use is to observe the application behavior during deliberate de-stabilization of the network affecting the message path from the application to the broker (the sending path).  This may be done by interfering with packets, TCP connections, or restarting some or all qdrouterd instances.

See the comments in the source code for a description of the design of the application.

## Installation

The example program is configured at the source level.  Configure the sender and receiver target addresses and desired thread allocations.  The program can be cleanly shut down by either specifying a maximum duration for the run or by creating a stop file ("touch /path/to/stopfile").

Compile and link the program as for any other multi-threaded Qpid Proton C++ example.

## Simple run

A simple example where the messages flow between the application, 2 Qpid Dispatch routers, a broker, and back to the application is described.

The application has been tested using either the Proton C example broker or ActiveMQ Artemis as the broker.

Proton: "/path/to/c/examples/broker 127.0.0.1 10400"

Artemis: "podman pull quay.io/artemiscloud/activemq-artemis-broker ; podman run --rm -e AMQ_USER=admin -e AMQ_PASSWORD=admin -p 10400:5672 --name artemis [pull_id]"

Start the broker.
Start router 2 using r2.conf from this project.
Start router 1 using r1.conf from this project.
start the senderciser application.
Wait for the message "Up and running...  destabilize network now."
Pause or kill one or both of the routers.
Unpause or restart routers as necessary to restore the network.
Allow the application to mostly or completely catch up based on the outgoing message backlog.
Repeat network mischief to taste.
When done create the stop file ("touch /path/to/stop_file").

## Additional notes

You can pause/resume the routers by using the kill command with SIGSTOP/SIGCONT.
