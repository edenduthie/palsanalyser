This is PALS analyser, which is a request queuing server.
It provides an uniform API to execute the analysis codes
that may be written in an language other than Java.

The API is Java RabbitMQ queuing channels, that is the reason
why this is a Java interface.

It requires Rserve engine to be running in a daemon mode.

As to how to run Rserve daemon, see the class comments of 
PalsRserveEngine.java