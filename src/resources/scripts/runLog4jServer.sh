#!/bin/bash

# A server that writes log4j messages onto stdout.
#
# Note: You need a log4j "server" configuration file for this.  It
# only needs to specify the appender (where to write the stuff) and
# the layout.  You should control what gets logged in the log4j
# configuration file used by the applications generating the log events
# so that you don't spam the network with log events that will not be
# logged by the server.
#
# Note: This can also be started automatically on the configured host
# from the main bigdata configuration file.

source `dirname $0`/bigdataenv

java ${JAVA_OPTS} \
	-cp ${CLASSPATH} \
    org.apache.log4j.net.SimpleSocketServer \
    @LOG4J_SOCKET_LOGGER_PORT@ \
    @LOG4J_SOCKET_LOGGER_CONFIG@
