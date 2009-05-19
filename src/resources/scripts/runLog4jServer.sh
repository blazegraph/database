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
# @deprecated I am modifying the configuration to start this automatically
# on the configured host.

source `dirname $0`/bigdataenv

java ${JAVA_OPTS} \
	-cp ${CLASSPATH} \
    org.apache.log4j.net.SimpleSocketServer \
    ${LOG4J_SOCKET_LOGGER_PORT} \
    ${BIGDATA_LOG4J_SERVER_CONFIG}
