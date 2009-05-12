#!/bin/bash

# Starts the jini services (normally done automatically by the
# bigdata script).

source `dirname $0`/bigdataenv

# Start the jini services and put the JVM in the background.
java ${JAVA_OPTS} \
    com.sun.jini.start.ServiceStarter \
    ${JINI_CONFIG}&
