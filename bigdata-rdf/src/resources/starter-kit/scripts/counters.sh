#!/bin/bash

# Starts an httpd service displaying post-mortem counters loaded from one
# or more files.

# usage: counters.sh [file]+
#
# where [file] is one of the performance counters XML serialization files.

source /nas/scripts/env.sh

java -cp ${CLASSPATH}\
    -Dlog4j.configuration=file:/nas/config/log4j.properties\
	com.bigdata.counters.httpd.CounterSetHTTPDServer -p 8080 $1 
