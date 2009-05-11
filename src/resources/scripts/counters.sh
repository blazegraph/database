#!/bin/bash

# Script starts an httpd server which may be used to view historical
# or post-mortem performance counters and/or events.

# usage: counters.sh [-p port] [-events events.log] counterFile(s)

source /nas/scripts/env.sh

java -cp ${CLASSPATH}\
    -Dlog4j.configuration=file:/nas/config/log4j.properties\
    com.bigdata.counters.httpd.CounterSetHTTPDServer -p 8080 $1 
