#!/bin/bash

source /nas/scripts/env.sh

java -cp ${CLASSPATH} com.bigdata.counters.httpd.CounterSetHTTPDServer -p 8080 $1 
