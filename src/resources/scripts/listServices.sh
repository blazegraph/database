#!/bin/bash

# Lists the discovered services.
#
# Note: You can override the repeatCount by adding the following to the
# command line.
#
# com.bigdata.service.jini.util.ListServices.repeatCount=0

source `dirname $0`/bigdataenv

java ${JAVA_OPTS} \
    com.bigdata.service.jini.util.ListServices \
    ${BIGDATA_CONFIG} ${BIGDATA_CONFIG_OVERRIDES} $*
