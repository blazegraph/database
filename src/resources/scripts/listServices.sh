#!/bin/bash

# Lists the discovered services.

source `dirname $0`/bigdataenv

java ${JAVA_OPTS} \
    com.bigdata.service.jini.util.ListServices \
    ${BIGDATA_CONFIG} ${BIGDATA_CONFIG_OVERRIDES}

