#!/bin/bash

source `dirname $0`/bigdataenv

# Run a master designed to test throughput on the federation.  The
# behavior of the throughput test is specified in the configuration
# file.
#
java ${JAVA_OPTS} \
    com.bigdata.service.jini.benchmark.ThroughputMaster \
    ${BIGDATA_CONFIG} ${BIGDATA_CONFIG_OVERRIDES}
