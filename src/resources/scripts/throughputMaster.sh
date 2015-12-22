#!/bin/bash

# Run a master designed to test throughput on the federation.  The
# behavior of the throughput test is specified in the configuration
# file.
#

source `dirname $0`/bigdataenv

java ${JAVA_OPTS} \
	-cp ${CLASSPATH} \
    com.bigdata.service.jini.benchmark.ThroughputMaster \
    ${BIGDATA_CONFIG} ${BIGDATA_CONFIG_OVERRIDES}
