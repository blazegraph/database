#!/bin/bash

source /opt2/scripts/env.sh

# Dump out some stuff for a bigdata federation.
#
# usage: configFile

java ${JAVA_OPTS} \
    -cp ${CLASSPATH} \
    com.bigdata.zookeeper.DumpZookeeper \
    /opt2/src/bigdata-lubm/src/resources/config/bigdataCluster.config
