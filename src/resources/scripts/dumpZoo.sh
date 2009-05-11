#!/bin/bash

#
# Dump out some stuff for a bigdata federation.
#
# usage: configFile

source `dirname $0`/bigdataenv

java ${JAVA_OPTS} \
    -cp ${CLASSPATH} \
    com.bigdata.zookeeper.DumpZookeeper \
    /opt2/src/bigdata-lubm/src/resources/config/bigdataCluster.config
