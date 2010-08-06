#!/bin/bash

#
# Start a NanoSparqlServer fronting for a bigdata federation.
#
# usage: configFile

source `dirname $0`/bigdataenv

port=$1
namespace=$2

echo "port=$port namespace=$namespace config=$BIGDATA_CONFIG"

java ${JAVA_OPTS} \
    -cp ${CLASSPATH} \
    com.bigdata.rdf.sail.bench.NanoSparqlServer \
    $port \
    $namespace \
    ${BIGDATA_CONFIG} ${BIGDATA_CONFIG_OVERRIDES}

