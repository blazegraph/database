#!/bin/bash

#
# Start a NanoSparqlServer fronting for a bigdata federation.
#
# usage: configFile

source `dirname $0`/bigdataenv

port=$1
namespace=$2

# Note: This will cause the NanoSparqlServer instance to ping the lastCommitTime
# on the federation. This provides more efficient query since all queries issued
# by this instance will use efficient read-historical operations. It will also
# prevent the history associated with that commit point from being recycled. The
# read lock is optional and may be any commit time on the database.
readLock=-1

# The default is 16 threads.  This raises the #of threads up to 16 for the 
# cluster.
nthreads=64

echo "port=$port namespace=$namespace config=$BIGDATA_CONFIG"

java ${JAVA_OPTS} \
    -cp ${CLASSPATH} \
    com.bigdata.rdf.sail.webapp.NanoSparqlServer \
    -readLock $readLock \
	-nthreads $nthreads \
    $port \
    $namespace \
    ${BIGDATA_CONFIG} ${BIGDATA_CONFIG_OVERRIDES}

