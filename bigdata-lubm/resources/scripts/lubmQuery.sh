#!/bin/bash

# Runs the LUBM queries as configured using the LUBM test harness.
#
# Pre-conditions: jini is running; services are running.
#
# usage: namespace [queryFile]

source `dirname $0`/bigdataenv

if [ -z "$1" ]; then
	echo "usage: $0 namespace [queryFile]"
	echo "   where 'namespace' is the namespace of the KB."
	echo "   where 'queryFile' is the name of a file containing queries to be executed (defaults to all LUBM queries)."
	exit 1;
fi

namespace=$1

# use the given query file or use a default (all LUBM queries).
queryFile=$2
if [ -z "$queryFile" ]; then
	queryFile="@install.lubm.config.dir@/config.query.sparql"
fi

# @todo This code is relatively old.  It does not know how many DS there
# really are since it is not consulting the bigdata configuration file.
# It will just wait 10 seconds and then run with however many it finds.
# You can edit [minDataServices] here or the timeout in the code.
  
java ${JAVA_OPTS} \
	-cp ${CLASSPATH}:${libDir}/lubm/bigdata-lubm.jar \
    -Dlubm.warmUp=false \
    -Dlubm.queryTime=10 \
    -Dlubm.queryParallel=1 \
    -DminDataServices=1 \
    -Dnamespace=$1 \
    edu.lehigh.swat.bench.ubt.Test \
    query \
    @install.lubm.config.dir@/config.kb.bigdataCluster \
    $queryFile
