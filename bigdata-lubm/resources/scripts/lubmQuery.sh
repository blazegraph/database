#!/bin/bash

# Runs the LUBM queries as configured using the LUBM test harness.
#
# Pre-conditions: jini is running; services are running.
#
# usage: namespace [queryFile]

source `dirname $0`/bigdataenv

if [ -z "$1" ]; then
    echo "usage: $0 namespace [queryFile [ntrials [nparallel]]]"
    echo "   where 'namespace' is the namespace of the KB."
    echo "   where 'queryFile' is the name of a file containing queries to be executed (defaults to all LUBM queries)."
    echo "   where 'ntrials is the #of trials for each query in the file (defaults to 10)."
    echo "   where 'nparallel is the concurrent presentations of a query in each trial (defaults to 1)."
    exit 1;
fi

namespace=$1

# use the given query file or use a default (all LUBM queries).
queryFile=$2
if [ -z "$queryFile" ]; then
    queryFile="@install.lubm.config.dir@/config.query.sparql"
fi

# The #of trials for each query.
ntrials=$3
if [ -z "$ntrials" ]; then
    ntrials=10
fi

# The #of current presentation within each query trial.  This does not
# present a mixture of the queries, but only a concurrent presentation
# of each query within a given trial.
nparallel=$4
if [ -z "$nparallel" ]; then
    nparallel=1
fi

# Uncomment if you want to see all the httpd requests.
#verbose=-verbose

# Starts an httpd server which exposes the classes in the lubm
# integration to RMI clients.  The -trees option does not quite do
# what I want -- it appears to use a different URL path naming
# convention.  Therefore I am unpacking the JAR before starting
# the class server.
#
# Note: You can nohup this script to prevent accidental disconnects.
# Or be fancy and use 'disown' or 'screen'.
#
TFILE="/tmp/$(basename $0).$$.tmp"
echo "Unpacking jar to $TFILE"
mkdir $TFILE; pushd $TFILE; jar xf @install.lubm.lib.dir@/bigdata-lubm.jar; popd
# Run the ClassServer in the background (uses very little RAM).
java -Xmx80m\
	-jar ${libDir}/jini/lib/tools.jar\
	-dir $TFILE\
	$verbose\
	-port @LUBM_CLASS_SERVER_PORT@ &
pid1=$!
echo $"ClassServer running: pid=$pid1"

# Run the queries.
#
# @todo This code is relatively old.  It does not know how many DS there
# really are since it is not consulting the bigdata configuration file.
# It will just wait 10 seconds and then run with however many it finds.
# You can edit [minDataServices] here or the timeout in the code.
java ${JAVA_OPTS} \
    -cp ${CLASSPATH}:@install.lubm.lib.dir@/bigdata-lubm.jar \
    -Dlubm.warmUp=false \
    -Dlubm.queryTime=$ntrials \
    -Dlubm.queryParallel=$nparallel \
    -DminDataServices=1 \
    -Dnamespace=$namespace \
    edu.lehigh.swat.bench.ubt.Test \
    query \
    @install.lubm.config.dir@/config.kb.bigdataCluster \
    $queryFile

# kill the class server when done.
kill $pid1

# remove the temp directory containing the unpacked class files.
rm -rf $TFILE
