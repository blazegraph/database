#!/bin/bash

# Archives the various files which we need for post-mortem analysis of
# an experimental run. The files are copied into the specified target
# directory and then a tarball is made of that directory.
#
# Note: You need to run this on the host that is running the load balancer.
# It will send a HUP signal to the LBS in order to force the production of
# a snapshot of the performance counters.  It will also need access to the
# service directory for the LBS so that it can copy the performance counters
# into the target directory.
#
# Note: You also need to collect the run from the console.

# usage: targetDir

if [ -z "$1" ]; then
	echo $"usage: $0 <targetDir>"
	exit 1
fi

source `dirname $0`/bigdataenv

targetDir=$1

echo "COLLECT RUN FROM CONSOLE!"

mkdir -p $targetDir
mkdir -p $targetDir/counters
mkdir -p $targetDir/indexDumps

# Look for the load balancer service directory on the local host. If
# we find it, then we read the pid for the LBS and send it a HUP signal
# so it will write a snapshot of its performance counters.
waitDur=60
if [ -f "$lockFile" ]; then
    read pid < `find $LAS -name pid | grep LoadBalancerServer`
    if [ -z "$pid" ]; then
        echo "Could not find LoadBalancer process: `hostname` LAS=$LAS."
    else
        echo "Sending HUP to the LoadBalancer: $pid"
        kill -hup $pid
        echo "Waiting $waitDur seconds for the performance counter dump."
        sleep $waitDur
    fi
fi

# Copy the configuration file and the various log files.
cp -v $BIGDATA_CONFIG \
   $eventLog* \
   $errorLog* \
   $detailLog* \
   $targetDir

# the journal containing the events (and eventually the counters).
cp -v $LAS/LoadBalancerServer/logicalService*/*/events.jnl $targetDir

# text files containing the logged performance counters.
cp -v $LAS/LoadBalancerServer/logicalService*/*/counters* $targetDir/counters

# Copy the index dumps if you are running the lubm test harness.
if [ -d "$NAS/lubm" ]; then
	cp -vr $NAS/lubm/*indexDumps* $targetDir/indexDumps
fi

# Extract performance counters for analysis.
#
# Note: This is easier to do on a server class machine, but it will nail the
# CPU for a few minutes while it processes the counter set XML files so you
# need to be sure that nothing will be adversely effected by that.
# 
# Note: This will only execute if it is run from the directory containing the
# bigdata source (it looks for the queries to run in a known location but also
# has a dependency on the build.properties and build.xml files for ant).
#
# Note: This creates a 2nd archive with just the extracted performance counters.
#
if [ -d "src/resources/analysis/queries" ]; then
	ant \
		"-Danalysis.counters.dir=$targetDir/counters"\
		"-Danalysis.queries=src/resources/analysis/queries"\
		"-Danalysis.out.dir=$targetDir/output"\
		analysis
	tar -cvz -C "$targetDir/.." -f $targetDir-output.tgz $targetDir/output
fi

tar -cvz -C "$targetDir/.." -f $targetDir.tgz $targetDir

echo "ready: $targetDir.tgz"
