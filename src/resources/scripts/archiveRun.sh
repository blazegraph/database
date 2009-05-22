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
if [ -f "$lockFile" ]; then
    read pidFile < `find $LAS -name pid | grep LoadBalancerServer`
    if [ -e "$pidFile" ]; then
        echo "Sending HUP to the LoadBalancer: $pidFile"
        read pid < $pidFile
        kill -hup $pid
    fi
fi

# Copy the configuration file and the various log files.
cp -v $BIGDATA_CONFIG \
   $eventLog \
   $errorLog \
   $targetDir

# the journal containing the events (and eventually the counters).
cp -v /var/bigdata/benchmark/LoadBalancer*/logicalService*/*/events.jnl $targetDir

# text files containing the logged performance counters.
cp -v $LAS/LoadBalancerServer/logicalService*/*/counters* $targetDir/counters

# Copy the index dumps if you are running the lubm test harness.
if [ -d "$NAS/lubm" ]; then
	cp -vr /opt2/trials/indexDumps/* $targetDir/indexDumps
fi

tar -cvz -C "$targetDir/.." -f $targetDir.tgz $targetDir
rm -rf $targetDir/*
echo "ready: $targetDir.tgz"

# The detail log is so large that it gets copied and compressed separately.
cp -v $detailLog $targetDir
gzip $targetDir/detail.log 
echo "ready: $targetDir/detail.log.gz"
