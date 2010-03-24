#!/bin/bash

# Extracts various performance counters useful for post-mortem or offline
# analysis. The performance counter files are read in place and the extracted
# counters are written into the specified target directory and then a tarball
# is made of that directory.
#
# Note: You need to run this on the host that is running the load balancer.
# It will send a HUP signal to the LBS in order to force the production of
# a snapshot of the performance counters.  It will also need access to the
# service directory for the LBS so that it can copy the performance counters
# into the target directory.

# usage: targetDir

if [ -z "$1" ]; then
    echo $"usage: $0 <targetDir>"
    exit 1
fi

source `dirname $0`/bigdataenv

targetDir=$1

# Where to find the queries that will be run to extract the performance
# counters. @todo make parameter for this.
queryDir=src/resources/analysis/queries

if [ ! -d "$queryDir" ]; then
    echo "Could not find query directory: `hostname` dir=$queryDir"
    exit 1
fi

#
# Find the LBS directory.
#
lbsDir=`find $LAS/LoadBalancerServer/*/* -type d`

if [ -z "$lbsDir" ]; then
    echo "Could not find LBS directory: `hostname` LAS=$LAS"
    exit 1
fi

#
# Look for the load balancer service directory on the local host. If
# we find it, then we read the pid for the LBS and send it a HUP signal
# so it will write a snapshot of its performance counters.
#

# How long to wait for the LBS to dump a current snapshot.
waitDur=60

# The name of the tarball that gets generated.
tarball=$targetDir-output.tgz

if [ -f "$lockFile" ]; then
    read pid < "$lbsDir/pid"
    if [ -z "$pid" ]; then
        echo "Could not find LoadBalancer process: `hostname` lbsDir=$lbsDir"
        exit 1
    fi
    echo "Sending HUP to the LoadBalancer: $pid"
    kill -hup $pid
    echo "Waiting $waitDur seconds for the performance counter dump."
    sleep $waitDur
    ant "-Danalysis.counters.dir=$lbsDir"\
        "-Danalysis.queries=src/resources/analysis/queries"\
        "-Danalysis.out.dir=$targetDir/output"\
        analysis
# Copy the configuration file, error log, and rule execution log files and the
# event log.
	cp -v $BIGDATA_CONFIG \
   	$ruleLog* \
   	$errorLog* \
   	$eventLog \
   	$targetDir
    tar -cvz -C "$targetDir/.." -f $tarball $targetDir
    echo "extracted performance counter archive is ready: $tarball"
    ls -lh $tarball
else
    echo "bigdata subsystem lock file not found: $lockFile"
    exit 1
fi

exit 0
