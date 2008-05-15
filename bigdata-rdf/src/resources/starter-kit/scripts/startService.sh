#!/bin/bash

# Starts a bigdata service.

# usage: startService class configFile+

# where [class] is [com.bigdata.service.jini.{DataServer,MetadataServer,TimestampServer,LoadBalancerServer}]

# Note: this script MUST be run from the directory containing the
# service jini.config and bigdata.properties files.  The nohup.out
# file will be written into the current working directory.  There is
# an expectation that the policy.all file is in the parent directory.
# Likewise, there is an expectation that the log4j.properties configuration
# file is in /nas/config -- note that it is resolved using a URL.

# Note: When using NAS to hold the scripts and configuration files, be
# sure that the data directory for each service is located on the host
# on which that service will execute.  E.g., /var/bigdata/....  It is
# a good practice to have the Jini service ID file written into the
# data directory as well so that confusions about which service ID
# goes with which persistent data can be avoided.

# The recommended JAVA_OPTS on a server class machine include 2G of
# RAM and the server JVM.  Also a data or metadata service needs
# around 10M of native memory for its write cache.

source /nas/scripts/env.sh

# clear old file.
rm -f nohup.out

# start the service.
echo "Starting $1 in `pwd`"
nohup java ${JAVA_OPTS} \
    "-Djava.security.policy=../policy.all" \
    "-Dcom.sun.jini.jeri.tcp.useNIO=true" \
    -Dlog4j.debug -Dlog4j.configuration=file:/nas/config/log4j.properties\
    -cp ${CLASSPATH} $1 jini.config $2 $3 $4 $5 $6 $7 $8 $9 &

# The PID for the java process.
export pid="$!"
echo "pid=$pid"
echo "$pid" > pid.txt
