#!/bin/bash

# Run a master that generates an LUBM data set using clients that are
# distributed across the federation.  The parameters for the data set
# to be generated are read from @BIGDATA_CONFIG@.  The LUBM and the
# LUBM integration classes are made available to a running bigdata
# federation using a ClassServer (they do not need to be installed
# before you start the federation).

source `dirname $0`/bigdataenv

# Starts an httpd server which exposes the classes in the lubm
# integration to RMI clients.
#
# @todo verify whether or not an absolute path to the directory is
# required here (that is what -dir is using `pwd`).  Make this work
# with a jar for better encapsulation and install that jar onto NAS
# so that this script can be run from anywhere.
#
# Note: use -verbose for debugging if you need to verify requests.
#
# This will run in background, writing all output onto the named file.
#    -dir `pwd`/ant-build/classes \
java -jar ${libDir}/jini/lib/tools.jar\
    -port @LUBM_CLASS_SERVER_PORT@ \
    -jar ${libDir}/lubm/bigdata-lubm.jar\
    > classServer.out 2>&1 < /dev/null &
# save the pid
pid1=$!
echo $"ClassServer running: pid=$pid1"

# Start the lubm master.
#
# This will run in background, writing all output onto the named file.
java ${JAVA_OPTS} \
    -cp ${CLASSPATH}:${libDir}/lubm/bigdata-lubm.jar \
    -Djava.rmi.server.codebase=@LUBM_RMI_CODEBASE_URL@ \
    edu.lehigh.swat.bench.ubt.bigdata.LubmGeneratorMaster \
    ${BIGDATA_CONFIG} ${BIGDATA_CONFIG_OVERRIDES}& \
    > lubmMaster.out 2>& < /dev/null &
# save the pid
pid2=$!
echo $"LUBM Master running: pid=$pid2"

# disown jobs so that they will not be stopped if the terminal is closed.
disown -h $pid1
disown -h $pid2

# tail the output file(s)
tail -f lubmMaster.out classServer.out
