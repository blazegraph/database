#!/bin/bash

# Run a master that generates an LUBM data set using clients that are
# distributed across the federation.  The parameters for the data set
# to be generated are read from @BIGDATA_CONFIG@.  The LUBM and the
# LUBM integration classes are made available to a running bigdata
# federation using a ClassServer (they do not need to be installed
# before you start the federation).

source `dirname $0`/bigdataenv

# Uncomment if you want to see all the httpd requests.
#verbose=-verbose

# Starts an httpd server which exposes the classes in the lubm
# integration to RMI clients.  The -trees option does not quite do
# what I want -- it appears to use a different URL path naming
# convention.  Therefore I am unpacking the JAR before starting
# the class server.
#
# FIXME Could run on `hostname` and the configured port as an (optional)
# option.
#
# @todo should use an unique temporary directory.
#
# Note: You can nohup this script to prevent accidental disconnects.
# Or be fancy and use 'disown' or 'screen'.
#
TFILE="/tmp/$(basename $0).$$.tmp"
echo "Unpacking jar to $TFILE"
mkdir $TFILE; pushd $TFILE; jar xf @install.lubm.lib.dir@/bigdata-lubm.jar; popd
java -jar ${libDir}/jini/lib/tools.jar -dir $TFILE $verbose -port @LUBM_CLASS_SERVER_PORT@ &
pid1=$!
echo $"ClassServer running: pid=$pid1"

# Start the lubm master.
java ${JAVA_OPTS} \
    -cp ${CLASSPATH}:@install.lubm.lib.dir@/bigdata-lubm.jar \
    -Djava.rmi.server.codebase=@LUBM_RMI_CODEBASE_URL@ \
    edu.lehigh.swat.bench.ubt.bigdata.LubmGeneratorMaster \
    ${BIGDATA_CONFIG} ${BIGDATA_CONFIG_OVERRIDES} \
#    > lubmMaster.out 2>&1 < /dev/null & 
## save the pid
#pid2=$!
#echo $"LUBM Master running: pid=$pid2"

## disown jobs so that they will not be stopped if the terminal is closed.
#disown -h $pid1
#disown -h $pid2

# tail the output file(s)
#tail -f lubmMaster.out classServer.out

# kill the class server when done.
kill $pid1
rm -rf /tmp/lubm
