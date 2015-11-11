#!/bin/bash

# Reads the identified HALog file(s)
#
# usage: HALogReader fileOrDir(s)

# Setup the source environment.
source /etc/default/blazegraph-ha

LIB_DIR="$BLZG_HOME/lib"

# TODO Explicitly enumerate JARs so we can control order if necessary and
# deploy on OS without find and tr.
export CLASSPATH=`find ${LIB_DIR} -name '*.jar' -print0 | tr '\0' ':'`

java ${JAVA_OPTS} \
	-cp ${CLASSPATH} \
    -Djava.security.policy=${POLICY_FILE}\
    com.bigdata.ha.halog.HALogReader \
    $*
