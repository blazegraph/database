#!/bin/bash

# Lists the discovered services.
#
# Note: You can override the repeatCount by adding the following to the
# command line.
#
# com.bigdata.service.jini.util.ListServices.repeatCount=0

# Setup the source environment.
source /etc/default/blazegraph-ha

LIB_DIR="$BLZG_HOME/lib"

# TODO Explicitly enumerate JARs so we can control order if necessary and
# deploy on OS without find and tr.
export CLASSPATH=`find ${LIB_DIR} -name '*.jar' -print0 | tr '\0' ':'`

java ${JAVA_OPTS} \
	-cp ${CLASSPATH} \
    -Djava.security.policy=${POLICY_FILE}\
     -Dlog4j.configuration=file:"${BLZG_HOME}"/conf/log4jHA.properties \
    com.bigdata.service.jini.util.ListServices \
    ${HAJOURNAL_CONFIG} $*
