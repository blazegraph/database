#!/bin/bash

# Lists the discovered services.
#
# Note: You can override the repeatCount by adding the following to the
# command line.
#
# com.bigdata.service.jini.util.ListServices.repeatCount=0

# Setup the source environment.
source src/resources/HAJournal/HAJournal.env

java ${JAVA_OPTS} \
	-cp ${CLASSPATH} \
    -Djava.security.policy=${POLICY_FILE}\
    com.bigdata.service.jini.util.ListServices \
    ${HAJOURNAL_CONFIG} $*
