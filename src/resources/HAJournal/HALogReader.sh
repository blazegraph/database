#!/bin/bash

# Reads the identified HALog file(s)
#
# usage: HALogReader fileOrDir(s)

# Setup the source environment.
source src/resources/HAJournal/HAJournal.env

java ${JAVA_OPTS} \
	-cp ${CLASSPATH} \
    -Djava.security.policy=${POLICY_FILE}\
    com.bigdata.ha.halog.HALogReader \
    *
