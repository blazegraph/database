#!/bin/bash

# Setup the environment.                                                                                                                       
source src/resources/HAJournal/HAJournal.env

# Start an HAJournalServer.  
java\
 -cp ${CLASSPATH}\
 -Djava.security.policy=${POLICY_FILE}\
 -Dlog4j.configuration=${LOG4J_CONFIG}\
 com.bigdata.zookeeper.DumpZookeeper\
 ${HAJOURNAL_CONFIG}
 
 