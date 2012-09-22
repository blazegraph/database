#!/bin/bash

# Setup the environment.                                                                                                                       
source src/resources/HAJournal/HAJournal.env

# Start an HAJournalServer.  
java\
 -cp ${CLASSPATH}\
 -Djava.security.policy=${POLICY_FILE}\
 com.bigdata.journal.jini.ha.HAJournalServer\
 ${HAJOURNAL_CONFIG}
