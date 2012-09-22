#!/bin/bash

# Setup the environment.
source ./HAJournal.env

# Start an HAJournalServer.  
java\
 -Djava.security.policy=${POLICY_FILE}\
 com.bigdata.journal.jini.ha.HAJournalServer\
 ${HAJOURNAL_CONFIG}
