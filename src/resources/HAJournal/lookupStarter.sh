#!/bin/bash

# Setup the environment.                                                                                                                       
source src/resources/HAJournal/HAJournal.env

# Start Lookup (on one or more machines as configured).
java \
 -cp ${CLASSPATH}\
 -Dapp.home=.\
 -Djini.lib=${JINI_LIB}\
 -Djini.lib.dl=${JINI_LIBDL}\
 -Djava.security.policy=${POLICY_FILE}\
 -Djava.security.debug=off\
 -Djava.protocol.handler.pkgs=net.jini.url\
 -Dlog4j.configuration=${LOG4J_CONFIG}\
 -Dcodebase.port=${CODEBASE_PORT}\
 -Djava.net.preferIPv4Stack=true\
 -Dbigdata.fedname=${FEDNAME}\
 -Ddefault.nic=\
 com.bigdata.service.jini.util.LookupStarter 
