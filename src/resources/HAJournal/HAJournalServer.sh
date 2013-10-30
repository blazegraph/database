#!/bin/bash

# Setup the environment.                                                                                                                       
source src/resources/HAJournal/HAJournal.env

# Uncomment to enable profiler.
#profilerAgent=-agentpath:/nas/install/yjp-12.0.6/bin/linux-x86-64/libyjpagent.so

# Uncomment to have all profiling initially disabled.
#profilerAgentOptions=-agentlib:yjpagent=disableexceptiontelemetry,disablestacktelemetry

# Uncomment to enable remote debugging at the specified port.
#debug=-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=1046

# Start an HAJournalServer.  
java\
 ${JAVAOPTS}\
 -cp ${CLASSPATH}\
 -Djava.security.policy=${POLICY_FILE}\
 -Dlog4j.configuration=${LOG4J_CONFIG}\
 -Djava.util.logging.config.file=${LOGGING_CONFIG}\
 ${debug}\
 ${profilerAgent} ${profilerAgentOptions}\
 com.bigdata.journal.jini.ha.HAJournalServer\
 ${HAJOURNAL_CONFIG}