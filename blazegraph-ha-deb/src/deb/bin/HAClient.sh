#!/bin/bash

# Setup the source environment.
source /etc/default/blazegraph-ha

LIB_DIR="$BLZG_HOME/lib"

# TODO Explicitly enumerate JARs so we can control order if necessary and
# deploy on OS without find and tr.
export CLASSPATH=`find ${LIB_DIR} -name '*.jar' -print0 | tr '\0' ':'`

# Uncomment to enable profiler.
#profilerAgent=-agentpath:/nas/install/yjp-12.0.6/bin/linux-x86-64/libyjpagent.so

# Uncomment to have all profiling initially disabled.
#profilerAgentOptions=-agentlib:yjpagent=disableexceptiontelemetry,disablestacktelemetry

# Uncomment to enable remote debugging at the specified port.
#debug=-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=1046

# Run HAClient.  
java\
 ${JAVAOPTS}\
 -cp ${CLASSPATH}\
 -Djava.security.policy=${POLICY_FILE}\
 -Dlog4j.configuration=${LOG4J_CONFIG}\
 -Djava.util.logging.config.file=${LOGGING_CONFIG}\
 ${debug}\
 ${profilerAgent} ${profilerAgentOptions}\
 com.bigdata.journal.jini.ha.HAClient\
 ${HAJOURNAL_CONFIG}
