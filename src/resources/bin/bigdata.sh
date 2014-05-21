#!/bin/bash

# Start the services and put the JVM in the background.  All services will
# run in a single JVM.  See Apache River com.sun.jini.start.ServiceStarter
# for more details.  The services are configured in the accompanying 
# startHAServices.config file.  Specific configuration options for each
# service are defined in the documentation for that service.  
#
# Note: One drawback with running each service in the same JVM is that the
# GC load of all services is combined and all services would be suspended
# at the same time by a Full GC pass.  If this is a problem, then you can
# break out the river services (ClassServer and Reggie) into a separate 
# ServiceStarter instance from the HAJournalServer.

# The top-level of the installation.
pushd `dirname $0` > /dev/null;cd ..;INSTALL_DIR=`pwd`;popd > /dev/null

##
# HAJournalServer configuration parameter overrides (see HAJournal.config).
#
# The bigdata HAJournal.config file may be heavily parameterized through 
# environment variables that get passed through into the JVM started by 
# this script and are thus made available to the HAJournalServer when it
# interprets the contents of the HAJournal.config file. See HAJournal.config
# for the meaning of these environment variables.
#
# Note: Many of these properties have defaults.
##

export JETTY_XML="${INSTALL_DIR}/var/jetty/jetty.xml"
export JETTY_RESOURCE_BASE="${INSTALL_DIR}/var/jetty"
export LIB_DIR=${INSTALL_DIR}/lib
export CONFIG_DIR=${INSTALL_DIR}/var/config
export LOG4J_CONFIG=${CONFIG_DIR}/logging/log4j.properties

# TODO Explicitly enumerate JARs so we can control order if necessary and
# deploy on OS without find and tr.
export HAJOURNAL_CLASSPATH=`find ${LIB_DIR} -name '*.jar' -print0 | tr '\0' ':'`

export JAVA_OPTS="\
 -server -Xmx4G\
 -Dlog4j.configuration=${LOG4J_CONFIG}\
 -Djetty.resourceBase=${JETTY_RESOURCE_BASE}\
 -DJETTY_XML=${JETTY_XML}\
"

cmd="java ${JAVA_OPTS} \
    -server -Xmx4G \
    -cp ${HAJOURNAL_CLASSPATH} \
    com.bigdata.rdf.sail.webapp.NanoSparqlServer \
    9999 kb \
    ${INSTALL_DIR}/var/jetty/WEB-INF/GraphStore.properties \
"
echo "Running: $cmd"
$cmd&
pid=$!
# echo "PID=$pid"
echo "kill $pid" > stop.sh
chmod +w stop.sh

# Note: To obtain the pid, do: read pid < "$pidFile"
