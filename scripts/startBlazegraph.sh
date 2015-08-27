#!/bin/bash

PORT=9999
PROPERTIES_FILE=./bigdata-war-html/src/main/webapp/WEB-INF/RWStore.properties
NAMESPACE="kb"

mvn -f bigdata-jar/pom.xml clean package

if [ -z "${JAVA_OPTS}" ] ; then
	JAVA_OPTS="-ea -Xmx4g -server"
fi

if [ -z ${JAVA_HOME} ] ; then
	JAVA=${JAVA_HOME}/bin/java
else
	JAVA=`which java`
fi

echo "Starting with JAVA_OPTS: $JAVA_OPTS."

$JAVA ${JAVA_OPTS} -cp bigdata-jar/target/bigdata-jar*.jar com.bigdata.rdf.sail.webapp.NanoSparqlServer $PORT $NAMESPACE $PROPERTIES_FILE  $*

