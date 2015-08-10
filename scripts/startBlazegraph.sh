#!/bin/bash

PORT=9999
PROPERTIES_FILE=./bigdata-war-html/src/main/webapp/WEB-INF/RWStore.properties
NAMESPACE="kb"
JETTY_XML=./bigdata-war-html/src/main/resources/jetty.xml
JETTY_RESOURCE_BASE=./bigdata-war-html/src/main/webapp/

mvn -f bigdata-jar/pom.xml clean package

JAVA_OPTS="$JAVA_OPTS -ea -Xmx4g -server -XX:+UseParallelOldGC"


if [ -z $JAVA_HOME ] ; then
	JAVA=${JAVA_HOME}/bin/java
else
	JAVA=`which java`
fi

$JAVA ${JAVA_OPTS} -jar bigdata-jar/target/bigdata-jar*.jar

#$JAVA ${JAVA_OPTS} -cp bigdata-jar/target/bigdata-jar*.jar -Djetty.resourceBase=$JETTY_RESOURCE_BASE -DjettyXml=$JETTY_XML com.bigdata.rdf.sail.webapp.NanoSparqlServer $PORT $NAMESPACE $PROPERTIES_FILE 

