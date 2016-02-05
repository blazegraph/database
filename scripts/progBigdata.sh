#!/bin/bash

#Script for legacy compatibility with the /bigdata context path
#This is deprecated, but included for compatibility

BASE_DIR=`dirname $0`

mvn -f "${BASE_DIR}"/../bigdata-jar/pom.xml clean package

if [ -z "$JAVA_OPTS" ] ; then
	JAVA_OPTS="-ea -Xmx4g -server"
fi

if [ -z "$JAVA_HOME" ] ; then
	JAVA="${JAVA_HOME}/bin/java"
else
	JAVA=`which java`
fi

echo "Starting with JAVA_OPTS: $JAVA_OPTS."

"$JAVA" $JAVA_OPTS -cp "${BASE_DIR}"/../bigdata-jar/target/bigdata-jar*.jar $*

