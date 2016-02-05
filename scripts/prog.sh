#!/bin/bash

BASE_DIR=`dirname $0`

mvn -f "${BASE_DIR}"/../blazegraph-jar/pom.xml clean package

if [ -z "$JAVA_OPTS" ] ; then
	JAVA_OPTS="-ea -Xmx4g -server"
fi

if [ -z "$JAVA_HOME" ] ; then
	JAVA="${JAVA_HOME}/bin/java"
else
	JAVA=`which java`
fi

echo "Starting with JAVA_OPTS: $JAVA_OPTS."

"$JAVA" $JAVA_OPTS -cp "${BASE_DIR}"/../blazegraph-jar/target/blazegraph-jar*.jar $*

