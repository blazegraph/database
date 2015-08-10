#!/bin/bash

mvn -f bigdata-jar/pom.xml package

JAVA_OPTS="$JAVA_OPTS -ea -Xmx4g -server -XX:+UseParallelOldGC"

$JAVA_HOME/bin/java -jar bigdata-jar/target/bigdata-jar*.jar
