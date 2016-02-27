#!/bin/bash
BASE_DIR=`dirname $0`

#First install the parent POM.
mvn -f "${BASE_DIR}"/../pom.xml clean install -DskipTests=true -N

#Only build some modules (deployers are not normally needed)
mvn -f "${BASE_DIR}"/../pom.xml clean install -DskipTests=true 

#Also package the executable jars as they are used by some external script dependencies
mvn -f "${BASE_DIR}"/../pom.xml clean install -P Deployment -U -DskipTests=true --projects blazegraph-jar,bigdata-jar
