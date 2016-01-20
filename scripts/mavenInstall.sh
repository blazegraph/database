#!/bin/bash
BASE_DIR=`dirname $0`

mvn -f "${BASE_DIR}"/../pom.xml clean install -DskipTests=true
#Also package the executable jars as they are used by some external script dependencies
mvn -f "${BASE_DIR}"/blazegraph-jar/pom.xml clean package -DskipTests=true
mvn -f "${BASE_DIR}"/bigdata-jar/pom.xml clean package -DskipTests=true
