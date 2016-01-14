#!/bin/bash
BASE_DIR=`dirname $0`

mvn -f "${BASE_DIR}"/../pom.xml -U install -DskipTests=true
mvn -f "${BASE_DIR}"/../blazegraph-artifacts/pom.xml -U clean install -DskipTests=true
