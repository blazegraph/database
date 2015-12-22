#!/bin/bash
BASE_DIR=`dirname $0`

mvn -f "${BASE_DIR}"/../blazegraph-artifacts/pom.xml clean install -DskipTests=true
