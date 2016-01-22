#!/bin/bash
BASE_DIR=`dirname $0`

mvn -f "${BASE_DIR}"/../blazegraph-parent/pom.xml clean deploy -DskipTests=true
