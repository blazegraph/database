#!/bin/bash
BASE_DIR=`dirname $0`

PARENT_POM="${BASE_DIR}"/../blazegraph-parent/pom.xml
CURRENT_VERSION=1.6.0
BRANCH=`git rev-parse --abbrev-ref HEAD`
YYMMDD=`date +%Y%m%d`
SNAPSHOT="SNAPSHOT"

echo "Updating POM versions to ${CURRENT_VERSION}-${BRANCH}-${YYMMDD}"

mvn versions:set -DnewVersion=${CURRENT_VERSION}-${BRANCH}-${YYMMDD} versions:update-child-modules -f ${PARENT_POM}

mvn -f ${PARENT_POM} -N clean install -DskipTests

mvn -f ${PARENT_POM} clean install -DskipTests
