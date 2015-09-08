#!/bin/bash
#assume the root of the repo


PARENT_POM=./blazegraph-parent/pom.xml
CURRENT_VERSION=1.5.3
BRANCH=`git rev-parse --abbrev-ref HEAD`
YYMMDD=`date +%Y%m%d`
SNAPSHOT="SNAPSHOT"

echo "Updating POM versions to ${CURRENT_VERSION}-${BRANCH}-${YYMMDD}"

mvn versions:set -DnewVersion=${CURRENT_VERSION}-${BRANCH}-${YYMMDD} versions:update-child-modules -f ${PARENT_POM}

mvn -f ${PARENT_POM} -N clean install -DskipTests

mvn -f ${PARENT_POM} clean install -DskipTests
