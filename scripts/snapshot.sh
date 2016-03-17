#!/bin/bash
BASE_DIR=`dirname $0`

PARENT_POM="${BASE_DIR}"/../pom.xml
CURRENT_VERSION=`grep "CURRENT_VERSION" ${BASE_DIR}/version.properties | cut -d= -f2`
BRANCH=`git rev-parse --abbrev-ref HEAD`
YYMMDD=`date +%Y%m%d`
SNAPSHOT="SNAPSHOT"

echo "Updating POM versions to ${CURRENT_VERSION}-${BRANCH}-${YYMMDD}"

mvn versions:set -DnewVersion=${CURRENT_VERSION}-${BRANCH}-${YYMMDD} versions:update-child-modules -f ${PARENT_POM}

mvn -f ${PARENT_POM} -N clean install -Dmaven.test.skip=true

mvn -f ${PARENT_POM} clean install -DskipTests=true
