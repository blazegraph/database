#!/bin/bash
# Script to update the version numbers with the git branch of the snapshot build.

BASE_DIR=`dirname $0`
PARENT_POM="${BASE_DIR}"/../blazegraph-parent/pom.xml
DEPLOYER_POM="${BASE_DIR}"/../blazegraph-artifacts/pom.xml
CURRENT_VERSION=2.0.0
BRANCH="master"
SNAPSHOT="RC2"

echo "Updating POM versions to ${CURRENT_VERSION}-${BRANCH}-${SNAPSHOT}"

mvn versions:set -DnewVersion=${CURRENT_VERSION}-${SNAPSHOT} versions:update-child-modules -f ${PARENT_POM}

mvn versions:set -DnewVersion=${CURRENT_VERSION}-${SNAPSHOT} versions:update-child-modules -f ${DEPLOYER_POM}


