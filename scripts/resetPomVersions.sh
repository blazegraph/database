#!/bin/bash
# Script to update the version numbers with the git branch of the snapshot build.

BASE_DIR=`dirname $0`

PARENT_POM="${BASE_DIR}"/../blazegraph-parent/pom.xml
CURRENT_VERSION=1.6.0
BRANCH="master"
SNAPSHOT="SNAPSHOT"

echo "Updating POM versions to ${CURRENT_VERSION}-${BRANCH}-${SNAPSHOT}"

mvn versions:set -DnewVersion=${CURRENT_VERSION}-${BRANCH}-${SNAPSHOT} versions:update-child-modules -f ${PARENT_POM}


