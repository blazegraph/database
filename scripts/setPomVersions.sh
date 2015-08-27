#!/bin/bash
# Script to update the version numbers with the git branch of the snapshot build.
# Must be run from the root of the bigdata project, i.e. ./scripts/updatePomVersions.sh

PARENT_POM=./blazegraph-parent/pom.xml
CURRENT_VERSION=1.5.3
BRANCH=`git rev-parse --abbrev-ref HEAD`
SNAPSHOT="SNAPSHOT"

echo "Updating POM versions to ${CURRENT_VERSION}-${BRANCH}-${SNAPSHOT}"

mvn versions:set -DnewVersion=${CURRENT_VERSION}-${BRANCH}-${SNAPSHOT} versions:update-child-modules -f ${PARENT_POM}


