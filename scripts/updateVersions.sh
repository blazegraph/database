#!/bin/bash
# Script to update the version numbers with the git branch of the snapshot build.

if [ "$#" -ne "1" ] ; then

    echo "New version parameter is required."
    exit 1

fi

BASE_DIR=`dirname $0`

PARENT_POM="${BASE_DIR}"/../pom.xml
ARTIFACTS_POM="${BASE_DIR}"/../blazegraph-artifacts/pom.xml
NEW_VERSION=$1

echo "Updating POM versions to $NEW_VERSION"

mvn versions:set -DnewVersion=${NEW_VERSION} versions:update-child-modules -P Deployment -f ${PARENT_POM}

mvn versions:set -DnewVersion=${NEW_VERSION} versions:update-child-modules -f ${ARTIFACTS_POM}
