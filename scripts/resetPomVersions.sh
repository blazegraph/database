#!/bin/bash
# Script to update the version numbers with the git branch of the snapshot build.

BASE_DIR=`dirname $0`
CURRENT_VERSION=`grep "CURRENT_VERSION" ${BASE_DIR}/version.properties | cut -d= -f2`
BRANCH="master"
SNAPSHOT="SNAPSHOT"
NEW_VERSION="$CURRENT_VERSION-$BRANCH-$SNAPSHOT"

"${BASE_DIR}"/updateVersions.sh "${NEW_VERSION}"

