#!/bin/bash
# Script to update the version numbers with the git branch of the snapshot build.

BASE_DIR=`dirname $0`
CURRENT_VERSION=2.0.1
BRANCH="master"
SNAPSHOT="SNAPSHOT"
NEW_VERSION="${CURRENT_VERSION}-${SNAPSHOT}"

${BASE_DIR}/updateVersions.sh "${NEW_VERSION}"
