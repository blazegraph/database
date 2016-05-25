#!/bin/bash
BASE_DIR=`dirname $0`

PARENT_POM="${BASE_DIR}"/../pom.xml
CURRENT_VERSION=`grep "CURRENT_VERSION" ${BASE_DIR}/version.properties | cut -d= -f2`
BRANCH=`git rev-parse --abbrev-ref HEAD`
YYMMDD=`date +%Y%m%d`
SNAPSHOT="SNAPSHOT"
GIT_CMD=`which git`
#GIT REPOS to push to.  
#This must be configured as ${GIT_CMD} remote add sourceforge ssh://beebs@git.code.sf.net/p/bigdata/git
REMOTE_GITS="px-blazegraph"

echo "Updating POM versions to ${CURRENT_VERSION}-${BRANCH}-${YYMMDD}"

RELEASE_BRANCH=${CURRENT_VERSION}-${BRANCH}-${YYMMDD}

mvn versions:set -DnewVersion=${RELEASE_BRANCH} versions:update-child-modules -f ${PARENT_POM}

mvn -f ${PARENT_POM} -N clean install -Dmaven.test.skip=true

mvn -f ${PARENT_POM} clean install -DskipTests=true

echo "Creating new release branch:  $RELEASE_BRANCH"

${GIT_CMD} checkout -b $RELEASE_BRANCH

echo "Commiting the POMs with updated versions."

${GIT_CMD} commit -a -m "POM version updates for $RELEASE_BRANCH"

echo "Creating Release Tag"

${GIT_CMD} tag -a $RELEASE_BRANCH  -m "Blazegraph Release ${CURRENT_VERSION}"

#Actually build and publish the release to maven central

for repo in $REMOTE_GITS; do

    echo "Pushing the release branch to $repo."
    ${GIT_CMD} push "${repo}" refs/heads/${RELEASE_BRANCH}

    echo "Pushing the release tags to $repo."
    ${GIT_CMD} push "${repo}" refs/tags/${RELEASE_BRANCH}

done

echo "Make sure to publish to the core Blazegraph github repo"
echo "${GIT_CMD} push origin refs/heads/${RELEASE_BRANCH}"
echo "${GIT_CMD} push origin refs/tags/${RELEASE_BRANCH}"

"$BASE_DIR"/resetPomVersions.sh"
git commit -a -m "POM version resets."

