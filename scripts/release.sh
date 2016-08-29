#!/bin/bash
#Utility Script to release
#Please see https://wiki.blazegraph.com/wiki/index.php/ReleaseGuide
#This assumes the release has passed CI and Benchmarking.
#You must have you public keys setup for Github and Sourceforge.
#You must have your ~/.m2/settings.xml configured to push to maven central and PGP signing.
#
#TODO:  Look at migrating to the Maven Release Plug-in.  Need to resolve the workflow for multiple GIT locations.
#

BASE_DIR=`dirname $0`

PARENT_POM="${BASE_DIR}"/../pom.xml
CURRENT_VERSION=`grep "CURRENT_VERSION" ${BASE_DIR}/version.properties | cut -d= -f2`
#GIT REPOS to push to.  
#This must be configured as ${GIT_CMD} remote add sourceforge ssh://beebs@git.code.sf.net/p/bigdata/git
REMOTE_GITS="origin blazegraph"
#Uncomment to only push to private GIT
#REMOTE_GITS="origin"

GIT_CMD=`which git`
#Use for testing
#GIT_CMD=echo

if [ "$#" -ne 1 ] ; then

	echo "New version is a required parameter."
	exit 1

fi

if [ ! -f ~/.m2/settings.xml ] ; then

	echo "There is no ~/.m2/settings.xml configured.  Please see https://wiki.blazegraph.com/wiki/index.php/ReleaseGuide."
	exit 1
fi

NEW_VERSION=$1

echo "Setting pom version to $CURRENT_VERSION.  New version will be ${NEW_VERSION}."

${BASE_DIR}/updateVersions.sh $CURRENT_VERSION

RELEASE_BRANCH=BLAZEGRAPH_RELEASE_`echo ${CURRENT_VERSION} | sed -e 's/\./_/g'`

echo "Creating new release branch:  $RELEASE_BRANCH"

${GIT_CMD} checkout -b $RELEASE_BRANCH

echo "Commiting the POMs with updated versions."

${GIT_CMD} commit -a -m "POM version updates for $RELEASE_BRANCH"

echo "Creating Release Tag"

${GIT_CMD} tag -a $RELEASE_BRANCH  -m "Blazegraph Release ${CURRENT_VERSION}"

#Actually build and publish the release to maven central

echo "Building core artifacts..."

"${BASE_DIR}"/buildArtifacts.sh

echo "Deploying to Maven Central..."

"${BASE_DIR}"/mavenCentral.sh

echo "Publishing javadoc to https://blazegraph.github.io/database/apidocs/index.html..."

"${BASE_DIR}"/publishJavadoc.sh


for repo in $REMOTE_GITS; do

	echo "Pushing the release branch to $repo."
	${GIT_CMD} push "${repo}" refs/heads/${RELEASE_BRANCH}

	echo "Pushing the release tags to $repo."
	${GIT_CMD} push "${repo}" refs/tags/${RELEASE_BRANCH}

done

#Preparing the reverse merge
MERGE_BRANCH=${CURRENT_VERSION}_reverse_merge

echo "Creating new release branch:  $MERGE_BRANCH"

${GIT_CMD} checkout -b $MERGE_BRANCH

echo "Setting pom ${NEW_VERSION} for the reverse merge."

cat ${BASE_DIR}/version.properties | sed -e "s/CURRENT_VERSION=.*/CURRENT_VERSION=${NEW_VERSION}/" > /tmp/$$.txt
cat /tmp/$$.txt > ${BASE_DIR}/version.properties


${BASE_DIR}/updateVersions.sh "${NEW_VERSION}-master-SNAPSHOT"

echo "Committing new POM versions"

${GIT_CMD} commit -a -m "Blazegraph release $CURRENT_VERSION reverse merge"

echo "Pushing merge branch to origin."

${GIT_CMD} push origin ${MERGE_BRANCH} 

echo "Now you must merge ${MERGE_BRANCH} into master:  git checkout origin ; git merge origin/${MERGE_BRANCH}"
