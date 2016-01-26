#!/bin/bash
#Script to publish javadoc to https://blazegraph.github.com/database/apidocs/
BASE_DIR=`dirname $0`
PARENT_POM="${BASE_DIR}/../pom.xml"
DEST_DIR=database/apidocs/
#You must have cloned https://github.com/blazegraph/blazegraph.github.io into a directory at the same level as where bigdata is checked out
GITHUB_PAGES="${BASE_DIR}/../../blazegraph.github.io"

if [ ! -d "${GITHUB_PAGES}" ] ; then

   echo "${GITHUB_PAGES} does not exist."
   echo "You must have cloned git@github.com:blazegraph/blazegraph.github.io.git into a directory at the same level as where bigdata is checked out."
   exit 1

fi

#Make sure everthing is built.
${BASE_DIR}/mavenInstall.sh

mvn -f "${PARENT_POM}" javadoc:aggregate

echo "Javadoc is located in ${BASE_DIR}/../target/site/apidocs/"

pushd `pwd`
mkdir -p "${GITHUB_PAGES}/${DEST_DIR}"
echo cp -rf "${BASE_DIR}"/../target/site/apidocs/* "${GITHUB_PAGES}/${DEST_DIR}"
cp -rf "${BASE_DIR}"/../target/site/apidocs/* "${GITHUB_PAGES}/${DEST_DIR}"
cd $"${GITHUB_PAGES}"
git add --all
git commit -m "Update for Blazegraph Database Javadocs"
git push origin master

popd 



