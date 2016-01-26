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

mvn versions:set -DnewVersion=${NEW_VERSION} versions:update-child-modules -f ${PARENT_POM}


#blazegraph-artifacts has a "step parent".  It uses the blazegraph-parent artifact
#for shared properties, etc., but is not actually a child.  Therefore,
#the maven versions plugin does not work.  We must fall back to awk.

#cat "${ARTIFACTS_POM}" | awk "NR==1,/<version>.*<\/version>/{sub(/<version>.*<\/version>/, "<version>$NEW_VERSION</version>")} 1" > /tmp/$$.pom.xml
#cat "/tmp/$$.pom.xml" | awk 'NR==2,/<version>.*<\/version>/{sub(/<version>.*<\/version>/, "<version>$NEW_VERSION</version>")} 2' > /tmp/$$.pom.2.xml
#cat /tmp/$$.pom.2.xml > "${ARTIFACTS_POM}"

mvn versions:set -DnewVersion=${NEW_VERSION} versions:update-child-modules -f ${ARTIFACTS_POM}
