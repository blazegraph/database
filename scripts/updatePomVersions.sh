#!/bin/bash
# Script to update the version numbers with the git branch of the snapshot build.
# Must be run from the root of the bigdata project, i.e. ./scripts/updatePomVersions.sh

PARENT_POM=./blazegraph-parent/pom.xml

CURRENT_SNAPSHOT=`cat $PARENT_POM | perl -n -e '/^.*\<version\>(.*-SNAPSHOT)\<\/version\>.*$/ && printf("%s", $1)'`
CURRENT_BRANCH=`cat .git/HEAD | cut -d\/ -f3`

echo "${CURRENT_SNAPSHOT}-${CURRENT_BRANCH}"


for file in `find . -name "pom.xml" -maxdepth 2 -print`; do

	cat $file | sed "s/${CURRENT_SNAPSHOT}/${CURRENT_SNAPSHOT}-${CURRENT_BRANCH}/" > /tmp/$$
	cat /tmp/$$ > $file
	rm -f /tmp/$$

done



