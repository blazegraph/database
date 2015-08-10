#!/bin/bash
# Script to update the version numbers with the git branch of the snapshot build.
# Must be run from the root of the bigdata project, i.e. ./scripts/updatePomVersions.sh

PARENT_POM=./blazegraph-parent/pom.xml
NEW_VERSION="1.5.3"

CURRENT_SNAPSHOT=`cat $PARENT_POM | perl -n -e '/^.*\<version\>(.*)-SNAPSHOT\<\/version\>.*$/ && printf("%s", $1)'`


for file in `find . -name "pom.xml" -maxdepth 2 -print`; do

	cat $file | sed "s/${CURRENT_SNAPSHOT}/${NEW_VERSION}/" > /tmp/$$
	cat /tmp/$$ > $file
	rm -f /tmp/$$

done



