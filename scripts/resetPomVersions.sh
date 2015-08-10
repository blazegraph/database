#!/bin/bash
# Script to update the version numbers with the git branch of the snapshot build.
# Must be run from the root of the bigdata project, i.e. ./scripts/updatePomVersions.sh

PARENT_POM=./blazegraph-parent/pom.xml

CURRENT_SNAPSHOT=`cat $PARENT_POM | grep "SNAPSHOT" | perl -n -e '/^.*\<version\>(.*)\<\/version\>.*$/ && printf("%s", $1)'`
CURRENT_BRANCH=`cat .git/HEAD | cut -d\/ -f3`


RESET_SNAPSHOT=`echo $CURRENT_SNAPSHOT | cut -d\- -f1`

echo "${CURRENT_SNAPSHOT} -> ${RESET_SNAPSHOT}"

for file in `find . -name "pom.xml" -maxdepth 2 -print`; do

	cat $file | sed "s/\<version\>.*SNAPSHOT/\<version\>${RESET_SNAPSHOT}-SNAPSHOT/" > /tmp/$$
	cat /tmp/$$ > $file
	rm -f /tmp/$$

done



