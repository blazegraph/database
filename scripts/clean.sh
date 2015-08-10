#!/bin/bash
#must be run from repository root
#mvn -f blazegraph-parent/pom.xml clean
for pom in `find bin -name "pom.xml" -print`; do

	dir=`echo $pom | sed 's/\/pom\.xml//g'`
	echo "Cleaning $dir"
	rm -rf $dir
done

for file in `find bin -type f -iname ".project" -print`; do
	rm -f $file
done

