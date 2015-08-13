#!/bin/bash
#must be run from repository root
mvn -f blazegraph-parent/pom.xml clean

for file in `find . -type f -iname ".project" -print`; do
	rm -f $file
done

