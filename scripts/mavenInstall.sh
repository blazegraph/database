#!/bin/bash
#assume the root of the repo

mvn -f blazegraph-parent/pom.xml clean install -DskipTests
