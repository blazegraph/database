#!/bin/bash
#assume it is run as ./scripts/makeEclipse.sh
#Clean everything
./scripts/clean.sh
#Update the POM versions for a branch
./scripts/setPomVersions.sh
#Generate the eclipse projects and compile
mvn -f blazegraph-parent/pom.xml eclipse:eclipse
