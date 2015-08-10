#!/bin/bash
#assume it is run as ./scripts/makeEclipse.sh
./scripts/clean.sh
mvn -f blazegraph-parent/pom.xml eclipse:eclipse

