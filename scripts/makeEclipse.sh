#!/bin/bash
#assume it is run as ./scripts/makeEclipse.sh
#Clean everything
./scripts/clean.sh
#Update the POM versions for a branch
./scripts/setPomVersions.sh

#Generate the eclipse projects and compile
#Use the ECLIPSE_WORKSPACE variable, if it is present
#Pass along any commandline parameters to the maven command

if [ -z ${ECLIPSE_WORKSPACE} ] ; then

 	mvn -f blazegraph-parent/pom.xml eclipse:eclipse $*

else 

	mvn -f blazegraph-parent/pom.xml eclipse:eclipse -Declipse.workspace=${ECLIPSE_WORKSPACE} $*

fi
