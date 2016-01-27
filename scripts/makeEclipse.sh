#!/bin/bash
BASE_DIR=`dirname $0`

#Clean everything
${BASE_DIR}/clean.sh

#Generate the eclipse projects and compile
#Use the ECLIPSE_WORKSPACE variable, if it is present
#Pass along any commandline parameters to the maven command

if [ -z "${ECLIPSE_WORKSPACE}" ] ; then

 	mvn -f "${BASE_DIR}"/../pom.xml eclipse:eclipse $*

else 

	mvn -f "${BASE_DIR}"/../pom.xml eclipse:eclipse -Declipse.workspace="${ECLIPSE_WORKSPACE}" $*

fi
