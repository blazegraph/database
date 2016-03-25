#!/bin/bash
BASE_DIR=`dirname $0`

#Clean everything
${BASE_DIR}/clean.sh

#Generate the eclipse projects and compile
#Use the ECLIPSE_WORKSPACE variable, if it is present
#Pass along any commandline parameters to the maven command

if [ -z "${ECLIPSE_WORKSPACE}" ] ; then

    #Development projects
 	mvn -f "${BASE_DIR}"/../pom.xml eclipse:eclipse  $*
    #Deployment artifacts used in Eclipse
 	mvn -f "${BASE_DIR}"/../pom.xml eclipse:eclipse -P Deployment --projects bigdata-war,bigdata-jar,blazegraph-war,blazegraph-jar $*

else 

    #Development projects
	mvn -f "${BASE_DIR}"/../pom.xml eclipse:eclipse -Declipse.workspace="${ECLIPSE_WORKSPACE}" 
    #Deployment artifacts used in Eclipse
 	mvn -f "${BASE_DIR}"/../pom.xml eclipse:eclipse -Declipse.workspace="${ECLIPSE_WORKSPACE}" -P Deployment --projects bigdata-war,bigdata-jar,blazegraph-war,blazegraph-jar $*

fi
