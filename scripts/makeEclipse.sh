#!/bin/bash
BASE_DIR=`dirname $0`

#Clean everything
${BASE_DIR}/clean.sh

#Generate the eclipse projects and compile
#Use the ECLIPSE_WORKSPACE variable, if it is present
#Pass along any commandline parameters to the maven command

if [ -z "${ECLIPSE_WORKSPACE}" ] ; then

 	mvn -f "${BASE_DIR}"/../pom.xml eclipse:eclipse --projects junit-ext,ctc-striterators,lgpl-utils,dsi-utils,system-utils,rdf-properties,sparql-grammar,bigdata-util,bigdata-common-util,bigdata-statics,bigdata-cache,bigdata-client,bigdata-ganglia,bigdata-gas,bigdata-core/,bigdata-war-html,bigdata-blueprints,bigdata-runtime,bigdata-core-test,bigdata-rdf-test,bigdata-sails-test,blazegraph-jar,bigdata-jar,bigdata-war,blazegraph-war $*

else 

	mvn -f "${BASE_DIR}"/../pom.xml eclipse:eclipse -Declipse.workspace="${ECLIPSE_WORKSPACE}" --projects junit-ext,ctc-striterators,lgpl-utils,dsi-utils,system-utils,rdf-properties,sparql-grammar,bigdata-util,bigdata-common-util,bigdata-statics,bigdata-cache,bigdata-client,bigdata-ganglia,bigdata-gas,bigdata-core/,bigdata-war-html,bigdata-blueprints,bigdata-runtime,bigdata-core-test,bigdata-rdf-test,bigdata-sails-test,blazegraph-jar,bigdata-jar,blazegraph-war,bigdata-war $*

fi
