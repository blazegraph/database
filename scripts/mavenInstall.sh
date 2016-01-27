#!/bin/bash
BASE_DIR=`dirname $0`

#First install the parent POM.
mvn -f "${BASE_DIR}"/../pom.xml clean install -DskipTests=true -N

#Only build some modules (deployers are not normally needed)
mvn -f "${BASE_DIR}"/../pom.xml clean install -DskipTests=true --projects junit-ext,ctc-striterators,lgpl-utils,dsi-utils,system-utils,rdf-properties,sparql-grammar,bigdata-util,bigdata-common-util,bigdata-statics,bigdata-cache,bigdata-client,bigdata-ganglia,bigdata-gas,bigdata-core/,bigdata-war-html,bigdata-blueprints,bigdata-runtime,bigdata-core-test,bigdata-rdf-test,bigdata-sails-test

#Also package the executable jars as they are used by some external script dependencies
mvn -f "${BASE_DIR}"/../pom.xml clean package -U -DskipTests=true --projects blazegraph-jar,bigdata-jar
