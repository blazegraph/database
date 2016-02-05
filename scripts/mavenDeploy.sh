#!/bin/bash
BASE_DIR=`dirname $0`

mvn -f "${BASE_DIR}"/../pom.xml clean deploy -DskipTests=true -Plocal-deploy --projects junit-ext,ctc-striterators,lgpl-utils,dsi-utils,system-utils,rdf-properties,sparql-grammar,bigdata-util,bigdata-common-util,bigdata-statics,bigdata-cache,bigdata-client,bigdata-ganglia,bigdata-gas,bigdata-core/,bigdata-war-html,bigdata-blueprints,bigdata-runtime,bigdata-core-test,bigdata-rdf-test,bigdata-sails-test,blazegraph-jar,vocabularies
