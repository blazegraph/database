#!/bin/bash

JETTY_XML=./bigdata-war-html/src/main/resources/jetty.xml
JETTY_RESOURCE_BASE=./bigdata-war-html/src/main/webapp/

BASE_DIR=`dirname $0`

${BASE_DIR}/prog.sh -Djetty.resourceBase=$JETTY_RESOURCE_BASE -DjettyXml=$JETTY_XML com.bigdata.rdf.sail.webapp.NanoSparqlServer $*

