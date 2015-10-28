#!/bin/bash
BASE_DIR=`dirname $0`

JETTY_XML="${BASE_DIR}"/../bigdata-war-html/src/main/resources/jetty.xml
JETTY_RESOURCE_BASE="${BASE_DIR}"/../bigdata-war-html/src/main/webapp/


"${BASE_DIR}"/prog.sh -Djetty.resourceBase=$JETTY_RESOURCE_BASE -DjettyXml=$JETTY_XML com.bigdata.rdf.sail.webapp.NanoSparqlServer $*

