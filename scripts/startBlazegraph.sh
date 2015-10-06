#!/bin/bash

PORT=9999
PROPERTIES_FILE=./bigdata-war-html/src/main/webapp/WEB-INF/RWStore.properties
NAMESPACE="kb"

BASE_DIR=`dirname $0`

$BASE_DIR/prog.sh com.bigdata.rdf.sail.webapp.NanoSparqlServer $PORT $NAMESPACE $PROPERTIES_FILE  $*

