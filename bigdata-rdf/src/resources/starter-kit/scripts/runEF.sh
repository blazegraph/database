#!/bin/bash

source /nas/scripts/env.sh

# An embedded federation run.

# echo $CLASSPATH

java ${JAVA_OPTS} \
    -Dlog4j.configuration=file:/nas/config/log4j.properties\
    -Dnthreads=20 -DbufferCapacity=200000\
    -Ddocuments.directory=U100\
    -DndataServices=1\
    -DoverflowEnabled=true\
    -cp ${CLASSPATH} \
    com.bigdata.rdf.store.TestTripleStoreLoadRateWithEmbeddedFederation 
