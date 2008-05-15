#!/bin/bash

source /nas/scripts/env.sh

# A local data service federation run.

#echo $CLASSPATH

java ${JAVA_OPTS} \
    -Dlog4j.configuration=file:/nas/config/log4j.properties\
    -Djava.security.policy=policy.all\
    -Dnthreads=20 -DbufferCapacity=200000\
    -Ddocuments.directory=U100\
    -cp ${CLASSPATH} \
    com.bigdata.rdf.store.TestTripleStoreLoadRateWithLocalDataServiceFederation
