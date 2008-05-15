#!/bin/bash

source /nas/scripts/env.sh

# Runs a client against existing services using jini for service discovery.
#
# Pre-conditions: jini is running; services are running.
#
# usage: nclients clientNum
#
# where clientNum is in [0:nclients)

# echo $CLASSPATH

java ${JAVA_OPTS} \
    -Dlog4j.configuration=file:/nas/config/log4j.properties\
    -Djava.security.policy=policy.all\
    -Dcom.sun.jini.jeri.tcp.useNIO=true\
    -Dnthreads=20 -DbufferCapacity=200000\
    -DminDataServices=1\
    -Ddocuments.directory=/home/bthompso/U100\
    -Dnclients=$1 -DclientNum=$2 \
    -cp ${CLASSPATH} \
    com.bigdata.rdf.store.TestTripleStoreLoadRateWithExistingJiniFederation \
    Client/jini.config
