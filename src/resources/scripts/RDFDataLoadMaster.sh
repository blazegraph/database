#!/bin/bash

# Run a master that bulks loads RDF data into the federation.  The
# parameters for the data set to be generated are read from
# @BIGDATA_CONFIG@

source `dirname $0`/bigdataenv

# Start the master.  This does not need much RAM (unless you keep the
# pending sets in RAM).  It will distribute the work to be performed
# across a set of clients running on other machines.

java ${JAVA_OPTS} \
        -Xmx400m \
    -cp ${CLASSPATH} \
    com.bigdata.rdf.load.MappedRDFDataLoadMaster \
    ${BIGDATA_CONFIG} ${BIGDATA_CONFIG_OVERRIDES}
