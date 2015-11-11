#!/bin/bash

# Dumps the data in the specified Journal file. The journal MUST NOT
# be in use by another process.
#
# usage: (option*) filename+
#
# where option is any of:
#
# -namespace : Dump only those indices having the specified namespace prefix
# -history   : Dump metadata for indices in all commit records (default only dumps the metadata for the indices as of the most current committed state).
# -indices   : Dump the indices (does not show the tuples by default).
# -pages     : Dump the pages of the indices and reports some information on the page size.
# -tuples    : Dump the records in the indices.

# Setup the source environment.
source /etc/default/blazegraph-ha

LIB_DIR="$BLZG_HOME/lib"

# TODO Explicitly enumerate JARs so we can control order if necessary and
# deploy on OS without find and tr.
export CLASSPATH=`find ${LIB_DIR} -name '*.jar' -print0 | tr '\0' ':'`

java ${JAVA_OPTS} \
    -cp ${CLASSPATH} \
    -Djava.security.policy=${POLICY_FILE}\
    com.bigdata.journal.DumpJournal \
    $*
