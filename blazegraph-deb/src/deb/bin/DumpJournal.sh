#!/bin/sh

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
[ -f /etc/default/blazegraph ] && . /etc/default/blazegraph

[ -z "$BLZG_HOME" ] && BLZG_HOME=/usr/share/blazegraph
LIB_DIR="$BLZG_HOME"/lib

CLASSPATH=`find ${LIB_DIR} -name '*.jar' -print0 | tr '\0' ':'`:${CLASSPATH}

java ${JAVA_OPTS} \
    -cp ${CLASSPATH} \
    -Djava.security.policy=${POLICY_FILE}\
    com.bigdata.journal.DumpJournal \
    $*
