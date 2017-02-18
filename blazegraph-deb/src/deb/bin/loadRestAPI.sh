#!/bin/bash

if [ "$1" == "" -o "$1" == "-h" -o "$1" == "--help" ]; then
    echo "Usage: $0 {file_or_directory} [{graph_namespace}]"
    exit
else
    FILE_OR_DIR=$1
fi

if [ ! -z "$2" ]; then
    NAMESPACE=$2
else
    NAMESPACE=kb
fi

[ -f /etc/default/blazegraph ] && source /etc/default/blazegraph
[ -z "$JETTY_PORT" ] && JETTY_PORT=9999

[ -z "${NSS_PROPERTIES}" ] && NSS_PROPERTIES=/etc/blazegraph/RWStore.properties

export NSS_DATALOAD_PROPERTIES=${NSS_PROPERTIES}

BG_URL="http://localhost:${JETTY_PORT}/blazegraph/dataloader"

LOAD_PROP_FILE=/tmp/$$.properties

#Probably some unused properties below, but copied all to be safe.

cat <<EOT > $LOAD_PROP_FILE
quiet=false
verbose=0
closure=false
durableQueues=true
#Needed for quads
#defaultGraph=
com.bigdata.rdf.store.DataLoader.flush=false
com.bigdata.rdf.store.DataLoader.bufferCapacity=100000
com.bigdata.rdf.store.DataLoader.queueCapacity=10
#Namespace to load
namespace=${NAMESPACE}
#Files to load
fileOrDirs=${FILE_OR_DIR}
#Property file (if creating a new namespace)
propertyFile=$NSS_DATALOAD_PROPERTIES
EOT

echo "Submitting to loader at: ${BG_URL}"
echo "Submitting with properties..."
cat ${LOAD_PROP_FILE}
echo

curl -X POST --data-binary @${LOAD_PROP_FILE} --header 'Content-Type:text/plain' ${BG_URL}
echo

#Let the output go to STDOUT/ERR to allow script redirection

rm -f $LOAD_PROP_FILE
