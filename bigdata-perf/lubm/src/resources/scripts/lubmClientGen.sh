#!/bin/bash

source `dirname $0`/bigdataenv

##
# Generate an LUBM data set that is broken up for N clients.
#

if [ -z "$1" -o -z "$2" ]; then
    echo $"usage: $0 <univ_num> <nclients>"
    echo "	univ_num is the #of universities."
    echo ""
    echo "WARNING: Output is written into the CURRENT working directory."
    exit 1
fi

nuniv=$1
nclients=$2

mkdir U$1
cd U$1

for clientNum in `seq $nclients`
do
    let "clientNum = $clientNum - 1"
    mkdir $clientNum
    pushd $clientNum
    java ${JAVA_OPTS} \
	-Xmx400m \
	-cp /opt2/bigdata/benchmark/lubm/lib/bigdata-lubm.jar \
	edu.lehigh.swat.bench.uba.Generator \
	-univ $nuniv \
	-nclients $nclients \
	-clientNum $clientNum \
	-onto http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl
    popd
done
