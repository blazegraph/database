#!/bin/bash

source `dirname $0`/bigdataenv

##
# Generate an LUBM data set.
#

if [ -z "$1" ]; then
	echo $"usage: $0 <univ_num>"
	echo "	univ_num is the #of universities.
	echo ""
	echo "WARNING: Output is written into the CURRENT working directory."
	exit 1
fi

java ${JAVA_OPTS} \
	-Xmx400m \
    -cp @install.lubm.lib.dir@/bigdata-lubm.jar \
    edu.lehigh.swat.bench.uba.Generator \
    -univ $1 \
    -onto http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl
