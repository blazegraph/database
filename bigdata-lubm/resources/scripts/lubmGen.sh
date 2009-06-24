#!/bin/bash

source `dirname $0`/bigdataenv

# Generate an LUBM data set..
#
# usage: #of universities.
#
# WARNING: Output is written into the CURRENT working directory.

java ${JAVA_OPTS} \
	-Xmx400m \
    -cp @install.lubm.lib.dir@/bigdata-lubm.jar \
    edu.lehigh.swat.bench.uba.Generator \
    -univ $1 \
    -onto http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl
