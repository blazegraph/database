#!/bin/bash
#assumes run from root

export JAVA_OPTS="$JAVA_OPTS -ea -Xmx4g -server -Xdebug -agentlib:jdwp=transport=dt_socket,server=y,address=8000,suspend=n"

BASE_DIR=`dirname $0`
"$BASE_DIR"/startBlazegraph.sh
