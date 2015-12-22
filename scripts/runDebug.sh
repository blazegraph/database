#!/bin/bash
#assumes run from root

export JAVA_OPTS="$JAVA_OPTS -ea -Xmx4g -server -Xdebug -Xrunjdwp:transport=dt_socket,address=8000,server=y,suspend=n"

./scripts/run.sh $*
