#!/bin/bash
BASE_DIR=`dirname $0`

mvn -f "${BASE_DIR}"/../pom.xml clean deploy -DskipTests=true -Plocal-deploy
