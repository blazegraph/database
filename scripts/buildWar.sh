#!/bin/bash
BASE_DIR=`dirname $0`

echo "Building dependencies..."
"$BASE_DIR"/mavenInstall.sh
echo "Building artifacts..."
mvn -f "${BASE_DIR}"/../blazegraph-war/pom.xml -U clean package -DskipTests=true


ARTIFACT_DIR="$BASE_DIR/../artifacts"
echo "Artifacts will be placed in $ARTIFACT_DIR"

if [ -d "${ARTIFACT_DIR}" ] ; then
    echo "Deleting existing ${ARTIFACT_DIR}."
    rm -rf ${ARTIFACT_DIR}
fi

mkdir -p "${ARTIFACT_DIR}"

#copy the war artifacts
WAR_ARTIFACTS="blazegraph"

for file in $WAR_ARTIFACTS; do
    EXT=war
    ARTIFACT=`find "${BASE_DIR}/../${file}-${EXT}" -type f -name "${file}-*.${EXT}" | head -1`

    if [ -f "${ARTIFACT}" ] ; then
        cp -f "${ARTIFACT}" "${ARTIFACT_DIR}"/${file}.${EXT}
        echo "Copied ${ARTIFACT} to ${ARTIFACT_DIR}"
    else
        echo "${file}-${EXT} does not exist.  Skipping."
    fi
done

echo "Copied blazegraph.war to ${ARTIFACT_DIR}."
