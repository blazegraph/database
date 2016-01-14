#!/bin/bash
BASE_DIR=`dirname $0`

echo "Building dependencies..."
"$BASE_DIR"/mavenInstall.sh
echo "Building artifacts..."
mvn -f "${BASE_DIR}"/../blazegraph-artifacts/pom.xml clean package -DskipTests=true
#Assembly artifacts must uses the assembly:single lifecycle
mvn -f "${BASE_DIR}"/../blazegraph-tgz/pom.xml clean package assembly:single -DskipTests=true


ARTIFACT_DIR="$BASE_DIR/../artifacts"
echo "Artifacts will be placed in $ARTIFACT_DIR"

if [ -d "${ARTIFACT_DIR}" ] ; then
    echo "Deleting existing ${ARTIFACT_DIR}."
    rm -rf ${ARTIFACT_DIR}
fi

mkdir -p "${ARTIFACT_DIR}"

#copy the war artifacts
WAR_ARTIFACTS="bigdata blazegraph"

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

#copy the jar artifacts
JAR_ARTIFACTS="bigdata blazegraph"

for file in $JAR_ARTIFACTS; do
    EXT=jar
    ARTIFACT=`find "${BASE_DIR}/../${file}-${EXT}" -type f -name "${file}-*.${EXT}" | head -1`

    if [ -f "${ARTIFACT}" ] ; then
        cp -f "${ARTIFACT}" "${ARTIFACT_DIR}"/${file}.${EXT}
        echo "Copied ${ARTIFACT} to ${ARTIFACT_DIR}"
    else
        echo "${file}-${EXT} does not exist.  Skipping."
    fi
done

#Copy the debian deployer
EXT=deb
file=blazegraph
ARTIFACT=`find "${BASE_DIR}/../${file}-${EXT}" -type f -name "${file}-*.${EXT}" | head -1`

if [ -f "${ARTIFACT}" ] ; then
    cp -f "${ARTIFACT}" "${ARTIFACT_DIR}"/${file}.${EXT}
    echo "Copied ${ARTIFACT} to ${ARTIFACT_DIR}"
else
    echo "${file}-${EXT} does not exist.  Skipping."
fi

#Copy the rpm deployer
EXT=rpm
file=blazegraph
ARTIFACT=`find "${BASE_DIR}/../${file}-${EXT}" -type f -name "${file}-*.${EXT}" | head -1`

if [ -f "${ARTIFACT}" ] ; then
    cp -f "${ARTIFACT}" "${ARTIFACT_DIR}"/${file}.${EXT}
    echo "Copied ${ARTIFACT} to ${ARTIFACT_DIR}"
else
    echo "${file}-${EXT} does not exist.  Skipping."
fi

#Copy the tarball deployer
EXT=tgz
FILE_EXT="tar.gz tar.bz2 zip"
file=blazegraph

for tarball in $FILE_EXT; do 
   
   echo find "${BASE_DIR}/../${file}-${EXT}" -type f -name "${file}*${tarball}" 
   ARTIFACT=`find "${BASE_DIR}/../${file}-${EXT}" -type f -name "${file}*${tarball}" | head -1`
   
   if [ -f "${ARTIFACT}" ] ; then
       cp -f "${ARTIFACT}" "${ARTIFACT_DIR}"/${file}.${tarball}
       echo "Copied ${ARTIFACT} to ${ARTIFACT_DIR}"
   else
       echo "${file}-${EXT} does not exist.  Skipping."
   fi

done

echo "Copied the deployers to ${ARTIFACT_DIR}."
