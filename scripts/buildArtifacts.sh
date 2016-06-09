#!/bin/bash
BASE_DIR=`dirname $0`

echo "Building dependencies..."
"$BASE_DIR"/mavenInstall.sh
echo "Building artifacts..."

#Install the parent pom
mvn -f "${BASE_DIR}"/../blazegraph-artifacts/pom.xml clean
mvn -f "${BASE_DIR}"/../blazegraph-artifacts/pom.xml install -N -DskipTests=true
#jar artifacts need to be installed for the war files to build.
mvn -f "${BASE_DIR}"/../blazegraph-jar/pom.xml clean install package -DskipTests=true
mvn -f "${BASE_DIR}"/../bigdata-jar/pom.xml clean install package -DskipTests=true

#Due to BLZG-1725, we need to build these separately.
#mvn -f "${BASE_DIR}"/../blazegraph-artifacts/pom.xml clean package -DskipTests=true
ARTIFACTS="blazegraph-deb blazegraph-war bigdata-war"

for artifact in $ARTIFACTS; do
	mvn -f "${BASE_DIR}"/../${artifact}/pom.xml clean package -DskipTests=true
done

#Build RPM without signing the code see BLZG-1725
mvn -f "${BASE_DIR}"/../blazegraph-rpm/pom.xml clean package -P \!code-signing -DskipTests=true

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

#Copy the releases file as README.txt
VERSION=`cat ${BASE_DIR}/version.properties | grep VERSION | cut -d= -f2 | sed -e 's/\./_/g'`
RELEASE_NOTES="RELEASE_${VERSION}.txt"

if [ -f "${BASE_DIR}/../bigdata/src/releases/${RELEASE_NOTES}" ] ; then
	cp -f "${BASE_DIR}/../bigdata/src/releases/$RELEASE_NOTES" "${ARTIFACT_DIR}/README.txt"
fi

echo "Copied the deployers to ${ARTIFACT_DIR}."
