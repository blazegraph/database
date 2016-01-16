#!/bin/bash
#Utility to provide xml formatting for the pom files.

BASE_DIR=`dirname $0`

if [ -z `which xmllint` ] ; then

    echo "xmllint is required."
    exit 1

fi

TMP_FILE=/tmp/$$.pom.xml

for file in `find "${BASE_DIR}"/.. -maxdepth 2 -type f -name "pom.xml" -print`; do

    echo "Updating $file"
    xmllint --format $file > $TMP_FILE
    cat $TMP_FILE > $file

done

rm -f $TMP_FILE
