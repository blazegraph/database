#!/bin/bash

BASE_DIR=`dirname $0`

for file in `grep -R -E "2006\-2015|licenses@systap.com" ${BASE_DIR}/../* | grep -v "updateHeaders.sh" | grep -v "DBA" | cut -f 1 -d : | sort -u`; do
#for file in `ls -1 NOTICE_TEST`; do

    echo "Processing $file."

	sed \
	-e 's/licenses@systap.com/licenses@blazegraph.com/g'\
	-e 's/2006\-2015/2006\-2016/g'\
	-e 's/SYSTAP, LLC/SYSTAP, LLC DBA Blazegraph/g'\
	$file > ${file}.bak
	rm -f $file
	mv ${file}.bak $file

done

