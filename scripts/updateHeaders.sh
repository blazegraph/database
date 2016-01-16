#!/bin/bash

for file in `grep -R -E "2006\-2015|licenses@blazegraph.com" * | cut -f 1 -d : | sort -u`; do
#for file in `ls -1 NOTICE_TEST`; do

	sed \
	-e 's/licenses@blazegraph.com/licenses@blazegraph.com/g'\
	-e 's/2006\-2015/2006\-2016/g'\
	-e 's/SYSTAP, LLC DBA Blazegraph/SYSTAP, LLC DBA Blazegraph DBA Blazegraph/g'\
	$file > ${file}.bak
	rm -f $file
	mv ${file}.bak $file

done

