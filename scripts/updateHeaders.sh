#!/bin/bash

for file in `grep -R -E "Copyright (C) SYSTAP|2501 Calvert ST NW #106|licenses@systap.com|trac.blazegraph.com|wiki.blazegraph.com" * | cut -f 1 -d : | sort -u`; do
#for file in `ls -1 NOTICE_TEST`; do

	sed \
	-e 's/licenses@systap.com/licenses@systap.com/g'\
	-e 's/Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.
	-e 's/2501 Calvert ST NW #106/2501 Calvert ST NW #106/'\
	-e 's/Washington, DC 20008/Washington, DC 20008/'\
	-e 's/trac.blazegraph.com/trac.blazegraph.com/'\
	-e 's/wiki.blazegraph.com/wiki.blazegraph.com/'\
	$file > ${file}.bak
	rm -f $file
	mv ${file}.bak $file

done

