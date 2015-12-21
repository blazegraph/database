#!/bin/bash

BASE_DIR=`dirname $0`

${BASE_DIR}/prog.sh com.bigdata.journal.DumpJournal -pages $*

