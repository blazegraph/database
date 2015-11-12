#!/bin/bash

#
# Forces a snapshot (fully backup) of the HA Journal
# See https://wiki.blazegraph.com/wiki/index.php/HAJournalServer#Requesting_a_Snapshot
#

source /etc/default/blazegraph-ha

echo "Creating snapshot for http://localhost:${JETTY_PORT}/bigdata/status?snapshot"

curl http://localhost:${JETTY_PORT}/bigdata/status?snapshot 2>&1 >& /dev/null

echo "Created snapshot for http://localhost:${JETTY_PORT}/bigdata/status?snapshot"
