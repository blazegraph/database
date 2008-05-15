#!/bin/bash

# Clear out persistent data.
rm -rf /var/bigdata

cd /nas/config/DataServer0 && /nas/scripts/startService.sh com.bigdata.service.jini.DataServer && sleep 1

cd /nas/config/DataServer1 && /nas/scripts/startService.sh com.bigdata.service.jini.DataServer && sleep 1
