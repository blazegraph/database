#!/bin/bash

# Clear out persistent data.
rm -rf /var/bigdata

cd /nas/config/DataServer2 && /nas/scripts/startService.sh com.bigdata.service.jini.DataServer && sleep 1

cd /nas/config/DataServer3 && /nas/scripts/startService.sh com.bigdata.service.jini.DataServer && sleep 1
