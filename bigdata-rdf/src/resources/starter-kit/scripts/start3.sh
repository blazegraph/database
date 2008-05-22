#!/bin/bash

# Clear out persistent data and log files.
rm -rf /var/bigdata

cd /nas/config/TimestampServer0 && /nas/scripts/startService.sh com.bigdata.service.jini.TimestampServer && sleep 1

cd /nas/config/LoadBalancerServer0 && /nas/scripts/startService.sh com.bigdata.service.jini.LoadBalancerServer && sleep 1

cd /nas/config/DataServer4 && /nas/scripts/startService.sh com.bigdata.service.jini.DataServer && sleep 1

cd /nas/config/MetadataServer0 && /nas/scripts/startService.sh com.bigdata.service.jini.MetadataServer && sleep 1

