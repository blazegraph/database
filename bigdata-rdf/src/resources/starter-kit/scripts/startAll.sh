#!/bin/bash

# Note: Some time delays appear to be necessary - at least between launching jini and starting the services.

# Clear out persistent data.
rm -rf /var/bigdata

# Clear out the log files.
rm -rf /var/log/bigdata

(/usr/java/jini2_1/installverify/Launch-All&) && sleep 5

cd /nas/config/TimestampServer0 && /nas/scripts/startService.sh com.bigdata.service.jini.TimestampServer && sleep 1

cd /nas/config/LoadBalancerServer0 && /nas/scripts/startService.sh com.bigdata.service.jini.LoadBalancerServer && sleep 1

cd /nas/config/DataServer0 && /nas/scripts/startService.sh com.bigdata.service.jini.DataServer && sleep 1

cd /nas/config/DataServer1 && /nas/scripts/startService.sh com.bigdata.service.jini.DataServer && sleep 1

cd /nas/config/MetadataServer0 && /nas/scripts/startService.sh com.bigdata.service.jini.MetadataServer && sleep 1

