# Who runs bigdata?
default['systap-bigdataHA'][:bigdata_user] = "bigdata"
default['systap-bigdataHA'][:bigdata_group] = "bigdata"

# Where to find and build bigdata code
default['systap-bigdataHA'][:svn]     = "https://svn.code.sf.net/p/bigdata/code/branches/BIGDATA_RELEASE_1_3_0"
default['systap-bigdataHA'][:source]  = "/home/ubuntu/bigdata-code"

# Name of the federation of services (controls the Apache River GROUPS).
default['systap-bigdataHA'][:fedname] = 'my-cluster-1'

# Path for local storage for this federation of services.
default['systap-bigdataHA'][:fed_dir] = '/var/lib/bigdata'

# Where the bigdata-ha.jnl file will live:
default['systap-bigdataHA'][:data_dir]  = node['systap-bigdataHA'][:fed_dir] + "/data"

# Where the log files will live:
default['systap-bigdataHA'][:log_dir]  = node['systap-bigdataHA'][:fed_dir] + "/logs"

# Name of the replication cluster to which this HAJournalServer will belong.
default['systap-bigdataHA'][:logical_service_id] = 'HA-Replication-Cluster-1'

# Where to find the Apache River service registrars (can also use multicast).
default['systap-bigdataHA'][:river_locator1] = 'bigdataA'
default['systap-bigdataHA'][:river_locator2] = 'bigdataB'
default['systap-bigdataHA'][:river_locator3] = 'bigdataC'

# Where to find the Apache Zookeeper ensemble.
default['systap-bigdataHA'][:zk_server1]  = 'bigdataA'
default['systap-bigdataHA'][:zk_server2]  = 'bigdataB'
default['systap-bigdataHA'][:zk_server3]  = 'bigdataC'
