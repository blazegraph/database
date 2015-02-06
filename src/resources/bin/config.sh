## Configure basic environment variables.  Obviously, you must use your own parameters for LOCATORS and ZK_SERVERS.
# Name of the federation of services (controls the Apache River GROUPS).
export FEDNAME=my-cluster-1
# Path for local storage for this federation of services.
export FED_DIR=/opt/bigdata-current/data
# Name of the replication cluster to which this HAJournalServer will belong.
export LOGICAL_SERVICE_ID=HA-Replication-Cluster-1
# Where to find the Apache River service registrars (can also use multicast).
export LOCATORS="jini://localhost/"
#Use for a HA1+ configuration
#export LOCATORS="jini://bigdata15/,jini://bigdata16/,jini://bigdata17/"
# Where to find the Apache Zookeeper ensemble.
export ZK_SERVERS="localhost:2181"
#Use for a multiple ZK configuration
#export ZK_SERVERS="bigdata15:2081,bigdata16:2081,bigdata17:2081"
#Replication Factor (set to one for HA1) configuration
export REPLICATION_FACTOR=1
#Use for a HA1+ configuration
#export REPLICATION_FACTOR=3
#Port for the NanoSparqlServer Jetty
export JETTY_PORT=8080
