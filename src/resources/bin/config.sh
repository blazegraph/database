## Configure basic environment variables.  Obviously, you must use your own parameters for LOCATORS and ZK_SERVERS.
## This will not override parameters in the environment.

# Name of the federation of services (controls the Apache River GROUPS).

if [ -z "${FEDNAME}" ]; then
	export FEDNAME=my-cluster-1
fi

# Path for local storage for this federation of services.

if [ -z "${FED_DIR}" ]; then
	export FED_DIR=/opt/bigdata-current/data
fi

# Name of the replication cluster to which this HAJournalServer will belong.

if [ -z "${LOGICAL_SERVICE_ID}" ]; then
	export LOGICAL_SERVICE_ID=HA-Replication-Cluster-1
fi

# Where to find the Apache River service registrars (can also use multicast).

if [ -z "${LOCATORS}" ]; then
	#Use for a HA1+ configuration
	export LOCATORS="jini://localhost/"
	#HA3 example
	#export LOCATORS="jini://bigdata15/,jini://bigdata16/,jini://bigdata17/"
fi

# Where to find the Apache Zookeeper ensemble.

if [ -z "${ZK_SERVERS}" ] ; then
	#Use for single node configuration
	export ZK_SERVERS="localhost:2181"
	#Use for a multiple ZK configuration
	#export ZK_SERVERS="bigdata15:2081,bigdata16:2081,bigdata17:2081"
fi

#Replication Factor (set to one for HA1) configuration

if [ -z "${REPLICATION_FACTOR}" ] ; then
	#Use for a HA1 configuration
	export REPLICATION_FACTOR=1
	#Use for a HA1+ configuration
	#export REPLICATION_FACTOR=3
fi

#Port for the NanoSparqlServer Jetty

if [ -z "${JETTY_PORT}" ] ; then
	export JETTY_PORT=8080
fi

#Group commit (true|false)
#See http://wiki.blazegraph.com/wiki/index.php/GroupCommit and BLZG-192.

if [ -z "${GROUP_COMMIT}" ] ; then
	export GROUP_COMMIT=false
fi
