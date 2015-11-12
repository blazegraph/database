Blazegraph Debian Deployer
-----------------

This by default installs and starts a Blazegraph HA1 configuration.   See below for EC2 tuning and HA3 setup.

```
apt-get update #Update to the latest
cd blazegraph-ha-deb
mvn package
dpkg --install target/blazegraph-ha-deb-1.6.0-master-SNAPSHOT.deb
apt-get install -f -y #force install of dependencies without prompting for updates
```

This will start a Blazegraph instance running on port 9999 on localhost host.


Changing the configuration
-----------------

The blazegraph configuration is stored in `/usr/local/blazegraph/conf`.  The system configuration is in `/etc/default/blazegraph`.

EC2 Configuration
---------------
Configure your DATA_DIR, SNAPSHOT_DIR, and HALOG_DIR to the correct values for your installation.

`vi/emacs /etc/default/blazegraph-ha`

```
#This is the default location for the journal.  It should be a fast disk.  Instance disk on EC2.
DATA_DIR="${BLZG_HOME}"/data

#This is where to store snapshots.  It should be durable.  EBS disk on EC2.
SNAPSHOT_DIR="${BLZG_HOME}"/data

#This is where to store logs.  It should be durable.  EBS disk on EC2.
HALOG_DIR="${BLZG_HOME}"/data
```

Restoring a Journal from Snapshots and the HALogs
-------------------------------------------------
The script in `/usr/local/blazegraph-ha/bin/HARestore` may be used to restore a journal file from snapshots and the HALogs.   It uses the properties in /etc/default/blazegraph-ha to determine where to find the HALogs, snapshots, and place the new journal.  The journal file in the BlZG_HOME/data directory must not exist or the restore utility will fail.


```
cd /usr/local/blazegraph-ha/bin/
./HARestore
```

Taking a snapshot
-----------------
A snapshot will force a full backup.  See https://wiki.blazegraph.com/wiki/index.php/HAJournalServer#Requesting_a_Snapshot.  There is a utility script in `/usr/local/blazegraph-ha/bin/snapshot.sh`, which may be used to force a snapshot or configured as a cronjob to run at a particular time.

Example crontab entry for daily snapshot
```
#Take a full snapshot at 0200 every day
0 2 * * * /usr/local/blazegraph-ha/bin/snapshot.sh 2>&1 >& /dev/null
```

HA3 Configuration
-----------------
- Setup a VPC or other networking environment for three servers such that they may communicate internally without firewall restrictions, etc.
-  Install the Blazegraph HA1 debian package on three machines with a networking configuration that allows communication between the nodes.  Make a note of each of the server names or IPs.  You will need to use the internal IPs for the servers in the VPC.

```
server1
server2
server3
```

- Configure each server.  For each server, you'll need to update the blazegraph-ha configuration and setup the zookeeper configuration.

`vi/emacs /etc/default/blazegraph-ha`

```
#Enter the name of your federation
FEDNAME=blazegraph-ha-3

# Name of the replication cluster to which this HAJournalServer will belong.
LOGICAL_SERVICE_ID=HA-Replication-Cluster-3

# Where to find the Apache River service registrars (can also use multicast).
#HA3 example
export LOCATORS="jini://server1/,jini://server2/,jini://server3/"

# Where to find the Apache Zookeeper ensemble.
#Use for a multiple ZK configuration
ZK_SERVERS="server1:2181,server2:2181,server3:2181"

#Replication Factor (set to 3 for HA3) configuration
REPLICATION_FACTOR=3
```

`vi/emacs /opt/zookeeper/conf/zoo.cfg`

Update the servers in the zookeeper configuration

```
# specify all zookeeper servers
# The fist port is used by followers to connect to the leader
# The second one is used for leader election
server.1=server1:2888:3888
server.2=server2:2888:3888
server.3=server3:2888:3888 
``` 

Each server must have a unique id in the zooData directory myid file.  Assuming you have default install with zooDatadir set to /var/lib/zookeeper/ an example is below.

server1
```
echo 1 > /var/lib/zookeeper/myid 
```

server2
```
echo 2 > /var/lib/zookeeper/myid 
```

server3
```
echo 3 > /var/lib/zookeeper/myid 
```

-  Restart zookeeper and Blazegraph HA

`service zookeeper restart`

`service blazegraph-ha restart`

-  Enjoy!
