Blazegraph Debian Deployer
-----------------

To build a version, use `mvn package`.  

```
apt-get update #Update to the latest
#Steps for Oracle JDK on Ubuntu
apt-get install python-software-properties
add-apt-repository ppa:webupd8team/java
apt-get update
apt-get install oracle-java8-installer
dpkg --install blazegraph.deb #The path to the Debian package
apt-get install -f -y #force install of dependencies without prompting for updates
```


This will start a Blazegraph instance running on port 9999 on localhost host.

http://localhost:9999/


Changing the configuration
-----------------

The blazegraph configuration is stored in `/etc/blazegraph`.  The system configuration is in `/etc/default/blazegraph`.
