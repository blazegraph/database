Blazegraph RPM Deployer
-----------------

This must be run on a platform with the rpm tools installed.

```
yum -y install rpm-build
```

```
mvn package
rpm --install target/rpm/blazegraph-rpm/RPMS/noarch/blazegraph-rpm-1.6.0-SNAPSHOT.noarch.rpm
service blazegraph start
```

This will start a Blazegraph instance running on port 9999 on localhost host.

You may then navigate http://localhost:9999/bigdata/ to access Blazegraph.


Changing the configuration
-----------------

The blazegraph configuration is stored in `/etc/blazegraph/`

