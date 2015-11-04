Blazegraph Debian Deployer
-----------------

```
mvn package
dpkg --install target/blazegraph-deb-1.6.0-master-SNAPSHOT.deb
service blazegraph start
```

This will start a Blazegraph instance running on port 9999 on localhost host.


Changing the configuration
-----------------

The blazegraph configuration is stored in `/usr/local/blazegraph/conf`.  The system configuration is in `/etc/default/blazegraph`.

