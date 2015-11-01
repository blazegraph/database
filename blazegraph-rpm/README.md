Blazegraph RPM Deployer
-----------------

This must be run on a platform with the rpm tools installed.

```
mvn package

service blazegraph start
```

This will start a Blazegraph instance running on port 9999 on localhost host.


Changing the configuration
-----------------

The blazegraph configuration is stored in `/usr/local/blazegraph/conf`, which is soft-linked to  `/etc/blazegraph`.

