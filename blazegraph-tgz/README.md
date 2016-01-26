# Blazegraph Tarball Artifact #
This artifact builds the tgz, bz2 (if installed), and zip distribution for Blazegraph.

## Building ##

To build, use the command below.  `package` is required as there is some dependency munging used to the get the war file and configuration files.

```
mvn package assembly:single
```

## Running ##
Unpack the distribution to your location of choice.  It will typically unpack into a directory such as `blazegraph-tgz-2.0.0`.

You can then:

```
cd blazegraph-tgz-2.0.0
./bin/blazegraph.sh start # also supports start|stop|restart|status
```

The general layout is:

```
bin/   #Scripts and other utilities
conf/   #Configuration files for Journal properties, logging, and startup
data/   #data directory.  The journal properties must be configured to use this.
lib/    #Java libraries
log/    #Log file.  The default is blazegraph.out
pid/    #pid file
war/    #web application sources for the Blazegraph Workbench
```

