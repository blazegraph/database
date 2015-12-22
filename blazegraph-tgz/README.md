# Blazegraph Tarball Artifact #
This artifact builds the tgz, bz2 (if installed), and zip distribution for Blazegraph.

## Building ##

To build, use the command below.  `package` is required as there is some dependency munging used to the get the war file and configuration files.

```
mvn package assembly:single
```
