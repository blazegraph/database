## bigdata-ha-test ##

### Running Tests in Eclipse ###

To run tests in Eclipse, you must follow the steps below.

Download and package without running tests:

```
mvn clean package -DskipTests
```

Start the test services, which requires a local zookeeper installation (3.4.5+).   You may edit testServices.properties with the path location.

```
ant startTestServices
```

The unit tests should now run successfully in Eclipse.
