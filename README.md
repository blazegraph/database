## Welcome to the Blazegraph Database ##
Blazegraph™ DB is a ultra high-performance graph database supporting Blueprints and RDF/SPARQL APIs. It supports up to 50 Billion edges on a single machine. It is in production use for Fortune 500 customers such as EMC, Autodesk, and many others.  It is supporting key [Precision Medicine](http://www.syapse.com) applications and has wide-spread usage for life science applications.  It is used extensively to support Cyber analytics in commercial and government applications.  It powers the Wikimedia Foundation's [Wikidata Query Service](https://query.wikidata.org/). 

![image](http://blog.blazegraph.com/wp-content/uploads/2015/07/blazegraph_by_systap_favicon.png)

Please see the release notes in [releases](bigdata/src/releases) for version changes.

[Sign up](http://eepurl.com/VLpUj) to get the latest news on Blazegraph.

Please also visit us at our: [website](https://www.blazegraph.com), [wiki](https://wiki.blazegraph.com), and [blog](https://blog.blazegraph.com/).

Find an issue?   Need help?  See [JIRA](https://jira.blazegraph.com).

Reporting a security issue: [Security Reporting](Security.md).

### Quick Start with the Executable Jar ###
Up and running with Blazegraph in under 30 seconds:  [Quick Start](https://wiki.blazegraph.com/wiki/index.php/Quick_Start).

### Deploying in Production ###
Blazegraph is designed to be easy to use and get started. It ships without SSL or authentication by default for this reason. For production deployments, we _strongly_ recommend you enable SSL, authentication, and appropriate network configurations. There are some helpful links below to enable you to do this. 

#### Enabling SSL support ####
To enable SSL support, uncomment the example [jetty.xml](blazegraph-jar/src/main/resources/jetty.xml#L141) and configure it for your local keystore.

#### Configuration Authentication ####
By default, Blazegraph ships without authentication enabled. This is great for developing, getting started, and doing research with Blazegraph. However, it's not recommended for any production deployment. To configuration authentication, you must configure it either within the web app container or via a reverse-proxy configuration.

Note that the Blazegraph namespace feature for [multi-tenancy](https://wiki.blazegraph.com/wiki/index.php/REST_API#Multi-Tenancy_API) does not provide security isolation. Users that can access the base URI of the server can access any of the available namespaces. You can further restrict this through a combination of authentication configuration and restricting access to specific namespace URIs, i.e. `/blazegraph/namespace/NAMESPACE/sparql`.

There are three basic options:

1. **Configuring Jetty Authentication for a standalone Jetty deployment**:  Follow the [jetty](http://www.eclipse.org/jetty/documentation/9.2.22.v20170531/configuring-security-authentication.html) guide to configure authentication for the [jetty.xml](blazegraph-jar/src/main/resources/jetty.xml) you use to deploy the server by uncommenting the `<Get name="securityHandler">` section. You'll need to create a [realm.properties](blazegraph-jar/src/main/resources/realm.properties) and update the jetty.xml to point to its location on the filesystem.  Then configure the [web.xml](bigdata-war-html/src/main/webapp/WEB-INF/web.xml) to uncomment the security-constraint.
1. **Configuring Tomcat Authentication for a standalone Tomcat deployment**:  First configure a Tomcat [Realm](https://tomcat.apache.org/tomcat-7.0-doc/realm-howto.html) with your choice of authentication method (JDBC, JNDI, etc.). Then configure the [web.xml](bigdata-war-html/src/main/webapp/WEB-INF/web.xml) to uncomment the security-constraint.
1. **Setup a reverse-proxy configuration with authentication**:  You can setup an http or https reverse proxy configuration that has authentication and forward requests to the local Blazegraph instance (typically running on localhost:9999). This is a good option with [Nginx](https://community.openhab.org/t/using-nginx-reverse-proxy-authentication-and-https/14542) and [Apache](https://stackoverflow.com/questions/5011102/apache-reverse-proxy-with-basic-authentication). 

##### Mitigating Cross-Site Request Forgery (CSRF) #####
If you enable authentication and expose the Blazegraph workbench, you should also take steps to protect against CSRF. Tomcat8 provides a [CSRF filter](https://tomcat.apache.org/tomcat-8.0-doc/config/filter.html#CSRF_Prevention_Filter_for_REST_APIs) that can be configured. For Jetty, if you configure authentication the default value for `SecurityHandler.setSessionRenewedOnAuthentication(true)` can also be used. CSRF protection may require REST clients to implement HTTP headers to be used to interact with the service.

### Building the code ###
As a quick start, run `mvn install -DskipTests` or the utility script `./scripts/mavenInstall.sh `.

For more detailed maven information see the [wiki](https://wiki.blazegraph.com/wiki/index.php/MavenNotes). 

If you build with Java 7, you need to add Maven options for TLS 1.2, i.e. `export MAVEN_OPTS="-Dhttps.protocols=TLSv1.2"`.

### Samples and Examples ###
There are code samples and examples to get started with the Blazegraph Database [here] (https://github.com/blazegraph/blazegraph-samples).  Tinkerpop3 examples are included directly within the Tinkerpop3 repository per below.

### Javadocs ###
Click here to view the lastest [API Javadocs](https://blazegraph.github.io/database/apidocs/index.html).

### Maven Central ###
Starting with the 2.0.0 release, the Blazegraph Database is available on Maven Central.  To include the core platform and dependencies, include the artifact below in your dependencies.   [Developing with Maven](https://wiki.blazegraph.com/wiki/index.php/MavenNotes) has notes on developing with Blazegraph Database source code and Maven.

```
    <dependency>
        <groupId>com.blazegraph</groupId>
        <artifactId>bigdata-core</artifactId>
        <version>2.0.0</version>
    </dependency>
    <!-- Use if Tinkerpop 2.5 support is needed ; See also Tinkerpop3 below. -->
    <dependency>
        <groupId>com.blazegraph</groupId>
        <artifactId>bigdata-blueprints</artifactId>
        <version>2.0.0</version>
    </dependency>
```

If you'd just link the Blazegraph Database dependencies without any of the external libraries, use the bigdata-runtime artifact.

```
    <dependency>
        <groupId>com.blazegraph</groupId>
        <artifactId>bigdata-runtime</artifactId>
        <version>2.0.0</version>
    </dependency>
```

### Deployers ###

Starting with 2.0.0, the default context path for deployment is `http://localhost:9999/blazegraph/`.  There are also Maven artifacts for WAR deployers (`blazegraph-war`), executable Jar files (`blazegraph-jar`), [Debian Package](blazegraph-deb/) (`blazegraph-deb`), [RPM](blazegraph-rpm/) (`blazegraph-rpm`), and a [Tarball](blazegraph-tgz/) (`blazegraph-tgz`).

The `bigdata-war` and `bigdata-jar` artifacts are included for legacy purposes and use the `/bigdata/` context path.

### Tinkerpop3 ###
Tinkerpop3 supports requires Java 1.8 and is now in a separate repository.  See [Tinkerpop3](https://github.com/blazegraph/tinkerpop3).  It is also available as Maven Central artifact.

```
    <dependency>
        <groupId>com.blazegraph</groupId>
        <artifactId>blazegraph-gremlin</artifactId>
        <version>1.0.0</version>
    </dependency>
    
```

### Triple Pattern Fragment (TPF) Server ###
There is a [Blazegraph Triple Pattern Fragment TPF](https://github.com/hartig/BlazegraphBasedTPFServer) server that supports [Linked Data Fragments](http://linkeddatafragments.org/).

### Blazegraph Python Client ###
There is a Blazegraph Python Client [here](https://github.com/blazegraph/blazegraph-python)

### Blazegraph Dot Net RDF Client ###
There is a Blazegraph Dot Net RDF Client [here](https://github.com/blazegraph/blazegraph-dotnetrdf)
