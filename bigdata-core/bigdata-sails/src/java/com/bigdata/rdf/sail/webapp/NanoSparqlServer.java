/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package com.bigdata.rdf.sail.webapp;

import java.io.File;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.servlet.ServletContextListener;

import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.webapp.WebAppContext;
import org.eclipse.jetty.xml.XmlConfiguration;

import com.bigdata.Banner;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.resources.IndexManager;
import com.bigdata.util.config.NicUtil;

/**
 * Utility class provides a simple SPARQL end point with a REST API.
 * 
 * @author thompsonbry
 * @author martyncutcher
 * 
 * @see <a
 *      href="https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=NanoSparqlServer">
 *      NanoSparqlServer </a> on the wiki.
 * 
 * @see <a href="http://www.eclipse.org/jetty/documentation/current/"> Jetty
 *      Documentation </a>
 * 
 * @see <a href="http://wiki.eclipse.org/Jetty/Reference/jetty.xml_syntax" >
 *      Jetty XML Reference </a>
 * 
 * @see <a href="http://wiki.eclipse.org/Jetty/Tutorial/Embedding_Jetty">
 *      Embedding Jetty </a>
 * 
 * @todo If the addressed instance uses full transactions, then mutation should
 *       also use a full transaction. Does it?
 * 
 * @todo Remote command to advance the read-behind point. This will let people
 *       bulk load a bunch of stuff before advancing queries to read from the
 *       new consistent commit point.
 */
public class NanoSparqlServer {
    
    static private final Logger log = Logger.getLogger(NanoSparqlServer.class);

    public interface SystemProperties {

        /**
         * The name of the system property that can be used to override the default
         * HTTP port in the bundled <code>jetty.xml</code> file.
         */
        String JETTY_PORT = "jetty.port";
        
        /**
         * The name of the system property that can be used to override the
         * location of the <code>jetty.xml</code> file that will be used to
         * configure jetty (default {@value #DEFAULT_JETTY_XML}).
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/730" >
         *      Allow configuration of embedded NSS jetty server using
         *      jetty-web.xml </a>
         * 
         * @see #DEFAULT_JETTY_XML
         */
        String JETTY_XML = "jettyXml";

        /**
         * The default value works when deployed under the IDE with the
         * <code>bigdata-war/src</code> directory on the classpath. When
         * deploying outside of that context, the value needs to be set
         * explicitly.
         */
        String DEFAULT_JETTY_XML = "jetty.xml";

        /**
         * The timeout in seconds that we will await the start of the jetty
         * {@link Server} (default {@value #DEFAULT_JETTY_START_TIMEOUT}).
         */
        String JETTY_STARTUP_TIMEOUT = "jetty.start.timeout";

        String DEFAULT_JETTY_STARTUP_TIMEOUT = "10";

        /**
         * When <code>true</code>, the state of jetty will be dumped onto a
         * logger after the server start.
         */
        String JETTY_DUMP_START = "jetty.dump.start";
        
        /**
         * This property specifies the resource path for the web application. In
         * order for this mechanism to work, the <code>jetty.xml</code> file
         * MUST contain a line which allows the resourceBase of the web
         * application to be set from an environment variable. For example:
         * 
         * <pre>
         * &lt;SystemProperty name="jetty.resourceBase" default="bigdata-war/src" /&gt;
         * </pre>
         * 
         * The <code>jetty.resourceBase</code> variable may identify either a
         * file or a resource on the class path. To force the use of the web
         * application embedded within the <code>bigdata.jar</code> you need to
         * specify a JAR URL along the following lines (using the appropriate
         * file path and jar name and version:
         * 
         * <pre>
         * jar:file:../lib/bigdata-1.3.0.jar!/bigdata-war/src
         * </pre>
         * 
         * The use of absolute file paths are recommended for reliable
         * resolution.
         * <p>
         * The order of preference is:
         * <ol>
         * <li><code>jetty.resourceBase</code> is specified. The value of this
         * environment variable will be used to locate the web application.</li>
         * <li>
         * <code>jetty.resourceBase</code> is not specified (either
         * <code>null</code> or whitespace).
         * <ol>
         * <li>An attempt is made to locate the <code>bigdata-war/src</code>
         * resource in the file system (relative to the current working
         * directory). If found, the <code>jetty.resourceBase</code> environment
         * variable is set to this resource using a <code>file:</code> style
         * URL. This will cause jetty to use the web application directory in
         * the file system.</li>
         * <li>
         * An attempt is made to locate the resource
         * <code>/WEB-INF/web.xml</code> using the classpath (this handles the
         * case when running under the eclipse IDE). If found, the the
         * <code>jetty.resourceBase</code> is set to the URL formed by removing
         * the trailing <code>WEB-INF/web.xml</code> for the located resource.
         * This will cause jetty to use the web application resource on the
         * classpath. If there are multiple such resources on the classpath, the
         * first such resource will be discovered and used.</li>
         * <li>An attempt is made to locate the resource
         * <code>bigdata-war/src/main/webapp/WEB-INF/web.xml</code> using the classpath
         * (this handles the case when running from the command line using a
         * bigdata JAR). If found, the the <code>jetty.resourceBase</code> is
         * set to the URL formed by the trailing <code>WEB-INF/web.xml</code>
         * for the located resource. This will cause jetty to use the web
         * application resource on the classpath. If there are multiple such
         * resources on the classpath, the first such resource will be
         * discovered and used.</li>
         * <li>
         * Otherwise, the <code>jetty.resourceBase</code> environment variable
         * is not modified and the default location specified in the
         * <code>jetty.xml</code> file will be used. If jetty is unable to
         * resolve that resource, then the web application will not start.</li>
         * </ol>
         * </ol>
         * 
         * @see <a href="http://trac.blazegraph.com/ticket/939" > NSS does not
         *      start from command line: bigdata-war/src not found </a>
         */
        String JETTY_RESOURCE_BASE = "jetty.resourceBase";

        /**
         * The location of the <code>override-web.xml</code> resource. The
         * default is given in <code>jetty.xml</code> and serves to locate the
         * resource when deployed under an IDE. If not explicitly given, value
         * of the environment variable is set by the same logic that sets the
         * {@link #JETTY_RESOURCE_BASE} environment variable. This allows the
         * <code>override-web.xml</code> resource to be found in its default
         * location (which is the same directory / package as the
         * <code>web.xml</code> file) while still preserving the ability to
         * override the location of that resource explicitly by setting the
         * environment variable before starting the server.
         * 
         * @see <a href="http://trac.blazegraph.com/ticket/940" > ProxyServlet in
         *      web.xml breaks tomcat WAR (HA LBS) </a>
         */
        String JETTY_OVERRIDE_WEB_XML = "jetty.overrideWebXml";
        
        /**
         * The location to over propertyFile used to create the servlet.  This
         * to allow overriding the default value via passing a Java Property at
         * the command line.
         */
        
        String BIGDATA_PROPERTY_FILE = "bigdata.propertyFile";
        
        /**
         * The jetty.home property.
         */

		String JETTY_HOME = "jetty.home";

    }

    /**
     * Run an httpd service exposing a SPARQL endpoint. The service will respond
     * to the following URL paths:
     * <dl>
     * <dt>http://localhost:port/</dt>
     * <dd>The SPARQL end point for the default namespace as specified by the
     * <code>namespace</code> command line argument.</dd>
     * <dt>http://localhost:port/namespace/NAMESPACE</dt>
     * <dd>where <code>NAMESPACE</code> is the namespace of some triple store or
     * quad store, may be used to address ANY triple or quads store in the
     * bigdata instance.</dd>
     * <dt>http://localhost:port/status</dt>
     * <dd>A status page.</dd>
     * </dl>
     * 
     * @param args
     *            USAGE:<br/>
     *            To start the server:<br/>
     *            <code>(options) <i>port</i> <i>namespace</i> (propertyFile|configFile) )</code>
     *            <p>
     *            <i>Where:</i>
     *            <dl>
     *            <dt>port</dt>
     *            <dd>The port on which the service will respond -or-
     *            <code>0</code> to use any open port.</dd>
     *            <dt>namespace</dt>
     *            <dd>The namespace of the default SPARQL endpoint (the
     *            namespace will be <code>kb</code> if none was specified when
     *            the triple/quad store was created).</dd>
     *            <dt>propertyFile</dt>
     *            <dd>A java properties file for a standalone {@link Journal}.</dd>
     *            <dt>configFile</dt>
     *            <dd>A jini configuration file for a bigdata federation.</dd>
     *            </dl>
     *            and <i>options</i> are any of:
     *            <dl>
     *            <dt>-jettyXml</dt>
     *            <dd>The location of the jetty.xml resource that will be used
     *            to start the {@link Server} (default is the file in the JAR).
     *            The default will locate the <code>jetty.xml</code> resource
     *            that is bundled with the JAR. This preserves the historical
     *            behavior. If you want to use a different
     *            <code>jetty.xml</code> file, just override this property on
     *            the command line -or- specify the
     *            {@link NanoSparqlServer.SystemProperties#JETTY_XML} system
     *            property.</dd>
     *            <dt>-nthreads</dt>
     *            <dd>The #of threads which will be used to answer SPARQL
     *            queries (default
     *            {@value ConfigParams#DEFAULT_QUERY_THREAD_POOL_SIZE}).</dd>
     *            <dt>-forceOverflow</dt>
     *            <dd>Force a compacting merge of all shards on all data
     *            services in a bigdata federation (this option should only be
     *            used for benchmarking purposes).</dd>
     *            <dt>-readLock</dt>
     *            <dd>The commit time against which the server will assert a
     *            read lock by holding open a read-only transaction against that
     *            commit point OR <code>-1</code> (MINUS ONE) to assert a read
     *            lock against the last commit point. When given, queries will
     *            default to read against this commit point. Otherwise queries
     *            will default to read against the most recent commit point on
     *            the database. Regardless, each query will be issued against a
     *            read-only transaction.</dt>
     *            <dt>-servletContextListenerClass</dt>
     *            <dd>The name of a class that extends
     *            {@link BigdataRDFServletContextListener}. This allows you to
     *            hook the {@link ServletContextListener} events.</dd>
     *            </dl>
     *            </p>
     */
//   *            <dt>bufferCapacity [#bytes]</dt>
//   *            <dd>Specify the capacity of the buffers used to decouple the
//   *            query evaluation from the consumption of the HTTP response by
//   *            the client. The capacity may be specified in bytes or
//   *            kilobytes, e.g., <code>5k</code>.</dd>
    public static void main(final String[] args) throws Exception {

        Banner.banner();

        int port = 80;
        String namespace = "kb";
        int queryThreadPoolSize = ConfigParams.DEFAULT_QUERY_THREAD_POOL_SIZE;
        boolean forceOverflow = false;
        Long readLock = null;
        
        /*
         * Note: This default will locate the jetty.xml resource that is bundled
         * with the JAR. This preserves the historical behavior. If you want to
         * use a different jetty.xml file, just override this property on the
         * command line.
         */
        String jettyXml = System.getProperty(//
                SystemProperties.JETTY_XML,//
                "jetty.xml"//
//                SystemProperties.DEFAULT_JETTY_XML
                );
        
		// Set the resource base to inside of the jar file if jetty.home is not set
		// This is for running as the executable jar
		if (System.getProperty(SystemProperties.JETTY_HOME) == null) {

			final URL jettyJarXml = jettyXml.getClass().getResource("/war");

			System.setProperty(SystemProperties.JETTY_HOME,
					jettyJarXml.toExternalForm());

			// Also set the Jetty Resource Base to this value, if it is not
			// configured.
			if (System.getProperty(SystemProperties.JETTY_RESOURCE_BASE) == null) {
				System.setProperty(SystemProperties.JETTY_RESOURCE_BASE,
						jettyJarXml.toExternalForm());
			}
		}

        /*
         * Handle all arguments starting with "-". These should appear before
         * any non-option arguments to the program.
         */
        int i = 0;
        while (i < args.length) {
            final String arg = args[i];
            if (arg.startsWith("-")) {
                if (arg.equals("-forceOverflow")) {
                    forceOverflow = true;
                } else if (arg.equals("-nthreads")) {
                    final String s = args[++i];
                    queryThreadPoolSize = Integer.valueOf(s);
                    if (queryThreadPoolSize < 0) {
                        usage(1/* status */,
                                "-nthreads must be non-negative, not: " + s);
                    }
                } else if (arg.equals("-readLock")) {
                    final String s = args[++i];
                    readLock = Long.valueOf(s);
                    if (readLock != ITx.READ_COMMITTED
                            && !TimestampUtility.isCommitTime(readLock
                                    .longValue())) {
                        usage(1/* status */,
                                "Read lock must be commit time or -1 (MINUS ONE) to assert a read lock on the last commit time: "
                                        + readLock);
                    }
                } else if (arg.equals("-jettyXml")) {
                    jettyXml = args[++i];
                } else {
                    usage(1/* status */, "Unknown argument: " + arg);
                }
            } else {
                break;
            }
            i++;
        }

        /*
         * Finally, there should be exactly THREE (3) command line arguments
         * remaining. These are the [port], the [namespace] and the
         * [propertyFile] (journal) or [configFile] (scale-out).
         */
        final int nremaining = args.length - i;
        if (nremaining != 3) {
            /*
             * There are either too many or too few arguments remaining.
             */
            usage(1/* status */, nremaining < 3 ? "Too few arguments."
                    : "Too many arguments");
        }
        /*
         * http service port.
         */
        {
            final String s = args[i++];
            try {
                port = Integer.valueOf(s);
            } catch (NumberFormatException ex) {
                usage(1/* status */, "Could not parse as port# : '" + s + "'");
            }
        }

        /*
         * Namespace.
         */
        namespace = args[i++];

        // Note: This is checked by the ServletContextListener.
//        /*
//         * Property file.
//         */
        final String propertyFile = args[i++];
        
       
        //Through an Exception is the propertyFile does not exist.
        final File pFile = new File(propertyFile);
        
        if(!pFile.exists()) {
			final String errMsg = "Property or config file " + propertyFile
					+ " does not exist and is a required parameter.";
			System.err.println(errMsg);
        	log.error(errMsg);
        	throw new RuntimeException(errMsg);
        }
        
//        final File file = new File(propertyFile);
//        if (!file.exists()) {
//            throw new RuntimeException("Could not find file: " + file);
//        }
//        boolean isJini = false;
//        if (propertyFile.endsWith(".config")) {
//            // scale-out.
//            isJini = true;
//        } else if (propertyFile.endsWith(".properties")) {
//            // local journal.
//            isJini = false;
//        } else {
//            /*
//             * Note: This is a hack, but we are recognizing the jini
//             * configuration file with a .config extension and the journal
//             * properties file with a .properties extension.
//             */
//            usage(1/* status */,
//                    "File should have '.config' or '.properties' extension: "
//                            + file);
//        }

        /*
         * Setup the ServletContext properties.
         */

        final Map<String, String> initParams = new LinkedHashMap<String, String>();

        initParams.put(
                ConfigParams.PROPERTY_FILE,
                propertyFile);

        initParams.put(ConfigParams.NAMESPACE,
                namespace);

        initParams.put(ConfigParams.QUERY_THREAD_POOL_SIZE,
                Integer.toString(queryThreadPoolSize));

        initParams.put(
                ConfigParams.FORCE_OVERFLOW,
                Boolean.toString(forceOverflow));

        if (readLock != null) {
            initParams.put(
                    ConfigParams.READ_LOCK,
                    Long.toString(readLock));
        }
        
        // Create the service.
        final Server server = NanoSparqlServer.newInstance(port, jettyXml,
                null/* indexManager */, initParams);

        awaitServerStart(server);

        // Wait for the service to terminate.
        server.join();

    }

    /**
     * Await a {@link Server} start up to a timeout.
     * 
     * @parma server The {@link Server} to start.
     * @throws InterruptedException
     * @throws TimeoutException
     * @throws Exception
     */
    public static void awaitServerStart(final Server server)
            throws InterruptedException, TimeoutException, Exception {

//        Note: Does not appear to help.
//        
//        final WebAppContext wac = getWebApp(server);
//
//        if (wac == null)
//            throw new Exception("WebApp is not available?");

        final long timeout = Long.parseLong(System.getProperty(
                SystemProperties.JETTY_STARTUP_TIMEOUT,
                SystemProperties.DEFAULT_JETTY_STARTUP_TIMEOUT));
        
        boolean ok = false;
        final long begin = System.nanoTime();
        final long nanos = TimeUnit.SECONDS.toNanos(timeout);
        long remaining = nanos;
        try {
            // Start Server.
            log.warn("Starting NSS");
            server.start();
            // Await running.
            remaining = nanos - (System.nanoTime() - begin);
            while (server.isStarting() && !server.isRunning()
                   /* && !wac.isRunning() */ && remaining > 0) {
                Thread.sleep(100/* ms */);
                // remaining = nanos - (now - begin) [aka elapsed]
                remaining = nanos - (System.nanoTime() - begin);
            }
            if (remaining < 0) {
                throw new TimeoutException();
            }
//            if (!wac.isRunning())
//                throw new Exception("WebApp is not running?");
            ok = true;
        } finally {
            if (!ok) {
                // Complain if Server did not start.
                final String msg = "Server did not start.";
                System.err.println(msg);
                log.fatal(msg);
                if (server != null) {
                    /*
                     * Support the jetty dump-after-start semantics.
                     */
                    if (Boolean.getBoolean(SystemProperties.JETTY_DUMP_START)) {
                        log.warn(server.dump());
                    }
                    server.stop();
                    server.destroy();
                }
            }
        }

        /*
         * Support the jetty dump-after-start semantics.
         */
        if (Boolean.getBoolean(SystemProperties.JETTY_DUMP_START)) {
            log.warn(server.dump());
        }

        /*
         * Report *an* effective URL of this service.
         * 
         * Note: This is an effective local URL (and only one of them, and even
         * then only one for the first connector). It does not reflect any
         * knowledge about the desired external deployment URL for the service
         * end point.
         */
        final String serviceURL;
        {

            final int actualPort = getLocalPort(server);

            String hostAddr = NicUtil.getIpAddress("default.nic", "default",
                    true/* loopbackOk */);

            if (hostAddr == null) {

                hostAddr = "localhost";

            }

            serviceURL = new URL("http", hostAddr, actualPort, ""/* file */)
                    .toExternalForm();

            final String msg = "serviceURL: " + serviceURL;

            System.out.println(msg);
            
            if (log.isInfoEnabled())
                log.warn(msg);
            
        }
        
    }
    
    /**
     * Start the embedded {@link Server}.
     * <p>
     * Note: The port override argument given here is applied by setting the
     * {@link NanoSparqlServer.SystemProperties#JETTY_PORT} System property. The
     * old value of that property is restored afterwards, but there is a
     * side-effect which could be visible to concurrent threads.
     * 
     * @param port
     *            The port on which the service will run -OR- ZERO (0) for any
     *            open port.
     * @param indexManager
     *            The {@link IIndexManager} (optional).
     * @param initParams
     *            Initialization parameters for the web application as specified
     *            by {@link ConfigParams} (optional).
     * 
     * @return The server instance.
     * 
     * @see SystemProperties
     */
    static public Server newInstance(final int port,
            final IIndexManager indexManager,
            final Map<String, String> initParams) throws Exception {
        
        // The jetty.xml resource to be used.
        final String jettyXml = System.getProperty(SystemProperties.JETTY_XML,
                SystemProperties.DEFAULT_JETTY_XML);

        return newInstance(port, jettyXml, indexManager, initParams);
        
    }

    /**
     * Start the embedded {@link Server}.
     * <p>
     * Note: The port override argument given here is applied by setting the
     * {@link NanoSparqlServer.SystemProperties#JETTY_PORT} System property. The
     * old value of that property is restored afterwards, but there is a
     * side-effect which could be visible to concurrent threads.
     * 
     * @param port
     *            The port on which the service will run -OR- ZERO (0) for any
     *            open port.
     * @param jettyXml
     *            The location of the <code>jetty.xml</code> resource.
     * @param indexManager
     *            The {@link IIndexManager} (optional).
     * @param initParams
     *            Initialization parameters for the web application as specified
     *            by {@link ConfigParams} (optional).
     * 
     * @return The server instance.
     * 
     * @see SystemProperties
     */
    synchronized// synchronized to avoid having the side-effect on the port visible to concurrent jetty starts.
    static public Server newInstance(final int port,
            final String jettyXml,
            final IIndexManager indexManager,
            final Map<String, String> initParams) throws Exception {

        if (jettyXml == null)
            throw new IllegalArgumentException();
        
        // Set the property to override the value in jetty.xml.
        final String oldport = System.setProperty(SystemProperties.JETTY_PORT,
                Integer.toString(port));

        try {

            return newInstance(jettyXml, indexManager, initParams);
            
        } finally {

            if (oldport != null) {

                // Restore the old value for the port.
                System.setProperty(SystemProperties.JETTY_PORT, oldport);

            }

        }
        
    }

    /**
     * Variant used when you already have the {@link IIndexManager}.
     * <p>
     * When the optional {@link IIndexManager} argument is specified, it will be
     * set as an attribute on the {@link WebAppContext}. This will cause the
     * webapp to use the pre-existing {@link IIndexManager} rather than
     * attempting to open a new {@link IIndexManager} based on the
     * <code>propertyFile</code> init parameter specified in
     * <code>web.xml</code>. The HA initialization pattern relies on this.
     * <p>
     * When the {@link IIndexManager} is NOT specified, the life cycle of the
     * {@link IIndexManager} will be managed by the server and the
     * {@link IndexManager} instance will be opened (and eventually closed) by
     * the {@link BigdataRDFServletContextListener} based on the configured
     * value of the <code>propertyFile</code> init parameter in
     * <code>web.xml</code>. This form is used by {@link #main(String[])} and
     * can be used with either the {@link Journal} or the scale-out
     * architecture.
     * 
     * @param jettyXml
     *            The <code>jetty.xml</code> file that will be used to configure
     *            jetty.
     * @param indexManager
     *            The {@link IIndexManager} (optional).
     * @param initParams
     *            Optional map containing overrides for the init parameters
     *            declared in <code>web.xml</code>.
     * 
     * @return The server instance.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/730" >
     *      Allow configuration of embedded NSS jetty server using jetty-web.xml
     *      </a>
     */
    static public Server newInstance(//
            final String jettyXml,//
            final IIndexManager indexManager,//
            final Map<String, String> initParams//
    ) throws Exception {

        if (jettyXml == null)
            throw new IllegalArgumentException();

        // Used to search the classpath for jetty.xml.
        final ClassLoader classLoader = NanoSparqlServer.class
                .getClassLoader();
//        final ClassLoader classLoader = indexManager.getClass()
//                .getClassLoader();
 
        /*
         * Configure the jetty Server using a jetty.xml file. In turn, the
         * jetty.xml file configures the webapp using a web.xml file. The caller
         * can override the location of the jetty.xml file using the [jetty.xml]
         * environment variable if they need to change the way in which either
         * jetty or the webapp are configured. You can also override many of the
         * properties in the [jetty.xml] file using environment variables. For
         * example, they can also override the location of the web application
         * (including the web.xml file) using the [jetty.resourceBase]
         * environment variable.
         */
        final Server server;
        {

            // Find the effective jetty.xml URL.
            final URL jettyXmlURL = getEffectiveJettyXmlURL(classLoader,
                    jettyXml);

            // Build the server configuration from that jetty.xml resource.
            final XmlConfiguration configuration;
            {
                // Open jetty.xml resource.
                final Resource jettyConfig = Resource.newResource(jettyXmlURL);
                InputStream is = null;
                try {
                    is = jettyConfig.getInputStream();
                    // Build configuration.
                    configuration = new XmlConfiguration(is);
                } finally {
                    if (is != null) {
                        is.close();
                    }
                }
            }

            // Configure/apply jetty.resourceBase overrides.
            configureEffectiveResourceBase(classLoader);

            // Configure the jetty server.
            server = (Server) configuration.configure();

        }

        /*
         * Configure any overrides for the web application init-params.
         */
        configureWebAppOverrides(server, indexManager, initParams);

        return server;
        
    }

    private static URL getEffectiveJettyXmlURL(final ClassLoader classLoader,
            final String jettyXml) throws MalformedURLException {
    
        // Locate jetty.xml.
        URL jettyXmlUrl;
        boolean isFile = false;
        boolean isClassPath = false;
        if (new File(jettyXml).exists()) {

            // Check the file system.
            jettyXmlUrl = new URL("file:" + jettyXml);
            isFile = true;

        } else {

            // Check the classpath.
            jettyXmlUrl = classLoader.getResource(jettyXml);

            if(jettyXmlUrl == null)
            {
            	jettyXmlUrl = classLoader.getResource("/" + jettyXml);
            }

            isClassPath = true;

        }
        

		// See BLZG-1447
		// Check if it is running as a test suite in Eclipse or Maven.
		// If it is running as maven surefire execution the target/bigdata.war
		// will exist.

		final File warFile = new File("target/bigdata.war");

		if (jettyXmlUrl == null && warFile.exists()) {
			// This is the maven surefire plugin case.
			jettyXmlUrl = classLoader.getResource("jetty/jettyMavenTest.xml");
		} else if (jettyXmlUrl == null) {
			jettyXmlUrl = classLoader.getResource("jetty/jettyEclipseTest.xml");
		} 
	
		//If we still didn't get it, we may be running the executable jar.
		if (jettyXmlUrl == null) { 	// This is the executable jar a file
									// reference into the jar
			jettyXmlUrl = classLoader.getResource("jetty.xml");
			isFile = true;
		}
        
        if (jettyXmlUrl == null) {

        	
        	if(jettyXmlUrl == null)

        		throw new RuntimeException("Not found: " + jettyXml);

        }

        if (log.isInfoEnabled())
            log.info("jetty configuration: jettyXml=" + jettyXml + ", isFile="
                    + isFile + ", isClassPath=" + isClassPath
                    + ", jettyXmlUrl=" + jettyXmlUrl);
     
        return jettyXmlUrl;
        
    }

    /**
     * Search (a) the local file system; and (b) the classpath for the web
     * application. If the resource is located, then set the
     * [jetty.resourceBase] property. This search sequence gives preference to
     * the local file system and then searches the classpath (which jetty does
     * not known how to do by itself.)
     * 
     * @throws MalformedURLException
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/939" > NSS does not start
     *      from command line: bigdata-war/src not found </a>
     */
    private static void configureEffectiveResourceBase(
            final ClassLoader classLoader) throws MalformedURLException {

        // Check the environment variable.
        String resourceBaseStr = System
                .getProperty(SystemProperties.JETTY_RESOURCE_BASE);
        
        //Try JETTY_HOME if the Resource Base is null
        if(resourceBaseStr == null ) {
        	   resourceBaseStr = System
                       .getProperty(SystemProperties.JETTY_HOME);
        }

        // Check the environment variable for the override web.
        final String jettyOverrideWeb = System
                .getProperty(SystemProperties.JETTY_OVERRIDE_WEB_XML);

        // true iff declared as an environment variable.
        final boolean isDeclared = resourceBaseStr != null
                && resourceBaseStr.trim().length() > 0;
        boolean isFile = false; // iff found in local file system.
        boolean isClassPath = false; // iff found on classpath.

        if (!isDeclared) {

            /*
             * jetty.resourceBase not declared in the environment.
             */

            // The default location to check in the file system.
            final File file = new File("bigdata-war-html/src/main/webapp/");

            final URL resourceBaseURL;
            if (file.exists()) {

                // Check the file system. See #1108
//                resourceBaseURL = new URL("file:" + file.getAbsolutePath());
                resourceBaseURL = file.toURI().toURL();
                isFile = true;
                
            } else {

                /*
                 * Check the classpath.
                 * 
                 * Note: When checking the classpath we need to test different
                 * resources depending on whether we are running under the
                 * eclipse IDE or at the command line!
                 */
                URL tmp = null;
                String src = null;
                if (tmp == null) {
                    /**
                     * Eclipse IDE class path.
                     * 
                     * Note: This is what gets found when running under eclipse.
                     * The URL will be in the configured build directory for the
                     * eclipse project. So, something like:
                     * 
                     * <pre>
                     * file:/Users/bryan/Documents/workspace/BIGDATA_RELEASE_1_3_0_NEW_SVN/bin/WEB-INF/web.xml
                     * </pre>
                     */
                    tmp = ClassLoader.getSystemClassLoader().getResource(
                            src = "bigdata-war-html/src/main/webapp/WEB-INF/web.xml");
                }
                
//                if (tmp == null)// Eclipse IDE class path (system class loader).
//                    tmp = ClassLoader.getSystemClassLoader().getResource(
//                            src = "WEB-INF/web.xml");
//                if (tmp == null)
//                    tmp = classLoader // JAR class path.
//                            .getResource(src = "/bigdata-war/src/WEB-INF/web.xml");
                if (tmp == null) {
                    /**
                     * JAR class path (system class loader).
                     * 
                     * Note: This is what gets located when we run from the
                     * command line (outside of eclipse). The resulting JAR URL
                     * will be something like:
                     * 
                     * <pre>
                     * jar:file:/Users/bryan/Documents/workspace/BIGDATA_RELEASE_1_3_0_NEW_SVN/ant-build/lib/bigdata-1.3.0-20140517.jar!/bigdata-war/src/WEB-INF/web.xml
                     * </pre>
                     */
                    tmp = ClassLoader.getSystemClassLoader().getResource(
                            src = "war/src/main/webapp/WEB-INF/web.xml");
                    
                    
                }
                
                if (tmp != null) {
                    if (src != null) {
                        if(log.isInfoEnabled())
                            log.info("Found: src=" + src + ", url=" + tmp);
                    }
                   	resourceBaseURL = new URL(trimURISubstring(tmp,"WEB-INF/web.xml"));
                } else {
					resourceBaseURL = null;
                }
                isClassPath = resourceBaseURL != null;

            }


            if (resourceBaseURL != null) {

                /*
                 * We found the resource either in the file system or in the
                 * classpath.
                 * 
                 * Explicitly set the discovered value on the jetty.resourceBase
                 * property. This will cause jetty to use the version of that
                 * resource that we discovered above.
                 * 
                 * Note: If we did not find the resource, then the default value
                 * from the jetty.xml SystemProperty expression will be used by
                 * jetty. If it can not find a resource using that default
                 * value, then the startup will fail. We leave this final check
                 * to jetty itself since it will interpret the jetty.xml file
                 * itself.
                 */
                resourceBaseStr = resourceBaseURL.toExternalForm();

                System.setProperty(SystemProperties.JETTY_RESOURCE_BASE,
                        resourceBaseStr);
                
            }

        }

        //Don't override the value if it is explicitly declared.
        //If we have a resource base, but not a declared jetty override
        //use the WEB-INF/override-web.xml as the default.
		if (resourceBaseStr != null && jettyOverrideWeb == null) {
					final URL overrideWebXmlURL = new URL(resourceBaseStr
							+ (resourceBaseStr.endsWith("/") ? "" : "/")
							+ "WEB-INF/override-web.xml");

					System.setProperty(SystemProperties.JETTY_OVERRIDE_WEB_XML,
							overrideWebXmlURL.toExternalForm());
        }

        if (log.isInfoEnabled())
            log.info("jetty configuration"//
                    + ": resourceBaseStr=" + resourceBaseStr
                    + ", isDeclared="
                    + isDeclared + ", isFile=" + isFile
                    + ", isClassPath="
                    + isClassPath
                    + ", jetty.resourceBase(effective)="
                    + System.getProperty(SystemProperties.JETTY_RESOURCE_BASE)
                    + ", jetty.overrideWebXml(effective)="
                    + System.getProperty(SystemProperties.JETTY_OVERRIDE_WEB_XML));
        
    }
    
    /**
     * Convenience method to prune last substring occurance from URL.
     * @param src
     * @param sub
     * @return
     */
    final static String trimURISubstring(URL src, String sub) {
    	final String s = src.toExternalForm();
        final int endIndex = s.lastIndexOf(sub);
        final String t = s.substring(0, endIndex);
        
        return t;
    	
    }
    
    
    /**
     * Configure the webapp (overrides, IIndexManager, etc.)
     * <p>
     * Note: These overrides are achieved by setting the {@link WebAppContext}
     * attribute named
     * {@link BigdataRDFServletContextListener#INIT_PARAM_OVERRIDES}. The
     * {@link BigdataRDFServletContextListener} then consults the attribute when
     * reporting the effective value of the init-params. This convoluted
     * mechanism is required because you can not otherwise override the
     * init-params without editing <code>web.xml</code>.
     */
    private static void configureWebAppOverrides(//
            final Server server,//
            final IIndexManager indexManager,//
            final Map<String, String> initParams//
    ) {

        final WebAppContext wac = getWebApp(server);

        if (wac == null) {

            /*
             * This is a fatal error. If we can not set the IIndexManager, the
             * NSS will try to interpret the propertyFile in web.xml rather than
             * using the one that is already open and specified by the caller.
             * Among other things, that breaks the HAJournalServer startup.
             */

            throw new RuntimeException("Could not locate "
                    + WebAppContext.class.getName());

        }

        /*
         * Force the use of the caller's IIndexManager. This is how we get the
         * NSS to use the already open Journal for the HAJournalServer.
         */
        if (indexManager != null) {

            // Set the IIndexManager attribute on the WebAppContext.
            wac.setAttribute(IIndexManager.class.getName(), indexManager);

        }

        /*
         * Note: You simply can not override the init parameters specified in
         * web.xml. Therefore, this sets the overrides on an attribute. The
         * attribute is then consulted when the web app starts and its the
         * override values are used if given.
         */
        if (initParams != null) {

            wac.setAttribute(
                    BigdataRDFServletContextListener.INIT_PARAM_OVERRIDES,
                    initParams);

        }

    }

    /**
     * Return the {@link WebAppContext} for the {@link Server}.
     * 
     * @param server
     *            The {@link Server}.
     *            
     * @return The {@link WebAppContext} associated with the bigdata webapp.
     */
    public static WebAppContext getWebApp(final Server server) {

        final WebAppContext wac = server
                .getChildHandlerByClass(WebAppContext.class);

        /*
         * Note: This assumes that this is the webapp for bigdata. If there are
         * multiple webapps then this assumption is no longer valid and things
         * will break.
         */
 
        return wac;

    }
    
    /**
     * Best effort attempt to return the port at which the local jetty
     * {@link Server} is receiving http connections.
     * 
     * @param server
     *            The server.
     *            
     * @return The local port.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     */
    public static int getLocalPort(final Server server) {

        if (server == null)
            throw new IllegalArgumentException();

        final Connector[] a = server.getConnectors();
        
        if (a.length == 0) {
         
            /*
             * This will happen if there are no configured connectors in
             * jetty.xml, jetty-http.xml, etc. 
             */
            
            throw new IllegalStateException("No connectors?");
            
        }
        
        return ((ServerConnector) a[0]).getLocalPort();

    }
    
    /**
     * Print the optional message on stderr, print the usage information on
     * stderr, and then force the program to exit with the given status code.
     * 
     * @param status
     *            The status code.
     * @param msg
     *            The optional message
     */
    protected static void usage(final int status, final String msg) {

        if (msg != null) {

            System.err.println(msg);

        }

        System.err
                .println("[options] port namespace (propertyFile|configFile)");

        System.exit(status);

    }

}
