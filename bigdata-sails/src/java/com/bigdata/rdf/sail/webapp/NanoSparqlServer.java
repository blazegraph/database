/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;

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
 *       also use a full transaction.
 * 
 * @todo Remote command to advance the read-behind point. This will let people
 *       bulk load a bunch of stuff before advancing queries to read from the
 *       new consistent commit point.
 * 
 * @todo Note: There appear to be plenty of ways to setup JSP support for
 *       embedded jetty, and plenty of ways to get it wrong. I wound up adding
 *       to the classpath the following jars for jetty 7.2.2 to get this
 *       running:
 * 
 *       <pre>
 * com.sun.el_1.0.0.v201004190952.jar
 * ecj-3.6.jar
 * javax.el_2.1.0.v201004190952.jar
 * javax.servlet.jsp.jstl_1.2.0.v201004190952.jar
 * javax.servlet.jsp_2.1.0.v201004190952.jar
 * jetty-jsp-2.1-7.2.2.v20101205.jar
 * org.apache.jasper.glassfish_2.1.0.v201007080150.jar
 * org.apache.taglibs.standard.glassfish_1.2.0.v201004190952.jar
 * </pre>
 * 
 *       Note: JSP pages for the servlet 2.5 specification add the following
 *       dependencies:
 * 
 *       <pre>
 *     ant-1.6.5.jar
 *     core-3.1.1.jar
 *     jsp-2.1.jar
 *     jsp-api-2.1.jar
 * </pre>
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
     *            * The default will locate the <code>jetty.xml</code> resource
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
        String servletContextListenerClass = ConfigParams.DEFAULT_SERVLET_CONTEXT_LISTENER_CLASS;
        
        /*
         * Note: This default will locate the jetty.xml resource that is bundled
         * with the JAR. This preserves the historical behavior. If you want to
         * use a different jetty.xml file, just override this property on the
         * command line.
         */
        String jettyXml = "bigdata-war/src/jetty.xml";

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
                } else if (arg.equals("-servletContextListenerClass")) {
                    servletContextListenerClass = args[++i];
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
        
        initParams.put(ConfigParams.SERVLET_CONTEXT_LISTENER_CLASS,
                servletContextListenerClass);

        final Server server;

        boolean ok = false;
        try {
            // Create the service.
            server = NanoSparqlServer.newInstance(port, jettyXml,
                    null/* indexManager */, initParams);
            // Start Server.
            server.start();
            // Await running.
            while (server.isStarting() && !server.isRunning()) {
                Thread.sleep(100/* ms */);
            }
            ok = true;
        } finally {
            if (!ok) {
                // Complain if Server did not start.
                System.err.println("Server did not start.");
            }
        }

        /*
         * Report *an* effective URL of this service.
         * 
         * Note: This is an effective local URL (and only one of them, and
         * even then only one for the first connector). It does not reflect
         * any knowledge about the desired external deployment URL for the
         * service end point.
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

            System.out.println("serviceURL: " + serviceURL);

        }
        
        // Wait for the service to terminate.
        server.join();

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
        
//        final Server server = new Server(port);
//
//        final ServletContextHandler context = getContextHandler(initParams);
//
//        final ResourceHandler resourceHandler = new ResourceHandler();
//        
//        setupStaticResources(NanoSparqlServer.class.getClassLoader(),
//                resourceHandler);
//        
//        // same resource base.
//        context.setResourceBase(resourceHandler.getResourceBase());
//        context.setWelcomeFiles(resourceHandler.getWelcomeFiles());
//
//        final HandlerList handlers = new HandlerList();
//
//        handlers.setHandlers(new Handler[] {
//                context,// maps servlets
//                resourceHandler,// maps welcome files.
//                new DefaultHandler() // responsible for anything not explicitly served.
//                });
//
//        server.setHandler(handlers);
//        
//        // Force the use of the caller's IIndexManager.
//        context.setAttribute(IIndexManager.class.getName(), indexManager);
//        
//        return server;
        
    }
    
//    /**
//     * Variant used when the life cycle of the {@link IIndexManager} will be
//     * managed by the server - this form is used by {@link #main(String[])}.
//     * <p>
//     * Note: This is mostly a convenience for scripts that do not need to take
//     * over the detailed control of the jetty container and the bigdata webapp.
//     * 
//     * @param port
//     *            The port on which the service will run -OR- ZERO (0) for any
//     *            open port.
//     * @param propertyFile
//     *            The <code>.properties</code> file (for a standalone database
//     *            instance) or the <code>.config</code> file (for a federation).
//     * @param initParams
//     *            Initialization parameters for the web application as specified
//     *            by {@link ConfigParams}.
//     * 
//     * @return The server instance.
//     */
//    static public Server newInstance(final int port, final String propertyFileIsNotUsed,
//            final Map<String, String> initParams) throws Exception {
//
//        final Server server = new Server(port);
//
//        final ServletContextHandler context = getContextHandler(initParams);
//
//        final ResourceHandler resourceHandler = new ResourceHandler();
//        
//        setupStaticResources(NanoSparqlServer.class.getClassLoader(),
//                resourceHandler);
//
//        // same resource base.
//        context.setResourceBase(resourceHandler.getResourceBase());
//        context.setWelcomeFiles(resourceHandler.getWelcomeFiles());
//        
//        final HandlerList handlers = new HandlerList();
//
//        handlers.setHandlers(new Handler[] {//
//            context,// maps servlets
//            resourceHandler,// maps welcome files.
//            new DefaultHandler() // responsible for anything not explicitly served.
//        });
//
//        server.setHandler(handlers);
//        
//        return server;
//    }

    /**
     * Variant used when you already have the {@link IIndexManager} on hand and
     * want to use <code>web.xml</code> to configure the {@link WebAppContext}
     * and <code>jetty.xml</code> to configure the jetty {@link Server}.
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
    static public Server newInstance(final String jettyXml,
            final IIndexManager indexManager,
            final Map<String, String> initParams) throws Exception {

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
         * can override the location of the jetty.xml file if they need to
         * change the way in which either jetty or the webapp are configured.
         * You can also override many of the properties in the jetty.xml file
         * using environment variables.
         */
        final Server server;
        {

            // Locate jetty.xml.
            final URL jettyXmlUrl;
            if (new File(jettyXml).exists()) {

                // Check the file system.
//                jettyXmlUrl = new File(jettyXml).toURI();
                jettyXmlUrl = new URL("file:" + jettyXml);

            } else {

                // Check the classpath.
                jettyXmlUrl = classLoader.getResource(jettyXml);
//                jettyXmlUrl = classLoader.getResource("bigdata-war/src/jetty.xml");

            }

            if (jettyXmlUrl == null) {

                throw new RuntimeException("Not found: " + jettyXml);

            }
            
            if (log.isInfoEnabled())
                log.info("jetty configuration: jettyXml=" + jettyXml
                        + ", jettyXmlUrl=" + jettyXmlUrl);

            // Build configuration from that resource.
            final XmlConfiguration configuration;
            {
                // Open jetty.xml resource.
                final Resource jettyConfig = Resource.newResource(jettyXmlUrl);
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
            
            // Configure the jetty server.
            server = (Server) configuration.configure();

        }

        /*
         * Configure the webapp (overrides, IIndexManager, etc.) 
         */
        {

            final WebAppContext wac = getWebApp(server);

            if (wac == null) {

                /*
                 * This is a fatal error. If we can not set the IIndexManager,
                 * the NSS will try to interpret the propertyFile in web.xml
                 * rather than using the one that is already open and specified
                 * by the caller. Among other things, that breaks the
                 * HAJournalServer startup.
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
             * Note: You simply can not override the init parameters specified
             * in web.xml. Therefore, this sets the overrides on an attribute.
             * The attribute is then consulted when the web app starts and its
             * the override values are used if given.
             */
            if (initParams != null) {

                wac.setAttribute(BigdataRDFServletContextListener.INIT_PARAM_OVERRIDES, initParams);

            }
            
        }

        return server;
        
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
    
//    /**
//     * Construct a {@link ServletContextHandler}.
//     * <p>
//     * Note: The {@link ContextHandler} uses the longest prefix of the request
//     * URI (the contextPath) to select a specific {@link Handler}.
//     * <p>
//     * Note: If you are using <code>web.xml</code>, then all of this stuff is
//     * done there instead.
//     * 
//     * @param initParams
//     *            The init parameters, per the web.xml definition.
//     */
//    static private ServletContextHandler getContextHandler(
//            final Map<String, String> initParams) throws Exception {
//
//        if (initParams == null)
//            throw new IllegalArgumentException();
//        
//        final ServletContextHandler context = new ServletContextHandler(
//                ServletContextHandler.NO_SECURITY
////                        | ServletContextHandler.NO_SESSIONS
//                        );
//
//        // Path to the webapp.
//        context.setContextPath(BigdataStatics.getContextPath());
//
//        /*
//         * Register a listener which will handle the life cycle events for the
//         * ServletContext.
//         */
//        {
//
//            String className = initParams
//                    .get(ConfigParams.SERVLET_CONTEXT_LISTENER_CLASS);
//
//            if (className == null)
//                className = ConfigParams.DEFAULT_SERVLET_CONTEXT_LISTENER_CLASS;
//
//            final Class<BigdataRDFServletContextListener> cls = (Class<BigdataRDFServletContextListener>) Class
//                    .forName(className);
//
//            if (!BigdataRDFServletContextListener.class.isAssignableFrom(cls)) {
//            
//                throw new RuntimeException("Invalid option: "
//                        + ConfigParams.SERVLET_CONTEXT_LISTENER_CLASS + "="
//                        + className + ":: Class does not extend "
//                        + BigdataRDFServletContextListener.class);
//
//            }
//
//            final BigdataRDFServletContextListener listener = cls.newInstance();
//
//            context.addEventListener(listener);
//
//        }
//
//        /*
//         * Set the servlet context properties.
//         */
//        for (Map.Entry<String, String> e : initParams.entrySet()) {
//
//            context.setInitParameter(e.getKey(), e.getValue());
//            
//        }
//
//        // html directory.
//        context.addServlet(new ServletHolder(new DefaultServlet()),
//                "/html/*");
//        
//        // Appears necessary for http://localhost:8080/bigdata to bring up index.html.
//        context.addServlet(new ServletHolder(new DefaultServlet()),
//                "/");
//
//        // Performance counters.
//        context.addServlet(new ServletHolder(new CountersServlet()),
//                "/counters");
//
//        // Status page.
//        context.addServlet(new ServletHolder(new StatusServlet()), "/status");
//
//        // Core RDF REST API, including SPARQL query and update.
//        context.addServlet(new ServletHolder(new RESTServlet()), "/sparql/*");
//
//        // Multi-Tenancy API.
//        context.addServlet(new ServletHolder(new MultiTenancyServlet()),
//                "/namespace/*");
//
//        // Incremental truth maintenance
//        context.addServlet(new ServletHolder(new InferenceServlet()),
//        		"/inference");
//        
////        context.setResourceBase("bigdata-war/src/html");
////        
////        context.setWelcomeFiles(new String[]{"index.html"});
//        
//        return context;
//        
//    }

//    /**
//     * Setup access to the web app resources, especially index.html.
//     * 
//     * @see https://sourceforge.net/apps/trac/bigdata/ticket/330
//     * 
//     * @param classLoader
//     * @param context
//     */
//    private static void setupStaticResources(final ClassLoader classLoader,
//            final ServletContextHandler context) {
//
//        final URL url = getStaticResourceURL(classLoader, "html");
//
//        if (url != null) {
//
//            /*
//             * We have located the resource. Set it as the resource base from
//             * which static content will be served.
//             */
//
//            final String webDir = url.toExternalForm();
//
//            context.setResourceBase(webDir);
//
//            context.setContextPath("/");
//
//        }
//
//    }
//
//    /**
//     * Setup access to the welcome page (index.html).
//     */
//    private static void setupStaticResources(final ClassLoader classLoader,
//            final ResourceHandler context) {
//
//        context.setDirectoriesListed(false); // Nope!
//
//        final String file = "html/index.html";
//
//        final URL url;
//        {
//
//            URL tmp = null;
//            
//            if (tmp == null) {
//
////                // project local file system path.
////                classLoader.getResource("bigdata-war/src/html/" + file);
//
//            }
//
//            if (tmp == null) {
//
//                // Check the classpath.
//                tmp = classLoader.getResource(file);
//
//            }
//
//            url = tmp;
//            
//            if (url == null) {
//
//                throw new RuntimeException("Could not locate index.html");
//
//            }
//
//        }
//
//        /*
//         * We have located the resource. Set it as the resource base from which
//         * static content will be served.
//         */
//        final String indexHtmlUrl = url.toExternalForm();
//
//        final String webDir = indexHtmlUrl.substring(0, indexHtmlUrl.length()
//                - file.length());
//
//        // Path to the content in the local file system or JAR.
//        context.setResourceBase(webDir);
//        
//        /*
//         * Note: replace with "new.html" for the new UX. Also change in
//         * web.xml.
//         */
//        context.setWelcomeFiles(new String[]{"html/index.html"});
//
//        if (log.isInfoEnabled())
//            log.info("\nindex.html: =" + indexHtmlUrl + "\nresourceBase="
//                    + webDir);
//
//    }

//    /**
//     * Return the URL for the static web app resources (for example,
//     * <code>index.html</code>).
//     * 
//     * @param classLoader
//     *            The {@link ClassLoader} that will be used to locate the
//     *            resource (required).
//     * @param path
//     *            The path for the resource (required)
//     * 
//     * @return The URL for the web app resource directory -or- <code>null</code>
//     *         if it could not be found on the class path.
//     * 
//     * @see https://sourceforge.net/apps/trac/bigdata/ticket/330
//     */
//    private static URL getStaticResourceURL(final ClassLoader classLoader,
//            final String path) {
//
//        if (classLoader == null)
//            throw new IllegalArgumentException();
//
//        if (path == null)
//            throw new IllegalArgumentException();
//        
//        /*
//         * This is the resource path in the JAR.
//         */
//        final String WEB_DIR_JAR = "bigdata-war/src/html"
//                + (path == null ? "" : "/" + path);
//
//        /*
//         * This is the resource path in the IDE when NOT using the JAR.
//         * 
//         * Note: You MUST have "bigdata-war/src" on the build path for the IDE.
//         */
//        final String WEB_DIR_IDE = "html/" + path; // "html";
//
//        URL url = classLoader.getResource(WEB_DIR_JAR);
//
//        if (url == null && path != null) {
//
//            url = classLoader.getResource(WEB_DIR_IDE);// "html");
//
//        }
//
//        if (url == null) {
//
//            log.error("Could not locate: " + WEB_DIR_JAR + ", " + WEB_DIR_IDE
//                    + ", -or- " + path);
//        }
//
//        return url;
//
//    }

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
    private static void usage(final int status, final String msg) {

        if (msg != null) {

            System.err.println(msg);

        }

        System.err
                .println("[options] port namespace (propertyFile|configFile)");

        System.exit(status);

    }

}
