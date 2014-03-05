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
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.webapp.WebAppContext;
import org.eclipse.jetty.xml.XmlConfiguration;

import com.bigdata.Banner;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TimestampUtility;
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
 */
public class NanoSparqlServer {
    
    static private final Logger log = Logger.getLogger(NanoSparqlServer.class);

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

        // Create the service.
        final Server server = NanoSparqlServer.newInstance(port, propertyFile,
                initParams);

        // Start the service.
        server.start();

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

            final int actualPort = server.getConnectors()[0].getLocalPort();

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
     * Variant used when you already have the {@link IIndexManager} on hand and
     * DO NOT want to use <code>web.xml</code> and <code>jetty.xml</code>. For
     * this case, the caller must specify the port and a default connection will
     * be established at that port. This form is used by code that wants to
     * embed a simple NSS end point.
     * 
     * @param port
     *            The port on which the service will run -OR- ZERO (0) for any
     *            open port.
     * @param indexManager
     *            The {@link IIndexManager}.
     * @param initParams
     *            Initialization parameters for the web application as specified
     *            by {@link ConfigParams}.
     * 
     * @return The server instance.
     * 
     * @see <a href="http://wiki.eclipse.org/Jetty/Tutorial/Embedding_Jetty">
     *      Embedding Jetty </a>
     */
    static public Server newInstance(final int port,
            final IIndexManager indexManager,
            final Map<String, String> initParams) throws Exception {

        final Server server = new Server(port);

        final ServletContextHandler context = getContextHandler(//server,
                initParams);

        // Force the use of the caller's IIndexManager.
        context.setAttribute(IIndexManager.class.getName(), indexManager);
        
        final HandlerList handlers = new HandlerList();

        final ResourceHandler resourceHandler = new ResourceHandler();
        
        setupStaticResources(NanoSparqlServer.class.getClassLoader(),
                resourceHandler);

        handlers.setHandlers(new Handler[] {
                context,//
                resourceHandler,//
//                new DefaultHandler()//
                });

        server.setHandler(handlers);

        return server;
        
    }

    /**
     * Variant used when you already have the {@link IIndexManager} on hand and
     * want to use <code>web.xml</code> to configure the {@link WebAppContext}
     * and <code>jetty.xml</code> to configure the jetty {@link Server}.
     * 
     * @param jettyXml
     *            The <code>jetty.xml</code> file that will be used to configure
     *            jetty. 
     * @param indexManager
     *            The {@link IIndexManager}.
     * 
     * @return The server instance.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/730" >
     *      Allow configuration of embedded NSS jetty server using jetty-web.xml
     *      </a>
     */
    static public Server newInstance(final String jettyXml,
            final IIndexManager indexManager) throws Exception {

        final ClassLoader classLoader = indexManager.getClass()
                .getClassLoader();
        
        final Server server;
        {

            // Locate jetty.xml.
            final URL jettyXmlUrl;
            if (new File(jettyXml).exists()) {

                jettyXmlUrl = new URL("file://" + jettyXml);

            } else {

                jettyXmlUrl = getStaticResourceURL(classLoader, jettyXml);

            }

            if (jettyXmlUrl == null) {

                throw new RuntimeException("Not found: " + jettyXml);

            }
            
            if (log.isInfoEnabled())
                log.info("jetty configuration: jettyXml, jettyXmlUrl="
                        + jettyXmlUrl);

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
         * Force the use of the caller's IIndexManager. This is how we get the
         * NSS to use the already open Journal for the HAJournalServer.
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

            // Set the IIndexManager attribute on the WebAppContext.
            wac.setAttribute(IIndexManager.class.getName(), indexManager);
            
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
    
    /**
     * Variant used when the life cycle of the {@link IIndexManager} will be
     * managed by the server - this form is used by {@link #main(String[])}.
     * <p>
     * Note: This is mostly a convenience for scripts that do not need to take
     * over the detailed control of the jetty container and the bigdata webapp.
     * 
     * @param port
     *            The port on which the service will run -OR- ZERO (0) for any
     *            open port.
     * @param propertyFile
     *            The <code>.properties</code> file (for a standalone database
     *            instance) or the <code>.config</code> file (for a federation).
     * @param initParams
     *            Initialization parameters for the web application as specified
     *            by {@link ConfigParams}.
     * 
     * @return The server instance.
     */
    static public Server newInstance(final int port, final String propertyFile,
            final Map<String, String> initParams) throws Exception {

        final Server server = new Server(port);

        final ServletContextHandler context = getContextHandler(//server,
                initParams);

        final HandlerList handlers = new HandlerList();

        final ResourceHandler resourceHandler = new ResourceHandler();
        
        setupStaticResources(NanoSparqlServer.class.getClassLoader(),
                resourceHandler);

        /**
         * Note: There appear to be plenty of ways to setup JSP support for
         * embedded jetty, and plenty of ways to get it wrong. I wound up adding
         * to the classpath the following jars for jetty 7.2.2 to get this
         * running:
         * 
         * <pre>
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
         * With those jars on the class path, the following code will run
         * JSP pages.
         * 
         * Note: In order for this to work, it must also be supported in the
         * alternative newInstance() method above.
         */
        handlers.setHandlers(new Handler[] {//
            context,//
            resourceHandler,//
        });

        server.setHandler(handlers);
        
        return server;
    }

    /**
     * Construct a {@link ServletContextHandler}.
     * <p>
     * Note: The {@link ContextHandler} uses the longest prefix of the request
     * URI (the contextPath) to select a specific {@link Handler}.
     * <p>
     * Note: If you are using <code>web.xml</code>, then all of this stuff is
     * done there instead.
     * 
     * @param initParams
     *            The init parameters, per the web.xml definition.
     */
    static private ServletContextHandler getContextHandler(
//            final Server server,
            final Map<String, String> initParams) throws Exception {

        if (initParams == null)
            throw new IllegalArgumentException();
        
        final ServletContextHandler context = new ServletContextHandler(
                ServletContextHandler.NO_SECURITY
//                        | ServletContextHandler.NO_SESSIONS
                        );

//        /*
//         * Setup resolution for the static web app resources (index.html).
//         */
//        setupStaticResources(server, context);
        
        /*
         * Register a listener which will handle the life cycle events for the
         * ServletContext.
         */
        {

            String className = initParams
                    .get(ConfigParams.SERVLET_CONTEXT_LISTENER_CLASS);

            if (className == null)
                className = ConfigParams.DEFAULT_SERVLET_CONTEXT_LISTENER_CLASS;

            final Class<BigdataRDFServletContextListener> cls = (Class<BigdataRDFServletContextListener>) Class
                    .forName(className);

            if (!BigdataRDFServletContextListener.class.isAssignableFrom(cls)) {
            
                throw new RuntimeException("Invalid option: "
                        + ConfigParams.SERVLET_CONTEXT_LISTENER_CLASS + "="
                        + className + ":: Class does not extend "
                        + BigdataRDFServletContextListener.class);

            }

            final BigdataRDFServletContextListener listener = cls.newInstance();

            context.addEventListener(listener);

        }

        /*
         * Set the servlet context properties.
         */
        for (Map.Entry<String, String> e : initParams.entrySet()) {

            context.setInitParameter(e.getKey(), e.getValue());
            
        }

        // Performance counters.
        context.addServlet(new ServletHolder(new CountersServlet()),
                "/counters");

        // Status page.
        context.addServlet(new ServletHolder(new StatusServlet()), "/status");

        // Core RDF REST API, including SPARQL query and update.
        context.addServlet(new ServletHolder(new RESTServlet()), "/sparql/*");

        // Multi-Tenancy API.
        context.addServlet(new ServletHolder(new MultiTenancyServlet()),
                "/namespace/*");
        
        /**
         * Note: JSP pages for the servlet 2.5 specification add the following
         * dependencies:
         * 
         * <pre>
         *     ant-1.6.5.jar
         *     core-3.1.1.jar
         *     jsp-2.1.jar
         *     jsp-api-2.1.jar
         * </pre>
         * 
         * @see http://docs.codehaus.org/display/JETTY/Embedding+Jetty
         */

//        context.setResourceBase("bigdata-war/src/html");
//        
//        context.setWelcomeFiles(new String[]{"index.html"});
        
        return context;
        
    }

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

    /**
     * Setup access to the welcome page (index.html).
     */
    private static void setupStaticResources(final ClassLoader classLoader,
            final ResourceHandler context) {

        context.setDirectoriesListed(false); // Nope!

        final String file = "index.html";

        final URL url = getStaticResourceURL(classLoader, file);

        if (url == null)
            throw new RuntimeException("Could not locate file: " + file);

        /*
         * We have located the resource. Set it as the resource base from which
         * static content will be served.
         */
        final String indexHtml = url.toExternalForm();

        final String webDir = indexHtml.substring(0,
                indexHtml.length() - file.length());

        context.setResourceBase(webDir);

        context.setWelcomeFiles(new String[]{"index.html"});

    }

    /**
     * Return the URL for the static web app resources (for example,
     * <code>index.html</code>).
     * 
     * @param classLoader
     *            The {@link ClassLoader} that will be used to locate the
     *            resource (required).
     * @param path
     *            The path for the resource (required)
     * 
     * @return The URL for the web app resource directory -or- <code>null</code>
     *         if it could not be found on the class path.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/330
     */
    private static URL getStaticResourceURL(final ClassLoader classLoader,
            final String path) {

        if (classLoader == null)
            throw new IllegalArgumentException();

        if (path == null)
            throw new IllegalArgumentException();
        
        /*
         * This is the resource path in the JAR.
         */
        final String WEB_DIR_JAR = "bigdata-war/src"
                + (path == null ? "" : "/" + path);

        /*
         * This is the resource path in the IDE when NOT using the JAR.
         * 
         * Note: You MUST have "bigdata-war/src" on the build path for the IDE.
         */
        final String WEB_DIR_IDE = path; // "html";

        URL url = classLoader.getResource(WEB_DIR_JAR);

        if (url == null && path != null) {

            url = classLoader.getResource(path);// "html");

        }

        if (url == null) {

            log.error("Could not locate: " + WEB_DIR_JAR + ", " + WEB_DIR_IDE
                    + ", -or- " + path);
        }

        return url;

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
