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
/*
 * Created on Apr 13, 2011
 */

package com.bigdata.rdf.sail.webapp;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.log4j.Logger;

import com.bigdata.Banner;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.btree.BaseIndexStats;
import com.bigdata.cache.SynchronizedHardReferenceQueueWithTimeout;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSetAccess;
import com.bigdata.counters.IProcessCounters;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITransactionService;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.WarmUpTask;
import com.bigdata.rdf.ServiceProviderHook;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.CreateKBTask;
import com.bigdata.rdf.sparql.ast.service.ServiceRegistry;
import com.bigdata.rdf.task.AbstractApiTask;
import com.bigdata.service.AbstractDistributedFederation;
import com.bigdata.service.AbstractScaleOutClient;
import com.bigdata.service.DefaultClientDelegate;
import com.bigdata.service.ScaleOutClientFactory;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.util.httpd.AbstractHTTPD;

/**
 * Listener provides life cycle management of the {@link IIndexManager} by
 * interpreting the configuration parameters in the {@link ServletContext}.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class BigdataRDFServletContextListener implements
        ServletContextListener {

    private static final transient Logger log = Logger
            .getLogger(BigdataRDFServletContextListener.class);

    private Journal jnl = null;
    private AbstractScaleOutClient<?> jiniClient = null;
    private ITransactionService txs = null;
    private Long readLock = null;
    private long readLockTx;
    private BigdataRDFContext rdfContext;
//    private SparqlCache sparqlCache;

    /**
     * The set of init parameters from the <code>web.xml</code> file after we
     * have applied any overrides specified by the
     * {@link BigdataRDFServletContextListener#INIT_PARAM_OVERRIDES} attributes.
     */
    private Map<String,String> effectiveInitParams;

    /**
     * <code>true</code> iff this class opened the {@link IIndexManager}, in
     * which case it will close it at the appropriate life cycle event.
     */
    private boolean closeIndexManager;

    /**
     * The name of the {@link ServletContext} attribute under which we store
     * any overrides for the init parameters of the {@link ServletContext}. Note
     * that it is NOT possible to actual modify the init parameters specified in
     * the <code>web.xml</code> file. Therefore, we attach the overrides as an
     * attribute and then consult them from within
     * {@link BigdataRDFServletContextListener#contextInitialized(javax.servlet.ServletContextEvent)}
     * .
     */
    public static final String INIT_PARAM_OVERRIDES = "INIT_PARAMS_OVERRIDES";

    /**
     * Return the effective value of the given init parameter, respecting any
     * overrides that were specified to the {@link NanoSparqlServer} when it
     * initialized the server.
     *
     * @param key
     *            The name of the desired init parameter.
     *
     * @return The effective value of that init parameter.
     */
    protected String getInitParameter(final String key) {

        return effectiveInitParams.get(key);

    }

    public BigdataRDFServletContextListener() {
        super();
    }

    protected BigdataRDFContext getBigdataRDFContext() {

        return rdfContext;

    }

    @Override
    public void contextInitialized(final ServletContextEvent e) {

        if(log.isInfoEnabled())
            log.info("");

        Banner.banner();

        final ServletContext context = e.getServletContext();

        /*
         * Figure out the effective init parameters that we want to use
         * for this initialization procedure.
         */
        {

            effectiveInitParams = new LinkedHashMap<String, String>();

            /*
             * Copy the init params from web.xml into a local map.
             */
            final Enumeration<String> names = context.getInitParameterNames();
            while(names.hasMoreElements()) {
                final String name = names.nextElement();
                final String value = context.getInitParameter(name);
                effectiveInitParams.put(name, value);
            }

            /*
             * Look for init parameter overrides that have been attached to the
             * WebAppContext by the NanoSparqlServer. If found, then apply them
             * before doing anything else. This is how we apply overrides to the
             * init parameters that were specified in "web.xml".
             */
            {

                @SuppressWarnings("unchecked")
                final Map<String, String> initParamOverrides = (Map<String, String>) context
                        .getAttribute(BigdataRDFServletContextListener.INIT_PARAM_OVERRIDES);

                if (initParamOverrides != null) {

                    effectiveInitParams.putAll(initParamOverrides);

                }

            }

        }

        final String namespace;
        {

            String s = getInitParameter(ConfigParams.NAMESPACE);

            if (s == null)
                s = ConfigParams.DEFAULT_NAMESPACE;

            namespace = s;

            if (log.isInfoEnabled())
                log.info(ConfigParams.NAMESPACE + "=" + namespace);

        }

        final boolean create;
        {

            final String s = getInitParameter(ConfigParams.CREATE);

            if (s != null)
                create = Boolean.valueOf(s);
            else
                create = ConfigParams.DEFAULT_CREATE;

            if (log.isInfoEnabled())
                log.info(ConfigParams.CREATE + "=" + create);

        }

        final IIndexManager indexManager;
        if (context.getAttribute(IIndexManager.class.getName()) != null) {

            /*
             * The index manager object was directly set by the caller.
             */

            indexManager = (IIndexManager) context
                    .getAttribute(IIndexManager.class.getName());

            // the caller is responsible for the life cycle.
            closeIndexManager = false;

        } else {

            /**
             * The index manager will be open based on the specified property
             * file or config file.
             *
             * Note: You may override the location of the property file using
             * <pre>
             * -Dcom.bigdata.rdf.sail.webapp.ConfigParams.propertyFile=FILE
             * </pre>
             */

            // The fully qualified name of the environment variable.
            final String FQN_PROPERTY_FILE = ConfigParams.class.getName() + "."
                    + ConfigParams.PROPERTY_FILE;

            // The default value is taken from the web.xml file.
            final String defaultValue = getInitParameter(
                    ConfigParams.PROPERTY_FILE);

            // The effective location of the property file.
            final String propertyFile = System.getProperty(//
                    FQN_PROPERTY_FILE,//
                    defaultValue//
                    );

            if (propertyFile == null)
                throw new RuntimeException("Required config-param: "
                        + ConfigParams.PROPERTY_FILE);

            if (log.isInfoEnabled())
                log.info(ConfigParams.PROPERTY_FILE + "=" + propertyFile);

            indexManager = openIndexManager(propertyFile);

            // we are responsible for the life cycle.
            closeIndexManager = true;

      }

      if (create) {

         /*
          * Note: Nobody is watching this future. The task will log any errors.
          */

         // indexManager.getExecutorService().submit(
         // new CreateKBTask(indexManager, namespace));

         final Properties properties;
         if (indexManager instanceof IBigdataFederation) {

            properties = ((IBigdataFederation<?>) indexManager).getClient()
                  .getProperties();
         } else {
            properties = ((Journal) indexManager).getProperties();
         }

         AbstractApiTask.submitApiTask(indexManager, new CreateKBTask(
               namespace, properties));

      } // if( create )

        txs = (indexManager instanceof Journal ? ((Journal) indexManager)
                .getTransactionManager().getTransactionService()
                : ((IBigdataFederation<?>) indexManager)
                        .getTransactionService());

        final long timestamp;
        {

            final String s = getInitParameter( ConfigParams.READ_LOCK);

            readLock = s == null ? null : Long.valueOf(s);

            if (readLock != null) {

                /*
                 * Obtain a read-only transaction which will assert a read lock
                 * for the specified commit time. The database WILL NOT release
                 * storage associated with the specified commit point while this
                 * server is running. Queries will read against the specified
                 * commit time by default, but this may be overridden on a query
                 * by query basis.
                 */

                try {

                    timestamp = txs.newTx(readLock);

                } catch (IOException ex) {

                    throw new RuntimeException(ex);

                }

                log.warn("Holding read lock: readLock=" + readLock + ", tx: "
                        + timestamp);

            } else {

                /*
                 * The default for queries is to read against then most recent
                 * commit time as of the moment when the request is accepted.
                 */

                timestamp = ITx.READ_COMMITTED;

            }

        }

        final int queryThreadPoolSize;
        {

            final String s = getInitParameter( ConfigParams.QUERY_THREAD_POOL_SIZE);

            queryThreadPoolSize = s == null ? ConfigParams.DEFAULT_QUERY_THREAD_POOL_SIZE
                    : Integer.valueOf(s);

            if (queryThreadPoolSize < 0) {

                throw new RuntimeException(ConfigParams.QUERY_THREAD_POOL_SIZE
                        + " : Must be non-negative, not: " + s);

            }

            if (log.isInfoEnabled())
                log.info(ConfigParams.QUERY_THREAD_POOL_SIZE + "="
                        + queryThreadPoolSize);

        }

        final boolean describeEachNamedGraph;
        {

            final String s = getInitParameter( ConfigParams.DESCRIBE_EACH_NAMED_GRAPH);

            describeEachNamedGraph = s == null ? ConfigParams.DEFAULT_DESCRIBE_EACH_NAMED_GRAPH
                    : Boolean.valueOf(s);

            if (log.isInfoEnabled())
                log.info(ConfigParams.DESCRIBE_EACH_NAMED_GRAPH + "="
                        + describeEachNamedGraph);

        }

        final boolean readOnly;
        {

            final String s = getInitParameter( ConfigParams.READ_ONLY);

            readOnly = s == null ? ConfigParams.DEFAULT_READ_ONLY : Boolean
                    .valueOf(s);

            if (log.isInfoEnabled())
                log.info(ConfigParams.READ_ONLY + "=" + readOnly);

        }

        final long queryTimeout;
        {

            final String s = getInitParameter( ConfigParams.QUERY_TIMEOUT);

            queryTimeout = s == null ? ConfigParams.DEFAULT_QUERY_TIMEOUT
                    : Long.valueOf(s);

            if (queryTimeout < 0) {

                throw new RuntimeException(ConfigParams.QUERY_TIMEOUT
                        + " : Must be non-negative, not: " + s);

            }

            if (log.isInfoEnabled())
                log.info(ConfigParams.QUERY_TIMEOUT + "=" + queryTimeout);

        }

        final long warmupTimeoutMillis;
        {

            final String s = getInitParameter( ConfigParams.WARMUP_TIMEOUT);

            warmupTimeoutMillis = s == null ? ConfigParams.DEFAULT_WARMUP_TIMEOUT
                    : Long.valueOf(s);

            if (warmupTimeoutMillis < 0) {

                throw new RuntimeException(ConfigParams.WARMUP_TIMEOUT
                        + " : Must be non-negative, not: " + s);

            }

            if (log.isInfoEnabled())
                log.info(ConfigParams.WARMUP_TIMEOUT + "=" + warmupTimeoutMillis);

        }

        final SparqlEndpointConfig config = new SparqlEndpointConfig(namespace,
                timestamp, queryThreadPoolSize, describeEachNamedGraph,
                readOnly, queryTimeout);

        rdfContext = new BigdataRDFContext(config, indexManager);

        // Used by BigdataBaseServlet
        context.setAttribute(BigdataServlet.ATTRIBUTE_INDEX_MANAGER,
                indexManager);

        // Used by BigdataRDFBaseServlet
        context.setAttribute(BigdataRDFServlet.ATTRIBUTE_RDF_CONTEXT,
                rdfContext);

      if (indexManager instanceof Journal && warmupTimeoutMillis > 0L) {

         // Note: null -or- empty => ALL namespaces.
         final List<String> warmupNamespaceList = new LinkedList<String>();
         {

            String s = getInitParameter(ConfigParams.WARMUP_NAMESPACE_LIST);

            if (s == null) {

               s = ConfigParams.DEFAULT_WARMUP_NAMESPACE_LIST;

            }

            if (s != null) {

               final String[] a = s.split(",");

               for (String t : a) {

                  t = t.trim();

                  if (t.isEmpty())
                     continue;

                  warmupNamespaceList.add(t);

               }

            }

            if (log.isInfoEnabled())
               log.info(ConfigParams.WARMUP_NAMESPACE_LIST + "="
                     + warmupNamespaceList);

         }

         final int warmupThreadPoolSize;
         {

            final String s = getInitParameter(ConfigParams.WARMUP_THREAD_POOL_SIZE);

            warmupThreadPoolSize = s == null ? ConfigParams.DEFAULT_WARMUP_THREAD_POOL_SIZE
                  : Integer.valueOf(s);

            if (warmupThreadPoolSize <= 0) {

               throw new RuntimeException(ConfigParams.WARMUP_THREAD_POOL_SIZE
                     + " : Must be positive, not: " + s);

            }

            if (log.isInfoEnabled())
               log.info(ConfigParams.WARMUP_THREAD_POOL_SIZE + "="
                     + warmupThreadPoolSize);

         }

         /*
          * Note: The [timestamp] will be READ_COMMITTED or the read lock as
          * specified above.
          */

         // Parameters that should not be messed with.
         final boolean visitLeaves = false; // Only materialize non-leaf pages.

         log.warn("Warming up the journal: namespaces="
               + (warmupNamespaceList == null || warmupNamespaceList.isEmpty() ? "ALL"
                     : warmupNamespaceList) + ", warmupTheads="
               + warmupThreadPoolSize + ", timeout="
               + TimeUnit.MILLISECONDS.toSeconds(warmupTimeoutMillis) + "s");

         // Submit the warmup procedure.
         final Future<Map<String, BaseIndexStats>> ft = indexManager
               .getExecutorService().submit(
                     new WarmUpTask(((Journal) indexManager),
                           warmupNamespaceList, timestamp,
                           warmupThreadPoolSize, visitLeaves));

         try {
            // Await the warmup procedure termination.
            final Map<String, BaseIndexStats> statsMap = ft.get(
                  warmupTimeoutMillis, TimeUnit.MILLISECONDS);
         } catch (ExecutionException e1) {
            /*
             * Abnormal termination.
             */
            throw new RuntimeException("Warmup failure: " + e1, e1);
         } catch (InterruptedException e1) {
            /*
             * This thread was interrupted. This probably indicates shutdown of
             * the container.
             */
            throw new RuntimeException(e1);
         } catch (TimeoutException e1) {
            /*
             * Ignore expected exception. The warmup procedure has reached its
             * timeout. This is fine.
             */
            if (log.isInfoEnabled())
               log.info("Warmup terminated by timeout.");
            // fall through.
         }

      }
        
//        // Initialize the SPARQL cache.
//        context.setAttribute(BigdataServlet.ATTRIBUTE_SPARQL_CACHE,
//                new SparqlCache(new MemoryManager(DirectBufferPool.INSTANCE)));

        {

            final boolean forceOverflow = Boolean
                    .valueOf(getInitParameter(ConfigParams.FORCE_OVERFLOW));

            if (forceOverflow && indexManager instanceof IBigdataFederation<?>) {

                log.warn("Forcing compacting merge of all data services: "
                        + new Date());

                ((AbstractDistributedFederation<?>) indexManager)
                        .forceOverflow(true/* compactingMerge */, false/* truncateJournal */);

                log.warn("Did compacting merge of all data services: "
                        + new Date());

            }

        }

        {
            final String serviceWhitelist = getInitParameter(ConfigParams.SERVICE_WHITELIST);
            if (serviceWhitelist != null) {
                log.info("Service whitelist: " + serviceWhitelist);
                ServiceRegistry reg = ServiceRegistry.getInstance();
                reg.setWhitelistEnabled(true);
                for(String url: serviceWhitelist.split("\\s*,\\s*")) {
                    reg.addWhitelistURL(url);
                }
            }
        }

        /*
         * Force service/format registration.
         *
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/439">
         * Class loader problems </a>
         */
        ServiceProviderHook.forceLoad();

        if (log.isInfoEnabled())
            log.info("done");

    }

    @Override
    public void contextDestroyed(final ServletContextEvent e) {

        if(log.isInfoEnabled())
            log.info("");

        if (rdfContext != null) {

            rdfContext.shutdownNow();

            rdfContext = null;

        }

        if (txs != null && readLock != null && readLock != -1L) {

            try {

                txs.abort(readLockTx);

            } catch (IOException ex) {

                log
                        .error("Could not release transaction: tx="
                                + readLockTx, ex);

            }

            txs = null;
            readLock = null;

        }

        if (jnl != null) {

            if (closeIndexManager)
                jnl.close();

            jnl = null;

        }

        if (jiniClient != null) {

            if (closeIndexManager)
                jiniClient.disconnect(true/* immediateShutdown */);

            jiniClient = null;

        }

//        // Clear the SPARQL cache.
//        if (sparqlCache != null) {
//
//            sparqlCache.close();
//
//            sparqlCache  = null;
//
//        }

        effectiveInitParams = null;

        /*
         * Terminate various threads which should no longer be executing once we
         * have destroyed the servlet context. If you do not do this then
         * servlet containers such as tomcat will complain that we did not stop
         * some threads.
         */
        {

            SynchronizedHardReferenceQueueWithTimeout.stopStaleReferenceCleaner();

        }

    }

    /**
     * Open the {@link IIndexManager} identified by the property file.
     *
     * @param propertyFile
     *            The property file (for a standalone bigdata instance) or the
     *            jini configuration file (for a bigdata federation). The file
     *            must end with either ".properties" or ".config".
     *
     * @return The {@link IIndexManager}.
     */
    private IIndexManager openIndexManager(final String propertyFile) {

        // Locate the named .properties or .config file.
        final URL propertyFileUrl;
        if (new File(propertyFile).exists()) {

            // Check the file system.
            try {
                propertyFileUrl = new URL("file:" + propertyFile);
            } catch (MalformedURLException ex) {
                throw new RuntimeException(ex);
            }

        } else {

            // Check the classpath.
            propertyFileUrl = BigdataRDFServletContextListener.class
                    .getClassLoader().getResource(propertyFile);

        }

        if (log.isInfoEnabled())
            log.info("bigdata configuration: propertyFile=" + propertyFile
                    + ", propertyFileUrl=" + propertyFileUrl);

        if (propertyFileUrl == null) {

            throw new RuntimeException("Could not find file: file="
                    + propertyFile + ", user.dir="
                    + System.getProperty("user.dir"));

        }

        boolean isJini = false;
        if (propertyFile.endsWith(".config")) {
            // scale-out.
            isJini = true;
        } else if (propertyFile.endsWith(".properties")) {
            // local journal.
            isJini = false;
        } else {
            /*
             * Note: This is a hack, but we are recognizing the jini
             * configuration file with a .config extension and the journal
             * properties file with a .properties extension.
             */
            throw new RuntimeException(
                    "File must have '.config' or '.properties' extension: "
                            + propertyFile);
        }

        final IIndexManager indexManager;
        try {

            if (isJini) {

                /*
                 * A bigdata federation.
                 *
                 * Note: The Apache River configuration mechanism will search
                 * both the file system and the classpath, much as we have done
                 * above.
                 *
                 * TODO This will use the ClassLoader associated with the
                 * JiniClient if that is different from the ClassLoader used
                 * above, then it could be possible for one ClassLoader to find
                 * the propertyFile resource and the other to not find that
                 * resource.
                 */

                jiniClient = ScaleOutClientFactory.getJiniClient(new String[] { propertyFile });

                jiniClient.setDelegate(new NanoSparqlServerFederationDelegate(
                        jiniClient, this));

                indexManager = jiniClient.connect();

            } else {

                /*
                 * Note: we only need to specify the FILE when re-opening a
                 * journal containing a pre-existing KB.
                 */
                final Properties properties = new Properties();
                {
                    // Read the properties from the file.
                    final InputStream is = new BufferedInputStream(
                            propertyFileUrl.openStream());
                    try {
                        properties.load(is);
                    } finally {
                        is.close();
                    }
                    if (System.getProperty(BigdataSail.Options.FILE) != null) {
                        // Override/set from the environment.
                        properties.setProperty(BigdataSail.Options.FILE, System
                                .getProperty(BigdataSail.Options.FILE));
                    }
                }

                indexManager = jnl = new Journal(properties);

            }

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

        return indexManager;

    }

    /**
     * Hooked to report the query engine performance counters to the federation.
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @param <T>
     */
    private static class NanoSparqlServerFederationDelegate<T> extends
            DefaultClientDelegate<T> {

        final private IBigdataClient<?> client;
        final private BigdataRDFServletContextListener servletContextListener;

        /**
         * The path component under which the query engine performance counters
         * will be reported.
         */
        static private final String QUERY_ENGINE = "Query Engine";

        public NanoSparqlServerFederationDelegate(final IBigdataClient<?> client,
                final BigdataRDFServletContextListener servletContextListener) {

            super(client, null/* clientOrService */);

            this.client = client;

            if (servletContextListener == null)
                throw new IllegalArgumentException();

            this.servletContextListener = servletContextListener;

        }

        /**
         * Overridden to attach the counters reporting on the things which are
         * either dynamic or not otherwise part of the reported counter set for
         * the client.
         */
        @Override
        public void reattachDynamicCounters() {

//        	final BigdataRDFContext rdfContext = servletContextListener.rdfContext;

            final IBigdataFederation<?> fed;

            try {

                fed = client.getFederation();

                assert fed != null;

            } catch (IllegalStateException ex) {

                log.warn("Closed: " + ex);

                return;
            }

            // The service's counter set hierarchy.
            final CounterSet serviceRoot = fed
                    .getServiceCounterSet();

            /*
             * DirectBufferPool counters.
             */
            {

                // Ensure path exists.
                final CounterSet tmp = serviceRoot
                        .makePath(IProcessCounters.Memory);

                synchronized (tmp) {

                    // detach the old counters (if any).
                    tmp.detach("DirectBufferPool");

                    // attach the current counters.
                    tmp.makePath("DirectBufferPool").attach(
                            DirectBufferPool.getCounters());

                }

            }

            /*
             * QueryEngine counters.
             */
            {

                /*
                 * TODO It would be better to have this on the BigdataRDFContext
                 * so we are not creating it lazily here if the NSS has not yet
                 * been issued a query.
                 */
                final QueryEngine queryEngine = QueryEngineFactory.getInstance()
                        .getQueryController(fed);

                final CounterSet tmp = serviceRoot;

                synchronized (tmp) {

                    tmp.detach(QUERY_ENGINE);

                    // attach the current counters.
                    tmp.makePath(QUERY_ENGINE)
                            .attach(queryEngine.getCounters());

                }

            }

        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to NOT start an embedded performance counter reporting
         * httpd instance. The {@link NanoSparqlServer} already provides a
         * {@link CountersServlet} through which this stuff gets reported to the
         * UI.
         */
        @Override
        public AbstractHTTPD newHttpd(final int httpdPort,
                final ICounterSetAccess access) throws IOException {

            return null;

        }

    }

}
