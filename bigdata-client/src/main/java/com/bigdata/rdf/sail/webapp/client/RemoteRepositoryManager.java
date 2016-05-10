/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2014.  All rights reserved.

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

package com.bigdata.rdf.sail.webapp.client;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.http.entity.ByteArrayEntity;
import org.apache.log4j.Logger;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpRequest;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.http.HttpMethod;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.query.impl.TupleQueryResultImpl;
import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.BooleanQueryResultParser;
import org.openrdf.query.resultio.BooleanQueryResultParserFactory;
import org.openrdf.query.resultio.BooleanQueryResultParserRegistry;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultParser;
import org.openrdf.query.resultio.TupleQueryResultParserFactory;
import org.openrdf.query.resultio.TupleQueryResultParserRegistry;
import org.openrdf.repository.sparql.query.InsertBindingSetCursor;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;
import org.openrdf.rio.RDFParserRegistry;

import com.bigdata.rdf.ServiceProviderHook;
import com.bigdata.rdf.properties.PropertiesFormat;
import com.bigdata.rdf.properties.PropertiesParser;
import com.bigdata.rdf.properties.PropertiesParserFactory;
import com.bigdata.rdf.properties.PropertiesParserRegistry;
import com.bigdata.rdf.properties.PropertiesWriter;
import com.bigdata.rdf.properties.PropertiesWriterRegistry;
import com.bigdata.rdf.sail.model.JsonHelper;
import com.bigdata.rdf.sail.model.RunningQuery;
import com.bigdata.util.InnerCause;
import com.bigdata.util.PropertyUtil;

/**
 * A manager for connections to one or more REST API / SPARQL end points for the
 * same bigdata service.
 * 
 * @author bryan
 */
public class RemoteRepositoryManager extends RemoteRepositoryBase implements AutoCloseable {

    private static final transient Logger log = Logger.getLogger(RemoteRepositoryManager.class);

    final static String EXCEPTION_MSG = "Class not found for service provider hook. " +
            "Blazegraph specific parser extensions will not be available.";

    /**
     * The path to the root of the web application (without the trailing "/").
     * <p>
     * Note: This SHOULD NOT be the SPARQL end point URL. The NanoSparqlServer
     * has a wider interface. This should be the base URL of that interface. The
     * SPARQL end point URL for the default data set is formed by appending
     * <code>/sparql</code>.
     */
    private final String baseServiceURL;

    /**
     * When <code>true</code>, the REST API methods will use the load balancer
     * aware requestURLs. The load balancer has essentially zero cost when not
     * using HA, so it is recommended to always specify <code>true</code>. When
     * <code>false</code>, the REST API methods will NOT use the load balancer
     * aware requestURLs.
     * 
     * @see <a href="http://wiki.blazegraph.com/wiki/index.php/HALoadBalancer">
     *      HALoadBalancer </a>
     */
    protected final boolean useLBS;

    /**
     * The client used for http connections.
     */
    protected final HttpClient httpClient;

    /**
     * IFF an {@link HttpClient} was allocated by the constructor, then this is
     * that reference. When non-<code>null</code> this is always the same
     * reference as {@link #httpClient}.
     */
    private final HttpClient our_httpClient;

    /**
     * Thread pool for processing HTTP responses in background.
     */
    protected final Executor executor;

    /**
     * IFF an {@link Executor} was allocated by the constructor, then this is
     * that reference. When non-<code>null</code> this is always the same
     * reference as {@link #executor}.
     */
    private final ExecutorService our_executor;

    /**
     * The maximum requestURL length before the request is converted into a POST
     * using a <code>application/x-www-form-urlencoded</code> request entity.
     */
    private volatile int maxRequestURLLength;

    /**
     * The HTTP verb that will be used for a QUERY (versus a UPDATE or other
     * mutation operation).
     * 
     * @see #QUERY_METHOD
     */
    private volatile String queryMethod;

    /**
     * Remote client for the transaction manager API.
     */
    private final RemoteTransactionManager transactionManager;

    /**
     * <code>true</code> iff already closed.
     */
    private volatile boolean m_closed = false;

    /**
     * Show Queries Query Parameter
     */
    private static String SHOW_QUERIES = "showQueries";

    /**
     * Return the remote client for the transaction manager API.
     * 
     * @since 1.5.2
     * 
     * @see <a href="http://trac.bigdata.com/ticket/1156"> Support read/write
     *      transactions in the REST API</a>
     */
    public RemoteTransactionManager getTransactionManager() {

        return transactionManager;

    }

    /**
     * The executor for processing http and other client operations.
     */
    public Executor getExecutor() {

        return executor;

    }

    /**
     * The path to the root of the web application (without the trailing "/").
     * <p>
     * Note: This SHOULD NOT be the SPARQL end point URL. The NanoSparqlServer
     * has a wider interface. This should be the base URL of that interface. The
     * SPARQL end point URL for the default data set is formed by appending
     * <code>/sparql</code>.
     */
    public String getBaseServiceURL() {

        return baseServiceURL;

    }

    /**
     * Return <code>true</code> iff the REST API methods will use the load
     * balancer aware requestURLs. The load balancer has essentially zero cost
     * when not using HA, so it is recommended to always specify
     * <code>true</code>. When <code>false</code>, the REST API methods will NOT
     * use the load balancer aware requestURLs.
     */
    public boolean getUseLBS() {

        return useLBS;

    }

    /**
     * Return the maximum requestURL length before the request is converted into
     * a POST using a <code>application/x-www-form-urlencoded</code> request
     * entity.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/619">
     *      RemoteRepository class should use application/x-www-form-urlencoded
     *      for large POST requests </a>
     */
    public int getMaxRequestURLLength() {

        return maxRequestURLLength;

    }

    public void setMaxRequestURLLength(final int newVal) {

        if (newVal <= 0)
            throw new IllegalArgumentException();

        this.maxRequestURLLength = newVal;

    }

    /**
     * Return the HTTP verb that will be used for a QUERY (versus an UPDATE or
     * other mutation operations) (default {@value #DEFAULT_QUERY_METHOD}). POST
     * can often handle larger queries than GET due to limits at the HTTP client
     * layer and will defeat http caching and thus provide a current view of the
     * committed state of the SPARQL end point when the end point is a
     * read/write database. However, GET supports HTTP caching and can scale
     * much better when the SPARQL end point is a read-only resource or a
     * read-mostly resource where stale reads are acceptable.
     * 
     * @see #setQueryMethod(String)
     */
    public String getQueryMethod() {

        return queryMethod;

    }

    /**
     * Set the default HTTP verb for QUERY and other idempotant operations.
     * 
     * @param method
     *            The method which may be "POST" or "GET".
     * 
     * @see #getQueryMethod()
     */
    public void setQueryMethod(final String method) {

        if ("POST".equalsIgnoreCase(method) || "GET".equalsIgnoreCase(method)) {

            this.queryMethod = method.toUpperCase();

        } else {

            throw new IllegalArgumentException();

        }

    }

    /**
     * Create a manager that is not aware of a specific blazegraph backend. This
     * constructor is intended for patterns where a sparql end point is
     * available but the top-level serviceURL for blazegraph is either not
     * visible or not known:
     * 
     * <pre>
     * new RemoteRepositoryManager().getRepositoryForURL(sparqlEndpointURL)
     * </pre>
     * 
     * The same pattern MAY be used to perform SPARQL QUERY or SPARQL UPDATE
     * operations against non-blazegraph sparql end points.
     */
    public RemoteRepositoryManager() {

        this("http://localhost/no-service-URL");

    }

    /**
     * Create a manager client for the specified serviceURL. The serviceURL has
     * the typical form
     * 
     * <pre>
     * http://host:port/bigdata
     * </pre>
     * 
     * The serviceURL can be used to obtain sparql end point URLs for:
     * <dl>
     * <dt>The default namespace</dt>
     * <dd>http://host:port/bigdata/sparql</dd>
     * <dt>The XYZ namespace</dt>
     * <dd>http://host:port/bigdata/namespace/XYZ/sparql</dd>
     * </dl>
     * 
     * The serviceURL can also be used to access the multi-tenancy API and the
     * transaction management API. See the wiki for more details.
     * 
     * @param serviceURL
     *            The path to the root of the web application (without the
     *            trailing "/"). <code>/sparql</code> will be appended to this
     *            path to obtain the SPARQL end point for the default data set.
     */
    public RemoteRepositoryManager(final String serviceURL) {

        /*
         * TODO Why is useLBS:=false? Is there a problem when it is true and we
         * are not actually using an HA deployment? E.g., single server
         * deployment under a non-jetty servlet container where the
         * LoadBalancerServlet is not deployed?
         */
        this(serviceURL, false/* useLBS */);

    }

    /**
     * Create a remote client for the specified serviceURL that optionally use
     * the load balanced URLs.
     * 
     * @param serviceURL
     *            The path to the root of the web application (without the
     *            trailing "/"). <code>/sparql</code> will be appended to this
     *            path to obtain the SPARQL end point for the default data set.
     * @param useLBS
     *            When <code>true</code>, the REST API methods will use the load
     *            balancer aware requestURLs. The load balancer has essentially
     *            zero cost when not using HA, so it is recommended to always
     *            specify <code>true</code>. When <code>false</code>, the REST
     *            API methods will NOT use the load balancer aware requestURLs.
     */
    public RemoteRepositoryManager(final String serviceURL, final boolean useLBS) {

        this(serviceURL, useLBS, null/* httpClient */, null/* executor */);

    }

    /**
     * Create a remote client for the specified serviceURL.
     * 
     * @param serviceURL
     *            The path to the root of the web application (without the
     *            trailing "/"). <code>/sparql</code> will be appended to this
     *            path to obtain the SPARQL end point for the default data set.
     * @param httpClient
     *            The client is managed externally and, in particular, 
     *            may be shared, so we don't close it in {@link #close()}.
     *            When not present (null), an {@link HttpClient} will be 
     *            allocated and scoped to this
     *            {@link RemoteRepositoryManager} instance, and closed
     *            in {@link #close()}.
     * @param executor
     *            An executor used to service http client requests. (optional).
     *            When not present, an {@link Executor} will be allocated and
     *            scoped to this {@link RemoteRepositoryManager} instance.
     * 
     *            TODO Should this be deprecated since it does not force the
     *            caller to choose a value for <code>useLBS</code>?
     *            <p>
     *            This version does not force the caller to decide whether or
     *            not the LBS pattern will be used. In general, it should be
     *            used if the end point is bigdata. This class is generally, but
     *            not always, used with a bigdata end point. The main exception
     *            is SPARQL Basic Federated Query. For that use case we can not
     *            assume that the end point is bigdata and thus we can not use
     *            the LBS prefix.
     */
    public RemoteRepositoryManager(final String serviceURL, final HttpClient httpClient, final Executor executor) {

        this(serviceURL, false/* useLBS */, httpClient, executor);

    }

    /**
     * Create a remote client for the specified serviceURL (core impl).
     * 
     * @param serviceURL
     *            The path to the root of the web application (without the
     *            trailing "/"). <code>/sparql</code> will be appended to this
     *            path to obtain the SPARQL end point for the default data set.
     * @param useLBS
     *            When <code>true</code>, the REST API methods will use the load
     *            balancer aware requestURLs. The load balancer has essentially
     *            zero cost when not using HA, so it is recommended to always
     *            specify <code>true</code>. When <code>false</code>, the REST
     *            API methods will NOT use the load balancer aware requestURLs.
     * @param httpClient
     *            The client is managed externally and, in particular, 
     *            may be shared, so we don't close it in {@link #close()}.
     *            When not present (null), an {@link HttpClient} will be 
     *            allocated and scoped to this
     *            {@link RemoteRepositoryManager} instance, and closed
     *            in {@link #close()}.
     * @param executor
     *            An executor used to service http client requests. (optional).
     *            When not present, an {@link Executor} will be allocated and
     *            scoped to this {@link RemoteRepositoryManager} instance.
     */
    public RemoteRepositoryManager(final String serviceURL, final boolean useLBS, final HttpClient httpClient,
            final Executor executor) {

        if (serviceURL == null)
            throw new IllegalArgumentException();

        this.baseServiceURL = serviceURL;

        this.useLBS = useLBS;

        if (httpClient == null) {

            /*
             * Allocate the HttpClient. It will be closed when this class is
             * closed.
             */
            this.httpClient = our_httpClient = HttpClientConfigurator.getInstance().newInstance();

        } else {

            /*
             * Note: Client *might* be AutoCloseable, in which case we will
             * close it.
             */
            this.httpClient = httpClient;
            this.our_httpClient = null;

        }

        if (executor == null) {

            /*
             * Allocate the executor. It will be shutdown when this class is
             * closed.
             * 
             * See #1191 (remote connection uses non-daemon thread pool).
             */
            this.executor = our_executor = Executors.newCachedThreadPool(DaemonThreadFactory.defaultThreadFactory());

        } else {

            // We are using the caller's executor. We will not shut it down.
            this.executor = executor;
            this.our_executor = null;

        }

        assertHttpClientRunning();

        this.transactionManager = new RemoteTransactionManager(this);

        setMaxRequestURLLength(Integer.parseInt(
                System.getProperty(MAX_REQUEST_URL_LENGTH, Integer.toString(DEFAULT_MAX_REQUEST_URL_LENGTH))));

        setQueryMethod(System.getProperty(QUERY_METHOD, DEFAULT_QUERY_METHOD));

        // See #1235 bigdata-client does not invoke
        // ServiceProviderHook.forceLoad()
        try {
            ServiceProviderHook.forceLoad();
        } catch (java.lang.NoClassDefFoundError | java.lang.ExceptionInInitializerError e) {
            // If we are running in unit tests in the client package this
            // introduces
            // a runtime dependency on the bigdata-core artifact. Just catch the
            // Exception and let the unit test complete.
            if (log.isInfoEnabled()) {
                log.info(EXCEPTION_MSG);
            }
        }

    }

    /**
     * {@inheritDoc}
     * <p>
     * Ensure resource is closed.
     * 
     * @see AutoCloseable
     * @see <a href="http://trac.bigdata.com/ticket/1207" > Memory leak in
     *      CI? </a>
     */
    @Override
    protected void finalize() throws Throwable {

        close();

        super.finalize();

    }

    @Override
    public void close() throws Exception {

        if (m_closed) {
            // Already closed.
            return;
        }


        if (our_httpClient != null) {
      
            /*
             * This HttpClient was allocated by our constructor. We will shut it
             * down now (unless it is already stopping or stopped).
             */

            if (our_httpClient instanceof AutoCloseable) {
                // The instance is managed by AutoCloseHttpClient or a similar 
                // class, so close() instead of just stop():
                ((AutoCloseable) our_httpClient).close();
            } else {
                // Not an AutoCloseable, but we should still try to stop it:

                if (!our_httpClient.isStopping() && !our_httpClient.isStopped()) {

                    our_httpClient.stop();

                }
            }

        }
        
        // Note that we do not close httpClient here, unless it is 
        // our_httpClient, because it came as 
        // a parameter to a constructor, may be shared by multiple 
        // RemoteRepositoryManager instances and other objects, and 
        // thus has to be closed elswhere (e.g., QueryEngine.shutdown()).
        
        

        if (our_executor != null) {

            /*
             * This thread pool was allocated by our constructor. Shut it down
             * now.
             */
            our_executor.shutdownNow();

        }

        m_closed = true;

    }

    @Override
    public String toString() {

        return super.toString() + "{baseServiceURL=" + baseServiceURL + ", useLBS=" + useLBS + "}";

    }

    /**
     * Return the base URL for a remote repository (less the /sparql path
     * component).
     * 
     * @param namespace
     *            The namespace.
     * 
     * @return The base URL.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/689" >
     *      Missing URL encoding in RemoteRepositoryManager </a>
     */
    protected String getRepositoryBaseURLForNamespace(final String namespace) {

        return baseServiceURL + "/namespace/" + ConnectOptions.urlEncode(namespace);
    }

    
    /**
     * Returns the SPARQL endpoint URL for the given namespace or the default SPARQL
     * endpoint in case namespace is null.
     * 
     * @param namespace
     * @return the namespace 
     */
    private String getSparqlEndpointUrlForNamespaceOrDefault(final String namespace) {
        
        return namespace==null ?
                baseServiceURL + "/sparql" : 
                getRepositoryBaseURLForNamespace(namespace) + "/sparql";
    }
    /**
     * Obtain a flyweight {@link RemoteRepository} for the default namespace
     * associated with the remote service.
     */
    public RemoteRepository getRepositoryForDefaultNamespace() {

        return getRepositoryForURL(baseServiceURL + "/sparql");

    }

    /**
     * Obtain a flyweight {@link RemoteRepository} for a data set managed by the
     * remote service.
     * 
     * @param namespace
     *            The name of the data set (its bigdata namespace).
     * 
     * @return An interface which may be used to talk to that data set.
     */
    public RemoteRepository getRepositoryForNamespace(final String namespace) {

        return getRepositoryForURL(getRepositoryBaseURLForNamespace(namespace) + "/sparql");

    }

    // /**
    // * Obtain a flyweight {@link RemoteRepository} for the data set having the
    // specified
    // * SPARQL end point.
    // *
    // * @param sparqlEndpointURL
    // * The URL of the SPARQL end point.
    // * @param useLBS
    // * When <code>true</code>, the REST API methods will use the load
    // * balancer aware requestURLs. The load balancer has essentially
    // * zero cost when not using HA, so it is recommended to always
    // * specify <code>true</code>. When <code>false</code>, the REST
    // * API methods will NOT use the load balancer aware requestURLs.
    // *
    // * @return An interface which may be used to talk to that data set.
    // */
    // @Deprecated // The useLBS property is on the RemoteRepositoryManager and
    // is ignored by this method.
    // public RemoteRepository getRepositoryForURL(final String
    // sparqlEndpointURL,
    // final boolean useLBS) {
    //
    // return new RemoteRepository(this, sparqlEndpointURL);
    //
    // }

    /**
     * Obtain a flyweight {@link RemoteRepository} for the data set having the
     * specified SPARQL end point. The load balancer will be used or not as per
     * the parameters to the {@link RemoteRepositoryManager} constructor.
     * 
     * @param sparqlEndpointURL
     *            The URL of the SPARQL end point.
     * 
     * @return An interface which may be used to talk to that data set.
     */
    public RemoteRepository getRepositoryForURL(final String sparqlEndpointURL) {

        return getRepositoryForURL(sparqlEndpointURL, null/* timestamp */);

    }

    /**
     * Obtain a flyweight {@link RemoteRepository} for the data set having the
     * specified SPARQL end point. The load balancer will be used or not as per
     * the parameters to the {@link RemoteRepositoryManager} constructor.
     * 
     * @param sparqlEndpointURL
     *            The URL of the SPARQL end point.
     * @param timestamp
     *            The timestamp that will be added to all requests for the
     *            sparqlEndPoint (optional).
     * 
     * @return An interface which may be used to talk to that data set.
     */
    public RemoteRepository getRepositoryForURL(final String sparqlEndpointURL, final IRemoteTx tx) {

        return new RemoteRepository(this, sparqlEndpointURL, tx);

    }

    /**
     * Obtain a <a href="http://vocab.deri.ie/void/"> VoID </a> description of
     * the configured KBs. Each KB has its own namespace and corresponds to a
     * VoID "data set".
     * <p>
     * Note: This method uses an HTTP GET and hence can be cached by the server.
     * 
     * @return A <a href="http://vocab.deri.ie/void/"> VoID </a> description of
     *         the configured KBs.
     * 
     * @throws Exception
     */
    public GraphQueryResult getRepositoryDescriptions() throws Exception {

        return getRepositoryDescriptions(UUID.randomUUID());

    }

    /**
     * Obtain a <a href="http://vocab.deri.ie/void/"> VoID </a> description of
     * the configured KBs. Each KB has its own namespace and corresponds to a
     * VoID "data set".
     * <p>
     * Note: This method uses an HTTP GET and hence can be cached by the server.
     * 
     * @param uuid
     *            The {@link UUID} to be associated with this request.
     * 
     * @return A <a href="http://vocab.deri.ie/void/"> VoID </a> description of
     *         the configured KBs.
     * 
     * @throws Exception
     */
    public GraphQueryResult getRepositoryDescriptions(final UUID uuid) throws Exception {

        final ConnectOptions opts = newConnectOptions(baseServiceURL + "/namespace", uuid, null/* tx */);

        opts.method = "GET";

        opts.setAcceptHeader(ConnectOptions.DEFAULT_GRAPH_ACCEPT_HEADER);

        // Note: asynchronous result set processing.
        return graphResults(opts, uuid, null /* listener */);

    }

    /**
     * Create a new KB instance.
     * 
     * @param namespace
     *            The namespace of the KB instance.
     * @param properties
     *            The configuration properties for that KB instance.
     * 
     * @throws Exception
     * 
     * @see <a href="http://trac.bigdata.com/ticket/1257"> createRepository()
     *      does not set the namespace on the Properties</a>
     */
    public void createRepository(final String namespace, final Properties properties) throws Exception {

        createRepository(namespace, properties, UUID.randomUUID());

    }

    /**
     * 
     * Create a new KB instance.
     * 
     * @param namespace
     *            The namespace of the KB instance.
     * @param properties
     *            The configuration properties for that KB instance.
     * @param uuid
     *            The {@link UUID} to be associated with this request.
     * 
     * @throws Exception
     * 
     * @see <a href="http://trac.bigdata.com/ticket/1257"> createRepository()
     *      does not set the namespace on the Properties</a>
     */
    public void createRepository(final String namespace, final Properties properties, final UUID uuid)
            throws Exception {

        if (namespace == null)
            throw new IllegalArgumentException();
        if (properties == null)
            throw new IllegalArgumentException();
        if (uuid == null)
            throw new IllegalArgumentException();
        // if (properties.getProperty(OPTION_CREATE_KB_NAMESPACE) == null)
        // throw new IllegalArgumentException("Property not defined: "
        // + OPTION_CREATE_KB_NAMESPACE);

        // Set the namespace property.
        final Properties tmp = PropertyUtil.flatCopy(properties);
        tmp.setProperty(OPTION_CREATE_KB_NAMESPACE, namespace);

        /*
         * Note: This operation does not currently permit embedding into a
         * read/write tx.
         */
        final ConnectOptions opts = newConnectOptions(baseServiceURL + "/namespace", uuid, null/* tx */);

        JettyResponseListener response = null;

        // Setup the request entity.
        {

            final PropertiesFormat format = PropertiesFormat.XML;

            final ByteArrayOutputStream baos = new ByteArrayOutputStream();

            final PropertiesWriter writer = PropertiesWriterRegistry.getInstance().get(format).getWriter(baos);

            writer.write(tmp);

            final byte[] data = baos.toByteArray();

            final ByteArrayEntity entity = new ByteArrayEntity(data);

            entity.setContentType(format.getDefaultMIMEType());

            opts.entity = entity;

        }

        try {

            checkResponseCode(response = doConnect(opts));
        } finally {
            if (response != null)
                response.abort();

        }

    }

    /**
     * 
     * Prepare configuration properties for a new KB instance.
     * 
     * @param namespace
     *            The namespace of the KB instance.
     * @param properties
     *            The configuration properties for that KB instance.
     * 
     * @return The effective configuration properties for that named data set.
     * 
     * @throws Exception
     */
    public Properties getPreparedProperties(final String namespace, final Properties properties) throws Exception {
        return getPreparedProperties(namespace, properties, UUID.randomUUID());
    }

    public Properties getPreparedProperties(final String namespace, final Properties properties, final UUID uuid)
            throws Exception {

        if (namespace == null)
            throw new IllegalArgumentException();
        if (properties == null)
            throw new IllegalArgumentException();
        if (uuid == null)
            throw new IllegalArgumentException();

        // Set the namespace property.
        final Properties tmp = PropertyUtil.flatCopy(properties);
        tmp.setProperty(OPTION_CREATE_KB_NAMESPACE, namespace);

        final String sparqlEndpointURL = baseServiceURL + "/namespace/prepareProperties";

        /*
         * Note: This operation does not currently permit embedding into a
         * read/write tx.
         */
        final ConnectOptions opts = newConnectOptions(baseServiceURL + "/namespace/prepareProperties", uuid,
                null/* tx */);

        JettyResponseListener response = null;

        // Setup the request entity.
        {

            final PropertiesFormat format = PropertiesFormat.XML;

            final ByteArrayOutputStream baos = new ByteArrayOutputStream();

            final PropertiesWriter writer = PropertiesWriterRegistry.getInstance().get(format).getWriter(baos);

            writer.write(tmp);

            final byte[] data = baos.toByteArray();

            final ByteArrayEntity entity = new ByteArrayEntity(data);

            entity.setContentType(format.getDefaultMIMEType());

            opts.entity = entity;

        }

        boolean consumeNeeded = true;
        try {

            checkResponseCode(response = doConnect(opts));

            final String contentType = response.getContentType();

            if (contentType == null)
                throw new RuntimeException("Not found: Content-Type");

            final MiniMime mimeType = new MiniMime(contentType);

            final PropertiesFormat format = PropertiesFormat.forMIMEType(mimeType.getMimeType());

            if (format == null)
                throw new IOException(
                        "Could not identify format for service response: serviceURI=" + sparqlEndpointURL +
                                ", contentType=" + contentType + " : response=" + response.getResponseBody());

            final PropertiesParserFactory factory = PropertiesParserRegistry.getInstance().get(format);

            if (factory == null)
                throw new RuntimeException(
                        "ParserFactory not found: Content-Type=" + contentType + ", format=" + format);

            final PropertiesParser parser = factory.getParser();

            final Properties preparedProperties = parser.parse(response.getInputStream());

            consumeNeeded = false;

            return preparedProperties;

        } catch (Exception e) {
            consumeNeeded = !InnerCause.isInnerCause(e, HttpException.class);
            throw e;

        } finally {
            if (response != null)
                response.abort();

        }

    }
    
    /**
     * 
     * 
     * @param 
     * @param 
     * 
     * @return 
     * 
     * @throws Exception
     */
    public void rebuildTextIndex(final String namespace,
            final boolean forceBuildTextIndex) throws Exception {
    	rebuildTextIndex(namespace, forceBuildTextIndex, UUID.randomUUID());
    }
    
    public void rebuildTextIndex(final String namespace,
    		final boolean forceBuildTextIndex, final UUID uuid) throws Exception {

        if (namespace == null)
            throw new IllegalArgumentException();
        if (uuid == null)
            throw new IllegalArgumentException();

        final String endpointURL = baseServiceURL + "/namespace/" + namespace + "/textIndex"; 
        
        
       
        /*
         * Note: This operation does not currently permit embedding into a
         * read/write tx.
         */
        final ConnectOptions opts = newConnectOptions(endpointURL, uuid, null/* tx */);
        
        if (forceBuildTextIndex) {
        	
        	opts.addRequestParam(RemoteRepositoryDecls.FORCE_INDEX_CREATE, "true");
        	
        } 
 
        JettyResponseListener response = null;

        boolean consumeNeeded = true;
        
        try {

            checkResponseCode(response = doConnect(opts));
            
            consumeNeeded = false;
            
        } catch (Exception e) {
            consumeNeeded = !InnerCause.isInnerCause(e,
                    HttpException.class);
        	throw e;
        	
        } finally {
        	
        	if (response != null)
        		response.abort();

        }
        
    }

    /**
     * Destroy a KB instance.
     * 
     * @param namespace
     *            The namespace of the KB instance.
     * 
     * @throws Exception
     */
    public void deleteRepository(final String namespace) throws Exception {

        deleteRepository(namespace, UUID.randomUUID());

    }

    /**
     * Destroy a KB instance.
     * 
     * @param namespace
     *            The namespace of the KB instance.
     * @param uuid
     *            The {@link UUID} to be assigned to the request.a
     * 
     * @throws Exception
     */
    public void deleteRepository(final String namespace, final UUID uuid) throws Exception {

        final ConnectOptions opts = newConnectOptions(getRepositoryBaseURLForNamespace(namespace), uuid,
                null/* txId */);

        opts.method = "DELETE";

        JettyResponseListener response = null;

        try {

            checkResponseCode(response = doConnect(opts));

        } finally {

            if (response != null)
                response.abort();

        }

    }

    /**********************************************************************
     ************************** Mapgraph Servlet **************************
     **********************************************************************/
    static private final String COMPUTE_MODE = "computeMode";
    static private final String MAPGRAPH = "mapgraph";
    static private final String MAPGRAPH_RESET = "reset";
    static private final String MAPGRAPH_PUBLISH = "publish";
    static private final String MAPGRAPH_DROP = "drop";
    static private final String MAPGRAPH_CHECK_RUNTIME_AVAILABLE = "runtimeAvailable";
    static private final String CHECK_PUBLISHED = "checkPublished";

    public enum ComputeMode {
        CPU,
        GPU;
    }
    
    /**
     * Publishes the given namespace to the mapgraph runtime. If
     * the namespace if already published, no action is performed.
     * The return value is false in the latter case, true otherwise.
     * If the namespace that is passed in is null, the default
     * namespace will be used.
     * 
     * @return true if the namespace was not yet published already, 
     *         false otherwise (i.e., in case no action has been taken)
     * @throws NoGPUAccelerationAvailableException
     */
    public boolean publishNamespaceToMapgraph(final String namespace)
    throws Exception {
        
        assertMapgraphRuntimeAvailable();

        if (namespacePublishedToMapgraph(namespace))
            return false; // nothing to be done
        
        
        final String repositoryUrl = 
            getSparqlEndpointUrlForNamespaceOrDefault(namespace);
        
        final ConnectOptions opts = 
                newConnectOptions(repositoryUrl, UUID.randomUUID(), null/* tx */);

        JettyResponseListener response = null;

        // Setup the request entity.
        {

            opts.addRequestParam(MAPGRAPH, MAPGRAPH_PUBLISH);
            opts.method = "POST";
        }

        try {

            checkResponseCode(response = doConnect(opts));

        } finally {

            if (response != null)
                response.abort();

        }
            
        return true;

    }

    /**
     * Drops the given namespace from the mapgraph runtime. If
     * the namespace was not registered in the runtime, no action is
     * performed. The return value is false in the latter case, true otherwise.
     * If the namespace that is passed in is null, the default
     * namespace will be used.
     * 
     * @return true if the namespace was published before and has been dropped, 
     *         false otherwise (i.e., in case no action has been taken)
     * @throws NoGPUAccelerationAvailableException
     */
    public boolean dropNamespaceFromMapgraph(final String namespace)
    throws Exception {

        assertMapgraphRuntimeAvailable();

        if (!namespacePublishedToMapgraph(namespace))
            return false; // nothing to be done
        
        final String repositoryUrl = 
                getSparqlEndpointUrlForNamespaceOrDefault(namespace);

        final ConnectOptions opts = 
            newConnectOptions(repositoryUrl, UUID.randomUUID(), null/* tx */);

        JettyResponseListener response = null;

        // Setup the request entity.
        {

            opts.addRequestParam(MAPGRAPH, MAPGRAPH_DROP);
            opts.method = "POST";
        }

        try {

            checkResponseCode(response = doConnect(opts));

        } finally {

            if (response != null)
                response.abort();

        }
        
        return true;
    }

    
    /**
     * Checks whether the given namespace has been published. If null is passed
     * in, the method performs a check for the default namespace.
     * 
     * @return true if the namespace is registered for acceleration,
     *         false otherwise
     * @throws NoGPUAccelerationAvailableException
     */
    public boolean namespacePublishedToMapgraph(final String namespace)
    throws Exception {
        
        assertMapgraphRuntimeAvailable();
        
        final String repositoryUrl = 
            getSparqlEndpointUrlForNamespaceOrDefault(namespace);
        
        final ConnectOptions opts = 
                newConnectOptions(repositoryUrl, UUID.randomUUID(), null/* tx */);

        JettyResponseListener response = null;

        // Setup the request entity.
        {

            opts.setAcceptHeader("Accept: text/plain");
            opts.addRequestParam(MAPGRAPH, CHECK_PUBLISHED);
            opts.method = "POST";
        }

        try {

            checkResponseCode(response = doConnect(opts));

            final String responseBody = response.getResponseBody();
            
            return responseBody!=null && responseBody.contains("true");

        } finally {

            if (response != null)
                response.abort();

        }
                    
        
    }
    
    /**
     * Resets the mapgraph runtime for the compute mode.
     * 
     * @param computeMode the desired compute mode
     */
    public void resetMapgraphRuntime(final ComputeMode computeMode)
    throws Exception {

        assertMapgraphRuntimeAvailable();

        if (computeMode==null) {
            throw new IllegalArgumentException("Compute mode must not be null");
        }

        final String repositoryUrl = 
                getSparqlEndpointUrlForNamespaceOrDefault(null /* default namespace */);

        final ConnectOptions opts = 
            newConnectOptions(repositoryUrl, UUID.randomUUID(), null/* tx */);

        JettyResponseListener response = null;

        // Setup the request entity.
        {

            opts.addRequestParam(MAPGRAPH, MAPGRAPH_RESET);
            opts.addRequestParam(COMPUTE_MODE, computeMode.toString());
            opts.method = "POST";
        }

        try {

            checkResponseCode(response = doConnect(opts));

        } finally {

            if (response != null)
                response.abort();

        }

    }
    
    /**
     * Returns the current status report for mapgraph.
     * 
     * @return the status report as human-readable string
     * @throws Exception
     */
    public String getMapgraphStatus() throws Exception {
        
        String repositoryUrl = baseServiceURL + "/status";
        
        /**
         * First reset the runtime
         */
        final ConnectOptions opts = 
            newConnectOptions(repositoryUrl, UUID.randomUUID(), null/* tx */);

        JettyResponseListener response = null;

        // Setup the request entity.
        {

            opts.addRequestParam(MAPGRAPH, "");
            opts.method = "GET";
        }

        try {

            response = doConnect(opts);
            return response.getResponseBody();
            
        } finally {

            if (response != null)
                response.abort();

        }
        
    }
    
    /**
     * Checks whether the mapgraph runtime is available.
     * @return
     */
    public boolean mapgraphRuntimeAvailable() throws Exception {

        final String repositoryUrl = 
            getSparqlEndpointUrlForNamespaceOrDefault(null /* default namespace */);

        /**
         * First reset the runtime
         */
        final ConnectOptions opts = 
            newConnectOptions(repositoryUrl, UUID.randomUUID(), null/* tx */);

        JettyResponseListener response = null;

        // Setup the request entity.
        {

            opts.addRequestParam(MAPGRAPH, MAPGRAPH_CHECK_RUNTIME_AVAILABLE);
            opts.method = "POST";
        }

        try {

            response = doConnect(opts);
            
            return response.getStatus()==200 /* HTTP OK */;
            
        } finally {

            if (response != null)
                response.abort();

        }

    }
    
    void assertMapgraphRuntimeAvailable() throws Exception {
        if (!mapgraphRuntimeAvailable()) 
            throw new NoGPUAccelerationAvailable();
    }


    
    /**
     * Return the effective configuration properties for the named data set.
     * <p>
     * Note: While it is possible to change some configuration options are a
     * data set has been created, many aspects of a "data set" configuration are
     * "baked in" when the data set is created and can not be changed. For this
     * reason, no general purpose mechanism is being offered to change the
     * properties for a configured data set instance.
     * 
     * @param namespace
     *            The name of the data set.
     * 
     * @return The effective configuration properties for that named data set.
     * 
     * @throws Exception
     */
    public Properties getRepositoryProperties(final String namespace) throws Exception {

        return getRepositoryProperties(namespace, UUID.randomUUID());

    }

    public Properties getRepositoryProperties(final String namespace, final UUID uuid) throws Exception {

        final String sparqlEndpointURL = getRepositoryBaseURLForNamespace(namespace);

        final ConnectOptions opts = newConnectOptions(sparqlEndpointURL + "/properties", uuid/* queryId */,
                null/* txId */);

        opts.method = "GET";

        JettyResponseListener response = null;

        opts.setAcceptHeader(ConnectOptions.MIME_PROPERTIES_XML);
        boolean consumeNeeded = true;
        try {

            checkResponseCode(response = doConnect(opts));

            final String contentType = response.getContentType();

            if (contentType == null)
                throw new RuntimeException("Not found: Content-Type");

            final MiniMime mimeType = new MiniMime(contentType);

            final PropertiesFormat format = PropertiesFormat.forMIMEType(mimeType.getMimeType());

            if (format == null)
                throw new IOException(
                        "Could not identify format for service response: serviceURI=" + sparqlEndpointURL +
                                ", contentType=" + contentType + " : response=" + response.getResponseBody());

            final PropertiesParserFactory factory = PropertiesParserRegistry.getInstance().get(format);

            if (factory == null)
                throw new RuntimeException(
                        "ParserFactory not found: Content-Type=" + contentType + ", format=" + format);

            final PropertiesParser parser = factory.getParser();

            final Properties properties = parser.parse(response.getInputStream());

            consumeNeeded = false;

            return properties;
        } catch (Exception e) {
            consumeNeeded = !InnerCause.isInnerCause(e, HttpException.class);
            throw e;
        } finally {

            if (response != null && consumeNeeded)
                response.abort();

        }

    }
    
    /**
     * Initiate an online backup using the {@link com.bigdata.rdf.sail.webapp.BackupServlet}.
     * 
     * 
     * @param file  -- The name of the file for the backup. (default = "backup.jnl")
     * @param compress -- Use compression for the snapshot (default = false)
     * @param block  -- Block on the response (default = true)
     * 
     * @see https://jira.blazegraph.com/browse/BLZG-1727
     */
	public void onlineBackup(final String file, final boolean compress,
			final boolean block) throws Exception {
		
		/**
		 * Use copies of these from {@link com.bigdata.rdf.sail.webapp.BackupServlet}
		 * to avoid introducing cyclical dependency with bigdata-core.
		 * 
		 */
		
		final String COMPRESS = "compress";

		final String FILE = "file";

		final String BLOCK = "block";
		
        final ConnectOptions opts = newConnectOptions(baseServiceURL + "/backup", UUID.randomUUID(), null/* tx */);

        JettyResponseListener response = null;

        // Setup the request entity.
        {

        	opts.addRequestParam(FILE, file);
        	opts.addRequestParam(COMPRESS, Boolean.toString(compress));
        	opts.addRequestParam(BLOCK, Boolean.toString(block));

            opts.method = "POST";
        }

        try {

            checkResponseCode(response = doConnect(opts));
        } finally {
            if (response != null)
                response.abort();

        }
    	
    }
    
    /**
     * 
     * Initiate the data loader for a namespace within the a NSS
     * 
     * @param properties
     *            The properties for the DataLoader Servlet
     * 
     * @throws Exception
     * 
     * @see BLZG-1713
     */
    public void doDataLoader(final Properties properties)
            throws Exception {

        if (properties == null)
            throw new IllegalArgumentException();
        
        final Properties tmp = PropertyUtil.flatCopy(properties);

        /*
         * Note: This operation does not currently permit embedding into a
         * read/write tx.
         */
        final ConnectOptions opts = newConnectOptions(baseServiceURL + "/dataloader", UUID.randomUUID(), null/* tx */);

        JettyResponseListener response = null;

        // Setup the request entity.
        {

            final PropertiesFormat format = PropertiesFormat.XML;

            final ByteArrayOutputStream baos = new ByteArrayOutputStream();

            final PropertiesWriter writer = PropertiesWriterRegistry.getInstance().get(format).getWriter(baos);

            writer.write(tmp);

            final byte[] data = baos.toByteArray();

            final ByteArrayEntity entity = new ByteArrayEntity(data);

            entity.setContentType(format.getDefaultMIMEType());

            opts.entity = entity;
            
            opts.method = "POST";

        }

        try {

            checkResponseCode(response = doConnect(opts));
        } finally {
            if (response != null)
                response.abort();

        }

    }

    /**
     * Connect to a SPARQL end point (GET or POST query only).
     * 
     * @param opts
     *            The connection options.
     * 
     * @return The connection.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/619">
     *      RemoteRepository class should use application/x-www-form-urlencoded
     *      for large POST requests </a>
     */
    public JettyResponseListener doConnect(final ConnectOptions opts) throws Exception {

        assertHttpClientRunning();

        /*
         * Generate the fully formed and encoded URL.
         */

        // The requestURL (w/o URL query parameters).
        final String requestURL = opts.getRequestURL(getContextPath(), getUseLBS());

        final StringBuilder urlString = new StringBuilder(requestURL);

        /*
         * FIXME (***) Why are we using one approach to add the parameters here
         * and then a different approach if we do a POST? Either one or the
         * other I think. Try moving this into an else {} block below (if not a
         * POST, then add query parameters).
         */
        ConnectOptions.addQueryParams(urlString, opts.requestParams);

        final boolean isLongRequestURL = urlString.length() > getMaxRequestURLLength();

        if (isLongRequestURL && opts.method.equals("POST") && opts.entity == null) {

            /*
             * URL is too long. Reset the URL to just the service endpoint and
             * use application/x-www-form-urlencoded entity instead. Only in
             * cases where there is not already a request entity (SPARQL query
             * and SPARQL update).
             */

            urlString.setLength(0);
            urlString.append(requestURL);

            opts.entity = ConnectOptions.getFormEntity(opts.requestParams);

        } else if (isLongRequestURL && opts.method.equals("GET") && opts.entity == null) {

            /*
             * Convert automatically to a POST if the request URL is too long.
             * 
             * Note: [opts.entity == null] should always be true for a GET so
             * this bit is a paranoia check.
             */

            opts.method = "POST";

            urlString.setLength(0);
            urlString.append(requestURL);

            opts.entity = ConnectOptions.getFormEntity(opts.requestParams);

        }

        if (log.isDebugEnabled()) {
            log.debug("*** Request ***");
            log.debug(requestURL);
            log.debug(opts.method);
            log.debug("query=" + opts.getRequestParam("query"));
            log.debug(urlString.toString());
        }

        Request request = null;
        try {

            request = (HttpRequest) newRequest(urlString.toString(), opts.method);

            if (opts.requestHeaders != null) {

                for (Map.Entry<String, String> e : opts.requestHeaders.entrySet()) {

                    request.header(e.getKey(), e.getValue());

                    if (log.isDebugEnabled())
                        log.debug(e.getKey() + ": " + e.getValue());

                }

            }

            if (opts.entity != null) {

                final EntityContentProvider cp = new EntityContentProvider(opts.entity);

                request.content(cp, cp.getContentType());

            }

            final long queryTimeoutMillis;
            {
                final String s = opts.getHeader(HTTP_HEADER_BIGDATA_MAX_QUERY_MILLIS);

                queryTimeoutMillis = s == null ? -1L : StringUtil.toLong(s);
            }

            final JettyResponseListener listener = new JettyResponseListener(request, queryTimeoutMillis);

            // Note: Send with a listener is non-blocking.
            request.send(listener);

            return listener;

        } catch (Throwable t) {
            /*
             * If something goes wrong, then close the http connection.
             * Otherwise, the connection will be closed by the caller.
             */
            try {

                if (request != null)
                    request.abort(t);

            } catch (Throwable t2) {
                log.warn(t2); // ignored.
            }
            throw new RuntimeException(requestURL + " : " + t, t);
        }

    }

    public Request newRequest(final String uri, final String method) {

        if (httpClient == null)
            throw new IllegalArgumentException();

        assertHttpClientRunning();

        return httpClient.newRequest(uri).method(getMethod(method));

    }

    private void assertHttpClientRunning() {

        if (httpClient.isStopped() || httpClient.isStopping())
            throw new IllegalStateException("The HTTPClient has been stopped");

    }

    HttpMethod getMethod(final String method) {
        if (method.equals("GET")) {
            return HttpMethod.GET;
        } else if (method.equals("POST")) {
            return HttpMethod.POST;
        } else if (method.equals("DELETE")) {
            return HttpMethod.DELETE;
        } else if (method.equals("PUT")) {
            return HttpMethod.PUT;
        } else {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Return the {@link ConnectOptions} which will be used by default for the
     * SPARQL end point for a QUERY or other idempotent operation.
     * 
     * @param sparqlEndpointURL
     *            The SPARQL end point.
     * @param uuid
     *            The unique identifier for the request that may be used to
     *            CANCEL the request.
     * @param tx
     *            A transaction that will isolate the operation (optional).
     */
    final protected ConnectOptions newQueryConnectOptions(final String sparqlEndpointURL, final UUID uuid,
            final IRemoteTx tx) {

        final ConnectOptions opts = newConnectOptions(sparqlEndpointURL, uuid, tx);

        opts.method = getQueryMethod();

        opts.update = false;

        return opts;

    }

    /**
     * Return the {@link ConnectOptions} which will be used by default for the
     * SPARQL end point for an UPDATE or other non-idempotant operation.
     * 
     * @param sparqlEndpointURL
     *            The SPARQL end point.
     * @param uuid
     *            The unique identifier for the request that may be used to
     *            CANCEL the request.
     * @param tx
     *            A transaction that will isolate the operation (optional).
     */
    final protected ConnectOptions newUpdateConnectOptions(final String sparqlEndpointURL, final UUID uuid,
            final IRemoteTx tx) {

        final ConnectOptions opts = newConnectOptions(sparqlEndpointURL, uuid, tx);

        opts.method = "POST";

        opts.update = true;

        return opts;

    }

    // /**
    // * Return the {@link ConnectOptions} which will be used by default for the
    // * SPARQL end point.
    // */
    // final protected ConnectOptions newConnectOptions() {
    //
    // return mgr.newConnectOptions(sparqlEndpointURL);
    //
    // }

    /**
     * Return the {@link ConnectOptions} which will be used by default for the
     * specified service URL.
     * <p>
     * There are three cases:
     * <dl>
     * <dt>The operation is not isolated by a transaction</dt>
     * <dd>This will return a {@link RemoteRepository} that DOES NOT specify a
     * timestamp to be used for read or write operations. For read operations,
     * this will cause it to use the default view of the namespace (as
     * configured on the server) and that will always be non-blocking (either
     * reading against the then current lastCommitTime on the database or
     * reading against an explicit read lock). For write operations, this will
     * cause it to use the UNISOLATED view of the namespace.</dd>
     * <dt>The operation is isolated by a read/write transaction</dt>
     * <dd>This will return a {@link RemoteRepository} which specifies the
     * transaction identifier (txId) for both read and write operations. This
     * ensures that they both have the same view of the write set of the
     * transaction (we can not use the readsOnCommitTime for read operations
     * because writes on the transaction are not visible unless we use the
     * txId).</dd>
     * <dt>The operation is isolated by a read-only transaction</dt>
     * <dd>This will return a {@link RemoteRepository} which use the
     * readsOnCommitTime for the transaction. This provides snapshot isolation
     * without any overhead and is also compatible with HA (where the
     * transaction management is performed on the leader and the followers are
     * not be aware of the txIds)</dd>
     * </dl>
     * 
     * @param serviceURL
     *            The URL of the service for the request.
     * @param uuid
     *            The unique identifier for the request that may be used to
     *            CANCEL the request.
     * @param tx
     *            A transaction that will isolate the operation (optional).
     */
    ConnectOptions newConnectOptions(final String serviceURL, final UUID uuid, final IRemoteTx tx) {

        final ConnectOptions opts = new ConnectOptions(serviceURL);

        if (tx != null) {

            /*
             * Some kind of transaction.
             */

            if (tx.isReadOnly()) {

                /*
                 * A read-only transaction.
                 * 
                 * FIXME This will not work for scale-out. We need to specify
                 * the txId itself.
                 */
                opts.addRequestParam("timestamp", Long.toString(tx.getReadsOnCommitTime()));

            } else {

                /*
                 * A read/write transaction. We must use the txId to have the
                 * correct isolation.
                 */

                opts.addRequestParam("timestamp", Long.toString(tx.getTxId()));

            }

        }

        if (uuid != null) {
            /**
             * Associate requests with a UUID so they may be cancelled.
             * 
             * @see #1254 (All REST API operations should be cancelable from
             *      both REST API and workbench.)
             */
            opts.addRequestParam(QUERYID, uuid.toString());
        }

        return opts;

    }

    /**
     * Builds a graph from an RDF result set (statements, not binding sets).
     * 
     * @param response
     *            The connection from which to read the results.
     * 
     * @return The graph
     * 
     * @throws Exception
     *             If anything goes wrong.
     */
    GraphQueryResult graphResults(final ConnectOptions opts, final UUID queryId, final IPreparedQueryListener listener)
            throws Exception {

        // The listener handling the http response.
        JettyResponseListener response = null;
        // Incrementally parse the response in another thread.
        BackgroundGraphResult result = null;
        try {

            response = doConnect(opts);

            checkResponseCode(response);

            final String baseURI = "";

            final String contentType = response.getContentType();

            if (contentType == null)
                throw new RuntimeException("Not found: Content-Type");

            final MiniMime mimeType = new MiniMime(contentType);

            final RDFFormat format = RDFFormat.forMIMEType(mimeType.getMimeType());

            if (format == null)
                throw new IOException(
                        "Could not identify format for service response: serviceURI=" + opts.getBestRequestURL() +
                                ", contentType=" + contentType + " : response=" + response.getResponseBody());

            final RDFParserFactory factory = RDFParserRegistry.getInstance().get(format);

            if (factory == null)
                throw new RuntimeException(
                        "RDFParserFactory not found: Content-Type=" + contentType + ", format=" + format);

            final RDFParser parser = factory.getParser();

            // TODO See #1055 (Make RDFParserOptions configurable)
            parser.setValueFactory(new ValueFactoryImpl());

            parser.setVerifyData(true);

            parser.setStopAtFirstError(true);

            parser.setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);
            /**
             * Note: The default charset depends on the MIME Type. The [charset]
             * MUST be [null] if the MIME Type is binary since this effects
             * whether a Reader or InputStream will be used to construct and
             * apply the RDF parser.
             * 
             * @see <a href="http://trac.blazegraph.com/ticket/920" > Content
             *      negotiation orders accept header scores in reverse </a>
             */
            Charset charset = format.getCharset();// Charset.forName(UTF8);
            try {

                final String encoding = response.getContentEncoding();
                if (encoding != null)
                    charset = Charset.forName(encoding);
            } catch (IllegalCharsetNameException e) {
                // work around for Joseki-3.2
                // Content-Type: application/rdf+xml;
                // charset=application/rdf+xml
            }

            final BackgroundGraphResult tmp = new BackgroundGraphResult(parser, response.getInputStream(), charset,
                    baseURI) {

                final AtomicBoolean notDone = new AtomicBoolean(true);

                @Override
                public boolean hasNext() throws QueryEvaluationException {

                    final boolean hasNext = super.hasNext();

                    if (hasNext == false) {

                        notDone.set(false);

                    }

                    return hasNext;

                }

                @Override
                public void close() throws QueryEvaluationException {

                    try {

                        super.close();

                    } finally {

                        if (notDone.compareAndSet(true, false)) {

                            try {
                                cancel(queryId);
                            } catch (Exception ex) {
                                log.warn(ex);
                            }

                        }

                        if (listener != null) {
                            listener.closed(queryId);
                        }

                    }

                };

            };

            /*
             * Note: Asynchronous execution. Typically does not even start
             * running until after we leave this method!
             */
            executor.execute(tmp);

            // The executor accepted the task for execution (at some point).
            result = tmp;

            /*
             * Result will be asynchronously produced.
             * 
             * Note: At this point the caller is responsible for calling close()
             * on this object to clean up the resources associated with this
             * request.
             */
            return result;

        } finally {

            if (response != null && result == null) {
                /*
                 * This code path only handles errors. We have a response, but
                 * we were not able to generate the asynchronous [result]
                 * object.
                 */
                response.abort();

                try {
                    /*
                     * POST back to the server in an attempt to cancel the
                     * request if already executing on the server.
                     */
                    cancel(queryId);
                } catch (Exception ex) {
                    log.warn(ex);
                }

                if (listener != null) {
                    listener.closed(queryId);
                }
            }

        }

    }

    /**
     * Processing the response for a SPARQL UPDATE request.
     * <p>
     * Note: This is not compatible with the MONITOR option. That option
     * requires the client to parse the response body to figure out whether or
     * not the UPDATE operation was successful.
     * 
     * @param response
     *            The connection from which to read the results.
     * 
     * @throws Exception
     *             If anything goes wrong.
     * 
     * @see <a href="http://trac.bigdata.com/ticket/1255" > RemoteRepository
     *      does not CANCEL a SPARQL UPDATE if there is a client error </a>
     */
    void sparqlUpdateResults(final ConnectOptions opts, final UUID queryId, final IPreparedQueryListener listener)
            throws Exception {

        JettyResponseListener response = null;
        try {

            // Note: No response body is expected.

            response = doConnect(opts);

            checkResponseCode(response);

        } finally {

            if (response == null) {
                try {
                    /*
                     * Some error prevented our obtaining a response.
                     * 
                     * POST back to the server in an attempt to cancel the
                     * request if already executing on the server.
                     */
                    cancel(queryId);
                } catch (Exception ex) {
                    log.warn(ex);
                }
            } else {
                /*
                 * Note: We are not reading anything from the response so I
                 * THINK that we do not need to call listener.abort(). If we do
                 * need to call this, then we might need to distinguish between
                 * a normal response and when we read the response entity.
                 */
                // response.abort();
            }
            if (listener != null) {
                // Notify client-side listener.
                listener.closed(queryId);
            }

        }

    }

    /**
     * Cancel a query running remotely on the server.
     * 
     * @param queryID
     *            the UUID of the query to cancel
     */
    public void cancel(final UUID queryId) throws Exception {

        if (queryId == null)
            return;

        /*
         * Note: The CANCEL request reuses the same parameter name ("queryId")
         * to identify the UUIDs of the request(s) that should be cancelled.
         * This means that we need to be careful on the server that the CANCEL
         * request does not get registered under the UUID of the request to be
         * canceled and thus cause itself to be cancelled....
         */
        final ConnectOptions opts = newUpdateConnectOptions(baseServiceURL, queryId, null/* txId */);

        opts.addRequestParam("cancelQuery");

        // Note: handled above.
        // opts.addRequestParam(QUERYID, queryId.toString());

        JettyResponseListener response = null;
        try {
            // Issue request, check response status code.
            checkResponseCode(response = doConnect(opts));
        } finally {
            /*
             * Ensure that the http response entity is consumed so that the http
             * connection will be released in a timely fashion.
             */
            if (response != null)
                response.abort();

        }

    }

    /**
     * List the currently running queries on the server
     * 
     */
    public Collection<RunningQuery> showQueries() throws Exception {

        final ConnectOptions opts = newUpdateConnectOptions(baseServiceURL, null, null/* txId */);

        opts.addRequestParam(SHOW_QUERIES);

        opts.setAcceptHeader(IMimeTypes.MIME_APPLICATION_JSON);

        JettyResponseListener response = null;

        try {
            // Issue request, check response status code.
            checkResponseCode(response = doConnect(opts));

            final String contentType = response.getContentType();

            if (!IMimeTypes.MIME_APPLICATION_JSON.equals(contentType))
                throw new RuntimeException("Expected MIME_TYPE " + IMimeTypes.MIME_APPLICATION_JSON +
                        " but received : " + contentType + ".");

            final InputStream is = response.getInputStream();

            final List<RunningQuery> runningQueries = JsonHelper.readRunningQueryList(is);

            return runningQueries;

        } finally {
            /*
             * Ensure that the http response entity is consumed so that the http
             * connection will be released in a timely fashion.
             */
            if (response != null)
                response.abort();

        }

    }

    /**
     * Extracts the solutions from a SPARQL query.
     * 
     * @param response
     *            The connection from which to read the results.
     * @param listener
     *            The listener to notify when the query result has been closed
     *            (optional).
     * 
     * @return The results.
     * 
     * @throws Exception
     *             If anything goes wrong.
     */
    public TupleQueryResult tupleResults(final ConnectOptions opts, final UUID queryId,
            final IPreparedQueryListener listener) throws Exception {

        // listener handling the http response.
        JettyResponseListener response = null;
        // future for parsing that response (in the background).
        FutureTask<Void> ft = null;
        // iteration pattern returned to caller. once they hold this they are
        // responsible for cleaning up the request by calling close().
        TupleQueryResultImpl tqrImpl = null;
        try {

            response = doConnect(opts);

            checkResponseCode(response);

            final String contentType = response.getContentType();

            final MiniMime mimeType = new MiniMime(contentType);

            final TupleQueryResultFormat format = TupleQueryResultFormat.forMIMEType(mimeType.getMimeType());

            if (format == null)
                throw new IOException(
                        "Could not identify format for service response: serviceURI=" + opts.getBestRequestURL() +
                                ", contentType=" + contentType + " : response=" + response.getResponseBody());

            final TupleQueryResultParserFactory parserFactory = TupleQueryResultParserRegistry.getInstance()
                    .get(format);

            if (parserFactory == null)
                throw new IOException("No parser for format for service response: serviceURI=" +
                        opts.getBestRequestURL() + ", contentType=" + contentType + ", format=" + format +
                        " : response=" + response.getResponseBody());

            final TupleQueryResultParser parser = parserFactory.getParser();

            final BackgroundTupleResult result = new BackgroundTupleResult(parser, response.getInputStream());

            final MapBindingSet bindings = new MapBindingSet();

            final InsertBindingSetCursor cursor = new InsertBindingSetCursor(result, bindings);

            // Wrap as FutureTask so we can cancel.
            ft = new FutureTask<Void>(result, null/* result */);

            /*
             * Submit task for execution. It will asynchronously consume the
             * response, pumping solutions into the cursor.
             * 
             * Note: Can throw a RejectedExecutionException!
             */
            executor.execute(ft);

            /*
             * Note: This will block until the binding names are received, so it
             * can not be done until we submit the BackgroundTupleResult for
             * execution.
             */
            final List<String> list = new ArrayList<String>(result.getBindingNames());

            /*
             * The task was accepted by the executor. Wrap with iteration
             * pattern. Once this object is returned to the caller they are
             * responsible for calling close() to provide proper error cleanup
             * of the resources associated with the request.
             */
            final TupleQueryResultImpl tmp = new TupleQueryResultImpl(list, cursor) {

                private final AtomicBoolean notDone = new AtomicBoolean(true);

                @Override
                public boolean hasNext() throws QueryEvaluationException {

                    final boolean hasNext = super.hasNext();

                    if (hasNext == false) {

                        notDone.set(false);

                    }

                    return hasNext;

                }

                @Override
                public void handleClose() throws QueryEvaluationException {

                    try {

                        super.handleClose();

                    } finally {

                        if (notDone.compareAndSet(true, false)) {

                            try {
                                cancel(queryId);
                            } catch (Exception ex) {
                                log.warn(ex);
                            }

                        }

                        /*
                         * Notify the listener.
                         */
                        if (listener != null) {
                            listener.closed(queryId);
                        }

                    }

                };

            };

            /*
             * Return the tuple query result listener to the caller. They now
             * have responsibility for calling close() on that object in order
             * to close the http connection and release the associated
             * resources.
             */
            return (tqrImpl = tmp);

        } finally {

            if (response != null && tqrImpl == null) {
                /*
                 * Error handling code path. We have an http response listener
                 * but we were not able to setup the tuple query result
                 * listener.
                 */
                if (ft != null) {
                    /*
                     * We submitted the task to parse the response. Since the
                     * code is not returning normally (tqrImpl:=null) we cancel
                     * the FutureTask for the background parse of that response.
                     */
                    ft.cancel(true/* mayInterruptIfRunning */);
                }
                // Abort the http response handling.
                response.abort();
                try {
                    /*
                     * POST back to the server to cancel the request in case it
                     * is still running on the server.
                     */
                    cancel(queryId);
                } catch (Exception ex) {
                    log.warn(ex);
                }
                if (listener != null) {
                    listener.closed(queryId);
                }
            }

        }

    }

    /**
     * Parse a SPARQL result set for an ASK query.
     * 
     * @param response
     *            The connection from which to read the results.
     * 
     * @return <code>true</code> or <code>false</code> depending on what was
     *         encoded in the SPARQL result set.
     * 
     * @throws Exception
     *             If anything goes wrong, including if the result set does not
     *             encode a single boolean value.
     */
    public boolean booleanResults(final ConnectOptions opts, final UUID queryId, final IPreparedQueryListener listener)
            throws Exception {

        JettyResponseListener response = null;
        Boolean result = null;
        try {

            response = doConnect(opts);

            checkResponseCode(response);

            final String contentType = response.getContentType();

            final MiniMime mimeType = new MiniMime(contentType);

            final BooleanQueryResultFormat format = BooleanQueryResultFormat.forMIMEType(mimeType.getMimeType());

            if (format == null)
                throw new IOException(
                        "Could not identify format for service response: serviceURI=" + opts.getBestRequestURL() +
                                ", contentType=" + contentType + " : response=" + response.getResponseBody());

            final BooleanQueryResultParserFactory factory = BooleanQueryResultParserRegistry.getInstance().get(format);

            if (factory == null)
                throw new RuntimeException("No factory for Content-Type: " + contentType);

            final BooleanQueryResultParser parser = factory.getParser();

            final InputStream is = response.getInputStream();
            try {
                result = parser.parse(is);
                return result;
            } finally {
                is.close();
            }

        } finally {

            if (result == null) {
                /*
                 * Error handling path. We issued the request, but were not able
                 * to parse out the response.
                 */
                if (response != null) {
                    // Make sure the response listener is closed.
                    response.abort();
                }
                try {
                    /*
                     * POST request to server to cancel query in case it is
                     * still running.
                     */
                    cancel(queryId);
                } catch (Exception ex) {
                    log.warn(ex);
                }
            }

            if (listener != null) {
                listener.closed(queryId);
            }

        }

    }

    // /**
    // * Counts the #of results in a SPARQL result set.
    // *
    // * @param response
    // * The connection from which to read the results.
    // *
    // * @return The #of results.
    // *
    // * @throws Exception
    // * If anything goes wrong.
    // */
    // protected long countResults(final JettyResponseListener response) throws
    // Exception {
    //
    // try {
    //
    // final String contentType = response.getContentType();
    //
    // final MiniMime mimeType = new MiniMime(contentType);
    //
    // final TupleQueryResultFormat format = TupleQueryResultFormat
    // .forMIMEType(mimeType.getMimeType());
    //
    // if (format == null)
    // throw new IOException(
    // "Could not identify format for service response: serviceURI="
    // + sparqlEndpointURL + ", contentType=" + contentType
    // + " : response=" + response.getResponseBody());
    //
    // final TupleQueryResultParserFactory factory =
    // TupleQueryResultParserRegistry
    // .getInstance().get(format);
    //
    // if (factory == null)
    // throw new RuntimeException("No factory for Content-Type: " +
    // contentType);
    //
    // final TupleQueryResultParser parser = factory.getParser();
    //
    // final AtomicLong nsolutions = new AtomicLong();
    //
    // parser.setTupleQueryResultHandler(new TupleQueryResultHandlerBase() {
    // // Indicates the end of a sequence of solutions.
    // @Override
    // public void endQueryResult() {
    // // connection close is handled in finally{}
    // }
    //
    // // Handles a solution.
    // @Override
    // public void handleSolution(final BindingSet bset) {
    // if (log.isDebugEnabled())
    // log.debug(bset.toString());
    // nsolutions.incrementAndGet();
    // }
    //
    // // Indicates the start of a sequence of Solutions.
    // @Override
    // public void startQueryResult(List<String> bindingNames) {
    // }
    // });
    //
    // parser.parse(response.getInputStream());
    //
    // if (log.isInfoEnabled())
    // log.info("nsolutions=" + nsolutions);
    //
    // // done.
    // return nsolutions.longValue();
    //
    // } finally {
    //
    // if (response != null) {
    // response.abort();
    // }
    //
    // }
    //
    // }

}
