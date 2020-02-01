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
package com.bigdata.rdf.sparql.ast.service;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.http.conn.ClientConnectionManager;
import org.eclipse.jetty.client.HttpClient;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.bop.join.BaseJoinStats;
import com.bigdata.rdf.graph.impl.bd.GASService;
import com.bigdata.rdf.sail.RDRHistoryServiceFactory;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.cache.DescribeServiceFactory;
import com.bigdata.rdf.sparql.ast.eval.GeoSpatialServiceFactory;
import com.bigdata.rdf.sparql.ast.eval.SampleServiceFactory;
import com.bigdata.rdf.sparql.ast.eval.SearchInSearchServiceFactory;
import com.bigdata.rdf.sparql.ast.eval.SearchServiceFactory;
import com.bigdata.rdf.sparql.ast.eval.SliceServiceFactory;
import com.bigdata.rdf.sparql.ast.eval.ValuesServiceFactory;
import com.bigdata.rdf.sparql.ast.service.history.HistoryServiceFactory;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.store.BDS;
import com.bigdata.service.fts.FTS;
import com.bigdata.service.fts.FulltextSearchServiceFactory;
import com.bigdata.service.geospatial.GeoSpatial;

import cutthecrap.utils.striterators.ReadOnlyIterator;

/**
 * Registry for service calls.
 *
 * @see <a
 *      href="https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=FederatedQuery">
 *      Federated Query and Custom Services</a>
 */
public class ServiceRegistry {

    /**
     * TODO Allow SPI pattern for override?
     */
    private static final ServiceRegistry DEFAULT = new ServiceRegistry();

    static public ServiceRegistry getInstance() {

        return DEFAULT;

    }

    /**
     * Primary {@link ServiceFactory} registration.
     */
    private final ConcurrentMap<URI, ServiceFactory> services;

    /**
     * Aliases for registered {@link ServiceFactory}s.
     */
    private final ConcurrentMap<URI/* from */, URI/* to */> aliases;

    /**
     * The set of registered {@link ServiceFactory}s is also maintained here for
     * fast, safe iteration by {@link #services()}.
     */
    private final CopyOnWriteArrayList<CustomServiceFactory> customServices;

    /**
     * The default {@link ServiceFactory} used for REMOTE SPARQL SERVICE end
     * points which are not otherwise registered.
     */
    private AtomicReference<ServiceFactory> defaultServiceFactoryRef;

    /**
     * Allowed service whitelist.
     */
    private final Set<String> serviceWhitelist = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    /**
     * Whether service whitelisting is enabled.
     */
    private boolean whitelistEnabled = false;

   /**
    * Service factory that always produces empty result.
    */
   private ServiceFactory nullServiceFactory = new NullServiceFactory();

    protected ServiceRegistry() {

        services = new ConcurrentHashMap<URI, ServiceFactory>();

        customServices = new CopyOnWriteArrayList<CustomServiceFactory>();

        aliases = new ConcurrentHashMap<URI, URI>();

        defaultServiceFactoryRef = new AtomicReference<ServiceFactory>(
                new RemoteServiceFactoryImpl(SPARQLVersion.SPARQL_11));

        // Add the Bigdata search service.
        add(BDS.SEARCH, new SearchServiceFactory());

        // Add the Geospatial search service.
        add(GeoSpatial.SEARCH, new GeoSpatialServiceFactory());

        // Add the external Solr search service
        add(FTS.SEARCH, new FulltextSearchServiceFactory());

        // Add the Bigdata search in search service.
        add(BDS.SEARCH_IN_SEARCH, new SearchInSearchServiceFactory());

        // Add the sample index service.
        add(SampleServiceFactory.SERVICE_KEY, new SampleServiceFactory());

        // Add the slice index service.
        add(SliceServiceFactory.SERVICE_KEY, new SliceServiceFactory());

        // Add the values service.
        add(ValuesServiceFactory.SERVICE_KEY, new ValuesServiceFactory());

        if (QueryHints.DEFAULT_DESCRIBE_CACHE) {

            add(new URIImpl(BD.NAMESPACE + "describe"),
                    new DescribeServiceFactory());

        }

        if (true) {

            /**
             * @see <a
             *      href="https://sourceforge.net/apps/trac/bigdata/ticket/607">
             *      HISTORY SERVICE </a>
             */
            add(new URIImpl(BD.NAMESPACE + "history"),
                    new HistoryServiceFactory());

            /**
             * Replacing with a history service using RDR instead of a custom
             * index.
             */
            add(new URIImpl(BD.NAMESPACE + "rdrhistory"),
                    new RDRHistoryServiceFactory());

        }

        // The Gather-Apply-Scatter RDF Graph Mining service.
        add(GASService.Options.SERVICE_KEY, new GASService());

    }

    /**
     * Set the default {@link ServiceFactory}. This will be used when the
     * serviceURI is not associated with an explicitly registered service. For
     * example, you can use this to control whether or not the service end point
     * is assumed to support <code>SPARQL 1.0</code> or <code>SPARQL 1.1</code>.
     *
     * @param defaultServiceFactory
     *            The default {@link ServiceFactory}.
     *
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     */
    public void setDefaultServiceFactory(
            final ServiceFactory defaultServiceFactory) {

        if (defaultServiceFactory == null)
            throw new IllegalArgumentException();

        this.defaultServiceFactoryRef.set(defaultServiceFactory);

    }

    public ServiceFactory getDefaultServiceFactory() {

        return defaultServiceFactoryRef.get();

    }

	public ServiceFactory getNullServiceFactory() {
		return nullServiceFactory;
	}

	/**
     * Register a service.
     *
     * @param serviceURI
     *            The service URI.
     * @param factory
     *            The factory to execute calls against that service.
     */
    public final void add(final URI serviceURI, final ServiceFactory factory) {

        synchronized (this) {

            if (aliases.containsKey(serviceURI)) {

                throw new UnsupportedOperationException("Already declared.");

            }

            if (services.putIfAbsent(serviceURI, factory) != null) {

                throw new UnsupportedOperationException("Already declared.");

            }

            if (factory instanceof CustomServiceFactory) {

                customServices.add((CustomServiceFactory) factory);

            }

        }

    }

    /**
     * Remove a service from the registry and/or set of known aliases.
     *
     * @param serviceURI
     *            The URI of the service -or- the URI of an alias registered
     *            using {@link #addAlias(URI, URI)}.
     *
     * @return <code>true</code> iff a service for that URI was removed.
     */
    public final boolean remove(final URI serviceURI) {

        boolean modified = false;

        synchronized (this) {

            if (aliases.remove(serviceURI) != null) {

                // removed an alias.
                modified = true;

            }

            // Remove the factory.
            final ServiceFactory factory = services.remove(serviceURI);

            if (factory != null) {

                modified = true;

                if(factory instanceof CustomServiceFactory) {

                    customServices.remove(factory);

                }

            }

        }

        return modified;

    }

    /**
     * Register one URI as an alias for another.
     *
     * @param serviceURI
     *            The URI of a service. It is expressly permitted to register an
     *            alias for a URI which does not have a registered
     *            {@link ServiceFactory}. This may be used to alias a remote URI
     *            which you want to intercept locally.
     * @param aliasURI
     *            The URI of an alias under which that service may be accessed.
     * @throws IllegalStateException
     *             if the <i>serviceURI</i> has already been registered as a
     *             alias (you must {@link #remove(URI)} the old alias before you
     *             can map it against a different <i>serviceURI</i>).
     * @throws IllegalStateException
     *             if the <i>aliasURI</i> has already been registered as a
     *             service (you can not mask an existing service registration).
     */
    public final void addAlias(final URI serviceURI, final URI aliasURI) {

        if (serviceURI == null)
            throw new IllegalArgumentException();

        if (aliasURI == null)
            throw new IllegalArgumentException();

        synchronized (this) {

            /*
             * Note: it is expressly permitted to register an alias for a URI
             * which does not have a registered ServiceFactory. This may be used
             * to alias a remote URI which you want to intercept locally.
             */

//            // Lookup the service.
//            final ServiceFactory service = services.get(serviceURI);
//
//            if (service == null) {
//
//                throw new IllegalStateException("No such service: uri="
//                        + serviceURI);
//
//            }

            if (services.containsKey(aliasURI)) {

                throw new IllegalStateException(
                        "Alias already registered as service: uri=" + aliasURI);

            }

            if (aliases.containsKey(aliasURI)) {

                throw new IllegalStateException(
                        "Alias already registered: uri=" + aliasURI);

            }

            aliases.put(aliasURI, serviceURI);

        }

    }

    /**
     * Add URL to service whitelist
     * @param URL the URL to add
     */
    public void addWhitelistURL(String URL) {
        serviceWhitelist.add(URL);
    }

    /**
     * Remove URL to service whitelist
     * @param URL the URL to remove
     */
    public void removeWhitelistURL(String URL) {
        serviceWhitelist.remove(URL);
    }

    /**
     * Set whitelist status.
     * @param enable true if enabled, false if disabled
     */
    public void setWhitelistEnabled(boolean enable) {
        whitelistEnabled = enable;
    }

    /**
     * Check if whitelisting is enabled
     */
    public boolean isWhitelistEnabled() {
        return whitelistEnabled;
    }

    /**
     * Return an {@link Iterator} providing a read-only view of the registered
     * {@link CustomServiceFactory}s.
     */
    public Iterator<CustomServiceFactory> customServices() {

        /*
         * Note: This relies on the copy-on-write array list for fast and
         * efficient traversal with snapshot isolation.
         */

        return new ReadOnlyIterator<CustomServiceFactory>(
                customServices.iterator());

    }

    /**
     * Return the {@link ServiceFactory} for that URI. If the {@link URI} is a
     * known alias, then it is resolved before looking up the
     * {@link ServiceFactory}.
     *
     * @param serviceURI
     *            The {@link URI}.
     *
     * @return The {@link ServiceFactory} if one is registered for that
     *         {@link URI}.
     */
    public ServiceFactory get(final URI serviceURI) {

        if (serviceURI == null) {
            throw new IllegalArgumentException("Null service URI");
        }
        checkServiceUri(serviceURI);
        final URI alias = aliases.get(serviceURI);

        if (alias != null) {

            return services.get(alias);

        }

        return services.get(serviceURI);

    }

    /**
     * Resolve a {@link ServiceCall} for a service {@link URI}. If a
     * {@link ServiceFactory} was registered for that <i>serviceURI</i>, then it
     * will be returned. Otherwise {@link #getDefaultServiceFactory()} is used
     * to obtain the {@link ServiceFactory} that will be used to create the
     * {@link ServiceCall} object for that end point.
     *
     * @param store
     *            The {@link AbstractTripleStore}.
     * @param cm
     *            The {@link ClientConnectionManager} will be used to make
     *            remote HTTP connections.
     * @param serviceURI
     *            The as-bound {@link URI} of the service end point.
     * @param serviceNode
     *            The AST model of the SERVICE clause.
     *
     * @return A {@link ServiceCall} for that service.
     */
    public final ServiceCall<? extends Object> toServiceCall(
            final AbstractTripleStore store, final HttpClient cm,
            URI serviceURI, final ServiceNode serviceNode, final BaseJoinStats stats) {

        if (serviceURI == null)
            throw new IllegalArgumentException();

        // Resolve URI in case it was an alias.
        final URI dealiasedServiceURI = aliases.get(serviceURI);

        if (dealiasedServiceURI != null) {

            // Use the de-aliased URI.
            serviceURI = dealiasedServiceURI;

        }

        ServiceFactory f = null;
        try {
            checkServiceUri(serviceURI);
        } catch(IllegalArgumentException e) {
            if (serviceNode.isSilent()) {
                f = nullServiceFactory;
            } else {
                throw e;
            }
        }
        if (f == null) {
            f = services.get(serviceURI);

            if (f == null) {

                f = getDefaultServiceFactory();

                if (f == null) {

                    // Should never be null at this point.
                    throw new AssertionError();

                }

            }
        }

        final ServiceCallCreateParams params = new ServiceCallCreateParamsImpl(
                serviceURI, store, serviceNode, cm, f.getServiceOptions(), stats);

        return f.create(params);

//        return f.create(store, serviceURI, serviceNode);

    }
    
    /**
     * Maps a URI to a service factory. If the URI is null or there is no custom
     * service, the default service factory (SPARQL 1.1 service) is returned.
     * 
     * @param serviceUri the URI for which we look up the service
     * @return the associated service factory or the default service factory
     *         as fallback
     */
    public ServiceFactory getServiceFactoryByServiceURI(URI serviceUri) {
       checkServiceUri(serviceUri);

       final ServiceFactory serviceFactory = 
          serviceUri==null ? 
          getDefaultServiceFactory() : services.get(serviceUri);
       
       return serviceFactory==null ? getDefaultServiceFactory() : serviceFactory;
       
    }

    private void checkServiceUri(final URI serviceUri) {
        if (!isWhitelistEnabled()) {
            return;
        }
        if (serviceWhitelist.contains(serviceUri.stringValue())) {
            return;
        }
        try {
            URL url = new URL(serviceUri.toString());
            URL serverWildcard = new URL(url.getProtocol(), url.getHost(), url.getPort(), "/");
            if (serviceWhitelist.contains(serverWildcard.toString() + "*")) {
                // Allow mask http://server.com/*
                return;
            }
            URL pathWildcard = new URL(url.getProtocol(), url.getHost(), url.getPort(), url.getPath());
            if (serviceWhitelist.contains(pathWildcard.toString() + '*')) {
                // Allow mask http://server.com/path*
                return;
            }
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Service URI " + serviceUri + " is not well-formed");
        }

        throw new IllegalArgumentException("Service URI " + serviceUri + " is not allowed");
    }

    private static class ServiceCallCreateParamsImpl implements ServiceCallCreateParams {

        private final URI serviceURI;
        private final AbstractTripleStore store;
        private final ServiceNode serviceNode;
        private final HttpClient cm;
        private final IServiceOptions serviceOptions;
        private final BaseJoinStats stats;

        public ServiceCallCreateParamsImpl(final URI serviceURI,
                final AbstractTripleStore store, final ServiceNode serviceNode,
                final HttpClient cm,
                final IServiceOptions serviceOptions,
                final BaseJoinStats stats) {

            this.serviceURI = serviceURI;

            this.store = store;

            this.serviceNode = serviceNode;

            this.cm = cm;

            this.serviceOptions = serviceOptions;
            
            this.stats = stats;

        }

        @Override
        public URI getServiceURI() {
            return serviceURI;
        }

        @Override
        public AbstractTripleStore getTripleStore() {
            return store;
        }

        @Override
        public ServiceNode getServiceNode() {
            return serviceNode;
        }

        @Override
        public HttpClient getClientConnectionManager() {
            return cm;
        }

        @Override
        public IServiceOptions getServiceOptions() {
            return serviceOptions;
        }
        
        @Override
        public BaseJoinStats getStats() {
           return stats;
        }

        @Override
        public String toString() {

            final StringBuilder sb = new StringBuilder();
            sb.append(getClass().getName());
            sb.append("{serviceURI=" + getServiceURI());
            sb.append(",serviceNode=" + getServiceNode());
            sb.append(",serviceOptions=" + getServiceOptions());
            sb.append(",tripleStore=" + getTripleStore());
            sb.append(",clientConnectionManager=" + getClientConnectionManager());
            sb.append("}");
            return sb.toString();

        }

    }

}
