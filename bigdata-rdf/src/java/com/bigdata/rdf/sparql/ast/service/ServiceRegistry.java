package com.bigdata.rdf.sparql.ast.service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.openrdf.model.URI;

import com.bigdata.rdf.sparql.ast.eval.SearchServiceFactory;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;

/**
 * Registry for service calls.
 */
public class ServiceRegistry {

    /**
     * TODO Allow SPI pattern for override?
     */
    private static ServiceRegistry DEFAULT = new ServiceRegistry();

    static public ServiceRegistry getInstance() {

        return DEFAULT;

    }

    private ConcurrentMap<URI, ServiceFactory> services;
    private ConcurrentMap<URI/*from*/, URI/*to*/> aliases;

    private AtomicReference<ServiceFactory> defaultServiceFactoryRef;
    
    protected ServiceRegistry() {

        services = new ConcurrentHashMap<URI, ServiceFactory>();

        aliases = new ConcurrentHashMap<URI, URI>();

        add(BD.SEARCH, new SearchServiceFactory());

        defaultServiceFactoryRef = new AtomicReference<ServiceFactory>(
                new RemoteServiceFactoryImpl(true/* isSparql11 */));
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
    
    public final void add(final URI serviceURI, final ServiceFactory factory) {

        if (aliases.containsKey(serviceURI)) {

            throw new UnsupportedOperationException("Already declared.");

        }

        if (services.putIfAbsent(serviceURI, factory) != null) {

            throw new UnsupportedOperationException("Already declared.");

        }

	}

    /**
     * Remove a service from the registry and/or set of known aliases.
     * 
     * @param serviceURI
     *            The service URI.
     * 
     * @return <code>true</code> iff a service for that URI was removed.
     */
    public final boolean remove(final URI serviceURI) {

        boolean modified = false;

        if (aliases.remove(serviceURI) != null) {

            modified = true;

        }

        if (services.remove(serviceURI) != null) {

            modified = true;

        }

        return modified;

    }

    /**
     * 
     * @param serviceURI
     *            The URI of a service which is already declared.
     * @param aliasURI
     *            The URI of an alias under which that service may be accessed.
     */
    public final void addAlias(final URI serviceURI, final URI aliasURI) {

        if (serviceURI == null)
            throw new IllegalArgumentException();

        if (aliasURI == null)
            throw new IllegalArgumentException();

        if (services.containsKey(serviceURI)) {

            throw new UnsupportedOperationException("ServiceURI:" + serviceURI
                    + " already registered.");

        }

        aliases.put(aliasURI, serviceURI);
        
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

        if (serviceURI == null)
            throw new IllegalArgumentException();
    
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
     * @param serviceURI
     *            The as-bound {@link URI} of the service end point.
     * @param serviceNode
     *            The AST model of the SERVICE clause.
     * 
     * @return A {@link ServiceCall} for that service.
     */
    public final ServiceCall<? extends Object> toServiceCall(
            final AbstractTripleStore store, URI serviceURI,
            final ServiceNode serviceNode) {

        if (serviceURI == null)
            throw new IllegalArgumentException();

        // Resolve URI in case it was an alias.
        final URI dealiasedServiceURI = aliases.get(serviceURI);

        if (dealiasedServiceURI != null) {
            
            // Use the de-aliased URI.
            serviceURI = dealiasedServiceURI;

        }
        
        ServiceFactory f = services.get(serviceURI);

        if (f == null) {

            f = getDefaultServiceFactory();

            if (f == null) {

                // Should never be null at this point.
                throw new AssertionError();

            }

        }

        return f.create(store, serviceURI, serviceNode);

    }

}
