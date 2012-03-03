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
 * 
 * TODO Should we restrict registration/management of services in the
 * {@link BD#NAMESPACE} to static initialization or otherwise secure the
 * registry?
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

    private AtomicReference<ServiceFactory> defaultServiceFactoryRef;
    
    protected ServiceRegistry() {

        services = new ConcurrentHashMap<URI, ServiceFactory>();

        add(BD.SEARCH, new SearchServiceFactory());

        defaultServiceFactoryRef = new AtomicReference<ServiceFactory>(
                new RemoteServiceFactoryImpl());
    }

    /**
     * Set the default {@link ServiceFactory}. This will be used when the
     * serviceURI is not associated with an explicitly registered service.
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
    
    public boolean containsService(final URI functionUri){
        
        return services.containsKey(functionUri);
        
    }

    public final void add(final URI serviceURI, final ServiceFactory factory) {

        if (services.putIfAbsent(serviceURI, factory) != null) {

            throw new UnsupportedOperationException("Already declared.");

	    }

	}

    /**
     * Return <code>true</code> iff a service for that URI was removed.
     * 
     * @param serviceURI
     *            The service URI.
     */
    public final boolean remove(final URI serviceURI) {

        return services.remove(serviceURI) != null;

    }

    public final void addAlias(final URI serviceURI, final URI aliasURI) {

        if (!services.containsKey(serviceURI)) {

            throw new UnsupportedOperationException("ServiceURI:" + serviceURI
                    + " not present.");

        }

        if (services.putIfAbsent(aliasURI, services.get(serviceURI)) != null) {

            throw new UnsupportedOperationException("Already declared.");

        }
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
            final AbstractTripleStore store, final URI serviceURI,
            final ServiceNode serviceNode) {

        if (serviceURI == null)
            throw new IllegalArgumentException("serviceURI is null");

        ServiceFactory f = services.get(serviceURI);

        if (f == null) {

            f = getDefaultServiceFactory();

            if( f == null ) {

                // Should never be null at this point.
                throw new AssertionError();
                
            }
            
        }

        return f.create(store, serviceURI, serviceNode);

    }

}
