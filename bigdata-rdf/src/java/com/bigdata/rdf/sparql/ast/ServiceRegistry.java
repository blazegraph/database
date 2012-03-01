package com.bigdata.rdf.sparql.ast;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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

    private static ConcurrentMap<URI, ServiceFactory> services;

    /**
     * Pre-register well known services.
     */
    static {
        
        services = new ConcurrentHashMap<URI, ServiceFactory>();
        
        ServiceRegistry.add(BD.SEARCH, new SearchServiceFactory());

    }
    
    public static boolean containsService(final URI functionUri){
        
        return services.containsKey(functionUri);
        
    }

    public static final void add(final URI serviceURI, final ServiceFactory factory) {

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
    public static final boolean remove(final URI serviceURI) {

        return services.remove(serviceURI) != null;

    }

    public static final void addAlias(final URI serviceURI, final URI aliasURI) {

        if (!services.containsKey(serviceURI)) {

            throw new UnsupportedOperationException("ServiceURI:" + serviceURI
                    + " not present.");

        }

        if (services.putIfAbsent(aliasURI, services.get(serviceURI)) != null) {

            throw new UnsupportedOperationException("Already declared.");

        }
    }

    /**
     * Resolve a {@link ServiceCall} for a service {@link URI}.
     * 
     * @param store
     *            The {@link AbstractTripleStore}.
     * @param functionURI
     *            The function URI.
     * @param scalarValues
     *            Scalar values for the function (optional). This is used for
     *            things like the <code>separator</code> in GROUP_CONCAT.
     * @param args
     *            The function arguments.
     * 
     * @return The {@link ServiceCall} -or- <code>null</code> if there is no
     *         service registered for that {@link URI}.
     */
    public static final ServiceCall<? extends Object> toServiceCall(
            final AbstractTripleStore store, final URI serviceURI,
            final IGroupNode<IGroupMemberNode> groupNode) {

        if (serviceURI == null)
            throw new IllegalArgumentException("serviceURI is null");

        final ServiceFactory f = services.get(serviceURI);

        if (f != null) {

            return f.create(store, groupNode, serviceURI, null/* exprImage */,
                    null/* prefixDecls */);
            
        }

        // Not found.
        return null;
        
    }

}
