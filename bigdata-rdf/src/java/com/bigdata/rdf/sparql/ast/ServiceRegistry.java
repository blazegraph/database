package com.bigdata.rdf.sparql.ast;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.openrdf.model.URI;

import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.sparql.ast.eval.SearchServiceFactory;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;

/**
 * Registry for service calls.
 * 
 * TODO We should restrict registration of services in the {@link BD#NAMESPACE}
 * to static initialization.
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
     * Convert a {@link FunctionNode} into an {@link IValueExpression}.
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
     * @return The {@link IValueExpression}.
     */
    public static final BigdataServiceCall toServiceCall(
            final AbstractTripleStore store, final URI serviceURI,
            final IGroupNode<IGroupMemberNode> groupNode) {

        if (serviceURI == null)
            throw new IllegalArgumentException("serviceURI is null");

        final ServiceFactory f = services.get(serviceURI);

        if (f == null) {
            /*
             * TODO If we eagerly translate FunctionNodes in the AST to IV value
             * expressions then we should probably attach a function which will
             * result in a runtime type error when it encounters value
             * expression for a function URI which was not known to the backend.
             * However, if we handle this translation lazily then this might not
             * be an issue.
             */
            throw new IllegalArgumentException("unknown service: " + serviceURI);
        }

        return f.create(store, groupNode);

    }

    /*
     * Skeleton of a mock ServiceCall implementation.
     */
//    static ServiceCall matchCal = new ServiceCall() {
//        @Override
//        public IAsynchronousIterator<IBindingSet[]> call(
//                IRunningQuery runningQueryx) {
//            IBindingSet[] solutions = new IBindingSet[vals.size()];
//            for (int i = 0; i < vals.size(); i++) {
//                solutions[i] = new ListBindingSet(
//                        new IVariable[] { (IVariable) subject },
//                        new IConstant[] { vals.get(i) });
//            }
//            final IChunkedOrderedIterator<IBindingSet> src3 = new ChunkedArrayIterator<IBindingSet>(
//                    solutions);
//            return new WrappedAsynchronousIterator<IBindingSet[], IBindingSet>(
//                    src3);
//        }
//    };

}
