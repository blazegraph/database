/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
/*
 * Created on Aug 18, 2010
 */

package com.bigdata.bop.controller;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.htree.HTree;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.BigdataServiceCall;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.ServiceCall;
import com.bigdata.rdf.sparql.ast.ServiceRegistry;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataOpenRDFBindingSetsResolverator;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Pipelined join with results from a {@link ServiceCall} invocation.
 * <p>
 * For each binding set presented, this operator executes the service joining
 * the solutions from the service against the source binding set. Since each
 * invocation of the service will (typically) produce the same solutions, this
 * operator should always be the first operator in a named subquery in order to
 * ensure that the service is invoked exactly once. The solutions written onto
 * the sink may then joined with other access paths before they reach the end of
 * the named subquery and are materialized (by the parent) on an {@link HTree}.
 * <p>
 * Any solutions produced by the service are copied to the default sink. The
 * service call will be cancelled (by closing its iterator) if the parent query
 * is cancelled.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 *         TODO Support invocations of external services.
 * 
 *         TODO Pass through the BindingsClause from the top-level query. The
 *         BindingsClause needs to be on the QueryRoot. It should be translated
 *         to {@link IV}s when we parse the SPARQL query.
 *         <p>
 *         If the service invocation is an external service then we need to
 *         materialize the {@link IV}s (or cache them up front) and send them
 *         along and, in addition, bulk resolve the visited BindingSets using
 *         the {@link BigdataOpenRDFBindingSetsResolverator} (there might be a
 *         {@link ServiceCall} base class which handles this and delegates
 *         through to an HTTP request against a remote Service URI).
 *         <p>
 *         Otherwise we should pass in the IBindingSet[].
 */
public class ServiceCallJoin extends PipelineOp {

//    private static final Logger log = Logger.getLogger(ServiceCallJoin.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations {

        /**
         * The service URI from the {@link ServiceRegistry}.
         */
        String SERVICE_URI = ServiceCallJoin.class.getName()+".serviceURI";

        /**
         * The <code>group graph pattern</code> used to invoke the service.
         */
        String GROUP_NODE = ServiceCallJoin.class.getName()+".groupNode";

        /**
         * The namespace of the {@link AbstractTripleStore} instance (not the
         * namespace of the lexicon relation). This resource will be located and
         * made available to the {@link ServiceCall}.
         */
        String NAMESPACE = ServiceCallJoin.class.getName()+".namespace";

        /**
         * The timestamp of the {@link AbstractTripleStore} view to be located.
         * 
         */
        String TIMESTAMP = ServiceCallJoin.class.getName()+".timestamp";

    }

    /**
     * Deep copy constructor.
     */
    public ServiceCallJoin(final ServiceCallJoin op) {
        super(op);
    }
    
    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public ServiceCallJoin(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

        getRequiredProperty(Annotations.SERVICE_URI);

        getRequiredProperty(Annotations.GROUP_NODE);

        getRequiredProperty(Annotations.NAMESPACE);

        getRequiredProperty(Annotations.TIMESTAMP);

    }

    public ServiceCallJoin(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));

    }
    
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ChunkTask(this, context));

    }

    /**
     * Evaluates the {@link ServiceCall} for each source binding set. If the
     * outer operator is interrupted, then the {@link ServiceCall} is cancelled
     * (by closing its iterator). If a {@link ServiceCall} fails, then that
     * error is propagated back to the outer operator.
     */
    private static class ChunkTask implements Callable<Void> {

        private final ServiceCallJoin op;

        private final BOpContext<IBindingSet> context;
        
        private final BigdataServiceCall serviceCall;
        
        public ChunkTask(final ServiceCallJoin op,
                final BOpContext<IBindingSet> context) {

            if (op == null)
                throw new IllegalArgumentException();

            if (context == null)
                throw new IllegalArgumentException();

            this.op = op;
            
            this.context = context;

            final URI serviceURI = (URI) op
                    .getRequiredProperty(Annotations.SERVICE_URI);

            @SuppressWarnings("unchecked")
            final IGroupNode<IGroupMemberNode> groupNode = (IGroupNode<IGroupMemberNode>) op
                    .getRequiredProperty(Annotations.GROUP_NODE);

            final String namespace = (String) op
                    .getRequiredProperty(Annotations.NAMESPACE);

            final long timestamp = ((Long) op
                    .getRequiredProperty(Annotations.TIMESTAMP)).longValue();

            final AbstractTripleStore db = (AbstractTripleStore) context
                    .getResource(namespace, timestamp);

            // Lookup a class to "talk" to that Service URI.
            this.serviceCall = ServiceRegistry.toServiceCall(db, serviceURI,
                    groupNode);
            
        }

        /**
         * Evaluate the {@link ServiceCall}.
         */
        public Void call() throws Exception {
            
            try {

                final IAsynchronousIterator<IBindingSet[]> sitr = context
                        .getSource();

                boolean first = true;
                
                while (sitr.hasNext()) {

                    final IBindingSet[] chunk = sitr.next();

                    for (IBindingSet bset : chunk) {

                        /*
                         * Note: The query plan should be structured such that
                         * we invoke the service exactly once. This is done by
                         * having the ServiceCallJoin at the start of a named
                         * subquery. The named subquery is run once and should
                         * only see a single input solution.
                         */
                        if (!first)
                            throw new RuntimeException(
                                    "Multiple invocations of Service?");

                        new ServiceCallTask(bset).call();

                        first = false;

                    }

                }

                // Flush the sink.
                context.getSink().flush();
                
                // Done.
                return null;

            } finally {
                
                context.getSource().close();

                context.getSink().close();

            }
            
        }

        /**
         * Run a subquery.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         */
        private class ServiceCallTask implements Callable<Void> {

            /**
             * The source binding set. This will be copied to the output if
             * there are no solutions for the subquery (optional join
             * semantics).
             */
            private final IBindingSet left;
            
            public ServiceCallTask(final IBindingSet bset) {

                this.left = bset;

            }

            public Void call() throws Exception {

                final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = new UnsyncLocalOutputBuffer<IBindingSet>(
                        op.getChunkCapacity(), context.getSink());
                
            	// The iterator draining the subquery
                ICloseableIterator<IBindingSet> serviceSolutionItr = null;
                try {

                    // Iterator visiting the subquery solutions.
                    serviceSolutionItr = serviceCall
                            .call(null/* BindingsClause */);

                    // JOIN each service solution with the source solution.
                    while (serviceSolutionItr.hasNext()) {

                        final IBindingSet right = serviceSolutionItr.next();
                        
                        final IBindingSet out = BOpContext.bind(left, right,
                                true/* leftIsPipeline */,
                                null/* constraints */, null/*varsToKeep*/);
                        
                        if (out != null) {
                            
                            // Accept this binding set.
                            unsyncBuffer.add(out);

                        }

                    }

                    unsyncBuffer.flush();
                    
                    // done.
                    return null;

                } finally {

                    // ensure the service call iterator is closed.
                    if (serviceSolutionItr != null)
                        serviceSolutionItr.close();

                }

            }

        } // ServiceCallTask

    } // ChunkTask

}
