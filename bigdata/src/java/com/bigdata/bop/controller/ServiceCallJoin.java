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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.join.HashJoinAnnotations;
import com.bigdata.bop.join.JoinAnnotations;
import com.bigdata.htree.HTree;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.BigdataServiceCall;
import com.bigdata.rdf.sparql.ast.ExternalServiceCall;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.ServiceCall;
import com.bigdata.rdf.sparql.ast.ServiceRegistry;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;
import com.bigdata.striterator.CloseableIteratorWrapper;
import com.bigdata.striterator.ICloseableIterator;
import com.bigdata.util.InnerCause;

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
 */
public class ServiceCallJoin extends PipelineOp {

    private static final Logger log = Logger.getLogger(ServiceCallJoin.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations {

        /**
         * Optional constraints to be applied to each solution.
         * 
         * @see JoinAnnotations#CONSTRAINTS
         */
        String CONSTRAINTS = JoinAnnotations.CONSTRAINTS;

        /**
         * The service URI from the {@link ServiceRegistry}.
         */
        String SERVICE_URI = ServiceCallJoin.class.getName()+".serviceURI";

        /**
         * The <code>group graph pattern</code> used to invoke the service.
         */
        String GRAPH_PATTERN = ServiceCallJoin.class.getName()+".graphPattern";

        /**
         * The namespace of the {@link AbstractTripleStore} instance (not the
         * namespace of the lexicon relation). This resource will be located and
         * made available to the {@link ServiceCall}.
         */
        String NAMESPACE = ServiceCallJoin.class.getName()+".namespace";

        /**
         * The timestamp of the {@link AbstractTripleStore} view to be located.
         */
        String TIMESTAMP = ServiceCallJoin.class.getName()+".timestamp";

        /**
         * Service errors will be ignored when <code>true</code>.
         */
        String SILENT = ServiceCallJoin.class.getName() + ".silent";
        
        /**
         * The set of variables which can flow in/out of the SERVICE.
         */
        String PROJECTED_VARS = ServiceCallJoin.class.getName()
                + ".projectedVars";

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

        getRequiredProperty(Annotations.GRAPH_PATTERN);

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
        
        private final IConstraint[] constraints;
        
        private final URI serviceURI;
        
        private final ServiceCall<? extends Object> serviceCall;
        
        private final boolean silent;
        
        private final Set<IVariable<?>> projectedVars;
        
        private final BigdataValueFactory valueFactory;

        public ChunkTask(final ServiceCallJoin op,
                final BOpContext<IBindingSet> context) {

            if (op == null)
                throw new IllegalArgumentException();

            if (context == null)
                throw new IllegalArgumentException();

            this.op = op;
            
            this.context = context;

            serviceURI = (URI) op.getRequiredProperty(Annotations.SERVICE_URI);

            @SuppressWarnings("unchecked")
            final IGroupNode<IGroupMemberNode> groupNode = (IGroupNode<IGroupMemberNode>) op
                    .getRequiredProperty(Annotations.GRAPH_PATTERN);

            final String namespace = (String) op
                    .getRequiredProperty(Annotations.NAMESPACE);

            final long timestamp = ((Long) op
                    .getRequiredProperty(Annotations.TIMESTAMP)).longValue();

            constraints = op.getProperty(Annotations.CONSTRAINTS, null/* defaultValue */);

            final AbstractTripleStore db = (AbstractTripleStore) context
                    .getResource(namespace, timestamp);

            this.valueFactory = db.getValueFactory();
            
            // Lookup a class to "talk" to that Service URI.
            this.serviceCall = ServiceRegistry.toServiceCall(db, serviceURI,
                    groupNode);
            
            // Service errors are ignored when true.
            this.silent = op.getProperty(Annotations.SILENT, false);

            /*
             * FIXME We MUST use the projected variables for the SERVICE since
             * we can otherwise break the variable scope.
             */
            this.projectedVars = null;
            
        }

        /**
         * Evaluate the {@link ServiceCall}.
         */
        public Void call() throws Exception {
            
            try {

                final ICloseableIterator<IBindingSet[]> sitr = context
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

                    /*
                     * Invoke the service.
                     * 
                     * FIXME This task is not vectored. It does one SERVICE call
                     * for each source solution. We should generate a BINDINGS
                     * clause when the service is external to vector the
                     * request. When it is internal, just pass through all
                     * source solutions in one chunk.
                     * 
                     * Note: Vectoring is important for the efficiency of
                     * SERVICEs running in the same JVM, not just for those
                     * which are external to the JVM. Remote SERVICEs receive
                     * their vectored inputs through a BINDINGS clause rather
                     * than a IBindingSet[].
                     * 
                     * FIXME Support external service invocation. This might be
                     * best done through a vectored bridge to the openrdf
                     * federated query support. There are likely to be all kinds
                     * of questions concerning when the SERVICE's graph pattern
                     * can be decomposed and interpreted by the query planner.
                     */
                    serviceSolutionItr = doServiceCall(serviceCall,left);

                    /*
                     * JOIN each service solution with the *correlated* source
                     * solution in order to recover variables which were not
                     * projected by the SERVICE call. [When vectoring this
                     * operator, make sure that correlation is maintained!]
                     */
                    while (serviceSolutionItr.hasNext()) {

                        final IBindingSet right = serviceSolutionItr.next();
                        
                        final IBindingSet out = BOpContext.bind(left, right,
                                constraints, null/*varsToKeep*/);
                       
                        if (out != null) {
                            
                            // Accept this binding set.
                            unsyncBuffer.add(out);

                        }

                    }

                    unsyncBuffer.flush();
                    
                    // done.
                    return null;

                } catch (Throwable t) {
                    
                    if (silent
                            && !InnerCause.isInnerCause(t,
                                    InterruptedException.class)) {
                        /*
                         * If the SILENT attribute was specified, then do not
                         * halt the query if there is an error.
                         * 
                         * Note: The query must still be interruptable so we do
                         * not trap exceptions whose root cause is an interrupt.
                         */

                        log.warn("Service call: serviceUri=" + serviceURI
                                + " : " + t);

                        // Done.
                        return null;
                        
                    }

                    throw new RuntimeException(t);
                    
                } finally {

                    // ensure the service call iterator is closed.
                    if (serviceSolutionItr != null)
                        serviceSolutionItr.close();

                }

            }

            private ICloseableIterator<IBindingSet> doServiceCall(
                    final ServiceCall<? extends Object> serviceCall,
                    final IBindingSet left) {

                if (serviceCall instanceof BigdataServiceCall) {

                    return doBigdataServiceCall(
                            (BigdataServiceCall) serviceCall, left);

                } else {

                    return doExternalServiceCall(
                            (ExternalServiceCall) serviceCall, left);
                
                }
                
            }

            private ICloseableIterator<IBindingSet> doBigdataServiceCall(
                    final BigdataServiceCall serviceCall,
                    final IBindingSet left2) {

                return serviceCall.call(new IBindingSet[] { left });

            }

            private ICloseableIterator<IBindingSet> doExternalServiceCall(
                    final ExternalServiceCall serviceCall,
                    final IBindingSet left) {

                final BindingSet left2 = bigdata2Openrdf(projectedVars, left);

                ICloseableIterator<BindingSet> results = null;
                final List<IBindingSet> serviceResults = new LinkedList<IBindingSet>();
                try {
                    
                    results = serviceCall.call(new BindingSet[] { left2 });
                    
                    while (results.hasNext()) {

                        final BindingSet bset = results.next();
                        
                        /*
                         * Convert the solution into a bigdata IBindingSet.
                         */
                        final IBindingSet bset2 = openrdf2Bigdata(projectedVars,
                                valueFactory, bset);
                        
                        serviceResults.add(bset2);

                    }

                    return new CloseableIteratorWrapper<IBindingSet>(
                            serviceResults.iterator());

                } finally {

                    if (results != null)
                        results.close();
                    
                }
                
            }

        } // ServiceCallTask

    } // ChunkTask

    /**
     * Convert the {@link IBindingSet} into an openrdf {@link BindingSet}.
     * <p>
     * Note: The {@link IV} MUST have the cache set. An exception WILL be thrown
     * if the {@link IV} has not been materialized.
     * 
     * @param vars
     *            The set of variables which are to be projected (optional).
     *            When given, only the projected variables are in the returned
     *            {@link BindingSet}.
     * @param in
     *            A bigdata {@link IBindingSet} with materialized values.
     * 
     *            TODO Move these methods to a utility class and add test suites
     *            for them.  In particular, look at the handling of new bindings
     *            from an openrdf aware SERVICE which do not have IVs.
     */
    static private BindingSet bigdata2Openrdf(final Set<IVariable<?>> vars,
            final IBindingSet in) {

        final MapBindingSet out = new MapBindingSet();
        
        @SuppressWarnings("rawtypes")
        final Iterator<Map.Entry<IVariable,IConstant>> itr = in.iterator();
        
        while(itr.hasNext()) {
        
            @SuppressWarnings("rawtypes")
            final Map.Entry<IVariable,IConstant> e = itr.next();

            final IVariable<?> var = e.getKey();
            
            if (vars != null && !vars.contains(var)) {

                // This variable is not being projected.
                continue;
                
            }
            
            final String name = var.getName();
            
            @SuppressWarnings("rawtypes")
            final IV iv = (IV) e.getValue().get();
            
            final BigdataValue value = iv.getValue();
            
            out.addBinding(name, value);
            
        }
        
        return out;

    }
    
    /**
     * Convert an openrdf {@link BindingSet} into a bigdata {@link IBindingSet}.
     * {@link BigdataValue}s associated with {@link IV}s will be turned into
     * those {@link IV}s. RDF {@link Value}s and {@link BigdataValue}s NOT
     * associated with {@link IV}s will be turned into mock {@link IV}.
     * 
     * @param vars
     *            The variables to be projected (optional). When given, only the
     *            projected variables are in the returned {@link IBindingSet}.
     * @param in
     *            The openrdf {@link BindingSet}
     * 
     * @return The bigdata {@link IBindingSet}.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    static private IBindingSet openrdf2Bigdata(final Set<IVariable<?>> vars,
            final BigdataValueFactory valueFactory,
            final BindingSet in) {

        final IBindingSet out = new ListBindingSet();
        
        final Iterator<Binding> itr = in.iterator();
        
        while(itr.hasNext()) {
        
            final Binding e = itr.next();

            final String name = e.getName();

            final IVariable<?> var = Var.var(name);
            
            if (vars != null && !vars.contains(var)) {

                // This variable is not being projected.
                continue;

            }

            final Value value = e.getValue();

            if (value instanceof BigdataValue
                    && ((BigdataValue) value).getIV() != null) {

                final IV iv = ((BigdataValue) value).getIV();

                out.set(var, new Constant<IV>(iv));

            } else {

                final IV iv = TermId.mockIV(VTE.valueOf(value));

                iv.setValue(valueFactory.asValue(value));

                out.set(var, new Constant(iv));
                
            }
            
        }
        
        return out;

    }

}
