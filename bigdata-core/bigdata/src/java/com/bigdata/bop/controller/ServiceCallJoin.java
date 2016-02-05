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
 * Created on Aug 18, 2010
 */

package com.bigdata.bop.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.eclipse.jetty.client.HttpClient;
import org.openrdf.query.BindingSet;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.join.BaseJoinStats;
import com.bigdata.bop.join.HashJoinAnnotations;
import com.bigdata.bop.join.JVMHashJoinUtility;
import com.bigdata.bop.join.JoinAnnotations;
import com.bigdata.bop.join.JoinTypeEnum;
import com.bigdata.htree.HTree;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.sparql.ast.service.BigdataServiceCall;
import com.bigdata.rdf.sparql.ast.service.ExternalServiceCall;
import com.bigdata.rdf.sparql.ast.service.MockIVReturningServiceCall;
import com.bigdata.rdf.sparql.ast.service.RemoteServiceCall;
import com.bigdata.rdf.sparql.ast.service.ServiceCall;
import com.bigdata.rdf.sparql.ast.service.ServiceCallUtility;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.sparql.ast.service.ServiceRegistry;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.AbstractUnsynchronizedArrayBuffer;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.Chunkerator;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.LatchedExecutor;

import cutthecrap.utils.striterators.ICloseableIterator;
import cutthecrap.utils.striterators.SingleValueIterator;

/**
 * Vectored pipeline join of the source solution(s) with solutions from a a
 * SERVICE invocation. This operator may be used to invoke: (a) internal,
 * bigdata-aware services; (b) internal openrdf aware services; and (c) remote
 * services.
 * <p>
 * Source solutions are vectored for the same target service. Source solutions
 * which target different services are first grouped by the target service and
 * then vectored to each target service. Remote SERVICEs receive their vectored
 * inputs through a BINDINGS clause rather than a {@link IBindingSet}[]. The
 * service call(s) will be cancelled if the parent query is cancelled.
 * <p>
 * For each binding set presented, this operator executes the service joining
 * the solutions from the service against the source binding set. Since each
 * invocation of the service will (typically) produce the same solutions, this
 * operator should always be the first operator in a named subquery in order to
 * ensure that the service is invoked exactly once. The solutions written onto
 * the sink may then joined with other access paths before they reach the end of
 * the named subquery and are materialized (by the parent) on an {@link HTree}.
 * <p>
 * Any solutions produced by the service are copied to the default sink.
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
         * The {@link ServiceNode} modeling the SERVICE clause to be invoked.
         * <p>
         * Note: This presence of the {@link ServiceNode} as an attribute on the
         * {@link ServiceCallJoin} blends the bop (physical query plan) and the
         * AST (logical query plan). However, we basically need all of the data
         * from the {@link ServiceNode} in order to handle remote service end
         * points so it is much simpler to reuse the encapsulation here.
         * 
         * @see ServiceRegistry
         */
        String SERVICE_NODE = ServiceCallJoin.class.getName() + ".serviceNode";

        /**
         * The namespace of the {@link AbstractTripleStore} instance (not the
         * namespace of the lexicon relation). This resource will be located and
         * made available to the {@link ServiceCall}.
         */
        String NAMESPACE = ServiceCallJoin.class.getName() + ".namespace";

        /**
         * The timestamp of the {@link AbstractTripleStore} view to be located.
         */
        String TIMESTAMP = ServiceCallJoin.class.getName() + ".timestamp";

        /**
         * The join variables. This is used to establish a correlation between
         * the solutions vectored into the SERVICE call and the solutions
         * flowing out of the SERVICE call.
         * 
         * @see HashJoinAnnotations#JOIN_VARS
         */
        String JOIN_VARS = HashJoinAnnotations.JOIN_VARS;

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

        getRequiredProperty(Annotations.SERVICE_NODE);

        getRequiredProperty(Annotations.NAMESPACE);

        getRequiredProperty(Annotations.TIMESTAMP);

//        getRequiredProperty(Annotations.PROJECTED_VARS);

        getRequiredProperty(Annotations.JOIN_VARS);
        
    }

    public ServiceCallJoin(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));

    }
    
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ChunkTask(this, context));

    }

    @Override
    public BaseJoinStats newStats() {

       return new BaseJoinStats();

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
        
//        private final IConstraint[] constraints;

        private final AbstractTripleStore db;

        private final HttpClient cm;
        
        private final IVariableOrConstant<?> serviceRef;

        private final ServiceNode serviceNode;
        
//        final IGroupNode<IGroupMemberNode> groupNode;
        
        private final boolean silent;

        private final long timeout;
        
        private final Set<IVariable<?>> projectedVars;
        
//        private final Set<IVariable<?>> joinVars;

//        @SuppressWarnings("unchecked")
        public ChunkTask(final ServiceCallJoin op,
                final BOpContext<IBindingSet> context) {

            if (op == null)
                throw new IllegalArgumentException();

            if (context == null)
                throw new IllegalArgumentException();

            this.op = op;
            
            this.context = context;

//            this.constraints = op
//                    .getProperty(Annotations.CONSTRAINTS, null/* defaultValue */);

            this.serviceNode = (ServiceNode) op
                    .getRequiredProperty(Annotations.SERVICE_NODE);
            
            this.serviceRef = serviceNode.getServiceRef().getValueExpression();
            
            final String namespace = (String) op
                    .getRequiredProperty(Annotations.NAMESPACE);

            final long timestamp = ((Long) op
                    .getRequiredProperty(Annotations.TIMESTAMP)).longValue();

            this.db = (AbstractTripleStore) context
                    .getResource(namespace, timestamp);
            
            this.cm = context.getClientConnectionManager();

//            this.valueFactory = db.getValueFactory();

            // Service errors are ignored when true.
            this.silent = serviceNode.isSilent();//op.getProperty(Annotations.SILENT, false);

            // The service request timeout.
            this.timeout = serviceNode.getTimeout();//op.getProperty(Annotations.TIMEOUT, Long.MAX_VALUE);
            
            /*
             * Note: We MUST use the projected variables for the SERVICE since
             * we can otherwise break the variable scope.
             */
            this.projectedVars = serviceNode.getProjectedVars();

            if(projectedVars == null)
                throw new AssertionError();

//            this.joinVars = (Set<IVariable<?>>) op
//                    .getRequiredProperty(Annotations.JOIN_VARS);

        }

        /**
         * Evaluate the {@link ServiceCall}.
         */
        public Void call() throws Exception {
            
            if(serviceRef.isConstant()) {

                doServiceCallWithConstant();
                
            } else {
                
                doServiceCallWithExpression();
                
            }
            
            return (Void) null;
            
        }

        /**
         * The value expression for the SERVICE reference is a constant (fast
         * path).
         * 
         * @throws Exception
         */
        private void doServiceCallWithConstant() throws Exception {

            final BigdataURI serviceURI = ServiceCallUtility
                    .getConstantServiceURI(serviceRef);

            if (serviceURI == null)
                throw new AssertionError();
            
            // Lookup a class to "talk" to that Service URI.
            final ServiceCall<? extends Object> serviceCall = resolveService(serviceURI);

            try {

                final ICloseableIterator<IBindingSet[]> sitr = context
                        .getSource();

                while (sitr.hasNext()) {

                    final IBindingSet[] chunk = sitr.next();

                    final ServiceCallChunk serviceCallChunk = new ServiceCallChunk(
                            serviceURI, serviceCall, chunk);

                    final FutureTask<Void> ft = new FutureTask<Void>(
                            new ServiceCallTask(serviceCallChunk));
                    
                    context.getExecutorService().execute(ft);
                    
                    try {

                        ft.get(timeout, TimeUnit.MILLISECONDS);
                        
                    } catch (TimeoutException ex) {
                        
                        if (!silent)
                            throw ex;
                        
                    } finally {

                        ft.cancel(true/* mayInterruptIfRunning */);
                        
                    }

                }

                // Flush the sink.
                context.getSink().flush();
                
                // Done.
                return;

            } finally {
                
                context.getSource().close();

                context.getSink().close();

            } 
            
        }

        /**
         * The SERVICE reference value expression is not a constant.
         * <p>
         * We need to evaluate the value expression for each source solution and
         * group the solutions by the distinct as-bound serviceRef values. If is
         * an error if any given serviceRef expression does not evaluate to a
         * URI. Once grouped by the target service URI, we vector the solutions
         * to each service. If there are multiple distinct services, then they
         * are vectored with limited parallelism to reduce latency.
         * 
         * @throws Exception 
         */
        private void doServiceCallWithExpression() throws Exception {
            
            try {

                final ICloseableIterator<IBindingSet[]> sitr = context
                        .getSource();

                while (sitr.hasNext()) {

                    final Map<BigdataURI, ServiceCallChunk> serviceCallChunks = new HashMap<BigdataURI, ServiceCallChunk>();

                    final IBindingSet[] chunk = sitr.next();

                    for (int i = 0; i < chunk.length; i++) {

                        final IBindingSet bset = chunk[i];

                        final BigdataURI serviceURI = ServiceCallUtility
                                .getServiceURI(serviceRef, bset);

                        ServiceCallChunk serviceCallChunk = serviceCallChunks
                                .get(serviceURI);

                        if (serviceCallChunk == null) {

                            // Lookup a class to "talk" to that Service URI.
                            final ServiceCall<? extends Object> serviceCall = resolveService(serviceURI);

                            serviceCallChunks.put(serviceURI,
                                    serviceCallChunk = new ServiceCallChunk(
                                            serviceURI, serviceCall));

                        }

                        serviceCallChunk.addSourceSolution(bset);

                    }
                    
                    /*
                     * Submit vectored service calls to each target service in
                     * parallel.
                     * 
                     * Note: Parallelism evaluation of multiple services can
                     * radically reduce the latency of this operation. Limited
                     * parallelism is used to avoid too many threads being tied
                     * up in those service requests.
                     * 
                     * Note: [nparallel] as reported by getMaxParallel() is a
                     * hint to the QueryEngine to indicate how many instances of
                     * an operator may be executed in parallel. This is using
                     * the same hint to specify how many service requests each
                     * operator instance may execute in parallel. That means
                     * that the real parallelism of this operator is limited by
                     * [nparallel * nparallel].
                     * 
                     * In order to manage threads growth for the
                     * ServiceCallJoin, the query plan generator SHOULD specify
                     * this as an "at-once" operator (or possible "blocked")
                     * operator. That way the QueryEngine will wait until all
                     * source solutions are on hand and then invoke the
                     * ServiceCallJoin exactly once.
                     */
                    
                    final int nparallel = op.getMaxParallel();
                    
                    final LatchedExecutor executorService = new LatchedExecutor(
                            context.getExecutorService(), nparallel);

                    final List<FutureTask<Void>> tasks = new ArrayList<FutureTask<Void>>(
                            serviceCallChunks.size());

                    try {

                        for (ServiceCallChunk serviceCallChunk : serviceCallChunks
                                .values()) {

                            final FutureTask<Void> ft = new FutureTask<Void>(
                                    new ServiceCallTask(serviceCallChunk));

                            tasks.add(ft);

                            executorService.execute(ft);

                        }

                        for (FutureTask<Void> ft : tasks) {

                            /*
                             * Each service request is faced with the same
                             * timeout.
                             */

                            try {

                                ft.get(timeout, TimeUnit.MILLISECONDS);
                                
                            } catch (TimeoutException ex) {
                                
                                ft.cancel(true/* mayInterruptIfRunning */);
                                
                                if (!silent)
                                    throw ex;

                            }

                        }

                    } finally {

                        // Ensure that all tasks are cancelled.
                        for (FutureTask<Void> ft : tasks) {

                            ft.cancel(true/* mayInterruptIfRunning */);

                        }
                        
                    }

                } // next source solution chunk.

                // Flush the sink.
                context.getSink().flush();
                
                // Done.
                return;

            } finally {
                
                context.getSource().close();

                context.getSink().close();

            } 
                        
        }
        
        /**
         * Return a {@link ServiceCall} which may be used to talk to a service
         * at that URI.
         * 
         * @param serviceURI
         *            The service URI.
         *            
         * @return The {@link ServiceCall} and never <code>null</code>.
         */
        private ServiceCall<? extends Object> resolveService(
                final BigdataURI serviceURI) {

            final ServiceCall<?> serviceCall = ServiceRegistry.getInstance()
                    .toServiceCall(db, cm, serviceURI, serviceNode, (BaseJoinStats)context.getStats());

            return serviceCall;

        }

        /**
         * Invoke a SERVICE.
         */
        private class ServiceCallTask implements Callable<Void> {

            /**
             * The source binding set. This will be copied to the output if
             * there are no solutions for the subquery (optional join
             * semantics).
             */
            private final IBindingSet[] chunk;

            /** The service URI. */
            private final BigdataURI serviceURI;

            /** The object used to talk to that service. */
            private final ServiceCall<?> serviceCall;

            /**
             * @param serviceCallChunk
             *            A chunk of solutions to be vectored to some target
             *            service.
             */
            public ServiceCallTask(final ServiceCallChunk serviceCallChunk) {

                if (serviceCallChunk == null)
                    throw new IllegalArgumentException();

                serviceURI = serviceCallChunk.serviceURI;
                
                serviceCall = serviceCallChunk.serviceCall;
                
                chunk = serviceCallChunk.getSourceSolutions();

            }

            @Override
            public Void call() throws Exception {

                final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = new UnsyncLocalOutputBuffer<IBindingSet>(
                        op.getChunkCapacity(), context.getSink());

                final IBlockingBuffer<IBindingSet[]> sink2 = context.getSink();

                // Thread-local buffer iff optional sink is in use.
                final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer2 = sink2 == null ? null
                        : new UnsyncLocalOutputBuffer<IBindingSet>(
                                op.getChunkCapacity(), sink2);

                final JVMHashJoinUtility state = new JVMHashJoinUtility(op,
                        silent ? JoinTypeEnum.Optional : JoinTypeEnum.Normal
                        );

                // Pump the solutions into the hash map.
                state.acceptSolutions(new SingleValueIterator<IBindingSet[]>(
                        chunk), null/* stats */);

                // The iterator draining the subquery
                ICloseableIterator<IBindingSet[]> serviceSolutionItr = null;
                try {

                    /*
                     * Invoke the service.
                     * 
                     * Note: Returns [null] IFF SILENT and SERVICE ERROR.
                     */
                    
                    serviceSolutionItr = doServiceCall(serviceCall, chunk);

                    if (serviceSolutionItr != null) {
                       
                        /*
                         * Do a hash join of the source solutions with the
                         * solutions from the service, outputting any solutions
                         * which join.
                         * 
                         * Note: 
                         */
                        
                        state.hashJoin(serviceSolutionItr, null/* stats */,
                                unsyncBuffer);

                    }

                } finally {

                    // ensure the service call iterator is closed.
                    if (serviceSolutionItr != null)
                        serviceSolutionItr.close();

                }
                    
                /*
                 * Note: This only handles Normal and Optional. Normal is used
                 * unless the SERVICE is SILENT.
                 * 
                 * The semantics of SILENT are that it returns an "empty"
                 * solution. An empty solution joins with anything (it is the
                 * identity solution). Since there may have been join variables,
                 * we need to use an OPTIONAL join to ensure that the original
                 * solutions are passed through.
                 */
                if (state.getJoinType().isOptional()) {

                    final IBuffer<IBindingSet> outputBuffer;
                    if (unsyncBuffer2 == null) {
                        // use the default sink.
                        outputBuffer = unsyncBuffer;
                    } else {
                        // use the alternative sink.
                        outputBuffer = unsyncBuffer2;
                    }

                    state.outputOptionals(outputBuffer);

                    if (sink2 != null) {
                        unsyncBuffer2.flush();
                        sink2.flush();
                    }

                } // if(optional)

                unsyncBuffer.flush();

                // done.
                return null;

            }

            /**
             * Invoke the SERVICE.
             * 
             * @param serviceCall
             * @param left
             * 
             * @return An iterator from which solutions may be drained -or-
             *         <code>null</code> if the SERVICE invocation failed and
             *         SILENT is <code>true</code>.
             * 
             * @throws Exception
             * 
             *             TODO RECHUNKING Push down the
             *             ICloseableIterator<IBindingSet[]> return type into
             *             the {@link ServiceCall} interface and the various
             *             ways in which we can execute a service call. Do this
             *             as part of vectoring solutions in and out of service
             *             calls?
             */
            private ICloseableIterator<IBindingSet[]> doServiceCall(
                    final ServiceCall<? extends Object> serviceCall,
                    final IBindingSet[] left) throws Exception {

                try {
                    
                    final ICloseableIterator<IBindingSet> itr;
                    
                    if (serviceCall instanceof BigdataServiceCall) {

                        itr = doBigdataServiceCall(
                                (BigdataServiceCall) serviceCall, left);

                    } else if (serviceCall instanceof ExternalServiceCall) {

                        itr = doExternalServiceCall(
                                (ExternalServiceCall) serviceCall, left);

                    } else if (serviceCall instanceof RemoteServiceCall) {

                        itr = doRemoteServiceCall(
                                (RemoteServiceCall) serviceCall, left);
                    } else if (serviceCall instanceof MockIVReturningServiceCall) {
                       
                       itr = doExternalMockIVServiceCall(
                                (MockIVReturningServiceCall)serviceCall, left);
                       
                    } else {

                        throw new AssertionError();

                    }

                    
                    final ICloseableIterator<IBindingSet[]> itr2 = new Chunkerator<IBindingSet>(
                            itr, op.getChunkCapacity(), IBindingSet.class);

                    return itr2;
                    
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
                                + " :" + t);

                        // Done.
                        return null;

                    }

                    throw new RuntimeException(t);

                }
                
            }

            /**
             * Evaluate a bigdata aware "service" call in the same JVM.
             */
            private ICloseableIterator<IBindingSet> doBigdataServiceCall(
                    final BigdataServiceCall serviceCall,
                    final IBindingSet left[]) throws Exception {

                return serviceCall.call(left);

            }
            
            /**
             * Evaluate an openrdf "service" call in the same JVM.
             */
            private ICloseableIterator<IBindingSet> doExternalMockIVServiceCall(
                    final MockIVReturningServiceCall serviceCall,
                    final IBindingSet left[]) throws Exception {

                return doNonBigdataMockIVServiceCall(serviceCall, left);
                
            }

            /**
             * Evaluate an generic service producing mock IVs.
             */
            private ICloseableIterator<IBindingSet> doExternalServiceCall(
                    final ExternalServiceCall serviceCall,
                    final IBindingSet left[]) throws Exception {

                return doNonBigdataSesameServiceCall(serviceCall, left);
                
            }

            /**
             * Evaluate an remote SPARQL service call.
             */
            private ICloseableIterator<IBindingSet> doRemoteServiceCall(
                    final RemoteServiceCall serviceCall,
                    final IBindingSet left[]) throws Exception {

                return doNonBigdataSesameServiceCall(serviceCall, left);
                
            }
            
            /**
             * The "openrdf" internal and REMOTE SPARQL invocations look the
             * same at this abstraction. The differences are hidden in the
             * {@link ServiceCall} objects.
             * 
             * @param serviceCall
             *            The object which will make the service call.
             * @param left
             *            The source solutions.
             *            
             * @return The solutions.
             */
            private ICloseableIterator<IBindingSet> doNonBigdataSesameServiceCall(
                    final ServiceCall<BindingSet> serviceCall,
                    final IBindingSet left[]) throws Exception {

                final LexiconRelation lex = db.getLexiconRelation(); 
                
                // Convert IBindingSet[] to openrdf BindingSet[].
                final BindingSet[] left2 = ServiceCallUtility.convert(lex,
                        projectedVars, left);

                /*
                 * Note: This operation is "at-once" over the service solutions.
                 * It could be turned into a "chunked" operator over those
                 * solutions. That would make sense if the service was capable
                 * of delivering a very large number of solutions.
                 */
                ICloseableIterator<BindingSet> results = null;
                final List<BindingSet> serviceResults = new LinkedList<BindingSet>();
                try {
                    
                    results = serviceCall.call(left2);
                    
                    while (results.hasNext()) {

                        serviceResults.add(results.next());

                    }

                } finally {

                    if (results != null)
                        results.close();
                    
                }
                
                /*
                 * Batch resolve BigdataValues to IVs. This is necessary in
                 * order to have subsequent JOINs succeed when they join on
                 * variables which are bound to terms which are in the
                 * lexicon.
                 */
                
                final BindingSet[] serviceResultChunk = serviceResults
                        .toArray(new BindingSet[serviceResults.size()]);

                final IBindingSet[] bigdataSolutionChunk = ServiceCallUtility
                        .resolve(db, serviceResultChunk);

                return new ChunkedArrayIterator<IBindingSet>(
                        bigdataSolutionChunk);

            }
            
            /**
             * Service call against a (non sesame based) external endpoint.
             * 
             * @param serviceCall
             *            The object which will make the service call.
             * @param left
             *            The source solutions.
             *            
             * @return The solutions.
             */
            private ICloseableIterator<IBindingSet> doNonBigdataMockIVServiceCall(
                    final ServiceCall<IBindingSet> serviceCall,
                    final IBindingSet left[]) throws Exception {
               
               /**
                * Note: a MockTermResolverOp will be wrapped around this
                * service in AST2BopUtitlity.addService(), so we don't need to
                * care about dictionary resolving mocked URIs here, but just
                * call the service.
                */
               return serviceCall.call(left);

            }

        } // ServiceCallTask

    } // ChunkTask

    /**
     * A chunk of solutions for the same target service.
     */
    private static class ServiceCallChunk {

        public final BigdataURI serviceURI;

        public final ServiceCall<?> serviceCall;

        private IBindingSet[] chunk;
        
        private final List<IBindingSet> sourceSolutions;
        
        public ServiceCallChunk(final BigdataURI serviceURI,
                final ServiceCall<?> serviceCall, final IBindingSet[] chunk) {

            if(serviceURI == null)
                throw new IllegalArgumentException();

            if(serviceCall == null)
                throw new IllegalArgumentException();

            if (chunk == null)
                throw new IllegalArgumentException();
            
            if (chunk.length == 0)
                throw new IllegalArgumentException();

            this.serviceURI = serviceURI;
            
            this.serviceCall = serviceCall;
            
            this.chunk = chunk;
            
            this.sourceSolutions = null;

        }

        public ServiceCallChunk(final BigdataURI serviceURI,
                final ServiceCall<?> serviceCall) {
    
            if(serviceURI == null)
                throw new IllegalArgumentException();

            if(serviceCall == null)
                throw new IllegalArgumentException();

            this.serviceURI = serviceURI;
            
            this.serviceCall = serviceCall;

            this.chunk = null;
            
            this.sourceSolutions = new LinkedList<IBindingSet>();
            
        }
        
        public void addSourceSolution(final IBindingSet bset) {
        
            if (sourceSolutions == null)
                throw new UnsupportedOperationException();
            
            sourceSolutions.add(bset);
            
        }

        public IBindingSet[] getSourceSolutions() {
            
            if(chunk != null) {
                
                return chunk;
                
            }

            chunk = sourceSolutions.toArray(new IBindingSet[sourceSolutions
                    .size()]);
            
            return chunk;
            
        }
        
        public int hashCode() {

            return serviceURI.hashCode();
            
        }
        
        public boolean equals(final Object o) {
         
            if (this == o)
                return true;
            
            final ServiceCallChunk c = (ServiceCallChunk) o;
            
            return this.serviceURI.equals(c.serviceURI);

        }
        
    }

}
