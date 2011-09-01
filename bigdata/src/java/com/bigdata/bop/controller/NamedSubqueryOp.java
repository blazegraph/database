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
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.HTreeAnnotations;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.HashJoinAnnotations;
import com.bigdata.bop.join.HashJoinUtility;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.htree.HTree;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.rwstore.sector.MemStore;

/**
 * Evaluation of a subquery, producing a named result set. This operator passes
 * through any source binding sets without modification. The subquery is
 * evaluated exactly once, the first time this operator is invoked for a given
 * query plan. No bindings are pushed into the subquery. If some variables are
 * known to be bound, then they should be rewritten into constants or their
 * bindings should be inserted into the subquery using LET() operator.
 * <p>
 * This operator is NOT thread-safe. It relies on the query engine to provide
 * synchronization for the "run-once" contract of the subquery. The operator
 * MUST be run on the query controller.
 * 
 * @see NamedSubqueryIncludeOp
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class NamedSubqueryOp extends PipelineOp {

    static private final transient Logger log = Logger
            .getLogger(NamedSubqueryOp.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends SubqueryAnnotations, HTreeAnnotations,
            HashJoinAnnotations {

        /**
         * The name of {@link IQueryAttributes} attribute under which the
         * subquery solution set is stored (a {@link HTree} reference). The
         * attribute name includes the query UUID. The query UUID must be
         * extracted and used to lookup the {@link IRunningQuery} to which the
         * solution set was attached.
         * 
         * @see NamedSolutionSetRef
         */
        final String NAMED_SET_REF = "namedSetRef";
        
    }

//    /**
//     * Return the {@link IQueryAttributes} key for a given named set having the
//     * specified join variables.
//     * 
//     * @param namedSet
//     *            The named set.
//     * @param joinvars
//     *            The join variables.
//     * 
//     * @return The key.
//     */
//    public static String getSolutionSetName(final String namedSet,
//            final VarNode[] joinvars) {
//
//        final StringBuilder sb = new StringBuilder();
//
//        sb.append("namedSet{name=" + namedSet + ",joinvars=[");
//
//        for (int i = 0; i < joinvars.length; i++) {
//
//            if (i > 0)
//                sb.append(",");
//
//            sb.append(joinvars[i].getValueExpression().getName());
//
//        }
//
//        sb.append("])");
//
//        return sb.toString();
//
//    }

    /**
     * Deep copy constructor.
     */
    public NamedSubqueryOp(final NamedSubqueryOp op) {
        super(op);
    }
    
    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public NamedSubqueryOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

        if (getEvaluationContext() != BOpEvaluationContext.CONTROLLER) {
            throw new IllegalArgumentException(
                    BOp.Annotations.EVALUATION_CONTEXT + "="
                            + getEvaluationContext());
        }

        if (getMaxParallel() != 1) {
            throw new IllegalArgumentException(
                    PipelineOp.Annotations.MAX_PARALLEL + "="
                            + getMaxParallel());
        }

        getRequiredProperty(Annotations.SUBQUERY);

        getRequiredProperty(Annotations.NAMED_SET_REF);

        // Join variables must be specified.
        final IVariable<?>[] joinVars = (IVariable[]) getRequiredProperty(Annotations.JOIN_VARS);

//        if (joinVars.length == 0)
//            throw new IllegalArgumentException(Annotations.JOIN_VARS);

        for (IVariable<?> var : joinVars) {

            if (var == null)
                throw new IllegalArgumentException(Annotations.JOIN_VARS);

        }

    }

    public NamedSubqueryOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }

    /**
     * @see Annotations#ADDRESS_BITS
     */
    public int getAddressBits() {

        return getProperty(Annotations.ADDRESS_BITS,
                Annotations.DEFAULT_ADDRESS_BITS);

    }

    /**
     * @see Annotations#RAW_RECORDS
     */
    public boolean getRawRecords() {

        return getProperty(Annotations.RAW_RECORDS,
                Annotations.DEFAULT_RAW_RECORDS);

    }
    
    /**
     * @see Annotations#MAX_RECLEN
     */
    public int getMaxRecLen() {

        return getProperty(Annotations.MAX_RECLEN,
                Annotations.DEFAULT_MAX_RECLEN);

    }

    @Override
    public BOpStats newStats() {

        return new NamedSolutionSetStats();

    }

    /**
     * Adds reporting for the size of the named solution set.
     */
    private static class NamedSolutionSetStats extends BOpStats {
        
        private static final long serialVersionUID = 1L;
        
        final AtomicLong solutionSetSize = new AtomicLong();

        public void add(final BOpStats o) {

            super.add(o);

            if (o instanceof NamedSolutionSetStats) {

                final NamedSolutionSetStats t = (NamedSolutionSetStats) o;

                solutionSetSize.addAndGet(t.solutionSetSize.get());

            }

        }

        @Override
        protected void toString(final StringBuilder sb) {
            super.toString(sb);
            sb.append(",solutionSetSize=" + solutionSetSize.get());
        }

    }
    
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ControllerTask(this, context));
        
    }
    
	/**
	 * Evaluates the subquery for each source binding set. If the controller
	 * operator is interrupted, then the subqueries are cancelled. If a subquery
	 * fails, then all subqueries are cancelled.
	 */
    private static class ControllerTask implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        private final NamedSolutionSetStats stats;
        
        /** The subquery which is evaluated for each input binding set. */
        private final PipelineOp subquery;
        
        /** Metadata to identify the named solution set. */
        private final NamedSolutionSetRef namedSetRef;

        /**
         * The join variables.
         */
        private final IVariable[] joinVars;
        
        /**
         * <code>true</code> iff this is the first time the task is being
         * invoked, in which case we will evaluate the subquery and save its
         * result set on {@link #solutions}.
         */
        private final boolean first;
        
        /**
         * The generated solution set (hash index using the specified join
         * variables).
         */
        private final HTree solutions;
        
        public ControllerTask(final NamedSubqueryOp op,
                final BOpContext<IBindingSet> context) {

            if (op == null)
                throw new IllegalArgumentException();

            if (context == null)
                throw new IllegalArgumentException();

            this.context = context;

            this.stats = ((NamedSolutionSetStats) context.getStats());
            
            this.subquery = (PipelineOp) op
                    .getRequiredProperty(Annotations.SUBQUERY);

            this.namedSetRef = (NamedSolutionSetRef) op
                    .getRequiredProperty(Annotations.NAMED_SET_REF);

            this.joinVars = (IVariable[]) op
                    .getRequiredProperty(Annotations.JOIN_VARS);
            
            {

                /*
                 * First, see if the map already exists.
                 * 
                 * Note: Since the operator is not thread-safe, we do not need
                 * to use a putIfAbsent pattern here.
                 */
                final IQueryAttributes attrs = context.getRunningQuery()
                        .getAttributes();

                HTree solutions = (HTree) attrs.get(namedSetRef);

                if (solutions == null) {

                    /*
                     * Create the map(s).
                     */
                    
                    final IndexMetadata metadata = new IndexMetadata(
                            UUID.randomUUID());

                    metadata.setAddressBits(op.getAddressBits());

                    metadata.setRawRecords(op.getRawRecords());

                    metadata.setMaxRecLen(op.getMaxRecLen());

                    metadata.setKeyLen(Bytes.SIZEOF_INT); // int32 hash code
                                                          // keys.

                    /*
                     * TODO This sets up a tuple serializer for a presumed case
                     * of 4 byte keys (the buffer will be resized if necessary)
                     * and explicitly chooses the SimpleRabaCoder as a
                     * workaround since the keys IRaba for the HTree does not
                     * report true for isKeys(). Once we work through an
                     * optimized bucket page design we can revisit this as the
                     * FrontCodedRabaCoder should be a good choice, but it
                     * currently requires isKeys() to return true.
                     */
                    final ITupleSerializer<?, ?> tupleSer = new DefaultTupleSerializer(
                            new ASCIIKeyBuilderFactory(Bytes.SIZEOF_INT),
                            new SimpleRabaCoder(),// keys : TODO Optimize for int32!
                            new SimpleRabaCoder() // vals
                    );

                    metadata.setTupleSerializer(tupleSer);

                    /*
                     * This wraps an efficient raw store interface around a
                     * child memory manager created from the IMemoryManager
                     * which is backing the query.
                     */
                    final IRawStore store = new MemStore(context
                            .getRunningQuery().getMemoryManager()
                            .createAllocationContext());

                    // Will support incremental eviction and persistence.
                    solutions = HTree.create(store, metadata);

                    /*
                     * Note: This is done once the subquery has been run so we
                     * can checkpoint the HTree first and put the read-only
                     * reference on the attribute.
                     */ 
//                    if (attrs.putIfAbsent(namedSetRef, solutions) != null)
//                        throw new AssertionError();

                    this.first = true;
                    
                } else {
                    
                    this.first = false;
                    
                }

                this.solutions = solutions;

            }
            
        }

        /**
         * Evaluate.
         */
        public Void call() throws Exception {
            
            try {
                
                if(first) {

                    // Generate the result set and write it on the HTree.
                    new SubqueryTask(new ListBindingSet(), subquery, context)
                            .call();

                }

                // source.
                final IAsynchronousIterator<IBindingSet[]> source = context
                        .getSource();

                // default sink
                final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

                BOpUtility.copy(source, sink, null/* sink2 */,
                        null/* select */, null/* constraints */,
                        context.getStats());

                sink.flush();

                // Done.
                return null;

            } finally {
                
                context.getSource().close();

                context.getSink().close();
                
                if (context.getSink2() != null)
                    context.getSink2().close();

            }
            
        }

        /**
         * Checkpoint the {@link HTree} containing the results of the subquery,
         * re-load the {@link HTree} in a read-only mode from that checkpoint
         * and then set the reference to the read-only view of the {@link HTree}
         * on the {@link IQueryAttributes}. This exposes a view of the
         * {@link HTree} which is safe for concurrent readers.
         */
        private void saveSolutionSet() {
            
            // Checkpoint the HTree.
            final Checkpoint checkpoint = solutions.writeCheckpoint2();
            
            // Get a read-only view of the HTree.
            final HTree readOnly = HTree.load(solutions.getStore(),
                    checkpoint.getCheckpointAddr(), true/* readOnly */);

            final IQueryAttributes attrs = context.getRunningQuery()
                    .getAttributes();

            if (attrs.putIfAbsent(namedSetRef, readOnly) != null)
                throw new AssertionError();

        }
        
        /**
         * Run a subquery.
         */
        private class SubqueryTask implements Callable<Void> {

            /**
             * The evaluation context for the parent query.
             */
            private final BOpContext<IBindingSet> parentContext;

            /**
             * The source binding set.
             */
            private final IBindingSet bset;

            /**
             * The root operator for the subquery.
             */
            private final BOp subQueryOp;

            public SubqueryTask(final IBindingSet bset, final BOp subQuery,
                    final BOpContext<IBindingSet> parentContext) {

                this.bset = bset;
                
                this.subQueryOp = subQuery;

                this.parentContext = parentContext;

            }

            public Void call() throws Exception {

            	// The subquery
                IRunningQuery runningSubquery = null;
            	// The iterator draining the subquery
                IAsynchronousIterator<IBindingSet[]> subquerySolutionItr = null;
                try {

                    final QueryEngine queryEngine = parentContext.getRunningQuery()
                            .getQueryEngine();
                    
                    runningSubquery = queryEngine.eval((PipelineOp) subQueryOp,
                            bset);

					try {
						
						// Iterator visiting the subquery solutions.
						subquerySolutionItr = runningSubquery.iterator();

						// Buffer the solutions on the hash index.
                        final long ncopied = HashJoinUtility.acceptSolutions(
                                subquerySolutionItr, joinVars, stats,
                                solutions, false/* optional */);

						// Wait for the subquery to halt / test for errors.
						runningSubquery.get();

                        // Report the #of solutions in the named solution set.
                        stats.solutionSetSize.addAndGet(ncopied);

                        // Publish the solution set on the query context.
                        saveSolutionSet();

                        if (log.isInfoEnabled())
                            log.info("Solution set " + namedSetRef + " has "
                                    + ncopied + " solutions.");
                        
					} catch (InterruptedException ex) {

						// this thread was interrupted, so cancel the subquery.
						runningSubquery
								.cancel(true/* mayInterruptIfRunning */);

						// rethrow the exception.
						throw ex;
						
					}
					
                } catch (Throwable t) {

					if (runningSubquery == null
							|| runningSubquery.getCause() != null) {
						/*
						 * If things fail before we start the subquery, or if a
						 * subquery fails (due to abnormal termination), then
						 * propagate the error to the parent and rethrow the
						 * first cause error out of the subquery.
						 * 
						 * Note: IHaltable#getCause() considers exceptions
						 * triggered by an interrupt to be normal termination.
						 * Such exceptions are NOT propagated here and WILL NOT
						 * cause the parent query to terminate.
						 */
                        throw new RuntimeException(ControllerTask.this.context
                                .getRunningQuery().halt(
                                        runningSubquery == null ? t
                                                : runningSubquery.getCause()));
                    }
					
                } finally {

					try {

						// ensure subquery is halted.
						if (runningSubquery != null)
							runningSubquery
									.cancel(true/* mayInterruptIfRunning */);
						
					} finally {

						// ensure the subquery solution iterator is closed.
						if (subquerySolutionItr != null)
							subquerySolutionItr.close();

					}
					
                }

                // Done.
                return null;
                
            }

        } // SubqueryTask

    } // ControllerTask

}
