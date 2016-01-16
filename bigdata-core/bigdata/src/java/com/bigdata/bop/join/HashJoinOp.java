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
 * Created on Nov 14, 2011
 */

package com.bigdata.bop.join;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.IShardwisePipelineOp;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.controller.INamedSolutionSetRef;
import com.bigdata.bop.controller.NamedSetAnnotations;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.AbstractUnsynchronizedArrayBuffer;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IBindingSetAccessPath;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Abstract base class for both JVM and native memory hash join against an
 * {@link IAccessPath}. The source solutions from the pipeline are buffered on a
 * hash index. Depending on the implementation, the hash index may have a
 * threshold that will trigger an evaluation pass of the hash join. If not, then
 * the hash join will run exactly once. When the hash join runs, the access path
 * is scanned and the hash index (of intermediate solutions from the pipeline)
 * is probed for each solution read from the {@link IAccessPath}. Solutions
 * which join are output.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
abstract public class HashJoinOp<E> extends PipelineOp implements
        IShardwisePipelineOp<E> {

    static private final transient Logger log = Logger
            .getLogger(HashJoinOp.class);

    private static final long serialVersionUID = 1L;

    public interface Annotations extends AccessPathJoinAnnotations,
            NamedSetAnnotations, HashJoinAnnotations {

    }

    /**
     * @param op
     */
    public HashJoinOp(final HashJoinOp<E> op) {

        super(op);

    }

    public HashJoinOp(final BOp[] args, final NV... annotations) {

        this(args, NV.asMap(annotations));

    }

    /**
     * @param args
     * @param annotations
     */
    public HashJoinOp(final BOp[] args, final Map<String, Object> annotations) {

        super(args, annotations);

        /*
         * Validate common requirements for all concrete implementations of this
         * operator.
         */
        
        switch (getEvaluationContext()) {
        case CONTROLLER:
        case SHARDED:
        case HASHED:
            break;
        default:
            throw new UnsupportedOperationException(
                    Annotations.EVALUATION_CONTEXT + "="
                            + getEvaluationContext());
        }

        // Predicate for the access path must be specified.
        getPredicate();

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

    /**
     * {@inheritDoc}
     * 
     * @see Annotations#PREDICATE
     */
    @Override
    @SuppressWarnings("unchecked")
    public IPredicate<E> getPredicate() {

        return (IPredicate<E>) getRequiredProperty(Annotations.PREDICATE);

    }
        
    /**
     * Return <code>true</code> iff the predicate associated with the join is
     * optional.
     * 
     * @see IPredicate.Annotations#OPTIONAL
     */
    protected boolean isOptional() {
        
        return getPredicate().isOptional();
        
    }
    
    @Override
    public BaseJoinStats newStats() {

        return new BaseJoinStats();

    }

    /**
     * Return the instance of the {@link IHashJoinUtility} to be used by this
     * operator. This method is invoked once, the first time this operator is
     * evaluated. The returned {@link IHashJoinUtility} reference is attached to
     * the {@link IQueryAttributes} and accessed there on subsequent evaluation
     * passes for this operator.
     * 
     * @param context
     *            The {@link BOpEvaluationContext}
     * @param namedSetRef
     *            Metadata to identify the named solution set.
     * @param joinType
     *            The type of join.
     */
    abstract protected IHashJoinUtility newState(//
            final BOpContext<IBindingSet> context,//
            final INamedSolutionSetRef namedSetRef, //
            final JoinTypeEnum joinType//
            );

    /**
     * Return <code>true</code> if {@link ChunkTask#doHashJoin()} should be
     * executed in a given operator {@link ChunkTask} invocation.
     * 
     * @param context
     *            The operator evaluation context.
     * @param state
     *            The {@link IHashJoinUtility} instance.
     */
    abstract protected boolean runHashJoin(final BOpContext<?> context,
            final IHashJoinUtility state);

    @Override
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ChunkTask<E>(context, this));
        
    }

    /**
     * Task executing on the node.
     */
    private static class ChunkTask<E> implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        private final HashJoinOp<E> op;

        private final IRelation<E> relation;
        
        private final IPredicate<E> pred;
        
        private final BaseJoinStats stats;

        private final IHashJoinUtility state;
        
        private final IBlockingBuffer<IBindingSet[]> sink;
        
        private final IBlockingBuffer<IBindingSet[]> sink2;

        public ChunkTask(final BOpContext<IBindingSet> context,
                final HashJoinOp<E> op) {

            this.context = context;

            this.stats = (BaseJoinStats) context.getStats();

            this.pred = op.getPredicate();

            this.relation = context.getRelation(pred);

            this.sink = context.getSink();

            this.sink2 = context.getSink2();

            this.op = op;

            {

                /*
                 * First, see if the map already exists.
                 * 
                 * Note: Since the operator is not thread-safe, we do not need
                 * to use a putIfAbsent pattern here.
                 * 
                 * Note: Publishing the [state] as a query attribute provides
                 * visibility into the hash join against the access path even
                 * for implementations (such as the JVMHashJoinOp) where the
                 * entire operation will occur within a single evaluation pass.
                 */

                final INamedSolutionSetRef namedSetRef = (INamedSolutionSetRef) op
                        .getRequiredProperty(Annotations.NAMED_SET_REF);

                /*
    			 * Lookup the attributes for the query on which we will hang the
    			 * solution set. See BLZG-1493 (if queryId is null, use the query
    			 * attributes for this running query).
    			 */
				final IQueryAttributes attrs = context.getQueryAttributes(namedSetRef.getQueryId());

                IHashJoinUtility state = (IHashJoinUtility) attrs
                        .get(namedSetRef);

                if (state == null) {

                    state = op.newState(context, namedSetRef,
                            op.isOptional() ? JoinTypeEnum.Optional
                                    : JoinTypeEnum.Normal);

                    attrs.put(namedSetRef, state);

                }

                this.state = state;

            }

        }

        @Override
        public Void call() throws Exception {

            boolean didRun = false;
            try {

                acceptSolutions();

                if(op.runHashJoin(context, state)) {

                    didRun = true;
                    
                    doHashJoin();
                    
                }

                // Done.
                return null;
                
            } finally {

                if (didRun) {

                    /*
                     * The state needs to be released each time this operator
                     * runs in order to discard the intermediate solutions
                     * buffered on the hash index that were just joined against
                     * the access path. If we do not discard the state after
                     * processing the intermediate solutions, then they will
                     * continue to accumulate and we will over-report joins
                     * (duplicate solutions will be output for things already in
                     * the hash index the next time we evaluate the hash join
                     * against the access path).
                     */

                    state.release();

                }
                
                sink.close();

                if (sink2 != null)
                    sink2.close();
                
            }

        }

        /**
         * Buffer intermediate resources.
         */
        private void acceptSolutions() {

            state.acceptSolutions(context.getSource(), stats);

        }

        /**
         * Return the access path that to be scanned. Solutions read from this
         * access path will be used to probe the hash index to identify
         * solutions that can join.
         */
        private IBindingSetAccessPath<?> getAccessPath() {

            return (IBindingSetAccessPath<?>) context.getAccessPath(relation,
                    pred);

        }
        
        /**
         * Do a hash join of the buffered solutions with the access path.
         */
        private void doHashJoin() {

            if (state.isEmpty())
                return;

            final IBindingSetAccessPath<?> accessPath = getAccessPath();

            if (log.isInfoEnabled())
                log.info("accessPath=" + accessPath);

            stats.accessPathCount.increment();

            stats.accessPathRangeCount.add(accessPath
                    .rangeCount(false/* exact */));

            final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = new UnsyncLocalOutputBuffer<IBindingSet>(
                    op.getChunkCapacity(), sink);

            final long cutoffLimit = pred.getProperty(
                    IPredicate.Annotations.CUTOFF_LIMIT,
                    IPredicate.Annotations.DEFAULT_CUTOFF_LIMIT);

            // Obtain the iterator for the current join dimension.
            final ICloseableIterator<IBindingSet[]> itr = accessPath
                  .solutions(context, cutoffLimit, stats);

            /*
             * Note: The [stats] are NOT passed in here since the chunksIn and
             * unitsIn were updated when the pipeline solutions were accepted
             * into the hash index. If we passed in stats here, they would be
             * double counted when we executed the hash join against the access
             * path.
             */
            state.hashJoin(
                    itr,// left
                    null, // stats
                    unsyncBuffer// out
                    );

            switch (state.getJoinType()) {
            case Normal:
                /*
                 * Nothing to do.
                 */
                break;
            case Optional:
            case NotExists: {
                /*
                 * Output the optional solutions.
                 */

                // where to write the optional solutions.
                final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer2 = sink2 == null ? unsyncBuffer
                        : new UnsyncLocalOutputBuffer<IBindingSet>(
                                op.getChunkCapacity(), sink2);

                state.outputOptionals(unsyncBuffer2);

                unsyncBuffer2.flush();
                if (sink2 != null)
                    sink2.flush();

                break;
            }
            case Exists: {
                /*
                 * Output the join set.
                 */
                state.outputJoinSet(unsyncBuffer);
                break;
            }
            default:
                throw new AssertionError();
            }

            unsyncBuffer.flush();
            sink.flush();

        }
        
    } // class ChunkTask

}
