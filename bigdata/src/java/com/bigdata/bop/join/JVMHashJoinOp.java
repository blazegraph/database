/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Aug 14, 2011
 */

package com.bigdata.bop.join;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.HashMapAnnotations;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.controller.NamedSolutionSetRef;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.AbstractUnsynchronizedArrayBuffer;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IBindingSetAccessPath;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;
import com.bigdata.striterator.ICloseableIterator;

/**
 * A hash join against an {@link IAccessPath} based on the Java collections
 * classes. Source solutions are buffered on the Java collection on each
 * evaluation pass. Once all source solutions have been buffered, the hash join
 * will run a single pass over the {@link IAccessPath} for the target
 * {@link IPredicate}. For some queries, this can be more efficient than probing
 * as-bound instances of the target {@link IPredicate} using a nested indexed
 * join, such as {@link PipelineOp}. This can also be more efficient on a
 * cluster where the key range scan of the target {@link IPredicate} will be
 * performed using predominately sequential IO.
 * <p>
 * The source solutions presented to a hash join MUST have bindings for the
 * {@link HashJoinAnnotations#JOIN_VARS} in order to join (they can still
 * succeed as optionals if the join variables are not bound).
 * 
 * @see JVMHashJoinUtility
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JVMHashJoinOp<E> extends AbstractHashJoinOp<E> {
    
    static private final transient Logger log = Logger
            .getLogger(JVMHashJoinOp.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends AbstractHashJoinOp.Annotations,
            HashMapAnnotations, HashJoinAnnotations {
        
    }
    
    /**
     * @param op
     */
    public JVMHashJoinOp(final JVMHashJoinOp<E> op) {
    
        super(op);
        
    }
    
    public JVMHashJoinOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));

    }

    /**
     * @param args
     * @param annotations
     */
    public JVMHashJoinOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

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

        if (getMaxParallel() != 1)
            throw new UnsupportedOperationException(Annotations.MAX_PARALLEL
                    + "=" + getMaxParallel());

        // Predicate for the access path must be specified.
        getPredicate();
        
        assertAtOnceJavaHeapOp();

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
    @SuppressWarnings("unchecked")
    public IPredicate<E> getPredicate() {

        return (IPredicate<E>) getRequiredProperty(Annotations.PREDICATE);

    }
        
    public BaseJoinStats newStats() {

        return new BaseJoinStats();

    }

    @Override
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ChunkTask<E>(context, this));
        
    }

    /**
     * Task executing on the node.
     */
    private static class ChunkTask<E> implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        private final JVMHashJoinOp<E> op;

        private final IRelation<E> relation;
        
        private final IPredicate<E> pred;
        
        private final BaseJoinStats stats;

        private final IBlockingBuffer<IBindingSet[]> sink;
        
        private final IBlockingBuffer<IBindingSet[]> sink2;

        private final JVMHashJoinUtility state;
        
        public ChunkTask(final BOpContext<IBindingSet> context,
                final JVMHashJoinOp<E> op) {

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
                 * visiblility into the hash join against the access path even
                 * though the entire operation will occur within a single
                 * evaluation pass for this operator.
                 */

                final NamedSolutionSetRef namedSetRef = (NamedSolutionSetRef) op
                        .getRequiredProperty(Annotations.NAMED_SET_REF);

                // Lookup the attributes for the query on which we will hang the
                // solution set.
                final IQueryAttributes attrs = context
                        .getQueryAttributes(namedSetRef.queryId);

                JVMHashJoinUtility state = (JVMHashJoinUtility) attrs
                        .get(namedSetRef);

                if (state == null) {

                    state = new JVMHashJoinUtility(op,
                            pred.isOptional() ? JoinTypeEnum.Optional
                                    : JoinTypeEnum.Normal);

                    attrs.put(namedSetRef, state);

                }

                this.state = state;

            }

        }
        
        public Void call() throws Exception {

            /*
             * Note: Because this is an at-once operator, the solutions are all
             * buffered on the query engine and this operator is invoked exactly
             * once.
             * 
             * Unlike the HTreeHashJoinOp, the concept of a LAST PASS evaluation
             * does not enter in to the evaluation of this operator. However,
             * but publishing the [state] on the query attribute we do gain
             * visiblity into the dynamics of the hash join while it is
             * executing against the B+Tree access path.
             */
            
            try {

                acceptSolutions();

//                if (context.isLastInvocation()) {

                    doHashJoin();
                    
//                }

                // Done.
                return null;
                
            } finally {

//                if (context.isLastInvocation()) {

                    state.release();

//                }
                
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
         * Do a hash join of the buffered solutions with the access path.
         */
        private void doHashJoin() {

            if (state.isEmpty())
                return;
            
            final IAccessPath<?> accessPath = context.getAccessPath(relation,
                    pred);

            if (log.isDebugEnabled())
                log.debug("accessPath=" + accessPath);

            stats.accessPathCount.increment();

            stats.accessPathRangeCount.add(accessPath
                    .rangeCount(false/* exact */));

            final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = new UnsyncLocalOutputBuffer<IBindingSet>(
                    op.getChunkCapacity(), sink);

        	final long cutoffLimit = pred.getProperty(
        			IPredicate.Annotations.CUTOFF_LIMIT, 
        			IPredicate.Annotations.DEFAULT_CUTOFF_LIMIT);
        	
            // Obtain the iterator for the current join dimension.
            final ICloseableIterator<IBindingSet> itr;
            if (cutoffLimit == Long.MAX_VALUE) 
            	itr = ((IBindingSetAccessPath<?>) accessPath).solutions(stats);
            else
            	itr = ((IBindingSetAccessPath<?>) accessPath).solutions(cutoffLimit, stats);
            
            state.hashJoin(
                    itr,// left
                    unsyncBuffer // where to write the solutions which join.
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

//            if (state.getJoinType().isOptional()) {
//
//                // where to write the optional solutions.
//                final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer2 = sink2 == null ? unsyncBuffer
//                        : new UnsyncLocalOutputBuffer<IBindingSet>(
//                                op.getChunkCapacity(), sink2);
//
//                state.outputOptionals(unsyncBuffer2);
//
//                unsyncBuffer2.flush();
//                if (sink2 != null)
//                    sink2.flush();
//
//            }

            unsyncBuffer.flush();
            sink.flush();

        }
        
    } // class ChunkTask

}
