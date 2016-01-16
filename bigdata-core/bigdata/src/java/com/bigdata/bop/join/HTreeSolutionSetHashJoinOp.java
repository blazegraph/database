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
 * Created on Aug 30, 2011
 */

package com.bigdata.bop.join;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.NV;
import com.bigdata.htree.HTree;

/**
 * {@inheritDoc}
 * <p>
 * {@link HTree} Specific version.
 * 
 * @see HTreeHashJoinUtility
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: NamedSubqueryIncludeOp.java 5178 2011-09-12 19:09:23Z
 *          thompsonbry $
 */
public class HTreeSolutionSetHashJoinOp extends SolutionSetHashJoinOp {

//    static private final transient Logger log = Logger
//            .getLogger(HTreeSolutionSetHashJoinOp.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

//    public interface Annotations extends PipelineOp.Annotations,
//            NamedSetAnnotations {
//
//        /**
//         * An {@link IConstraint}[] to be applied to solutions when they are
//         * joined (optional).
//         */
//        final String CONSTRAINTS = JoinAnnotations.CONSTRAINTS;
//        
//        /**
//         * When <code>true</code> the hash index identified by
//         * {@link #NAMED_SET_REF} will be released when this operator is done
//         * (default {@value #DEFAULT_RELEASE}).
//         * <p>
//         * Note: Whether or not the hash index can be released depends on
//         * whether or not the hash index will be consumed by more than one
//         * operator in the query plan. For example, a named solution set can be
//         * consumed by more than one operator and thus must not be released
//         * until all such operators are done.
//         */
//        final String RELEASE = HTreeSolutionSetHashJoinOp.class + ".release";
//
//        final boolean DEFAULT_RELEASE = true;
//        
//    }

    /**
     * Deep copy constructor.
     */
    public HTreeSolutionSetHashJoinOp(HTreeSolutionSetHashJoinOp op) {

        super(op);
        
    }

    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public HTreeSolutionSetHashJoinOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

//        if (getProperty(Annotations.RELEASE, Annotations.DEFAULT_RELEASE)
//                && !isLastPassRequested()) {
//            /*
//             * In order to release the hash index, this operator needs to be
//             * notified when no more source solutions will become available.
//             */
//            throw new IllegalArgumentException(Annotations.RELEASE
//                    + " requires " + Annotations.LAST_PASS);
//        }
//        
//        // The RHS annotation must be specified.
//        getRequiredProperty(Annotations.NAMED_SET_REF);
        
    }

    public HTreeSolutionSetHashJoinOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }
    
//    public BaseJoinStats newStats() {
//
//        return new BaseJoinStats();
//
//    }
//
//    @Override
//    public FutureTask<Void> eval(BOpContext<IBindingSet> context) {
//
//        return new FutureTask<Void>(new ChunkTask<IBindingSet>(context, this));
//        
//    }
//
//    /**
//     * Task executing on the node.
//     */
//    private static class ChunkTask<E> implements Callable<Void> {
//
//        private final BOpContext<IBindingSet> context;
//
//        private final HTreeSolutionSetHashJoinOp op;
//
//        private final HTreeHashJoinUtility state;
//
//        private final IConstraint[] constraints;
//        
//        private final boolean release;
//        
//        private final BaseJoinStats stats;
//
//        private final IBlockingBuffer<IBindingSet[]> sink;
//        
//        private final IBlockingBuffer<IBindingSet[]> sink2;
//
//        public ChunkTask(final BOpContext<IBindingSet> context,
//                final HTreeSolutionSetHashJoinOp op) {
//
//            this.context = context;
//
//            this.stats = (BaseJoinStats) context.getStats();
//
//            this.release = op.getProperty(Annotations.RELEASE,
//                    Annotations.DEFAULT_RELEASE);
//
//            this.sink = context.getSink();
//
//            this.sink2 = context.getSink2();
//
//            this.op = op;
//
//            // The name of the attribute used to discover the solution set.
//            final NamedSolutionSetRef namedSetRef = (NamedSolutionSetRef) op
//                    .getRequiredProperty(Annotations.NAMED_SET_REF);
//
//            // Lookup the attributes for the query on which we will hang the
//            // solution set.
//            final IQueryAttributes attrs = context
//                    .getQueryAttributes(namedSetRef.queryId);
//
//            state = (HTreeHashJoinUtility) attrs.get(namedSetRef);
//
//            if (state == null) {
//                
//                // The solution set was not found!
//                
//                throw new RuntimeException("Not found: " + namedSetRef);
//                
//            }
//
//            if (!state.getJoinType().isNormal() && !op.isLastPassRequested()) {
//
//                /*
//                 * Anything but a Normal join requires that we observe all solutions
//                 * and then do some final reporting. This is necessary for Optional,
//                 * Exists, and NotExists. 
//                 */
//
//                throw new UnsupportedOperationException(state.getJoinType()
//                        + " requires " + Annotations.LAST_PASS);
//
//            }
//
//            /*
//             * Combine the original constraints (if any) with those attached to
//             * this operator (if any).
//             * 
//             * Note: The solution set hash join is used to join in a hash index
//             * generated by some other part of the query plan. Since it is also
//             * used for named subqueries, which can be included in more than one
//             * location, it is necessary that we can override/expand on the join
//             * constraints for this operator.
//             */
//            this.constraints = BOpUtility.concat(
//                    (IConstraint[]) op.getProperty(Annotations.CONSTRAINTS),
//                    state.getConstraints());
//
//        }
//
//        public Void call() throws Exception {
//
//            try {
//
//                doHashJoin();
//                
//                // Done.
//                return null;
//                
//            } finally {
//
//                if (release && context.isLastInvocation()) {
//
//                    /*
//                     * Note: It is possible to INCLUDE the named temporary
//                     * solution set multiple times within a query. If we want to
//                     * release() the hash tree then we need to know how many
//                     * times the temporary solution set is being included and
//                     * decrement a counter each time. When the counter reaches
//                     * zero, we can release the hash index.
//                     */
//
//                    state.release();
//
//                }
//                
//                sink.close();
//
//                if (sink2 != null)
//                    sink2.close();
//                
//            }
//
//        }
//        
//        /**
//         * Do a hash join of the buffered solutions with the access path.
//         */
//        private void doHashJoin() {
//
//            if (state.isEmpty())
//                return;
//            
//            stats.accessPathCount.increment();
//
//            stats.accessPathRangeCount.add(state.getRightSolutionCount());
//
//            final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = new UnsyncLocalOutputBuffer<IBindingSet>(
//                    op.getChunkCapacity(), sink);
//
//            final ICloseableIterator<IBindingSet> leftItr = new Dechunkerator<IBindingSet>(
//                    context.getSource());
//
//            state.hashJoin2(leftItr, unsyncBuffer, //true/* leftIsPipeline */,
//                    constraints);
//
//            if (context.isLastInvocation()) {
//
//                switch (state.getJoinType()) {
//                case Normal:
//                    /*
//                     * Nothing to do.
//                     */
//                    break;
//                case Optional:
//                case NotExists: {
//                    /*
//                     * Output the optional solutions.
//                     */
//
//                    // where to write the optional solutions.
//                    final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer2 = sink2 == null ? unsyncBuffer
//                            : new UnsyncLocalOutputBuffer<IBindingSet>(
//                                    op.getChunkCapacity(), sink2);
//
//                    state.outputOptionals(unsyncBuffer2);
//
//                    unsyncBuffer2.flush();
//                    if (sink2 != null)
//                        sink2.flush();
//
//                    break;
//                }
//                case Exists: {
//                    /*
//                     * Output the join set.
//                     * 
//                     * Note: This has special hooks to support (NOT) EXISTS
//                     * graph patterns, which must bind the "ASK_VAR" depending
//                     * on whether or not the graph pattern is satisified.
//                     */
//                    final IVariable<?> askVar = state.getAskVar();
//                    // askVar := true
//                    state.outputJoinSet(unsyncBuffer);
//                    if (askVar != null) {
//                        // askVar := false;
//                        state.outputOptionals(unsyncBuffer);
//                    }
//                    break;
//                }
//                default:
//                    throw new AssertionError();
//                }
//
//            }
//
//            unsyncBuffer.flush();
//            sink.flush();
//
//        }
//        
//    } // class ChunkTask

}
