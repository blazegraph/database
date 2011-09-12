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
 * Created on Aug 30, 2011
 */

package com.bigdata.bop.controller;


import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.join.BaseJoinStats;
import com.bigdata.bop.join.HashJoinAnnotations;
import com.bigdata.bop.join.HashJoinUtility;
import com.bigdata.bop.join.JoinAnnotations;
import com.bigdata.htree.HTree;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;
import com.bigdata.striterator.Dechunkerator;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Operator joins a named solution set into the pipeline.
 * 
 * @see NamedSubqueryOp
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NamedSubqueryIncludeOp extends PipelineOp {

    static private final transient Logger log = Logger
            .getLogger(NamedSubqueryIncludeOp.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends PipelineOp.Annotations,
            HashJoinAnnotations {

        /**
         * The {@link NamedSolutionSetRef} used to locate the {@link HTree}
         * having the data for the named solution set. The query UUID must be
         * extracted and used to lookup the {@link IRunningQuery} to which the
         * solution set was attached. The {@link HTree} is then resolved against
         * the {@link IQueryAttributes} on that {@link IRunningQuery}.
         * 
         * @see NamedSolutionSetRef
         * @see NamedSubqueryOp.Annotations#NAMED_SET_REF
         */
        final String NAMED_SET_REF = NamedSubqueryOp.Annotations.NAMED_SET_REF;

        /**
         * An optional {@link IVariable}[] identifying the variables to be
         * retained in the {@link IBindingSet}s written out by the operator. All
         * variables are retained unless this annotation is specified. This is
         * normally set to the <em>projection</em> of the subquery, in which
         * case the lexical scope of the variables is will be properly managed
         * for the subquery INCLUDE join.
         * 
         * @see JoinAnnotations#SELECT
         */
        final String SELECT = JoinAnnotations.SELECT;

    }

    /**
     * Deep copy constructor.
     */
    public NamedSubqueryIncludeOp(NamedSubqueryIncludeOp op) {
        super(op);
    }

    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public NamedSubqueryIncludeOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

    }

    public NamedSubqueryIncludeOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }

    public BaseJoinStats newStats() {

        return new BaseJoinStats();

    }

    @Override
    public FutureTask<Void> eval(BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ChunkTask<IBindingSet>(context, this));
        
    }

    /**
     * Task executing on the node.
     */
    private static class ChunkTask<E> implements Callable<Void> {

        private final BOpContext<IBindingSet> context;

        private final NamedSubqueryIncludeOp op;

        private final IVariable<E>[] joinVars;
        
        private final IConstraint[] constraints = null;

        private final IVariable<?>[] selectVars;

        private final boolean optional = false;
        
//        private final String namedSet;
        
        private final BaseJoinStats stats;

        private final IBlockingBuffer<IBindingSet[]> sink;
        
//        private final IBlockingBuffer<IBindingSet[]> sink2;

        /**
         * A map whose keys are the bindings on the specified variables. The
         * values in the map are <code>null</code>s.
         * <p>
         * Note: The map is shared state and can not be discarded or cleared
         * until the last invocation!!!
         */
        private final HTree rightSolutions;

        /**
         * This is not an optional join, so this is always <code>null</code>.
         */
        private final HTree joinSet = null;

        @SuppressWarnings("unchecked")
        public ChunkTask(final BOpContext<IBindingSet> context,
                final NamedSubqueryIncludeOp op) {

            this.context = context;

            this.stats = (BaseJoinStats) context.getStats();

            this.selectVars = (IVariable<?>[]) op
                    .getProperty(Annotations.SELECT);

            this.joinVars = (IVariable<E>[]) op
                    .getRequiredProperty(Annotations.JOIN_VARS);
            
//            this.constraints = op.constraints();
//
//            this.optional = op.isOptional();

            this.sink = context.getSink();

//            this.sink2 = context.getSink2();

            this.op = op;

            // The name of the attribute used to discover the solution set.
            final NamedSolutionSetRef namedSetRef = (NamedSolutionSetRef) op
                    .getRequiredProperty(Annotations.NAMED_SET_REF);

            // Lookup the attributes for the query on which we will hang the
            // solution set.
            final IQueryAttributes attrs = context
                    .getQueryAttributes(namedSetRef.queryId);

            // The HTree holding the solutions.
            rightSolutions = (HTree) attrs.get(namedSetRef);

            if (rightSolutions == null) {
             
                // The solution set was not found!
                
                throw new RuntimeException("Not found: " + namedSetRef);
                
            }

        }

//        /**
//         * Discard the {@link HTree} data.
//         */
//        private void release() {
//
//            if (joinSet != null) {
//
//                joinSet.close();
//
////                joinSet = null;
//                
//            }
//
//            if (rightSolutions != null) {
//
//                final IRawStore store = rightSolutions.getStore();
//
//                rightSolutions.close();
//                
////                sourceSolutions = null;
//                
//                store.close();
//
//            }
//
//        }
        
        public Void call() throws Exception {

            try {

                doHashJoin();
                
                // Done.
                return null;
                
            } finally {

                if (context.isLastInvocation()) {

                    /*
                     * Note: It is possible to INCLUDE the named temporary
                     * solution set multiple times within a query. If we want to
                     * release() the hash tree then we need to know how many
                     * times the temporary solution set is being included and
                     * decrement a counter each time. When the counter reaches
                     * zero, we can release the HTree.
                     */
//                    release();

                }
                
                sink.close();

//                if (sink2 != null)
//                    sink2.close();
                
            }

        }
        
        /**
         * Do a hash join of the buffered solutions with the access path.
         */
        private void doHashJoin() {

            if (rightSolutions.getEntryCount() == 0)
                return;
            
            if (log.isDebugEnabled()) {
                log.debug("rightSolutions=" + rightSolutions.getEntryCount());
                log.debug("joinVars=" + Arrays.toString(joinVars));
            }

            stats.accessPathCount.increment();

            stats.accessPathRangeCount.add(rightSolutions.getEntryCount());

            final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = new UnsyncLocalOutputBuffer<IBindingSet>(
                    op.getChunkCapacity(), sink);//, new PopFilter(selectVars));

            final ICloseableIterator<IBindingSet> leftItr = new Dechunkerator<IBindingSet>(
                    context.getSource());

            HashJoinUtility.hashJoin(leftItr, unsyncBuffer, joinVars,
                    selectVars, constraints, rightSolutions/* hashIndex */,
                    joinSet, optional, true/* leftIsPipeline */);

//            if (optional) {
//
//                // where to write the optional solutions.
//                final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer2 = sink2 == null ? unsyncBuffer
//                        : new UnsyncLocalOutputBuffer<IBindingSet>(
//                                op.getChunkCapacity(), sink2);
//
//                HashJoinUtility.outputOptionals(unsyncBuffer2, rightSolutions,
//                        joinSet);
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
