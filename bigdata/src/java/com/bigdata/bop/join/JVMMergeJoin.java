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
 * Created on Nov 7, 2011
 */

package com.bigdata.bop.join;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IQueryAttributes;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.controller.HTreeNamedSubqueryOp;
import com.bigdata.bop.controller.NamedSolutionSetRef;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;

/**
 * An N-way merge join on the Java heap.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JVMMergeJoin extends PipelineOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends AccessPathJoinAnnotations,
            JVMHashJoinAnnotations {

        /**
         * The {@link NamedSolutionSetRef} used to locate the named solution
         * sets. The value stored under the attribute must be an
         * {@link IHashJoinUtility}[]. There must be at least 2 entries in the
         * array.
         * 
         * @see NamedSolutionSetRef
         * @see HTreeNamedSubqueryOp.Annotations#NAMED_SET_REF
         */
        String NAMED_SET_REF = HTreeNamedSubqueryOp.Annotations.NAMED_SET_REF;
        
        /**
         * Constraints to be applied by the join (in addition to any associated
         * with the {@link HTreeHashJoinUtility} state in the
         * {@link #NAMED_SET_REF}).
         */
        String CONSTRAINTS = JoinAnnotations.CONSTRAINTS;
        
        String OPTIONAL = JoinAnnotations.OPTIONAL;
        
        boolean DEFAULT_OPTIONAL = JoinAnnotations.DEFAULT_OPTIONAL;
        
        /**
         * When <code>true</code> the hash index identified by
         * {@link #NAMED_SET_REF} will be released when this operator is done
         * (default {@value #DEFAULT_RELEASE}).
         * <p>
         * Note: Whether or not the hash index can be released depends on
         * whether or not the hash index will be consumed by more than one
         * operator in the query plan. For example, a named solution set can be
         * consumed by more than one operator and thus must not be released
         * until all such operators are done.
         * 
         * TODO Alternatively, we could specify the #of different locations in
         * the query plan where the named solution set will be consumed. This
         * could be part of the {@link HTreeHashJoinUtility} state, in which
         * case it would only be set as an annotation on the operator which
         * generates the hash index.
         */
        final String RELEASE = HTreeSolutionSetHashJoinOp.class + ".release";

        final boolean DEFAULT_RELEASE = true;
        
    }

    /**
     * @param args
     * @param annotations
     */
    public JVMMergeJoin(BOp[] args, Map<String, Object> annotations) {
        
        super(args, annotations);

        if (!isLastPassRequested()) {

            /*
             * FIXME I am not convinced that "LAST PASS" evaluation semantics
             * are required here. However, we should not be evaluating this
             * operator more than once.
             */

            throw new UnsupportedOperationException(
                    JoinAnnotations.OPTIONAL + " requires "
                            + Annotations.LAST_PASS);
        
        }

    }

    /**
     * @param op
     */
    public JVMMergeJoin(JVMMergeJoin op) {

        super(op);
        
    }

    public JVMMergeJoin(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));
        
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

        private final JVMMergeJoin op;

        private final JVMHashJoinUtility[] state;
        
        private final IConstraint[] constraints;

//        private final IVariable<?>[] selectVars;

        private final boolean optional;
        
        private final boolean release;
        
//        private final BaseJoinStats stats;

        private final IBlockingBuffer<IBindingSet[]> sink;
        
        private final IBlockingBuffer<IBindingSet[]> sink2;

        public ChunkTask(final BOpContext<IBindingSet> context,
                final JVMMergeJoin op) {

            this.context = context;

//            this.stats = (BaseJoinStats) context.getStats();

//            this.selectVars = (IVariable<?>[]) op
//                    .getProperty(Annotations.SELECT);

            this.constraints = (IConstraint[]) op
                    .getProperty(Annotations.CONSTRAINTS);

            this.optional = op.getProperty(Annotations.OPTIONAL,
                    Annotations.DEFAULT_OPTIONAL);

            this.release = op.getProperty(Annotations.RELEASE,
                    Annotations.DEFAULT_RELEASE);

            this.sink = context.getSink();

            this.sink2 = context.getSink2();

            this.op = op;

            // The names of the attributes used to discover the solution sets.
            final NamedSolutionSetRef[] namedSetRef = (NamedSolutionSetRef[]) op
                    .getRequiredProperty(Annotations.NAMED_SET_REF);

            state = new JVMHashJoinUtility[namedSetRef.length];

            if (state.length < 2) {

                throw new RuntimeException(
                        "Merge join requires at least 2 sources.");

            }
            
            for (int i = 0; i < state.length; i++) {

                final IQueryAttributes attrs = context
                        .getQueryAttributes(namedSetRef[i].queryId);

                state[i] = (JVMHashJoinUtility) attrs.get(namedSetRef[i]);
                
                if (state[i] == null) {

                    // The solution set was not found!
                    throw new RuntimeException("Not found: " + namedSetRef[i]);

                }

            }
            
        }
        
        public Void call() throws Exception {

            try {

                if (context.isLastInvocation()) {

                    final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = new UnsyncLocalOutputBuffer<IBindingSet>(
                            op.getChunkCapacity(), sink);

                    final IHashJoinUtility[] others = new IHashJoinUtility[state.length - 1];

                    for (int i = 1; i < state.length; i++) {

                        others[i - 1] = state[i];

                    }

                    state[0].mergeJoin(others, unsyncBuffer, constraints,
                            optional);

                    unsyncBuffer.flush();

                    sink.flush();
                    
                }
                
                // Done.
                return null;
                
            } finally {

                if (release && context.isLastInvocation()) {

                    /*
                     * Note: It is possible to INCLUDE the named temporary
                     * solution set multiple times within a query. If we want to
                     * release() the hash tree then we need to know how many
                     * times the temporary solution set is being included and
                     * decrement a counter each time. When the counter reaches
                     * zero, we can release the hash index.
                     */
                    
                    for (IHashJoinUtility h : state) {
                     
                        h.release();
                        
                    }

                }
                
                sink.close();

                if (sink2 != null)
                    sink2.close();
                
            }

        }
                
    } // class ChunkTask

}
