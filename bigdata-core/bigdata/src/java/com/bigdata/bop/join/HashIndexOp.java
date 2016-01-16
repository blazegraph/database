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

package com.bigdata.bop.join;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.NV;
import com.bigdata.htree.HTree;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;

/**
 * Operator builds a hash index from the source solutions. Once all source
 * solutions have been indexed, the source solutions are output on the default
 * sink. The set of variables to be copied to the sink may be restricted by an
 * annotation.
 * <p>
 * The main use case for building a hash index is to execute a sub-group or
 * sub-select. In both cases, the {@link HashIndexOp} is generated before we
 * enter the sub-plan. All solutions from the hash index are then flowed into
 * the sub-plan. Solutions emitted by the sub-plan are then re-integrated into
 * the parent using a {@link SolutionSetHashJoinOp}.
 * <p>
 * There are two concrete implementations of this operator. One for the
 * {@link HTree} and one for the JVM {@link ConcurrentHashMap}. Both hash index
 * build operators have the same general logic, but differ in their specifics.
 * Those differences are mostly encapsulated by the {@link IHashJoinUtility}
 * interface. They also have somewhat different annotations, primarily because
 * the {@link HTree} version needs access to the lexicon to setup its ivCache.
 * <p>
 * This operator is NOT thread-safe. It relies on the query engine to provide
 * synchronization. The operator MUST be run on the query controller.
 * 
 * @see SolutionSetHashJoinOp
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HashIndexOp extends HashIndexOpBase {

//    static private final transient Logger log = Logger
//            .getLogger(HashIndexOp.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends HashIndexOpBase.Annotations {

    }
    
    /**
     * Deep copy constructor.
     */
    public HashIndexOp(final HashIndexOp op) {
        super(op);
    }
    
    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public HashIndexOp(final BOp[] args, final Map<String, Object> annotations) {

        super(args, annotations);

    }

    public HashIndexOp(final BOp[] args, final NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }

    @Override
    protected ChunkTaskBase createChunkTask(final BOpContext<IBindingSet> context) {

        return new ChunkTask(this, context);
        
    }
    
    protected static class ChunkTask extends ChunkTaskBase {

        public ChunkTask(final HashIndexOp op,
                final BOpContext<IBindingSet> context) {

            super(op, context);

        }
        
        /**
         * Evaluate.
         */
        @Override
        public Void call() throws Exception {

            try {

                if (sourceIsPipeline) {

                    // Buffer all source solutions.
                    acceptSolutions();

                    if (context.isLastInvocation()) {

                        // Checkpoint the solution set.
                        checkpointSolutionSet();

                        // Output the buffered solutions.
                        outputSolutions();

                    }

                } else {
                    
                    if(first) {
                    
                        // Accept ALL solutions.
                        acceptSolutions();
                        
                        // Checkpoint the generated solution set index.
                        checkpointSolutionSet();
                        
                    }

                    // Copy all solutions from the pipeline to the sink.
                    BOpUtility.copy(context.getSource(), context.getSink(),
                            null/* sink2 */, null/* mergeSolution */,
                            null/* selectVars */, null/* constraints */, stats);

                    // Flush solutions to the sink.
                    context.getSink().flush();

                }

                // Done.
                return null;

            } finally {
                
                context.getSource().close();

                context.getSink().close();

            }
            
        }
        
        /**
         * Output the buffered solutions.
         */
        private void outputSolutions() {

            // default sink
            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

            final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = new UnsyncLocalOutputBuffer<IBindingSet>(
                    op.getChunkCapacity(), sink);

            state.outputSolutions(unsyncBuffer);
            
            unsyncBuffer.flush();

            sink.flush();

        }

    } // ControllerTask

}
