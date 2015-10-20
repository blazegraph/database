/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.controller.INamedSolutionSetRef;
import com.bigdata.bop.controller.SubqueryAnnotations;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;

import cutthecrap.utils.striterators.ICloseableIterator;
import cutthecrap.utils.striterators.SingleValueIterator;

/**
 * Operator for pipelined hash index construction and subsequent join. Note that
 * this operator needs not to be combined with a solution set hash join, but
 * instead gets the subquery/subgroup passed as a parameter.
 * 
 * TODO: add documentation
 * 
 * @see JVMPipelinedHashJoinUtility for implementation
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 */
public class PipelinedHashIndexAndSolutionSetOp extends HashIndexOp {

//    static private final transient Logger log = Logger
//            .getLogger(HashIndexOp.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    public interface Annotations extends HashIndexOp.Annotations, SubqueryAnnotations {

    }
    
    /**
     * Deep copy constructor.
     */
    public PipelinedHashIndexAndSolutionSetOp(final PipelinedHashIndexAndSolutionSetOp op) {
       
        super(op);
        
    }
    
    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public PipelinedHashIndexAndSolutionSetOp(final BOp[] args, final Map<String, Object> annotations) {

        super(args, annotations);

    }
    
    public PipelinedHashIndexAndSolutionSetOp(final BOp[] args, final NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }

    @Override
    protected ChunkTaskBase createChunkTask(final BOpContext<IBindingSet> context) {

        return new ChunkTask(
           this, context,
           (PipelineOp)getRequiredProperty(Annotations.SUBQUERY));
        
    }
    
    private static class ChunkTask extends com.bigdata.bop.join.HashIndexOp.ChunkTask {

        final PipelineOp subquery;
        
        final IConstraint[] joinConstraints;
       
        public ChunkTask(final PipelinedHashIndexAndSolutionSetOp op,
                final BOpContext<IBindingSet> context, final PipelineOp subquery) {

            super(op, context);
            
            joinConstraints = BOpUtility.concat(
                  (IConstraint[]) op.getProperty(Annotations.CONSTRAINTS),
                  state.getConstraints());
            
            this.subquery = subquery;

        }
        
        /**
         * Evaluate.
         */
        @Override
        public Void call() throws Exception {

            try {

                if (sourceIsPipeline) {

                    // Buffer all source solutions.
                    acceptAndOutputSolutions();

                    if (context.isLastInvocation()) {

                        // Checkpoint the solution set.
                        checkpointSolutionSet();


                    }

                } else {
                    
                    if(first) {
                    
                        // Accept ALL solutions.
                        acceptAndOutputSolutions();
                        
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
        private void acceptAndOutputSolutions() {

            // default sink
            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

            final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = 
               new UnsyncLocalOutputBuffer<IBindingSet>(
                 op.getChunkCapacity(), sink);

            final ICloseableIterator<IBindingSet[]> src;

            // TODO: this is duplicate code, see HashIndexOp.acceptSolutions
            if (sourceIsPipeline) {
            
                src = context.getSource();
                
            } else if (op.getProperty(Annotations.NAMED_SET_SOURCE_REF) != null) {
                
                /*
                 * Metadata to identify the optional *source* solution set. When
                 * <code>null</code>, the hash index is built from the solutions flowing
                 * through the pipeline. When non-<code>null</code>, the hash index is
                 * built from the solutions in the identifier solution set.
                 */
                final INamedSolutionSetRef namedSetSourceRef = (INamedSolutionSetRef) op
                        .getRequiredProperty(Annotations.NAMED_SET_SOURCE_REF);

                src = context.getAlternateSource(namedSetSourceRef);
                
            } else if (op.getProperty(Annotations.BINDING_SETS_SOURCE) != null) {

                /*
                 * The IBindingSet[] is directly given. Just wrap it up as an
                 * iterator. It will visit a single chunk of solutions.
                 */
                final IBindingSet[] bindingSets = (IBindingSet[]) op
                        .getProperty(Annotations.BINDING_SETS_SOURCE);

                src = new SingleValueIterator<IBindingSet[]>(bindingSets);
                
            } else {

                throw new UnsupportedOperationException(
                        "Source was not specified");
                
            }
            
            
            ((JVMPipelinedHashJoinUtility)state).acceptAndOutputSolutions(
               unsyncBuffer, src, stats, joinConstraints, subquery);
            
            
            unsyncBuffer.flush();

            sink.flush();

        }

    } // ControllerTask

}
