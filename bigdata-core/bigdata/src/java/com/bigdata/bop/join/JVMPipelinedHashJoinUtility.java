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
 * Created on Oct 17, 2011
 */

package com.bigdata.bop.join;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.controller.INamedSolutionSetRef;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.join.JVMHashIndex.Bucket;
import com.bigdata.bop.join.JVMHashIndex.Key;
import com.bigdata.bop.join.JVMHashIndex.SolutionHit;
import com.bigdata.relation.accesspath.BufferClosedException;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;
import com.bigdata.util.InnerCause;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Utility class supporting a pipelined hash join. This is a variant of the
 * JVMHashJoinUtility.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class JVMPipelinedHashJoinUtility extends JVMHashJoinUtility {

   private static final Logger log = Logger.getLogger(JVMPipelinedHashJoinUtility.class);

   private final BOpContext<IBindingSet> context;
   
   private final int chunkCapacity;

   public JVMPipelinedHashJoinUtility(
      PipelineOp op, JoinTypeEnum joinType, BOpContext<IBindingSet> context,
      int chunkCapacity) {
      
      super(op, joinType);
      
      this.context = context;
      this.chunkCapacity = chunkCapacity;
      
   }
    
    /**
     * Singleton {@link IHashJoinUtilityFactory} that can be used to create a 
     * new {@link JVMPipelinedHashJoinUtility}.
     */
    static public final IHashJoinUtilityFactory factory =
            new IHashJoinUtilityFactory() {

        private static final long serialVersionUID = 1L;
        
        public IHashJoinUtility create(//
                final BOpContext<IBindingSet> context,//
                final INamedSolutionSetRef namedSetRef,//
                final PipelineOp op,//
                final JoinTypeEnum joinType//
                ) {

            return new JVMPipelinedHashJoinUtility(
               op, joinType, context, op.getChunkCapacity());

        }
        
    };
    

//    // TODO: do we want to keep this "as is"? It's not really nice design.
//    @Override
//    public long acceptSolutions(final ICloseableIterator<IBindingSet[]> itr,
//          final BOpStats stats) {
//       
//       throw new RuntimeException(
//          "acceptSolutions() not supported by pipelined hash join. "
//          + "Please use acceptAndOutputSolutions() method instead.");
//    }
//    
//    // TODO: do we want to keep this "as is"? It's not really nice design.
//    @Override
//    public void outputSolutions(final IBuffer<IBindingSet> out) {
//       
//       throw new RuntimeException(
//          "outputSolutions() not supported by pipelined hash join. "
//          + "Please use acceptAndOutputSolutions() method instead.");
//       
//    }
    
    /**
     * acceptAndOutputSolutions is a special method for the pipelined hash
     * index, which accepts and immediately forwards relevant solutions.
     */
    public long acceptAndOutputSolutions(
          UnsyncLocalOutputBuffer<IBindingSet> out,
          ICloseableIterator<IBindingSet[]> itr, NamedSolutionSetStats stats) {
       
       // TODO: distinct computation
       
       long naccepted = 0;
       
       try {

            final JVMHashIndex index = getRightSolutions();

            final IBindingSet[] all = BOpUtility.toArray(itr, stats);

            if (log.isDebugEnabled())
                log.debug("Materialized: " + all.length + " source solutions.");

            

            for (IBindingSet bset : all) {

                // add the solution to the index in any case
                // TODO: choose a different data structure at some point!
                if (index.add(bset) == null) {

                    continue;

                }

                // compute the projection on the join variables
                IBindingSet bsetDistinct = bset;
                bsetDistinct = bsetDistinct.copy(getJoinVars());
                
//                // TODO: we don't need the distinct set, but could implement
//                // this in a space-saving way against the right-hand side index
//                if (!distinctSet.add(bsetDistinct)) {
//                   
//                   // just don't delegate, ignore; this will be handled by
//                   // the join operation on the right at some point, but we
//                   // don't need to pipe the mapping through the subgroup again
//                   
//                } else {
                   
                   // output, piping the distinct binding through the inner
                   // group spanned by the hash join pattern
                   out.add(bsetDistinct);
                   
//                }
                   
                naccepted++;

            }

            if (log.isDebugEnabled())
                log.debug("There are " + index.bucketCount()
                        + " hash buckets, joinVars="
                        + Arrays.toString(getJoinVars()));

            rightSolutionCount.add(naccepted);

            return naccepted;

        } catch (Throwable t) {

            throw launderThrowable(t);
            
        }

    }

    
    /**
     * {@inheritDoc}
     * <p>
     * For each source solution materialized, the hash table is probed using the
     * as-bound join variables for that source solution. A join hit counter is
     * carried for each solution in the hash index and is used to support
     * OPTIONAL joins.
     */
    @Override
    public void hashJoin2(//
            final ICloseableIterator<IBindingSet[]> leftItr,//
            final BOpStats stats,
            final IBuffer<IBindingSet> outputBuffer,//
            final IConstraint[] constraints//
            ) {

        final JVMHashIndex rightSolutions = getRightSolutions();
          
        if (log.isInfoEnabled()) {
            log.info("rightSolutions: #buckets=" + rightSolutions.bucketCount()
                    + ",#solutions=" + getRightSolutionCount());
        }
        
        // build left hand side hash index
        final JVMHashIndex leftSolutions = 
           new JVMHashIndex(getJoinVars(), false /* TODO */, new HashMap<Key, Bucket>());
        
        
        // step 1: add solutions to another hash index
        while (leftItr.hasNext()) {
              
           final IBindingSet[] leftChunk = leftItr.next();
              

           // record solution stats
           if (stats != null) {
              stats.chunksIn.increment();
              stats.unitsIn.add(leftChunk.length);
           }
              
           // add solution to index
           for (IBindingSet bs : leftChunk) {
              leftSolutions.add(bs);                 
           }
              
        }
 
        
        // true iff there are no join variables.
        final boolean noJoinVars = getJoinVars().length == 0;
        try {

           final Iterator<Bucket> bucketIt = leftSolutions.buckets();
           while (bucketIt.hasNext()) {
              
              final Bucket b = bucketIt.next();
              
              final Iterator<SolutionHit> solutionIt = b.iterator();
              
              while (solutionIt.hasNext()) {

                 final SolutionHit solution = solutionIt.next();
                 
                 final IBindingSet left = solution.solution;

                    nleftConsidered.increment();

                    if (log.isDebugEnabled())
                        log.debug("Considering " + left);

                    final Bucket bucket = rightSolutions.getBucket(left);

                    if (bucket == null)
                        continue;

                    final Iterator<SolutionHit> ritr = bucket.iterator();

                    while (ritr.hasNext()) {

                        final SolutionHit right = ritr.next();

                        nrightConsidered.increment();

                        if (log.isDebugEnabled())
                            log.debug("Join with " + right);

                        nJoinsConsidered.increment();

                        if (noJoinVars
                                && nJoinsConsidered.get() == getNoJoinVarsLimit()) {

                            if (nleftConsidered.get() > 1
                                    && nrightConsidered.get() > 1) {

                                throw new UnconstrainedJoinException();

                            }

                        }

                        // See if the solutions join.
                        final IBindingSet outSolution = BOpContext.bind(//
                                right.solution,//
                                left,//
                                constraints,//
                                getSelectVars()//
                                );

                        switch (getJoinType()) {
                        case Normal: {
                            if (outSolution != null) {
                                // Output the solution.
                                outputSolution(outputBuffer, outSolution);
                            }
                            break;
                        }
                        case Optional: {
                            if (outSolution != null) {
                                // Output the solution.
                                outputSolution(outputBuffer, outSolution);
                                // Increment counter so we know not to output
                                // the rightSolution as an optional solution.
                                right.nhits.increment();
                            }
                            break;
                        }
                        case Exists: {
                            /*
                             * The right solution is output iff there is at
                             * least one left solution which joins with that
                             * right solution. Each right solution is output at
                             * most one time.
                             */
                            if (outSolution != null) {
                                // if (right.nhits.get() == 0L) {
                                // // Output the solution.
                                // outputSolution(outputBuffer, right.solution);
                                // }
                                // Increment counter so we know this solution joins.
                                right.nhits.increment();
                            }
                            break;
                        }
                        case NotExists: {
                            /*
                             * The right solution is output iff there does not
                             * exist any left solution which joins with that
                             * right solution. This basically an optional join
                             * where the solutions which join are not output.
                             */
                            if (outSolution != null) {
                                // Increment counter so we know not to output
                                // the rightSolution as an optional solution.
                                right.nhits.increment();
                            }
                            break;
                        }
                        default:
                            throw new AssertionError();
                        }

                    } // while(ritr.hasNext())

                } // for(left : leftChunk)
                
            } // while(leftItr.hasNext())

        } catch(Throwable t) {

            throw launderThrowable(t);
            
        } finally {

            leftItr.close();

        }

    }

    
    /**
     * Adds metadata about the {@link IHashJoinUtility} state to the stack
     * trace.
     * 
     * @param t
     *            The thrown error.
     * 
     * @return The laundered exception.
     * 
     * @throws Exception
     */
    protected RuntimeException launderThrowable(final Throwable t) {

        final String msg = "cause=" + t + ", state=" + toString();

        if (!InnerCause.isInnerCause(t, InterruptedException.class)
                && !InnerCause.isInnerCause(t, BufferClosedException.class)) {

            /*
             * Some sort of unexpected exception.
             */

            log.error(msg, t);

        }

        return new RuntimeException(msg, t);
        
    }


}
