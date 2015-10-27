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

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOpContext;
import com.bigdata.bop.HashMapAnnotations;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.controller.INamedSolutionSetRef;
import com.bigdata.bop.engine.AbstractRunningQuery;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.IJVMHashIndex.Bucket;
import com.bigdata.bop.join.IJVMHashIndex.Key;
import com.bigdata.bop.join.IJVMHashIndex.SolutionHit;
import com.bigdata.bop.paths.ArbitraryLengthPathTask;
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

   public JVMPipelinedHashJoinUtility(
      PipelineOp op, JoinTypeEnum joinType, BOpContext<IBindingSet> context,
      int chunkCapacity) {
      
      super(op, joinType);
      
      this.context = context;
      
   }
   
   /**
    * The pipelined hash join pattern has a special index at the right, which
    * supports multi-threaded access (incoming bindings are written, while
    * bindings that have been processed are removed.
    */
   @Override
   protected void initRightSolutionsRef(
      final PipelineOp op, final IVariable<?>[] keyVars, final boolean filter, 
      final boolean indexSolutionsHavingUnboundJoinVars) {


      rightSolutionsRef.set(//
            new ZeroMatchRecordingJVMHashIndex(//
                    keyVars,//
                    indexSolutionsHavingUnboundJoinVars,//
                    new LinkedHashMap<Key, Bucket>(op.getProperty(
                            HashMapAnnotations.INITIAL_CAPACITY,
                            HashMapAnnotations.DEFAULT_INITIAL_CAPACITY),//
                            op.getProperty(HashMapAnnotations.LOAD_FACTOR,
                                    HashMapAnnotations.DEFAULT_LOAD_FACTOR)//
                    )//
            ));

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
    
    /**
     * AcceptAndOutputSolutions is a special method for building the hash index
     * of the {@link JVMPipelinedHashIndex}, which accepts and immediately
     * forwards relevant solutions (non-blocking index).
     */
    public long acceptAndOutputSolutions(
          final UnsyncLocalOutputBuffer<IBindingSet> out,
          final ICloseableIterator<IBindingSet[]> itr, final NamedSolutionSetStats stats,
          final IConstraint[] joinConstraints, final PipelineOp subquery) {
       
      final ZeroMatchRecordingJVMHashIndex rightSolutions = 
         (ZeroMatchRecordingJVMHashIndex)getRightSolutions();

      final QueryEngine queryEngine = this.context.getRunningQuery()
            .getQueryEngine();

      long naccepted = 0;

      // 1. Compute those distinct binding sets in the chunk not seen before
      final Set<IBindingSet> distinctSet = new HashSet<IBindingSet>();
      final List<IBindingSet> dontRequireSubqueryEvaluation = new LinkedList<IBindingSet>();
      final List<IBindingSet> requireSubqueryEvaluation = new LinkedList<IBindingSet>();

      // first, join the mappings that can be joined immediately and
      // calculate the remaining ones, including the 
      while (itr.hasNext()) {
         

         final IBindingSet[] chunk = itr.next();         
         
         if (stats != null) {
            stats.chunksIn.increment();
            stats.unitsIn.add(chunk.length);
         }

         for (int i=0; i<chunk.length; i++) {
            // Take a distinct projection of the join variables.
            final IBindingSet bsetDistinct = chunk[i].copy(getJoinVars());
            // Find bucket in hash index for that distinct projection (bucket of solutions with the same join vars from the subquery).
            final Bucket b = rightSolutions.getBucket(bsetDistinct);
            if (b!=null || rightSolutions.isKeyWithoutMatch(bsetDistinct)) {
               // Either a match in the bucket or subquery was already computed for this distinct projection but did not produce any results.  Either way, it will take a fast path that avoids the subquery.
               dontRequireSubqueryEvaluation.add(chunk[i]);
               
            } else {
                // This is a new distinct projection.  It will need to run through the subquery.
               requireSubqueryEvaluation.add(chunk[i]);
               distinctSet.add(bsetDistinct);
               
            }

            naccepted++;

         }
      }
      
      // first, process those that can be processed without subquery evaluation
      // (i.e., for which the subquery has been evaluated before already)
      if (!dontRequireSubqueryEvaluation.isEmpty()) {
          // compute join for the fast path.
         hashJoinAndEmit(dontRequireSubqueryEvaluation.toArray(
            new IBindingSet[0]), stats, out, joinConstraints);
      }      
      
      /* Second, process those bindings that require subquery evaluation.
       * 
       * TODO If lastPass := true, then you could avoid a subquery here if the
       * #of solutions in this list was too small to bother with.
       */
      if (!requireSubqueryEvaluation.isEmpty()) {

         // next, we execute the subquery for the unseen distinct projections
         IRunningQuery runningSubquery = null;
         
         try {

            // set up a subquery for the chunk of distinct projections.
            runningSubquery = 
               queryEngine.eval(subquery, distinctSet.toArray(new IBindingSet[0]));

            // Notify parent of child subquery.
            ((AbstractRunningQuery) context.getRunningQuery())
                  .addChild(runningSubquery);

            // iterate over the results and store them in the index
            final ICloseableIterator<IBindingSet[]> subquerySolutionItr = 
               runningSubquery.iterator();

            try {
            
                while (subquerySolutionItr.hasNext()) {
    
                   final IBindingSet[] solutions = subquerySolutionItr.next();
                   
                   for (IBindingSet solution : solutions) {
    
                      // add solutions to the subquery into the hash index.
                      rightSolutions.add(solution);
                      
                      // we remove all mappings that generated at least one result
                      // from distinct set (which will be further processed later on);
                      // This is how we discover the set of distinct projections that
                      // did not join.
                      distinctSet.remove(solution.copy(getJoinVars()));
    
                  }
    
                }
                
                /**
                 * register the distinct keys for which the subquery did not yield
                 * any result as "keys without match" at the index; this is an 
                 * improvement (though not necessarily required) in order to avoid
                 * unnecessary re-computation of the subqueries for these keys
                 */
               rightSolutions.addKeysWithoutMatch(distinctSet);
               
            } finally {
               
                // finished with the iterator
                subquerySolutionItr.close();
                
            }

            // wait for the subquery to halt / test for errors.
            runningSubquery.get();

        } catch (Throwable t) {

             /*
              * If things fail before we start the subquery, or if a subquery
              * fails (due to abnormal termination), then propagate the error
              * to the parent and rethrow the first cause error out of the
              * subquery.
              * 
              * Note: IHaltable#getCause() considers exceptions triggered by
              * an interrupt to be normal termination. Such exceptions are
              * NOT propagated here and WILL NOT cause the parent query to
              * terminate.
              */
             final Throwable cause = (runningSubquery != null && runningSubquery
                     .getCause() != null) ? runningSubquery.getCause() : t;

            throw new RuntimeException(this.context.getRunningQuery().halt(cause));

         } finally {

             // ensure subquery is halted.
             if (runningSubquery != null)
                 runningSubquery
                         .cancel(true/* mayInterruptIfRunning */);

         }
         
         rightSolutionCount.add(naccepted);

         // hash index join for the subquery path.
         hashJoinAndEmit(
            requireSubqueryEvaluation.toArray(new IBindingSet[0]), 
            stats, out, joinConstraints);
         
      }

      return naccepted; // TODO: check stats

    }

    
    /**
     * Executes the hash join for the chunk of solutions that is passed in
     * over rightSolutions and outputs the solutions.
     */
    public void hashJoinAndEmit(//
            final IBindingSet[] chunk,//
            final BOpStats stats,
            final IBuffer<IBindingSet> outputBuffer,//
            final IConstraint[] joinConstraints) {

        final ZeroMatchRecordingJVMHashIndex rightSolutions = 
           (ZeroMatchRecordingJVMHashIndex)getRightSolutions();
          
        if (log.isInfoEnabled()) {
            log.info("rightSolutions: #buckets=" + rightSolutions.bucketCount()
                    + ",#solutions=" + getRightSolutionCount());
        }
        
        // join solutions with hash index
        final boolean noJoinVars = getJoinVars().length == 0;

        for (final IBindingSet left : chunk) {
              
           final Bucket bucket = rightSolutions.getBucket(left);
           nleftConsidered.increment();
              
           boolean matchExists = false; // try to prove otherwise
           if (bucket != null) {
              
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
                     joinConstraints,//
                     getSelectVars()//
                  );
                     
                  // record that we've seen a solution, if so
                  matchExists |= outSolution!=null;
   
                  // for normal joins and opt
                  switch (getJoinType()) {
                  case Normal:
                  case Optional:
                     outputSolution(outputBuffer, outSolution);
                     break;
                  case Exists:
                  case NotExists:
                     break; // will be handled at the end
                  default:
                     throw new AssertionError();
                     
                  }                      
              }
           }
              
           // handle other join types
           switch (getJoinType()) {
           case Optional:
           case NotExists:
              if (!matchExists) {
                 outputSolution(outputBuffer, left);
              }
              break;
           case Exists:
              if (matchExists) {
                 outputSolution(outputBuffer, left);
              }
              break;
           case Normal:
              // this has been fully handled already
              break;
           default:
              throw new AssertionError();
           }                      
              
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

//    
//    private static class AcceptAndOutputSolutionTask<E> extends Haltable<Void> implements
//    Callable<Void>

}
