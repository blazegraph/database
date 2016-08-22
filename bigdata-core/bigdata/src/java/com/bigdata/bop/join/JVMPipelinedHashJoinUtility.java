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
 * Created on Oct 17, 2015
 */

package com.bigdata.bop.join;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOpContext;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.controller.INamedSolutionSetRef;
import com.bigdata.bop.engine.AbstractRunningQuery;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.join.JVMHashIndex.Bucket;
import com.bigdata.bop.join.JVMHashIndex.SolutionHit;
import com.bigdata.counters.CAT;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.relation.accesspath.BufferClosedException;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;
import com.bigdata.util.InnerCause;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Utility class supporting a pipelined hash join. This is a variant of the
 * JVMHashJoinUtility. See {@link PipelinedHashIndexAndSolutionSetJoinOp} for a
 * documentation of this functionality.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class JVMPipelinedHashJoinUtility extends JVMHashJoinUtility implements PipelinedHashJoinUtility {

   private static final Logger log = Logger.getLogger(JVMPipelinedHashJoinUtility.class);

   /**
    * The #of distinct projections from the given input bindings
    */
   protected final CAT nDistinctBindingSets = new CAT();

   /**
    * The #of distinct binding sets that have flown into the subquery.
    */
   protected final CAT nDistinctBindingSetsReleased = new CAT();

   /**
    * The #of subqueries that have been issued.
    */
   protected final CAT nSubqueriesIssued = new CAT();
   
   /**
    * The #of results returned by the subqueries
    */
   protected final CAT nResultsFromSubqueries = new CAT();
  

   /**
    * See {@link PipelinedHashIndexAndSolutionSetJoinOp#distinctProjectionBuffer}
    */
   final Set<IBindingSet> distinctProjectionBuffer = new HashSet<IBindingSet>();
   
   /**
    * See {@link PipelinedHashIndexAndSolutionSetJoinOp#incomingBindingsBuffer}
    */
   final List<IBindingSet> incomingBindingsBuffer = new LinkedList<IBindingSet>();
 
   /**
    * See {@link PipelinedHashIndexAndSolutionSetJoinOp#distinctProjectionsWithoutSubqueryResult}
    */
   final Set<IBindingSet> distinctProjectionsWithoutSubqueryResult = new HashSet<IBindingSet>();
   
   /**
    * Set to true if processing binding sets are passed in via 
    * Annotations.BINDING_SETS_SOURCE *and* these bindings sets have been added
    * to the hash index. Used to avoid that we add them in twice.
    */
   private boolean bsFromBindingsSetSourceAddedToHashIndex = false;

   public JVMPipelinedHashJoinUtility(
      PipelineOp op, JoinTypeEnum joinType, BOpContext<IBindingSet> context,
      int chunkCapacity) {
      
      super(op, joinType);
      
      if (!(op instanceof PipelinedHashIndexAndSolutionSetJoinOp)) {
         throw new IllegalArgumentException();
      }
      
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
    
    @Override
    public long acceptAndOutputSolutions(
          final UnsyncLocalOutputBuffer<IBindingSet> out,
          final ICloseableIterator<IBindingSet[]> itr, final NamedSolutionSetStats stats,
          final IConstraint[] joinConstraints, final PipelineOp subquery,
          final IBindingSet[] bsFromBindingsSetSource, 
          final IVariable<?>[] projectInVars, final IVariable<?> askVar,
          final boolean isLastInvocation,
          final int distinctProjectionBufferThreshold,
          final int incomingBindingsBufferThreshold,
          final BOpContext<IBindingSet> context) {

       
        final JVMHashIndex rightSolutions = getRightSolutions();

        if (bsFromBindingsSetSource!=null) {
           addBindingsSetSourceToHashIndexOnce(rightSolutions, bsFromBindingsSetSource);
        }

        final QueryEngine queryEngine = context.getRunningQuery().getQueryEngine();

        long naccepted = 0;

        // 1. Compute those distinct binding sets in the chunk not seen before
        final List<IBindingSet> dontRequireSubqueryEvaluation = new LinkedList<IBindingSet>();

        // first, join the mappings that can be joined immediately and
        // calculate the remaining ones, including the
        final int nDistinctProjections = distinctProjectionBuffer.size();
        while (itr.hasNext()) {

            final IBindingSet[] chunk = itr.next();

            if (stats != null) {
                stats.chunksIn.increment();
                stats.unitsIn.add(chunk.length);
            }

            for (int i = 0; i < chunk.length; i++) {

                /**
                 * fast path: if we don't have a subquery but a join against
                 * mappings passed in via binding set annotation, these mappings
                 * can be processed immediately (the latter, 
                 * bsFromBindingsSetSource, have been added to the index right
                 * in the beginning of this method already).
                 */
                if (subquery==null) {
                   dontRequireSubqueryEvaluation.add(chunk[i]);
                   continue;
                }
               
                // Take a distinct projection of the join variables.
                final IBindingSet bsetDistinct = chunk[i].copy(projectInVars);

                /**
                 *  Find bucket in hash index for that distinct projection
                 * (bucket of solutions with the same join vars from the
                 *  subquery - basically a JOIN).
                 */
                final Bucket b = rightSolutions.getBucket(bsetDistinct);

                if (b != null || 
                    distinctProjectionsWithoutSubqueryResult.contains(bsetDistinct)) {
                    /*
                     * Either a match in the bucket or subquery was already
                     * computed for this distinct projection but did not produce
                     * any results. Either way, it will take a fast path that
                     * avoids the subquery.
                     */
                    dontRequireSubqueryEvaluation.add(chunk[i]);

                } else {
                    // This is a new distinct projection. It will need to run
                    // through the subquery. We buffer the solutions in a
                    // operator-global data structure
                    incomingBindingsBuffer.add(chunk[i]);
                    distinctProjectionBuffer.add(bsetDistinct);

                }

                naccepted++;

            }
        }
        
        // record the number of distinct projections seen in the chunk
        nDistinctBindingSets.add(distinctProjectionBuffer.size()-nDistinctProjections);

        /*
         * first, process those that can be processed without subquery
         * evaluation (i.e., for which the subquery has been evaluated before
         * already).
         */
        if (!dontRequireSubqueryEvaluation.isEmpty()) {
            // compute join for the fast path.
            hashJoinAndEmit(dontRequireSubqueryEvaluation.toArray(
               new IBindingSet[0]), stats, out, joinConstraints, askVar);
        }

        if (distinctProjectionBuffer.isEmpty()) {
            // Nothing to do on the slow code path.
            return naccepted;
        }
        
        // If this is not the last invocation and the buffer thresholds are not
        // yet exceeded, we wait for more incoming solutions, to benefit from
        // batch processing
        if (!isLastInvocation && !thresholdExceeded(
              distinctProjectionBuffer, distinctProjectionBufferThreshold,
              incomingBindingsBuffer, incomingBindingsBufferThreshold)) {
            return naccepted;         
        }

        // if we reach this code path, the subquery must be non null
        assert(subquery!=null);
        
        // next, we execute the subquery for the unseen distinct projections
        IRunningQuery runningSubquery = null;

        try {

            // set up a subquery for the chunk of distinct projections.
            runningSubquery = queryEngine.eval(
                subquery, distinctProjectionBuffer.toArray(new IBindingSet[0]));

            // Notify parent of child subquery.
            ((AbstractRunningQuery) context.getRunningQuery()).
                addChild(runningSubquery);

            // record statistics
            nDistinctBindingSetsReleased.add(distinctProjectionBuffer.size());
            nSubqueriesIssued.increment();
            
            // iterate over the results and store them in the index
            final ICloseableIterator<IBindingSet[]> subquerySolutionItr = 
                runningSubquery.iterator();

            try {

                while (subquerySolutionItr.hasNext()) {

                    final IBindingSet[] solutions = subquerySolutionItr.next();

                    for (IBindingSet solution : solutions) {

                        // add solutions to the subquery into the hash index.
                        rightSolutions.add(solution);

                        /*
                         * we remove all mappings that generated at least one
                         * result from distinct set (which will be further
                         * processed later on); This is how we discover the set
                         * of distinct projections that did not join.
                         */
                        distinctProjectionBuffer.remove(solution.copy(getJoinVars()));

                        nResultsFromSubqueries.increment();
                    }

                }

                /**
                 * register the distinct keys for which the subquery did not
                 * yield any result as "keys without match" at the index; this
                 * is an improvement (though not necessarily required) in order
                 * to avoid unnecessary re-computation of the subqueries for
                 * these keys
                 */
                distinctProjectionsWithoutSubqueryResult.addAll(distinctProjectionBuffer);
                
            } finally {

                // finished with the iterator
                subquerySolutionItr.close();

            }

            // wait for the subquery to halt / test for errors.
            runningSubquery.get();

        } catch (Throwable t) {

            /*
             * If things fail before we start the subquery, or if a subquery
             * fails (due to abnormal termination), then propagate the error to
             * the parent and rethrow the first cause error out of the subquery.
             * 
             * Note: IHaltable#getCause() considers exceptions triggered by an
             * interrupt to be normal termination. Such exceptions are NOT
             * propagated here and WILL NOT cause the parent query to terminate.
             */
            final Throwable cause = (runningSubquery != null && runningSubquery.getCause() != null)
                    ? runningSubquery.getCause() : t;

            throw new RuntimeException(context.getRunningQuery().halt(cause));

        } finally {

            // ensure subquery is halted.
            if (runningSubquery != null)
                runningSubquery.cancel(true/* mayInterruptIfRunning */);

        }

        
        rightSolutionCount.add(naccepted);

        // hash index join for the subquery path.
        hashJoinAndEmit(incomingBindingsBuffer.toArray(
            new IBindingSet[0]), stats, out, joinConstraints, askVar);

        // finally, we need to clear the buffers to avoid results being
        // processed multiple times
        distinctProjectionBuffer.clear();
        incomingBindingsBuffer.clear();
        
        return naccepted;

     }

     /**
      * Returns true if, for one of the buffers, the threshold has been
      * exceeded.
      */
     boolean thresholdExceeded(
         final Set<IBindingSet> distinctProjectionBuffer,
         final int distinctProjectionBufferThreshold,
         final List<IBindingSet> incomingBindingsBuffer,
         final int incomingBindingsBufferThreshold) {
        
        return 
            distinctProjectionBuffer.size()>=distinctProjectionBufferThreshold || 
            incomingBindingsBuffer.size()>=incomingBindingsBufferThreshold;
    }

    /**
      * Adds the binding sets passed in via Annotations.BINDING_SETS_SOURCE 
      * to the hash index.
      * 
      * @param rightSolutions the hash index
      * @param bsFromBindingsSetSource the solutions to add (must be non null)
      */
    void addBindingsSetSourceToHashIndexOnce(
       final JVMHashIndex rightSolutions,
       final IBindingSet[] bsFromBindingsSetSource) {

       if (!bsFromBindingsSetSourceAddedToHashIndex) {
          
          for (IBindingSet solution : bsFromBindingsSetSource) {

             // add solutions to the join with the binding set to hash index.
             rightSolutions.add(solution);
          
          }
          
          bsFromBindingsSetSourceAddedToHashIndex = true;
       }
   }

   /**
     * Executes the hash join for the chunk of solutions that is passed in
     * over rightSolutions and outputs the solutions.
     */
    public void hashJoinAndEmit(//
            final IBindingSet[] chunk,//
            final BOpStats stats,
            final IBuffer<IBindingSet> outputBuffer,//
            final IConstraint[] joinConstraints,//
            final IVariable<?> askVar) {

        final JVMHashIndex rightSolutions = getRightSolutions();
          
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
                  {
                     if (outSolution!=null) {
                        if (askVar!=null) {
                           outSolution.set(
                              askVar, new Constant<XSDBooleanIV<?>>(XSDBooleanIV.valueOf(true)));
                        }
                        outputSolution(outputBuffer, outSolution);
                     }
                     break;
                  }
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
           /**
            * Semantics of EXISTS is defined as follows: it only takes effect
            * if the ASK var is not null; in that case, it has the same
            * semantics as OPTIONAL, but binds the askVar to true or false
            * depending on whether a match exists.
            */
           case Exists:
           {
              if (askVar!=null) {
                 left.set(
                    askVar, 
                    new Constant<XSDBooleanIV<?>>(XSDBooleanIV.valueOf(matchExists)));
                outputSolution(outputBuffer, left);
              }
              break;
           }
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

    /**
     * Human readable representation of the {@link IHashJoinUtility} metadata
     * (but not the solutions themselves).
     */
    @Override
    public String toString() {

        final StringBuilder sb = new StringBuilder();
        
        sb.append(getClass().getSimpleName());
        
        sb.append("{open=" + open);
        sb.append(",joinType="+joinType);
        if (askVar != null)
            sb.append(",askVar=" + askVar);
        sb.append(",joinVars=" + Arrays.toString(joinVars));
        sb.append(",outputDistinctJVs=" + outputDistinctJVs);        
        if (selectVars != null)
            sb.append(",selectVars=" + Arrays.toString(selectVars));
        if (constraints != null)
            sb.append(",constraints=" + Arrays.toString(constraints));
        sb.append(",size=" + getRightSolutionCount());
        sb.append(", distinctProjectionsWithoutSubqueryResult=" + distinctProjectionsWithoutSubqueryResult.size());
        sb.append(", distinctBindingSets (seen/released)=" + nDistinctBindingSets + "/" + nDistinctBindingSetsReleased);
        sb.append(", subqueriesIssued=" + nSubqueriesIssued);
        sb.append(", resultsFromSubqueries=" + nResultsFromSubqueries);
        sb.append(",considered(left=" + nleftConsidered + ",right="
                + nrightConsidered + ",joins=" + nJoinsConsidered + ")");
        sb.append("}");
        
        return sb.toString();
        
    }

}
