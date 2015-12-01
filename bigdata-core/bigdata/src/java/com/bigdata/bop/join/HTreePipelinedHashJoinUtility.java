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
 * Created on Oct 17, 2015
 */

package com.bigdata.bop.join;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

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
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.counters.CAT;
import com.bigdata.htree.HTree;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.relation.accesspath.BufferClosedException;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;
import com.bigdata.rwstore.sector.IMemoryManager;
import com.bigdata.util.InnerCause;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Utility class supporting a pipelined hash join. This is a variant of the
 * {@link HTreeHashJoinUtility}. See {@link PipelinedHashIndexAndSolutionSetOp} for a
 * documentation of this functionality.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class HTreePipelinedHashJoinUtility extends HTreeHashJoinUtility implements PipelinedHashJoinUtility {

    public HTreePipelinedHashJoinUtility(IMemoryManager mmgr, PipelineOp op,
            JoinTypeEnum joinType) {
        super(mmgr, op, joinType);
    }

   private static final Logger log = Logger.getLogger(HTreePipelinedHashJoinUtility.class);

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
    * See {@link PipelinedHashIndexAndSolutionSetOp#distinctProjectionBuffer}
    */
   final Set<IBindingSet> distinctProjectionBuffer = new HashSet<IBindingSet>();
   
   /**
    * See {@link PipelinedHashIndexAndSolutionSetOp#incomingBindingsBuffer}
    */
   final List<IBindingSet> incomingBindingsBuffer = new LinkedList<IBindingSet>();
 
   /**
    * See {@link PipelinedHashIndexAndSolutionSetOp#distinctProjectionsWithoutSubqueryResult}
    */
   final Set<IBindingSet> distinctProjectionsWithoutSubqueryResult = new HashSet<IBindingSet>();
   
   /**
    * Set to true if processing binding sets are passed in via 
    * Annotations.BINDING_SETS_SOURCE *and* these bindings sets have been added
    * to the hash index. Used to avoid that we add them in twice.
    */
   private boolean bsFromBindingsSetSourceAddedToHashIndex = false;

   public HTreePipelinedHashJoinUtility(
      PipelineOp op, JoinTypeEnum joinType, BOpContext<IBindingSet> context,
      int chunkCapacity) {
      
      // TODO: is this problematic (usage of query's memory manager)?
      super(context.getMemoryManager(null /* use memory mgr of this query */), op, joinType);
      
      if (!(op instanceof PipelinedHashIndexAndSolutionSetOp)) {
         throw new IllegalArgumentException();
      }
      
   }

    /**
     * Singleton {@link IHashJoinUtilityFactory} that can be used to create a 
     * new {@link HTreePipelinedHashJoinUtility}.
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

            return new HTreePipelinedHashJoinUtility(
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

        /**
         * The hash index. The keys are int32 hash codes built from the join
         * variables. The values are an {@link IV}[], similar to the encoding in
         * the statement indices. The mapping from the index positions in the
         * {@link IV}s to the variables is managed by the {@link #encoder}.
         */
        final HTree rightSolutions = getRightSolutions();
        
        final IKeyBuilder keyBuilder = 
                rightSolutions.getIndexMetadata().getKeyBuilder();

        if (bsFromBindingsSetSource!=null) {
           addBindingsSetSourceToHashIndexOnce(rightSolutions, bsFromBindingsSetSource);
        }

        final QueryEngine queryEngine = context.getRunningQuery().getQueryEngine();

        long naccepted = 0;

        // 1. Compute those distinct binding sets in the chunk not seen before
        final List<IBindingSet> dontRequireSubqueryEvaluation = new LinkedList<IBindingSet>();

        // first, divide the mappings into a set of mappings that can be joined immediately 
        // (i.e., those for which the subquery has previously been calculated) and those
        // for which we first need to evaluate the subquery (which is the case if we see
        // their distinct projection for the first time)
        final int nDistinctProjections = distinctProjectionBuffer.size();
        while (itr.hasNext()) {

            final IBindingSet[] chunk = itr.next();

            if (stats != null) {
                stats.chunksIn.increment();
                stats.unitsIn.add(chunk.length);
            }

            final AtomicInteger vectorSize = new AtomicInteger();
            final BS[] a = vector(chunk, getJoinVars(), null, false, vectorSize);
            
            final int n = vectorSize.get();

            /**
             * if we don't have a subquery but a join against mappings passed in via 
             * binding set annotation, these mappings can be processed immediately
             * (the latter, bsFromBindingsSetSource, have been added to the index
             * right in the beginning of this method already).
             */
            if (subquery==null) {
                
                for (int i=0; i<n; i++) {
                    dontRequireSubqueryEvaluation.add(a[i].bset);
                }
                
            /**
             * For the subquery case, we watch out for a join partner in the hash index
             * or, alternatively, the definite information that no such join partner
             * exists (which is recorded in distinctProjectionsWithoutSubqueryResult.
             */
            } else {
            
                int previousHashCode = -1; // invalid
                boolean previousElementHasMatch = false;
                for (int i=0; i<n; i++) {
                    
                    final IBindingSet curBs = a[i].bset;
                    
                    // The next hash code to be processed.
                    final int hashCode = a[i].hashCode;

                    // the distinct projection
                    final IBindingSet bsetDistinct = curBs.copy(projectInVars);
                    
                    // perform a lookup if the hash code of the element changed
                    final boolean currentElementHasMatch = 
                        hashCode==previousHashCode ?
                        previousElementHasMatch :
                        rightSolutions.contains(keyBuilder.reset().append(hashCode).getKey()) || 
                            distinctProjectionsWithoutSubqueryResult.contains(bsetDistinct);
                    
                    if (currentElementHasMatch) {
                        
                        /*
                         * Either a match in the bucket or subquery was already
                         * computed for this distinct projection but did not produce
                         * any results. Either way, it will take a fast path that
                         * avoids the subquery.
                         */
                        dontRequireSubqueryEvaluation.add(curBs);
                        
                    } else {
                        
                        // This is a new distinct projection. It will need to run
                        // through the subquery. We buffer the solutions in a
                        // operator-global data structure
                        incomingBindingsBuffer.add(curBs);
                        distinctProjectionBuffer.add(bsetDistinct);                    
                    }
                    
                    naccepted++;
                    
                    // record previous hash code + lookup result to avoid unrequired recomputation
                    previousHashCode = hashCode;
                    previousElementHasMatch = currentElementHasMatch;
                    
                }
            }
        }        
        
        // record the number of distinct projections seen in the chunk
        nDistinctBindingSets.add(distinctProjectionBuffer.size()-nDistinctProjections);

        /*
         * fast path: first, process those that can be processed without subquery
         * evaluation (i.e., for which the subquery has been evaluated previously).
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

                    // insert solutions into htree hash index
                    final AtomicInteger vectorSize = new AtomicInteger();
                    final BS[] a = vector(solutions, getJoinVars(), null, false, vectorSize);
                    final int n = vectorSize.get();
                    
                    for (int i = 0; i < n; i++) {
    
                        final BS tmp = a[i];
    
                        // Encode the key.
                        final byte[] key = keyBuilder.reset().append(tmp.hashCode).getKey();
    
                        // Encode the solution.
                        final byte[] val = encoder.encodeSolution(tmp.bset);
                        
                        rightSolutions.insert(key, val);
    
                    }
                    
                    for (IBindingSet solution : solutions) {
                        
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
       final HTree rightSolutions,
       final IBindingSet[] bsFromBindingsSetSource) {

       if (!bsFromBindingsSetSourceAddedToHashIndex) {

           final IKeyBuilder keyBuilder = 
                   rightSolutions.getIndexMetadata().getKeyBuilder();
           
           // insert solutions into htree hash index
           final AtomicInteger vectorSize = new AtomicInteger();
           final BS[] a = vector(bsFromBindingsSetSource, getJoinVars(), null, false, vectorSize);
           final int n = vectorSize.get();
           
           for (int i = 0; i < n; i++) {

               final BS tmp = a[i];

               // Encode the key.
               final byte[] key = keyBuilder.reset().append(tmp.hashCode).getKey();

               // Encode the solution.
               final byte[] val = encoder.encodeSolution(tmp.bset);
               
               rightSolutions.insert(key, val);

          }
           
          // remember we've added these bindings
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

        final HTree rightSolutions = getRightSolutions();
          
        final IKeyBuilder keyBuilder = 
            rightSolutions.getIndexMetadata().getKeyBuilder();
        
        if (log.isInfoEnabled()) {
            log.info("rightSolutions: #nnodes="
                    + rightSolutions.getNodeCount() + ",#leaves="
                    + rightSolutions.getLeafCount() + ",#entries="
                    + rightSolutions.getEntryCount());
        }
                // join solutions with hash index
        final boolean noJoinVars = getJoinVars().length == 0;

        
         // vectored solutions.
        final int n; // #of valid elements in a[].

        
        if (stats != null) {
            stats.chunksIn.increment();
            stats.unitsIn.add(chunk.length);
        }
        
        // vectored solutions
        final AtomicInteger vectorSize = new AtomicInteger();
        
        final BS[] a = vector(chunk, getJoinVars(), null, false, vectorSize);
        
        n = vectorSize.get();
        
        nleftConsidered.add(n);
        
        int fromIndex = 0;

        while (fromIndex < n) {
            
            /*
             * Figure out how many left solutions in the current chunk
             * have the same hash code. We will use the same iterator
             * over the right solutions for that hash code against the
             * HTree.
             */
            
            // The next hash code to be processed.
            final int hashCode = a[fromIndex].hashCode;
            
            // scan for the first hash code which is different.
            int toIndex = n; // assume upper bound.
            for (int i = fromIndex + 1; i < n; i++) {
                if (a[i].hashCode != hashCode) {
                    toIndex = i;
                    break;
                }
            }
            
            // #of left solutions having the same hash code.
            final int bucketSize = toIndex - fromIndex;
            
            if (log.isTraceEnabled())
                log.trace("hashCode=" + hashCode + ": #left="
                        + bucketSize + ", vectorSize=" + n
                        + ", firstLeft=" + a[fromIndex]);
            
            /*
             * Note: all source solutions in [fromIndex:toIndex) have
             * the same hash code. They will be vectored together.
             */
            
            // #of solutions which join for that collision bucket.
            int njoined = 0;
            // #of solutions which did not join for that collision bucket.
            int nrejected = 0;
            
            {

                final byte[] key = 
                    keyBuilder.reset().append(hashCode).getKey();
                
                /**
                 * Visit all source solutions having the same hash code.
                 */
                final ITupleIterator<?> titr = rightSolutions.lookupAll(key);

                long sameHashCodeCount = 0;
                
                /**
                 * The leftSolutionsWithoutMatch stores the left solutions for
                 * which no join partner was found. These solutions will be
                 * post-processed, to cover non-"Normal" join semantics such
                 * as OPTIONAL, NOT EXISTS, and negative EXISTS solutions.
                 */
                final Set<IBindingSet> leftSolutionsWithoutMatch = 
                    new LinkedHashSet<IBindingSet>();
                
                /**
                 * The positive EXISTS solutions. We can't output those directly,
                 * since this might result in wrong multiplicities (as we iterate
                 * over left multiple times here. Therefore, we delay outputting them
                 * by storing them in a hash set and output them in the end. 
                 */
                final Set<IBindingSet> existsSolutions = 
                    new LinkedHashSet<IBindingSet>();
                
                if (!titr.hasNext()) {
                    for (int i = fromIndex; i < toIndex; i++) {
                        
                        final IBindingSet leftSolution = a[i].bset;
                        leftSolutionsWithoutMatch.add(leftSolution);
                    }
                    
                } else {
                    
                    while (titr.hasNext()) {
    
                        sameHashCodeCount++;
                        
                        final ITuple<?> t = titr.next();
    
                        /*
                         * Note: The map entries must be the full source
                         * binding set, not just the join variables, even
                         * though the key and equality in the key is defined
                         * in terms of just the join variables.
                         * 
                         * Note: Solutions which have the same hash code but
                         * whose bindings are inconsistent will be rejected
                         * by bind() below.
                         */
                        final IBindingSet rightSolution = decodeSolution(t);
    
                        nrightConsidered.increment();
    
                        for (int i = fromIndex; i < toIndex; i++) {
                            
                            final IBindingSet leftSolution = a[i].bset;
                            leftSolutionsWithoutMatch.add(leftSolution); // unless proven otherwise
                            
                            // Join.
                            final IBindingSet outSolution = BOpContext
                                    .bind(leftSolution, rightSolution,
                                            constraints,
                                            selectVars);
    
                            nJoinsConsidered.increment();
    
                            if (noJoinVars
                                    && nJoinsConsidered.get() == noJoinVarsLimit) {
    
                                if (nleftConsidered.get() > 1
                                        && nrightConsidered.get() > 1) {
    
                                    throw new UnconstrainedJoinException();
    
                                }
    
                            }
    
                            if (outSolution == null) {
                                
                                nrejected++;
                                
                                if (log.isTraceEnabled())
                                    log.trace("Does not join"//
                                            +": hashCode="+ hashCode//
                                            + ", sameHashCodeCount="+ sameHashCodeCount//
                                            + ", #left=" + bucketSize//
                                            + ", #joined=" + njoined//
                                            + ", #rejected=" + nrejected//
                                            + ", left=" + leftSolution//
                                            + ", right=" + rightSolution//
                                            );
    
                            } else {
    
                                njoined++;
                                leftSolutionsWithoutMatch.remove(leftSolution); // match found
    
                                if (log.isDebugEnabled())
                                    log.debug("JOIN"//
                                        + ": hashCode=" + hashCode//
                                        + ", sameHashCodeCount="+ sameHashCodeCount//
                                        + ", #left="+ bucketSize//
                                        + ", #joined=" + njoined//
                                        + ", #rejected=" + nrejected//
                                        + ", solution=" + outSolution//
                                        );
                            
                            }
    
                            switch(joinType) {
                            case Normal:
                            case Optional: {
                                if (outSolution != null) {
                                    // Resolve against ivCache.
                                    encoder.resolveCachedValues(outSolution);
                                    
                                    if (askVar!=null) {
                                        outSolution.set(askVar, new Constant<XSDBooleanIV<?>>(XSDBooleanIV.TRUE));
                                    }
                                    
                                    // Output this solution.
                                    outputBuffer.add(outSolution);
                                }
                                
                                break;
                            }
                            case Exists: 
                                // handled via existsSolutions and leftSolutionsWithoutMatch in the end
                                if (askVar!=null) {
                                    existsSolutions.add(leftSolution); // record for later output
                                }
                                break;
                            case NotExists: 
                                // handled via leftSolutionsWithoutMatch in the end
                                break;
                            default:
                                throw new AssertionError();
                            }
    
                        } // next left in the same bucket.

                    } // next rightSolution with the same hash code.

                }
                
                // handle left solutions without match
                for (final IBindingSet leftSolutionWithoutMatch : leftSolutionsWithoutMatch) {
                    
                    // handle other join types
                    switch (getJoinType()) {
                    case Optional:
                    case NotExists:
                       outputBuffer.add(leftSolutionWithoutMatch);
                       break;
                    case Exists:
                    {
                        /**
                         * Semantics of EXISTS is defined as follows: it only takes effect
                         * if the ASK var is not null; in that case, it has the same
                         * semantics as OPTIONAL, but binds the askVar to true or false
                         * depending on whether a match exists.
                         */
                       if (askVar!=null) {
                           leftSolutionWithoutMatch.set(
                             askVar, 
                             new Constant<XSDBooleanIV<?>>(XSDBooleanIV.FALSE));
                           outputBuffer.add(leftSolutionWithoutMatch);
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

                for (final IBindingSet existsSolution : existsSolutions) {
                    
                    // handle other join types
                    switch (getJoinType()) {
                    case Optional:
                    case NotExists:
                       break;
                    case Exists:
                    {
                        /**
                         * Semantics of EXISTS is defined as follows: it only takes effect
                         * if the ASK var is not null; in that case, it has the same
                         * semantics as OPTIONAL, but binds the askVar to true or false
                         * depending on whether a match exists.
                         */
                       if (askVar!=null) {
                           existsSolution.set(
                             askVar, 
                             new Constant<XSDBooleanIV<?>>(XSDBooleanIV.TRUE));
                           outputBuffer.add(existsSolution);
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
                
                
            } // end block of leftSolutions having the same hash code.
            
            fromIndex = toIndex;
            
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
        sb.append(",joinType=" + joinType);
//        sb.append(",chunkSize=" + chunkSize);
//        sb.append(",optional=" + optional);
//        sb.append(",filter=" + filter);
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
        if (joinSet.get() != null)
            sb.append(",joinSetSize=" + getJoinSetSize());
//        sb.append(",encoder="+encoder);
        sb.append("}");
        
        return sb.toString();
        
    }    
    
    
}
