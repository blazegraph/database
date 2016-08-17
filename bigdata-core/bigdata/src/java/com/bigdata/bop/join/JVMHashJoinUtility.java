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
 * Created on Oct 17, 2011
 */

package com.bigdata.bop.join;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.HTreeAnnotations;
import com.bigdata.bop.HashMapAnnotations;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.controller.INamedSolutionSetRef;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.join.JVMHashIndex.Bucket;
import com.bigdata.bop.join.JVMHashIndex.Key;
import com.bigdata.bop.join.JVMHashIndex.SolutionHit;
import com.bigdata.counters.CAT;
import com.bigdata.htree.HTree;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.relation.accesspath.BufferClosedException;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.util.InnerCause;

import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.ICloseableIterator;
import cutthecrap.utils.striterators.IStriterator;
import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;
import cutthecrap.utils.striterators.Visitor;

/**
 * Utility class supporting hash join against a Java hash collection.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JVMHashJoinUtility implements IHashJoinUtility {

    private static final Logger log = Logger.getLogger(JVMHashJoinUtility.class);

    /**
     * Singleton {@link IHashJoinUtilityFactory} that can be used to create a 
     * new {@link JVMHashJoinUtility}.
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

            return new JVMHashJoinUtility(op, joinType);

        }
    };

    /**
     * <code>true</code> until the state is discarded by {@link #release()}.
     */
    protected final AtomicBoolean open = new AtomicBoolean(true);


   /**
     * The type of join to be performed.
     */
    protected final JoinTypeEnum joinType;
    
//    /**
//     * <code>true</code> iff the join is OPTIONAL.
//     */
//    private final boolean optional;
//    
//    /**
//     * <code>true</code> iff this is a DISTINCT filter.
//     */
//    private final boolean filter;
//    
//    /**
//     * <code>true</code> iff a solution having an unbound {@link #joinVars}
//     * should be dropped and <code>false</code> if it should be indexed anyway.
//     */
//    private final boolean dropSolutionsHavingUnboundJoinVars;

    /**
     * @see HashJoinAnnotations#ASK_VAR
     */
    protected final IVariable<?> askVar;
    
    /**
     * The join variables.
     */
    protected final IVariable<?>[] joinVars;

    /**
     * The variables to be retained (aka projected out) (optional, all variables
     * are retained if not specified).
     */
    protected final IVariable<?>[] selectVars;

    /**
     * True if the hash join utility class is to output the distinct join
     * variables.
     */
    protected boolean outputDistinctJVs = false;
    
    /**
     * The join constraints (optional).
     */
    protected final IConstraint[] constraints;

    /**
     * The hash index.
     * <p>
     * Note: There is no separate "joinSet". Instead, the {@link SolutionHit}
     * class provides a join hit counter.
     */
    protected final AtomicReference<JVMHashIndex> rightSolutionsRef = new AtomicReference<JVMHashIndex>();
    
    /**
     * The #of solutions accepted into the hash index.
     */
    protected final CAT rightSolutionCount = new CAT();
    
    /**
     * The maximum #of (left,right) solution joins that will be considered
     * before failing the join. This is used IFF there are no join variables.
     */
    private final long noJoinVarsLimit = HashJoinAnnotations.DEFAULT_NO_JOIN_VARS_LIMIT;
    
    /**
     * The #of left solutions considered for a join.
     */
    protected final CAT nleftConsidered = new CAT();

    /**
     * The #of right solutions considered for a join.
     */
    protected final CAT nrightConsidered = new CAT();

    /**
     * The #of solution pairs considered for a join.
     */
    protected final CAT nJoinsConsidered = new CAT();
    
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
        sb.append(",considered(left=" + nleftConsidered + ",right="
                + nrightConsidered + ",joins=" + nJoinsConsidered + ")");
        sb.append("}");
        
        return sb.toString();
        
    }

    /**
     * 
     * @param op
     *            The operator whose annotation will inform construction the
     *            hash index. The {@link HTreeAnnotations} may be specified for
     *            this operator and will control the initialization of the
     *            various {@link HTree} instances.
     * @param joinType
     *            The type of join to be performed.
     * 
     * @see JVMHashJoinAnnotations
     */
    public JVMHashJoinUtility(final PipelineOp op, final JoinTypeEnum joinType) {

        if (op == null)
            throw new IllegalArgumentException();

        if(joinType == null)
            throw new IllegalArgumentException();
        
        this.joinType = joinType;
        /*
         * Note: This flag needs to be [true] if we allow solutions to be stored
         * in the hash index that have unbound variables for the "joinVars". We
         * do this for OPTIONAL because all solutions must be indexed for an
         * OPTIONAL join since solutions that do not join will be reported.
         * 
         * A DISTINCT FILTER does this as well. This is because "joinVars" is
         * really the list of projected variables for a SELECT DISTINCT for a
         * DISTINCT FILTER.
         * 
         * By this reasoning, we should also do this for MINUS/NOT EXISTS. That
         * is, for everything except a NORMAL and EXISTS joins.
         * 
         * This suggests a historical bug (fixed below) in MINUS / NOT EXISTS
         * handling.
         * 
         * TODO Write a unit test for that bug involving a source solution with
         * an unbound join variable and see whether the solution is dropped or
         * reported (I believe that it should be reported).
         */
//        final boolean optional = joinType == JoinTypeEnum.Optional;
        final boolean filter = joinType == JoinTypeEnum.Filter;
        final boolean indexSolutionsHavingUnboundJoinVars;
        switch(joinType) {
        case Normal:
        case Exists:
            indexSolutionsHavingUnboundJoinVars = false;
            break;
        case Optional:  // OPTIONAL join.
        case NotExists: // NOT EXISTS and MINUS
        case Filter:    // SELECT DISTINCT
            indexSolutionsHavingUnboundJoinVars = true;
            break;
        default:
            throw new UnsupportedOperationException();
        }

        // Optional variable used for (NOT) EXISTS.
        this.askVar = (IVariable<?>) op
                .getProperty(HashJoinAnnotations.ASK_VAR);
        
        // The join variables (required).
        this.joinVars = (IVariable<?>[]) op
                .getRequiredProperty(HashJoinAnnotations.JOIN_VARS);

        /*
         * The projected OUT variables (optional and equal to the join variables
         * iff this is a DISTINCT filter).
         */
        this.selectVars = filter ? joinVars : (IVariable<?>[]) op
                .getProperty(JoinAnnotations.SELECT);
        
        this.outputDistinctJVs = 
           op.getProperty(
              HashIndexOp.Annotations.OUTPUT_DISTINCT_JVs, false);

        
        // The join constraints (optional).
        this.constraints = (IConstraint[]) op
                .getProperty(JoinAnnotations.CONSTRAINTS);

//        // Iff the join has OPTIONAL semantics.
//        this.optional = optional;
//        
//        // Iff this is a DISTINCT filter.
//        this.filter = filter;

        /*
         * TODO Parameter for the map implementation class.
         * 
         * - HashMap is going to be faster for insert and search.
         * 
         * - LinkedHashMap will be faster for the iterator.
         * 
         * - ConcurrentHashMap will be faster if (a) the JVMHashIndex is safe
         * for concurrent inserts; and (b) the JVMHashIndexOp is safe for
         * concurrent execution.  [These conditions are not currently true.]
         * 
         * Some join patterns do not require us to use an iterator at all, in
         * which case HashMap is the clear winner. (For example, a non-optional
         * hash join against an access path never uses the iterator over the
         * hash index.)
         */

        /*
         * The ordered variables for the keys in the hash index.
         * 
         * For SELECT DISTINCT, use the [select] variables.
         * 
         * Otherwise use the [joinVars].
         */
        final IVariable<?>[] keyVars = filter ? (IVariable<?>[]) op
                .getProperty(JoinAnnotations.SELECT) : joinVars;
                
        rightSolutionsRef.set(//
            new JVMHashIndex(//
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
    
    @Override
    public JoinTypeEnum getJoinType() {
        return joinType;
    }
    
    @Override
    public IVariable<?> getAskVar() {
        return askVar;
    }
    
    @Override
    public IVariable<?>[] getJoinVars() {
        return joinVars;
    }
    
    @Override
    public IVariable<?>[] getSelectVars() {
        return selectVars;
    }
    
    @Override
    public boolean isOutputDistinctJoinVars() {
       return outputDistinctJVs;
    }
    
    @Override
    public IConstraint[] getConstraints() {
        return constraints;
    }

    @Override
    public boolean isEmpty() {

        return getRightSolutionCount() == 0;
        
    }

    protected long getNoJoinVarsLimit() {
        return noJoinVarsLimit;
    }
    
    protected JVMHashIndex getRightSolutions() {
        
        return rightSolutionsRef.get();
        
    }
    
    @Override
    public long getRightSolutionCount() {
        /*
         * Note: This needs to be explicitly tracked and reported. Since each
         * entry in the rightSolutions map is a collision bucket, we can not
         * just report the size of the hash index. Instead we have to track and
         * report the #of solutions entered into the hash index in
         * acceptSolutions() and filterSolutions().
         */
        return rightSolutionCount.get();
    }

    @Override
    public void release() {

        if (open.compareAndSet(true/* expect */, false/* update */)) {
            // Already closed.
            return;
        }

        rightSolutionsRef.set(null);
        
    }

    @Override
    public long acceptSolutions(final ICloseableIterator<IBindingSet[]> itr,
            final BOpStats stats) {

        if (!open.get())
            throw new IllegalStateException();

        try {

            final JVMHashIndex index = getRightSolutions();

            final IBindingSet[] all = BOpUtility.toArray(itr, stats);

            if (log.isDebugEnabled())
                log.debug("Materialized: " + all.length + " source solutions.");

            long naccepted = 0;

            for (IBindingSet bset : all) {

                if (index.add(bset) == null) {

                    continue;

                }

                naccepted++;

            }

            if (log.isDebugEnabled())
                log.debug("There are " + index.bucketCount()
                        + " hash buckets, joinVars="
                        + Arrays.toString(joinVars));

            rightSolutionCount.add(naccepted);

            return naccepted;

        } catch (Throwable t) {

            throw launderThrowable(t);
            
        }

    }

    /*
     * Note: This implementation is not used. The JVMDistinctFilter is based on
     * a ConcurrenthashMap and provides better throughput. For this reason, the
     * JVMDistinctFilter is is used by the JVMDistinctBindingSetsOp.
     */
    @Override
    public long filterSolutions(final ICloseableIterator<IBindingSet[]> itr,
            final BOpStats stats, final IBuffer<IBindingSet> sink) {
        
        try {
        
            final JVMHashIndex index = getRightSolutions();

            final IBindingSet[] all = BOpUtility.toArray(itr, stats);

            if (log.isDebugEnabled())
                log.debug("Materialized: " + all.length + " source solutions.");

            for (IBindingSet bset : all) {

                /*
                 * Note: For a DISTINCT SOLUTIONS filter, we only consider the
                 * variables that are being projected. Further, all variables
                 * are used when computing the hash code. Therefore "joinVars"
                 * == "selectedVars" for a DISTINCT SOLUTIONS filter.
                 */
                bset = bset.copy(joinVars); // only consider the selected variables. 

                /*
                 * Note: Solutions are NOT dropped if a variable is not bound in
                 * a given solution. The variable is simply not used when
                 * computing the hash code. Specifying optional:=true here
                 * causes makeKey() to have this behavior.
                 */
                if (index.addDistinct(bset)) {

                    // Write on the output sink.
                    sink.add(bset);

                }

            }

            if (log.isDebugEnabled())
                log.debug("There are " + index.bucketCount()
                        + " hash buckets, joinVars="
                        + Arrays.toString(joinVars));

            final long naccepted = all.length;

            rightSolutionCount.add(naccepted);

            return naccepted;

        } catch (Throwable t) {

            throw launderThrowable(t);
            
        }

    }

    @Override
    public void hashJoin(//
            final ICloseableIterator<IBindingSet[]> leftItr,//
            final BOpStats stats,//
            final IBuffer<IBindingSet> outputBuffer//
            ) {

        hashJoin2(leftItr, stats, outputBuffer, constraints);
        
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

        if (!open.get())
            throw new IllegalStateException();

        final JVMHashIndex rightSolutions = getRightSolutions();
          
        if (log.isInfoEnabled()) {
            log.info("rightSolutions: #buckets=" + rightSolutions.bucketCount()
                    + ",#solutions=" + getRightSolutionCount());
        }

        // true iff there are no join variables.
        final boolean noJoinVars = joinVars.length == 0;

        try {

            while (leftItr.hasNext()) {

                // Next chunk of solutions from left.
                final IBindingSet[] leftChunk = leftItr.next();
                if (stats != null) {
                    stats.chunksIn.increment();
                    stats.unitsIn.add(leftChunk.length);
                }

                for (IBindingSet left : leftChunk) {

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
                                && nJoinsConsidered.get() == noJoinVarsLimit) {

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
                                selectVars//
                                );

                        switch (joinType) {
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
     * {@inheritDoc}
     * <p>
     * This implementation is a NOP since the underlying Java collection class
     * is thread-safe for concurrent readers.
     */
    @Override
    public void saveSolutionSet() {
        
        // NOP
        
    }
    
    /**
     * Output a solution.
     * 
     * @param outputBuffer
     *            Where to write the solution.
     * @param outSolution
     *            The solution.
     */
    protected void outputSolution(final IBuffer<IBindingSet> outputBuffer,
            final IBindingSet outSolution) {

        if (log.isDebugEnabled())
            log.debug("Output solution: " + outSolution);

        // Accept this binding set.
        outputBuffer.add(outSolution);
        
    }

    @Override
    public void outputOptionals(final IBuffer<IBindingSet> outputBuffer) {

        if (!open.get())
            throw new IllegalStateException();

        try {

            @SuppressWarnings({ "rawtypes", "unchecked" })
            final Constant f = askVar == null ? null : new Constant(XSDBooleanIV.valueOf(false));

            final JVMHashIndex rightSolutions = getRightSolutions();

            final IVariable<?>[] selected = getSelectVars();

            if (log.isInfoEnabled())
                log.info("rightSolutions: #buckets="
                        + rightSolutions.bucketCount());

            /*
             * Note: when NO solutions joined for a given source binding set AND
             * the join is OPTIONAL then we output the _original_ binding set to
             * the sink join task(s) and DO NOT apply the CONSTRAINT(s).
             */
            final Iterator<Bucket> bitr = rightSolutions.buckets();
            
            while (bitr.hasNext()) {

                final Bucket b = bitr.next();

                for (SolutionHit hit : b) {

                    if (hit.nhits.get() > 0)
                        continue;

                    IBindingSet bs = hit.solution;

                    if (selected != null) {

                        // Drop variables which are not projected.
                        bs = bs.copy(selected);

                    }

                    if (f != null) {

                        if (bs == hit.solution)
                            bs = bs.clone();

                        bs.set(askVar, f);

                    }

                    outputBuffer.add(bs);

                    if (log.isDebugEnabled())
                        log.debug("Optional solution: " + bs);

                }

            }

        } catch (Throwable t) {
            
            throw launderThrowable(t);
        
        }
        
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ICloseableIterator<IBindingSet> indexScan() {

        try {

//            /*
//             * The selected variables -or- <code>null</code> if all variables
//             * should be projected.
//             */
//            final IVariable<?>[] selected = getSelectVars();

            final JVMHashIndex rightSolutions = getRightSolutions();

            if (log.isInfoEnabled())
                log.info("rightSolutions: #buckets=" + rightSolutions.bucketCount());

            // Visit the buckets.
            IStriterator itr = new Striterator(rightSolutions.buckets());
            
            itr = itr.addFilter(new Expander() {

                private static final long serialVersionUID = 1L;

                /**
                 * Expand the bucket into the solutions in the bucket.
                 */
                @SuppressWarnings("rawtypes")
                @Override
                protected Iterator expand(final Object obj) {

                    final Bucket b = (Bucket) obj;

                    return b.iterator();

                }
            });

            /**
             * Copy only the variables that are projected.
             */
            itr = itr.addFilter(new Resolver() {

                private static final long serialVersionUID = 1L;

                @Override
                protected Object resolve(final Object obj) {

                    final IBindingSet bs = ((SolutionHit) obj).solution;

//                    if (selected != null) {
//
//                        // Drop variables which are not projected.
//                        bs = bs.copy(selected);
//
//                    }

                    return bs;

                }
            });
            
            return (ICloseableIterator<IBindingSet>) itr;

        } catch (Throwable t) {

            throw launderThrowable(t);

        }

    }
    
    @Override
    public void outputSolutions(final IBuffer<IBindingSet> out) {
        
       if (!open.get())
           throw new IllegalStateException();

       try {

          final JVMHashIndex rightSolutions = getRightSolutions();

          final IVariable<?>[] selected = getSelectVars();

          if (log.isInfoEnabled())
              log.info("rightSolutions: #buckets="
                      + rightSolutions.bucketCount());

          // source.
          final Iterator<Bucket> bucketIterator = rightSolutions.buckets();

          while (bucketIterator.hasNext()) {

              final Bucket bucket = bucketIterator.next();

              // New hash bucket so new DISTINCT set.
              final HashSet<IBindingSet> distinctSet = 
                 outputDistinctJVs ? new HashSet<IBindingSet>()// TODO Size estimate?
                      : null;

              for (SolutionHit solutionHit : bucket) {

                  IBindingSet bs = solutionHit.solution;

                  if( outputDistinctJVs) {

                      /*
                       * Output those solutions that are distinct on the join
                       * variables. We do this by laying a DISTINCT filter
                       * over the solutions drawn from each bucket that we
                       * visit. The DISTINCT filter does not need to consider
                       * solutions that fall into other buckets, just the
                       * current bucket.
                       */

                      // drop anything not in the join variables.
                      bs = bs.copy(joinVars);

                      if (!distinctSet.add(bs)) {

                          // Duplicate solution on JVs in this bucket.
                          continue;
                          
                      }
                      
//                  if (distinctFilter != null) {
//
//                      if ((bs = distinctFilter.accept(bs)) == null) {
//
//                          // Drop duplicate solutions.
//                          continue;
//
//                      }

                  } else if (selected != null) {

                      /*
                       * FIXME We should be using projectedInVars here since
                       * outputSolutions() is used to stream solutions into
                       * the child join group (at least for some kinds of
                       * joins, but there might be exceptions for joining with
                       * a named solution set).
                       */

                      // Drop variables which are not projected.
                      bs = bs.copy(selected);

                  }

                  out.add(bs);

                  if (log.isDebugEnabled())
                      log.debug("Output solution: " + bs);

              }

          }

      } catch (Throwable t) {
          
          throw launderThrowable(t);
          
      }

    }

    @Override
    public void outputJoinSet(final IBuffer<IBindingSet> outputBuffer) {

        try {

            @SuppressWarnings({ "rawtypes", "unchecked" })
            final Constant t = askVar == null ? null : new Constant(XSDBooleanIV.valueOf(true));
            
            final JVMHashIndex rightSolutions = getRightSolutions();

            final IVariable<?>[] selected = getSelectVars();

            if (log.isInfoEnabled())
                log.info("rightSolutions: #buckets=" + rightSolutions.bucketCount());

            final Iterator<Bucket> bitr = rightSolutions.buckets();

            while(bitr.hasNext()) {

                final Bucket b = bitr.next();

                for (SolutionHit hit : b) {

                    if (hit.nhits.get() == 0)
                        continue;

                    IBindingSet bs = hit.solution;

                    if (selected != null) {

                        // Drop variables which are not projected.
                        bs = bs.copy(selected);

                    }

                    if (t != null) {

                        if (bs == hit.solution)
                            bs = bs.clone();

                        bs.set(askVar, t);

                    }
                    
                    outputBuffer.add(bs);

                    if (log.isDebugEnabled())
                        log.debug("Output solution: " + bs);

                }

            }

        } catch (Throwable t) {
            
            throw launderThrowable(t);
        
        }
        
    }

    /**
     * Combine constraints for each source with the given constraints.
     * 
     * @param constraints
     *            Explicitly given constraints for this join.
     * @param all
     *            The sources for the join.
     * 
     * @return The combined constraints and <code>null</code> iff there are no
     *         constraints.
     */
    static IConstraint[] combineConstraints(final IConstraint[] constraints,
            final IHashJoinUtility[] all) {

        final List<IConstraint> list = new LinkedList<IConstraint>();

        // For each source.
        for (int i = 0; i < all.length; i++) {

            final IHashJoinUtility tmp = all[i];

            if (tmp.getConstraints() != null) {

                list.addAll(Arrays.asList(tmp.getConstraints()));

            }

        }

        // The join constraints specified by the caller.
        if (constraints != null) {

            list.addAll(Arrays.asList(constraints));

        }

        return list.isEmpty() ? null : list
                .toArray(new IConstraint[list.size()]);
        
    }

    /**
     * Advance each other source to the first hash code GTE the hashCode for the
     * first source.
     * <p>
     * If the source does not have a bucket for the hash code in the first
     * bucket then either (a) if this is a required join, this method will
     * return <code>false</code> and the caller must advance to the next bucket
     * in the first source; or (b) if this is an optional join, there will be a
     * <code>null</code> in the <i>currentBucket[]</i> for that source.
     * 
     * @param sortedSourceBuckets
     *            An array of {@link Bucket}[]s for each source. The vector of
     *            {@link Bucket}s for each source has been sorted. This means
     *            that we can scan down those vectors and observe {@link Bucket}
     *            s having strictly increasing hash codes for each source.
     * @param sourceIndex
     *            The next index into each source.
     * @param currentBucket
     *            The current bucket for each source.
     * @param optional
     *            <code>true</code> iff this is an optional join.
     * 
     * @return <code>true</code> if we are on a bucket which might join. if this
     *         method returns <code>false</code>, then the caller should
     *         immediately advance to the next bucket from the first source
     *         without attempting a join.
     */
    static private boolean advanceOtherSources(//
            final Bucket[][] sortedSourceBuckets,//
            final int[] sourceIndex,//
            final Bucket[] currentBucket,//
            final boolean optional//
            ) {

        // The next collision bucket in hash code order from the 1st source.
        final Bucket firstBucket = sortedSourceBuckets[0][sourceIndex[0]];
        final int hashCode = firstBucket.hashCode();
        currentBucket[0] = firstBucket;

        for (int i = 1; i < sourceIndex.length; i++) {

            // Advance source to first bucket GTE hashCode.
            while (true) {

                // Next bucket index for the other source
                final int j = sourceIndex[i];

                final Bucket otherBucket;
                if (j >= sortedSourceBuckets[i].length) {
                    // This source is exhausted.
                    if (!optional) {
                        // Nothing is left which can join.
                        return false;
                    }
                    otherBucket = null;
                } else {
                    otherBucket = sortedSourceBuckets[i][j];
                }

                if (otherBucket == null) {
                	assert optional;
                    currentBucket[i] = null;
                	break;
                }
                
                if (otherBucket.hashCode() < hashCode) {

                    sourceIndex[i]++;

                    continue;

                }

                if (otherBucket.hashCode() > hashCode) {

                    if (!optional) {

                        // The bucket on the first source can not join.
                        return false;
                        
                    } else {

                        // The bucket will be ignored.
                        currentBucket[i] = null;
                        
                        // Exit the inner loop.
                        break;
                        
                    }

                }

                currentBucket[i] = otherBucket;

                break;

            }

        }
        
        return true;
        
    }
    
    @Override
    public void mergeJoin(//
            final IHashJoinUtility[] others,//
            final IBuffer<IBindingSet> outputBuffer, //
            final IConstraint[] constraints,//
            final boolean optional//
            ) {

        /*
         * Validate arguments.
         */

        if (others == null)
            throw new IllegalArgumentException();
        
        if (others.length == 0)
            throw new IllegalArgumentException();
        
        if (outputBuffer == null)
            throw new IllegalArgumentException();
        
        final JVMHashJoinUtility[] all = new JVMHashJoinUtility[others.length + 1];
        {
            all[0] = this;
            for (int i = 0; i < others.length; i++) {
                final JVMHashJoinUtility o = (JVMHashJoinUtility) others[i];
                if (o == null)
                    throw new IllegalArgumentException();
                if (!Arrays.equals(this.joinVars, o.joinVars)) {
                    // Must have the same join variables.
                    throw new IllegalArgumentException();
                }
                all[i + 1] = o;
            }

        }

        if(isEmpty()) {
            return;
        }
        
        /*
         * Combine constraints for each source with the given constraints.
         */
        final IConstraint[] c = combineConstraints(constraints, all);

        /*
         * The JVM hash collections do not maintain the data in hash code order.
         * Therefore, we materialize and sort the collision buckets for each
         * hash index.
         */
        final Bucket[][] sortedSourceBuckets = new Bucket[all.length][];
        {

            for (int i = 0; i < all.length; i++) {
                
                // Fully materialize the solution set as a Bucket[].
                final Bucket[] t = all[i].getRightSolutions().toArray();

                /*
                 * Sort the array. It's natural sort order is by the hash code
                 * of the join variables.
                 */
                Arrays.sort(t);
                
                sortedSourceBuckets[i] = t;
                
            }
            
        }

        /*
         * Synchronize each source.
         */

        // The next index into each source (not used for the 1st source).
        final int[] sourceIndex = new int[all.length];

        // The current bucket for each source.
        final Bucket[] currentBucket = new Bucket[all.length];

        while (sourceIndex[0] < sortedSourceBuckets[0].length) {

            if (!optional) {
                /*
                 * If the join is not optional, then we are done as soon as any
                 * source is exhausted.
                 */
                for (int i = 1; i < sourceIndex.length; i++) {
                    if (sourceIndex[i] >= sortedSourceBuckets[i].length) {
                        // All done.
                        return;
                    }
                }
            }
            
            // Synchronous the other sources.
            if (advanceOtherSources(sortedSourceBuckets, sourceIndex,
                    currentBucket, optional)) {

//                log.error("sourceIndex[]=" + Arrays.toString(sourceIndex));
                
                // Join those buckets, outputting solutions which join.
                mergeJoin(currentBucket, c, optional, outputBuffer);

            }

            // Advance the first source to the next bucket.
            sourceIndex[0]++;
            
        }

    }

    /**
     * MERGE JOIN
     * <p>
     * Join the solution sets from each source. This will consider the full
     * cross product of the solutions in each source bucket. All buckets will
     * have the same hash code. If this is an optional join, then some entries
     * in buckets[] MAY be <code>null</code>. However, the first entry is never
     * <code>null</code> since that is the primary source for the join.
     * 
     * @param currentBucket
     *            The current {@link Bucket} from each source. The first entry
     *            in this array is the source from which optional solutions will
     *            be reported if the join is optional.
     * @param constraints
     * @param optional
     *            <code>true</code> iff the join is optional.
     * @param outputBuffer
     */
    static private void mergeJoin(//
            final Bucket[] currentBucket,//
            final IConstraint[] constraints,//
            final boolean optional,//
            final IBuffer<IBindingSet> outputBuffer) {

		final int nsources = currentBucket.length;

		// The bucket for the first source.
		final Bucket firstBucket = currentBucket[0];

		assert firstBucket != null; // never allowed for the 1st source.

		for (int i = 1; i < nsources; i++) {

			// A bucket having the same hash code for another source.
			final Bucket otherBucket = currentBucket[i];

			if (otherBucket == null) {

				assert optional; // only allowed if the join is optional.

				continue;

			}

			// Must be the same hash code.
			assert firstBucket.hashCode() == otherBucket.hashCode();

		}

		final SolutionHit[] set = new SolutionHit[nsources];
		
		final Striterator sols1 = new Striterator(firstBucket.iterator());

		sols1.addFilter(new Visitor() {
            private static final long serialVersionUID = 1L;
            @Override
			protected void visit(Object obj) {
				set[0] = (SolutionHit) obj;
			}

		});
		
		// now add in Expanders and Visitors for each Bucket
		for (int i = 1; i < nsources; i++) {
			// A bucket having the same hash code for another source.
			final int slot = i;
			final Bucket otherBucket = currentBucket[i];
			
			// if optional then if there are no solutions don't try and
			// expand further
			if (!(optional && (otherBucket == null || otherBucket.isEmpty()))) {
				sols1.addFilter(new Expander() {
                    private static final long serialVersionUID = 1L;
                    @Override
					protected Iterator<?> expand(final Object obj) {
						return otherBucket.iterator();
					}
	
				});
				sols1.addFilter(new Visitor() {
                    private static final long serialVersionUID = 1L;
                    @Override
					protected void visit(final Object obj) {
						set[slot] = (SolutionHit) obj;
					}
	
				});
			}
		}

		while (sols1.hasNext()) {
			sols1.next();
            IBindingSet in = set[0].solution;
            for (int i = 1; i < set.length; i++) {

                // See if the solutions join. 
            	if (set[i] != null) {
                in = 
                BOpContext.bind(//
                		in,// 
                        set[i].solution,// 
                        constraints,// TODO constraint[][]
                        null//
                        );
            	}

                if (in == null) {
                    // Join failed.
                    break;
                }

                if (log.isDebugEnabled())
                    log.debug("Output solution: " + in);

            }

            // Accept this binding set.
            if (in != null) {
            	outputBuffer.add(in);
            }
            
//            // now clear set!
//            for (int i = 1; i < set.length; i++) {
//            	set[i] = null;
//            }

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
     * 
     * @see http://sourceforge.net/apps/trac/bigdata/ticket/508 (LIMIT causes
     *      hash join utility to log errors)
     */
    private RuntimeException launderThrowable(final Throwable t) {

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
