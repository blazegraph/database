/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Oct 29, 2007
 */

package com.bigdata.relation.rule.eval;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IIndexStore;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.service.ClientIndexView;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

/**
 * Evaluation of an {@link IRule} using nested subquery (one or more JOINs plus
 * any {@link IElementFilter}s specified for the predicates in the tail or
 * {@link IConstraint}s on the {@link IRule} itself). The subqueries are formed
 * into tasks and submitted to an {@link ExecutorService}. The effective
 * parallelism is limited by the #of elements visited in a chunk for the first
 * join dimension, as only those subqueries will be parallelized. Subqueries for
 * 2nd+ join dimensions are run in the caller's thread to ensure liveness.
 * 
 * @todo Scale-out joins should be distributed. A scale-out join run with this
 *       task will use {@link ClientIndexView}s. All work (other than the
 *       iterator scan) will be performed on the client running the join.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NestedSubqueryWithJoinThreadsTask implements IStepTask {

    protected static final Logger log = Logger.getLogger(NestedSubqueryWithJoinThreadsTask.class);
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    protected static final boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    protected static final boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /*
     * from the ctor.
     */
    protected final IRule rule;
    protected final IJoinNexus joinNexus;
    protected final IBuffer<ISolution> buffer;
    protected final RuleState ruleState;
    protected final RuleStats ruleStats;
    protected final int tailCount;

    /**
     * When <code>true</code>, subqueries for the first join dimension will
     * be issued in parallel.
     */
    protected final boolean parallelSubqueries;

    /**
     * The {@link ExecutorService} to which parallel subqueries are submitted.
     */
    protected final ThreadPoolExecutor joinService;
    
    public NestedSubqueryWithJoinThreadsTask(final IRule rule,
            final IJoinNexus joinNexus, final IBuffer<ISolution> buffer) {

        if (rule == null)
            throw new IllegalArgumentException();

        if( joinNexus == null)
             throw new IllegalArgumentException();
        
        if (buffer == null)
            throw new IllegalArgumentException();

        this.rule = rule;
        
        this.joinNexus = joinNexus;
        
        this.buffer = buffer;

        this.ruleState = new RuleState(rule, joinNexus);

        // note: evaluation order is fixed by now.
        this.ruleStats = joinNexus.getRuleStatisticsFactory().newInstance(rule,
                ruleState.plan, ruleState.keyOrder);
        
        this.tailCount = rule.getTailCount();
        
        final IIndexManager im = joinNexus.getIndexManager();
        
        /*
         * Note: Parallel subqueries provide a 2x multipler for EDS and a 6x
         * multiplier for JDS (faster closure as measured on LUBM U1).  There
         * is a slight penalty for LTS (.99x) and LDS (.88x). 
         */
        this.parallelSubqueries = (im instanceof IBigdataFederation && ((IBigdataFederation) im)
                .isScaleOut());
        
        this.joinService = (ThreadPoolExecutor) (!parallelSubqueries ? null
                : useJoinService ? joinNexus.getJoinService() : joinNexus
                        .getIndexManager().getExecutorService());
        
    }
    
    /**
     * Recursively evaluate the subqueries.
     */
    final public RuleStats call() {

        if(INFO) {
            
            log.info("begin:\nruleState=" + ruleState + "\nplan="
                    + ruleState.plan);
            
        }

        if (ruleState.plan.isEmpty()) {

            if (INFO)
                log.info("Rule proven to have no solutions.");
            
            return ruleStats;
            
        }
        
        final long begin = System.currentTimeMillis();

        final IBindingSet bindingSet = joinNexus.newBindingSet(rule);

        apply( 0, bindingSet );
        
        ruleStats.elapsed += System.currentTimeMillis() - begin;
        
        if(DEBUG) {
            
            log.debug("done: ruleState=" + ruleState + ", ruleStats="
                    + ruleStats);
            
        }
        
        return ruleStats;
        
    }
    
    /**
     * Return the index of the tail predicate to be evaluated at the given index
     * in the evaluation order.
     * 
     * @param orderIndex
     *            The evaluation order index.
     * @return The tail index to be evaluated at that index in the evaluation
     *         order.
     */
    final protected int getTailIndex(int orderIndex) {
        
        final int tailIndex = ruleState.order[orderIndex];
        
        assert orderIndex >= 0 && orderIndex < tailCount : "orderIndex="
                + orderIndex + ", rule=" + rule;
        
        return tailIndex;
        
    }
    
    /**
     * Evaluate a join dimension.
     * 
     * @param orderIndex
     *            The current index in the evaluation order[] that is being
     *            scanned.
     * @param bindingSet
     *            The bindings from the prior join(s) (if any).
     */
    final protected void apply(final int orderIndex, final IBindingSet bindingSet) {

        // Obtain the iterator for the current join dimension.
        final IChunkedOrderedIterator itr = getAccessPath(orderIndex,
                bindingSet).iterator();
        
        try {

            final int tailIndex = getTailIndex(orderIndex);

            /*
             * Handles non-optionals and optionals with solutions in the
             * data.
             */
            
            final long solutionsBefore = ruleStats.solutionCount.get();
            
            while (itr.hasNext()) {

                if (orderIndex + 1 < tailCount) {

                    // Nested subquery.

                    final Object[] chunk;
                    if (reorderChunkToTargetOrder) {
                        
                        /*
                         * Re-order the chunk into the target order for the
                         * _next_ access path.
                         * 
                         * FIXME This imples that we also know the set of
                         * indices on which we need to read for a rule before we
                         * execute the rule. That knowledge should be captured
                         * and fed into the LDS and EDS/JDS rule execution logic
                         * in order to optimize JOINs.
                         */
                        
                        // target chunk order.
                        final IKeyOrder targetKeyOrder = ruleState.keyOrder[getTailIndex(orderIndex + 1)];

                        // Next chunk of results from the current access path.
                        chunk = itr.nextChunk(targetKeyOrder);
                        
                    } else {
                        
                        // Next chunk of results from the current access path.
                        chunk = itr.nextChunk();
                        
                    }

                    ruleStats.chunkCount[tailIndex]++;

                    // Issue the nested subquery.
                    runSubQueries(orderIndex, chunk, bindingSet);

                } else {

                    // bottomed out.
                    
                    /*
                     * Next chunk of results from that access path. The order of
                     * the elements in this chunk does not matter since this is
                     * the last join dimension.
                     */
                    final Object[] chunk = itr.nextChunk();

                    ruleStats.chunkCount[tailIndex]++;

                    // evaluate the chunk and emit any solutions.
                    emitSolutions(orderIndex, chunk, bindingSet);
                    
                }

            } // while

            final long nsolutions = ruleStats.solutionCount.get() - solutionsBefore;
            
            if (nsolutions == 0L) {
                
                applyOptional(orderIndex, bindingSet);
                
            }
            
        } finally {

            itr.close();

        }

    }

    /**
     * Method to be invoked IFF there were no solutions in the data that
     * satisified the constraints on the rule. If the tail is optional, then
     * subquery evaluation will simply skip the tail and proceed with the
     * successor of the tail in the evaluation order. If the tail is the last
     * tail in the evaluation order, then a solution will be emitted for the
     * binding set.
     * 
     * @param orderIndex
     *            The index into the evaluation order.
     * @param bindingSet
     *            The bindings from the prior join(s) (if any).
     */
    protected void applyOptional(final int orderIndex,
            final IBindingSet bindingSet) {

        final int tailIndex = getTailIndex(orderIndex);
        
        if( rule.getTail(tailIndex).isOptional()) {
            
            if (orderIndex + 1 < tailCount) {

                // ignore optional with no solutions in the data.
                apply(orderIndex + 1, bindingSet);

            } else {

                // emit solution since last tail is optional.
                final ISolution solution = joinNexus.newSolution(rule,
                        bindingSet);

                ruleStats.solutionCount.incrementAndGet();

                buffer.add(solution);

            }

        }
        
    }
    
    /**
     * Return the {@link IAccessPath} for the tail predicate to be evaluated at
     * the given index in the evaluation order.
     * 
     * @param orderIndex
     *            The index into the evaluation order.
     * @param bindingSet
     *            The bindings from the prior join(s) (if any).
     * 
     * @return The {@link IAccessPath}.
     */
    protected IAccessPath getAccessPath(final int orderIndex,
            final IBindingSet bindingSet) {

        final int tailIndex = getTailIndex(orderIndex);

        final IPredicate predicate = rule.getTail(tailIndex)
                .asBound(bindingSet);

        final IAccessPath accessPath = joinNexus.getTailAccessPath(predicate);

        if (DEBUG) {

            log.debug("orderIndex=" + orderIndex + ", tailIndex=" + tailIndex
                    + ", tail=" + ruleState.rule.getTail(tailIndex)
                    + ", bindingSet=" + bindingSet + ", accessPath="
                    + accessPath);

        }

        return accessPath;

    }

    /**
     * Evaluate the right-hand side (aka the subquery) of the join for each
     * element in the chunk. This method will not return until all subqueries
     * for the chunk have been evaluated.
     * 
     * @param orderIndex
     *            The current index in the evaluation order.
     * @param chunk
     *            A chunk of elements from the left-hand side of the join.
     * @param bindingSet
     *            The bindings from the prior joins (if any).
     */
    protected void runSubQueries(final int orderIndex, final Object[] chunk,
            final IBindingSet bindingSet) {
        
        /*
         * Note: At this stage all we want to do is build the subquery tasks.
         * For the _most_ part , the joinService should choose whether to run
         * the tasks in the caller's thread, start a new thread, or run leave
         * them on the work queue for a bit.
         * 
         * The CRITICAL exception is that if ALL workers wait on tasks in the
         * work queue then the JOIN will NOT progress.
         * 
         * This problem arises because of the control structure is recursion. An
         * element [e] from a chunk on the 1st join dimension can cause a large
         * number of tasks to be executed and the task for [e] is not complete
         * until those tasks are complete. This means that [e] is tying up a
         * worker thread while all nested subqueries for [e] are evaluated.
         * 
         * We work around this by only parallelizing subqueries for each element
         * in each chunk of the 1st join dimension. We can not simply let the
         * size of the thread pool grow without bound as it will (very) rapidly
         * exhaust the machine resources for some queries. Forcing the
         * evaluation of subqueries after the 1st join dimension in the caller's
         * thread ensures liveness while providing an effective parallelism up
         * to the minimum of {chunk size, the #of elements visited on the first
         * chunk, and the maximum size of the thread pool}.
         * 
         * Note: Many joins rapidly become more selective with a good evaluation
         * plan and there is an expectation that the #of results for the
         * subquery will be small. However, this is NOT always true. Some
         * queries will have a large fan out from the first join dimension.
         * 
         * Note: The other reason to force the subquery to run in the caller's
         * thread is when there are already too many threads executing
         * concurrently in this thread pool. (Note that this also reflects other
         * tasks that may be executing in parallel with this rule evaluation if
         * they are running on the same service).
         * 
         * Note: If there is only one element in the chunk then there is no
         * point in using the thread pool for the subquery. A lot of queries
         * rapidly become fully bound and therefore fall into this category.
         * Those subqueries are just run in the caller's thread.
         */
        
        if (!parallelSubqueries || orderIndex > 0 || chunk.length <= 1
//                || !useJoinService
//                || (orderIndex > 2 || joinService.getQueue().size() > 100)
                ) {
            
            /*
             * Force the subquery to run in the caller's thread (does not
             * allocate a task, just runs the subqueries directly).
             */
            
            runSubQueriesInCallersThread(orderIndex, chunk, bindingSet);
            
        } else {

            /*
             * Allocate a task for each subquery and queue it on the
             * joinService. The joinService will make a decision whether to run
             * the task in the caller's thread, to allocate a new thread, or to
             * let it wait on the work queue until a thread becomes available.
             */
            
            runSubQueriesOnThreadPool(orderIndex, chunk, bindingSet);
            
        }

    }

    private static final boolean reorderChunkToTargetOrder = true;
    
    /**
     * Controls which thread pool is used for parallelization of subqueries.
     * When <code>true</code> uses the {@link IJoinNexus#getJoinService()}
     * otherwise uses {@link IIndexStore#getExecutorService()}.
     * <p>
     * Note: The IJoinNexus#getJoinService() is currently configured to force a
     * task to run in the caller's thread when it's work queue is full. However,
     * this is not enough to satisify the liveness constraint (progress halts
     * when all workers are waiting on subtasks in the work queue).
     * 
     * FIXME get rid of {@link IJoinNexus#getJoinService()}? The
     * {@link IIndexStore#getExecutorService()} appears to be perfectly
     * sufficient.
     */
    private static final boolean useJoinService = false;
    
    /**
     * Runs the subquery in the caller's thread (this was the original
     * behavior).
     * 
     * @param orderIndex
     *            The current index in the evaluation order.
     * @param chunk
     *            A chunk of elements from the left-hand side of the join.
     * @param bindingSet
     *            The bindings from the prior joins (if any).
     */
    protected void runSubQueriesInCallersThread(final int orderIndex,
            final Object[] chunk, final IBindingSet bindingSet) {

        final int tailIndex = getTailIndex(orderIndex);
        
        for (Object e : chunk) {

            if (DEBUG) {
                log.debug("Considering: " + e.toString() + ", tailIndex="
                        + orderIndex + ", rule=" + rule.getName());
            }

            ruleStats.elementCount[tailIndex]++;

            /*
             * Then bind this statement, which propagates bindings to the next
             * predicate (if the bindings are rejected then the solution would
             * violate the constaints on the JOIN).
             */

            ruleState.clearDownstreamBindings(orderIndex + 1, bindingSet);

            if (ruleState.bind(tailIndex, e, bindingSet)) {

                // run the subquery.

                ruleStats.subqueryCount[tailIndex]++;

                apply(orderIndex + 1, bindingSet);

            }

        }

    }

    /**
     * Variant that creates a {@link SubqueryTask} for each element of the chunk
     * and submits those tasks to the {@link #joinService}. This method will
     * not return until all subqueries for the chunk have been evaluated.
     * <p>
     * The {@link #joinService} will decide for each task whether to allocate a
     * new thread, to run it on an existing thread, to leave it on the work
     * queue for a while, or to execute it in the caller's thread (the latter is
     * selected via a rejected exection handler option).
     * <p>
     * Note: This requires that we clone the {@link IBindingSet} so that each
     * parallel task will have its own state.
     * <p>
     * Note: The tasks should be executed (more or less) in order so as to
     * maximum the effect of ordered reads on the next join dimension.
     * 
     * @param orderIndex
     *            The current index in the evaluation order.
     * @param chunk
     *            A chunk of elements from the left-hand side of the join.
     * @param bindingSet
     *            The bindings from the prior joins (if any).
     */
    protected void runSubQueriesOnThreadPool(final int orderIndex,
            final Object[] chunk, final IBindingSet bindingSet) {

        final int tailIndex = getTailIndex(orderIndex);

        // at most one task per element in this chunk.
        final List<Callable<Void>> tasks = new LinkedList<Callable<Void>>();
        
        // for each element in the chunk.
        for (Object e : chunk) {

            if (DEBUG) {
                log.debug("Considering: " + e.toString() + ", tailIndex="
                        + orderIndex + ", rule=" + rule.getName());
            }

            ruleStats.elementCount[tailIndex]++;
            
            /*
             * Then bind this statement, which propagates bindings to the next
             * predicate (if the bindings are rejected then the solution would
             * violate the constaints on the JOIN).
             */

            ruleState.clearDownstreamBindings(orderIndex + 1, bindingSet);

            if (ruleState.bind(tailIndex, e, bindingSet)) {

                // we will run this subquery.
                ruleStats.subqueryCount[tailIndex]++;

                // create a task for the subquery.
                tasks.add(new SubqueryTask<Void>(orderIndex + 1, bindingSet
                        .clone()));

            }

        }

        /*
         * Submit subquery tasks and wait until they are done.
         */
        final List<Future<Void>> futures;
        try {

            // submit tasks and await completion of those tasks.
            futures = joinService.invokeAll(tasks);
            
            for(Future<Void> f : futures) {
                
                // verify that no task failed.
                f.get();
                
            }
            
        } catch (InterruptedException ex) {

            throw new RuntimeException("Terminated by interrupt", ex);

        } catch (ExecutionException ex) {

            throw new RuntimeException("Join failed: " + ex, ex);

        }

    }
    
    /**
     * Inner task uses
     * {@link NestedSubqueryWithJoinThreadsTask#apply(int, IBindingSet)} to
     * evaluate a subquery. This class is used when we want to evaluate the
     * subqueries in parallel.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class SubqueryTask<E> implements Callable<E> {
        
        protected final int orderIndex;
        protected final IBindingSet bindingSet;
        
        public SubqueryTask(int orderIndex, IBindingSet bindingSet) {
            
            this.orderIndex = orderIndex;
            
            this.bindingSet = bindingSet;
            
        }
        
        public E call() throws Exception {
            
            apply(orderIndex, bindingSet);

            return null;
            
        }
        
    }

    /**
     * Consider each element in the chunk in turn. If the element satisifies the
     * JOIN criteria, then emit an {@link ISolution} for the {@link IRule}.
     * 
     * @param orderIndex
     *            The index in the evaluation order.
     * @param chunk
     *            A chunk of elements from the right-most join dimension.
     * @param bindingSet
     *            The bindings from the prior joins.
     */
    protected void emitSolutions(final int orderIndex, final Object[] chunk,
            final IBindingSet bindingSet) {

        final int tailIndex = getTailIndex(orderIndex);

        for (Object e : chunk) {

            if (DEBUG) {
                log.debug("Considering: " + e.toString() + ", orderIndex="
                        + orderIndex + ", rule=" + rule.getName());
            }

            ruleStats.elementCount[tailIndex]++;

            // bind variables from the current element.
            if (ruleState.bind(tailIndex, e, bindingSet)) {

                /*
                 * emit entailment
                 */

                if (DEBUG) {
                    log.debug("solution: " + bindingSet);
                }

                final ISolution solution = joinNexus.newSolution(rule,
                        bindingSet);

                ruleStats.solutionCount.incrementAndGet();

                buffer.add(solution);

            }

        }

    }

}
