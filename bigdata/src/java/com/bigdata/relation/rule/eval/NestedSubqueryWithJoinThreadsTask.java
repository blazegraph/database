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

import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IQueryOptions;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.ISlice;
import com.bigdata.service.AbstractScaleOutFederation;
import com.bigdata.service.ClientIndexView;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.ExecutionHelper;

/**
 * Evaluation of an {@link IRule} using nested subquery (one or more JOINs plus
 * any {@link IElementFilter}s specified for the predicates in the tail or
 * {@link IConstraint}s on the {@link IRule} itself). The subqueries are formed
 * into tasks and submitted to an {@link ExecutorService}. The effective
 * parallelism is limited by the #of elements visited in a chunk for the first
 * join dimension, as only those subqueries will be parallelized. Subqueries for
 * 2nd+ join dimensions are run in the caller's thread to ensure liveness.
 * <p>
 * Note: This join strategy is not very efficient for scale-out joins. If
 * executed one an {@link AbstractScaleOutFederation}, this task will wind up
 * using {@link ClientIndexView}s rather than directly using the local index
 * objects. This means that all work (other than the iterator scan) will be
 * performed on the client running the join.
 * 
 * @todo modify access path to allow us to select specific fields from the
 *       relation to be returned to the join since not all will be used - we
 *       only need those that will be bound. this will require more
 *       generalization of the binding set and its serialization and a mix up of
 *       that with the iterators on the {@link IAccessPath}.
 * 
 * @todo support foreign key joins.
 * 
 * @see JoinMasterTask, which is designed for scale-out federations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NestedSubqueryWithJoinThreadsTask implements IStepTask {

    protected static final Logger log = Logger.getLogger(NestedSubqueryWithJoinThreadsTask.class);
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    protected static final boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    protected static final boolean DEBUG = log.isDebugEnabled();

    /*
     * from the ctor.
     */
    protected final IRule rule;
    protected final IJoinNexus joinNexus;
    protected final IBuffer<ISolution[]> buffer;
    protected final RuleState ruleState;
    protected final RuleStats ruleStats;
    protected final int tailCount;
    
    /**
     * The offset first computed solution that will be inserted into the
     * {@link #buffer}.
     * <p>
     * Note: We handle an {@link ISlice} directly in order to avoid having the
     * caller close an {@link IAsynchronousIterator} reading from a
     * {@link BlockingBuffer}. That causes the {@link Future} writing on the
     * {@link BlockingBuffer} to be interrupted, and if that interrupt is
     * noticed during an IO the {@link FileChannel} backing the store will be
     * asynchronously closed. While it will be automatically re-opened, it is
     * best to avoid the latency associated with that process (which requires
     * re-obtaining any necessary locks, etc).
     * <p>
     * The logic to handle an {@link ISlice} is scattered across several places
     * in this class and in the {@link SubqueryTask}. This is necessary in
     * order for processing to halt eagerly if limit {@link ISolution} have been
     * written onto the {@link #buffer}.
     * <p>
     * When a complex {@link IProgramTask} is being executed, the {@link ISlice}
     * is defined in terms of the total #of solutions generated. Since the
     * <code>solutionCount</code> is tracked on a per-rule execution basis,
     * you MUST serialize the {@link IRule}s in the {@link IProgram} by
     * specifying {@link IQueryOptions#isStable()} or simply marking the
     * {@link IProgram} as NOT {@link IProgram#isParallel()}.
     */
    protected final long offset;
    
    /**
     * The index of the last solutionCount that we will generate (OFFSET +
     * LIMIT). The value of this property MUST be computed such that overflow is
     * not possible. If OFFSET+LIMIT would be greater than
     * {@link Long#MAX_VALUE}, then use {@link Long#MAX_VALUE} instead.
     * <p>
     * Whether or not an {@link ISolution} is part of the slice is based on the
     * <em>post-increment</em> value of the {@link AtomicLong}
     * {@link RuleStats#solutionCount}. If the post-increment value is GT
     * {@link #last} then we have exhausted the slice. Else, if the
     * post-increment value is GT {@link #offset} then we add the
     * {@link ISolution} to the {@link IBuffer}. Else the {@link ISolution} is
     * before the offset of the slice and it will be ignored.
     * <p>
     * Note: {@link AtomicLong#incrementAndGet()} is used to make an atomic
     * decision concerning which solutions are allowed into the buffer. This
     * works even in the face of parallel-subquery, even though we in fact turn
     * off parallel subquery when evaluating a slice in order to have the
     * results be stable with respect to a given commit point. The fence posts
     * for the tests are different than you might expect because we must
     * consider the <em>post-increment</em> state of the
     * {@link RuleStats#solutionCount}.
     * <p>
     * Note: Each {@link IRule} executes with its own {@link RuleStats}
     * instance. This means that concurrent execution of rules would not see the
     * same {@link RuleStats#solutionCount} field and hence that a slice would
     * not be computed correctly when executing {@link IRule}s in parallel for
     * some program. However, as pointed out above, we always disable
     * parallelism for a slice in order to have a stable result set for a given
     * commit point. The stable property is required for client to be able to
     * page through the solutions using a series of slices.
     * <p>
     * Consider (OFFSET=0, LIMIT=1, LAST=1). We would emit a solution for the
     * post-increment solutionCount value ONE (1) (GT OFFSET) but NOT emit a
     * solution for the post-increment solutionCount value TWO (2) (GT LAST).
     * 
     * @see RuleStats#solutionCount
     */
    protected final long last;
    
    /**
     * The maximum #of subqueries for the first join dimension that will be
     * issued in parallel. Use ZERO(0) to avoid the use of the
     * {@link #joinService} entirely and ONE (1) to submit a single task at a
     * time to the {@link #joinService}.
     */
    protected final int maxParallelSubqueries;

    /**
     * The {@link ExecutorService} to which parallel subqueries are submitted.
     */
//    protected final ThreadPoolExecutor joinService;
    
    /**
     * The object that helps us to execute the queries on that service.
     */
    protected final ExecutionHelper<Void> joinHelper;
    
    /**
     * 
     * @param rule
     *            The rule to be executed.
     * @param joinNexus
     *            The {@link IJoinNexus}.
     * @param buffer
     *            A thread-safe buffer onto which chunks of {@link ISolution}
     *            will be flushed during rule execution.
     */
    public NestedSubqueryWithJoinThreadsTask(final IRule rule,
            final IJoinNexus joinNexus, final IBuffer<ISolution[]> buffer) {

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
        
        final ISlice slice = rule.getQueryOptions().getSlice();
        
        if (slice == null) {
            offset = 0L;
            last = 0L; //Long.MAX_VALUE;
        } else {
            offset = slice.getOffset();
            if(slice.getLast()==Long.MAX_VALUE) {
                last = 0L;
            } else {
                last = slice.getLast();
            }
        }

        // true if query evaluation should be stable (no concurrency)
        final boolean stable = rule.getQueryOptions().isStable();
        
        // turn off parallel subquery if [stable] was requested.
        this.maxParallelSubqueries = stable ? 0 : joinNexus
                .getMaxParallelSubqueries();
        
        final ExecutorService joinService = (ThreadPoolExecutor) (maxParallelSubqueries == 0 ? null
                : joinNexus.getIndexManager().getExecutorService());
        
        this.joinHelper = joinService == null ? null
                : new ExecutionHelper<Void>(joinService);
        
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

        try {
            
            apply(0/* orderIndex */, bindingSet);
            
        } catch (Throwable t) {

            if (InnerCause.isInnerCause(t, InterruptedException.class)||
                InnerCause.isInnerCause(t, ClosedByInterruptException.class)) {
                
                /*
                 * The root cause was the asynchronous close of the buffer that
                 * is the overflow() target for the unsynchronized buffer. This
                 * will occur if the high-level iterator was closed() while join
                 * thread(s) are still executing.
                 * 
                 * Note: InterruptedException will be thrown during query if the
                 * BlockingBuffer on which the query solutions are being written
                 * is closed, e.g., because someone closed a high-level iterator
                 * reading solutions from the BlockingBuffer. Closing the
                 * BlockingBuffer causes the Future that is writing on the
                 * BlockingBuffer to be interrupted in order to eagerly
                 * terminate processing.
                 * 
                 * Note: ClosedByInterruptException will be the cause if the
                 * interrupt was noticed during an IO by the thread in which
                 * this exception was thrown.
                 * 
                 * Note: AsynchronousCloseException will be the cause if the
                 * interrupt was noticed during an IO by a different thread
                 * resulting in the asynchronous close of the backing channel.
                 * However, the AsynchronousCloseException is trapped by
                 * DiskOnlyStrategy and results in the transparent re-opening of
                 * the backing channel. Since the target buffer will be closed,
                 * the AsynchronousCloseException should be swiftly followed by
                 * an BlockingBuffer#add() throwing an IllegalStateException if
                 * there is an attempt to write on a closed buffer.
                 * 
                 * Note: Using Thread#interrupt() to halt asynchronous
                 * processing for query is NOT ideal as it will typically force
                 * the FileChannel to be closed asynchronously. You are better
                 * off using a SLICE. However, when the query has a FILTER as
                 * well as a SLICE and the filter can not be evaluated inside of
                 * the the JOINs then the caller must pull solutions through the
                 * filter and close the iterator once the slice is satisified.
                 * That will trigger an interrupt of join thread(s) unless join
                 * processing is already complete.
                 */
                
                if (INFO)
                    log.info("Asyncronous terminatation: " + t);
                
            } else {
                
                // something else, something unexpected.
                throw new RuntimeException(t);
                
            }

        }

        ruleStats.elapsed += System.currentTimeMillis() - begin;

        if(INFO) {
            
            log.info("done:"
                    + "\nmaxParallelSubqueries="+maxParallelSubqueries
                    + "\nruleState=" + ruleState 
                    + ruleStats
//                    +"\nbufferClass="+buffer.getClass().getName()
                    );
            
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
     * Evaluate a join dimension. A private <em>non-thread-safe</em>
     * {@link IBuffer} will allocated and used to buffer a chunk of results. The
     * buffer will be flushed when it overflows and regardless when
     * {@link #apply(int, IBindingSet)} is done. When flushed, it will emit a
     * single chunk onto the thread-safe {@link #buffer}.
     * 
     * @param orderIndex
     *            The current index in the evaluation order[] that is being
     *            scanned.
     * @param bindingSet
     *            The bindings from the prior join(s) (if any).
     * 
     * @throws InterruptedException
     *             This exception will be thrown during query if the
     *             {@link BlockingBuffer} on which the query {@link ISolution}s
     *             are being written is closed, e.g., because someone closed a
     *             high-level iterator reading solutions from the
     *             {@link BlockingBuffer}. Note that closing the
     *             {@link BlockingBuffer} causes the {@link Future} that is
     *             writing on the {@link BlockingBuffer} to be interrupted in
     *             order to eagerly terminate processing. When that interrupt is
     *             detected, we handle it by no longer evaluating the JOIN(s)
     *             and the caller will get whatever {@link ISolution}s are
     *             already in the {@link BlockingBuffer}.
     */
    protected void apply(final int orderIndex, final IBindingSet bindingSet)
            throws InterruptedException {
        
        /*
         * Per-subquery buffer : NOT thread-safe (for better performance).
         * 
         * Note: parallel subquery MUST use distinct buffer instances for each
         * task executing in parallel.
         * 
         * FIXME allocate chunkSize based on expected maxCardinality, or perhaps
         * the subqueryCount #of the left-hand join dimension divided into the
         * range count of the right hand join dimension (assuming that there is
         * a shared variable) with a floor 1000 elements and otherwise the
         * default from IJoinNexus? The goal is to reduce the churn in the
         * nursery.
         * 
         * Note: We CAN NOT reduce the chunkSize when a SLICE is used to the
         * OFFSET+LIMIT since we do not know how many elements on each join
         * dimension must be consumed before we find OFFSET + LIMIT solutions
         * and can halt processing for the rule.
         */
        
        final int chunkSize = joinNexus.getChunkCapacity();
        
        final IBuffer<ISolution> tmp = joinNexus.newUnsynchronizedBuffer(
                buffer, chunkSize);

        //  run the (sub-)query.
        apply(orderIndex, bindingSet, tmp);
        
        // flush buffer onto the chunked buffer (unless interrupted).
//          long nflushed =
        tmp.flush();
//          System.err.println("flushed "+nflushed+" solutions onto the buffer");

    }
    
    /**
     * Evaluate a join dimension.
     * 
     * @param orderIndex
     *            The current index in the evaluation order[] that is being
     *            scanned.
     * @param bindingSet
     *            The bindings from the prior join(s) (if any).
     * @param buffer
     *            The buffer onto which {@link ISolution}s will be written.
     *            <p>
     *            Note: For performance reasons, this buffer is NOT thread-safe.
     *            Therefore parallel subquery MUST use distinct buffer instances
     *            each of which flushes a chunk at a time onto the thread-safe
     *            buffer specified to the ctor.
     */
    final protected void apply(final int orderIndex,
            final IBindingSet bindingSet, final IBuffer<ISolution> buffer)
            throws InterruptedException {

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

                if (last > 0 && ruleStats.solutionCount.get() > last) {

                    // no more solutions to be generated.
                    return;
                    
                }
                
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
                    runSubQueries(orderIndex, chunk, bindingSet, buffer);

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
                    emitSolutions(orderIndex, chunk, bindingSet, buffer);
                    
                }

            } // while

            final long nsolutions = ruleStats.solutionCount.get() - solutionsBefore;
            
            if (nsolutions == 0L) {
                
                applyOptional(orderIndex, bindingSet, buffer);
                
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
     * @param buffer
     *            A buffer onto which {@link ISolution}s will be written - this
     *            object is NOT thread-safe.
     */
    protected void applyOptional(final int orderIndex,
            final IBindingSet bindingSet, IBuffer<ISolution> buffer)
            throws InterruptedException {

        final int tailIndex = getTailIndex(orderIndex);
        
        if( rule.getTail(tailIndex).isOptional()) {
            
            if (orderIndex + 1 < tailCount) {

                // ignore optional with no solutions in the data.
                apply(orderIndex + 1, bindingSet, buffer);

            } else {

                // emit solution since last tail is optional.
                final ISolution solution = joinNexus.newSolution(rule,
                        bindingSet);

                final long solutionCount = ruleStats.solutionCount.incrementAndGet();
                
                if (last > 0 && solutionCount > last) {

                    // done generating solutions.
                    return;
                    
                }

                if (solutionCount > offset) {

                    // add the solution to the buffer.
                    buffer.add(solution);
                    
                }

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
                    + ", tail=" + rule.getTail(tailIndex)
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
     * @param buffer
     *            A buffer onto which {@link ISolution}s will be written - this
     *            object is NOT thread-safe.
     */
    protected void runSubQueries(final int orderIndex, final Object[] chunk,
            final IBindingSet bindingSet, final IBuffer<ISolution> buffer)
            throws InterruptedException {
        
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
        
        if (maxParallelSubqueries==0 || joinHelper==null || orderIndex > 0 || chunk.length <= 1
//                || !useJoinService
//                || (orderIndex > 2 || joinService.getQueue().size() > 100)
                ) {
            
            /*
             * Force the subquery to run in the caller's thread (does not
             * allocate a task, just runs the subqueries directly).
             */
            
            runSubQueriesInCallersThread(orderIndex, chunk, bindingSet, buffer);
            
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

    /**
     * Determines whether we will sort the elements in a chunk into the natural
     * order for the index for the target join dimension.
     * 
     * @todo Is there any advantage to re-order the chunk into the key order for
     *       the next join dimension?  Test with and without.
     *       <p>
     *       What we really need is to re-order the generated bindings and the
     *       next join dimension should be doing that, not this one. In order to
     *       do that we need to process a chunk of binding sets at a time for
     *       the next join dimension.
     *       <p>
     *       See {@link JoinMasterTask} which does this.
     */
    private static final boolean reorderChunkToTargetOrder = true;
        
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
     * @param buffer
     *            A buffer onto which {@link ISolution}s will be written - this
     *            object is NOT thread-safe.
     */
    protected void runSubQueriesInCallersThread(final int orderIndex,
            final Object[] chunk, final IBindingSet bindingSet,
            final IBuffer<ISolution> buffer) throws InterruptedException {

        final int tailIndex = getTailIndex(orderIndex);
        
        for (Object e : chunk) {

            if (last > 0 && ruleStats.solutionCount.get() > last) {

                // no more solutions to be generated.
                return;
                
            }
            
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

            // clone binding set.
            final IBindingSet bset = bindingSet.clone();
            
            // bind element to propagate bindings.
            if (joinNexus.bind(rule, tailIndex, e, bset)) {

                ruleStats.subqueryCount[tailIndex]++;

                // run the subquery.
                apply(orderIndex + 1, bset, buffer);

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
     * 
     * @todo Develop a pattern for feeding the {@link ExecutorService} such that
     *       it maintains at least N threads from a producer and no more than M
     *       threads overall. We need that pattern here for better sustained
     *       throughput and in the map/reduce system as well (it exists there
     *       but needs to be refactored and aligned with the simpler thread pool
     *       exposed by the {@link IIndexManager}). The pattern is similar to
     *       one in the RDF concurrent data loader and in the map/reduce
     *       package.  It should be encapsulated by the {@link ExecutionHelper}
     *       along with the submitOne() and submitSequence() methods.
     */
    protected void runSubQueriesOnThreadPool(final int orderIndex,
            final Object[] chunk, final IBindingSet bindingSet)
            throws InterruptedException {

        final int tailIndex = getTailIndex(orderIndex);

        /*
         * At most one task per element in this chunk, but never more than
         * [maxParallelSubqueries] tasks at once.
         * 
         * Note: We are not allowed to modify this while the tasks are being
         * executed so we create a new instance of the array each time we submit
         * some tasks for execution!
         */
        List<Callable<Void>> tasks = null;
        
        // #of elements remaining in the chunk.
        int nremaining = chunk.length;
        
        // for each element in the chunk.
        for (Object e : chunk) {

            if (last > 0 && ruleStats.solutionCount.get() > last) {

                // no more solutions to be generated.
                return;
                
            }

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

            // clone the binding set.
            final IBindingSet bset = bindingSet.clone();
            
            // propagate bindings.
            if (joinNexus.bind(rule, tailIndex, e, bset)) {

                // we will run this subquery.
                ruleStats.subqueryCount[tailIndex]++;

                if (tasks == null) {

                    // maximum #of subqueries to issue.
                    final int capacity = Math.min(maxParallelSubqueries,
                            nremaining);

                    // allocate for the new task(s).
                    tasks = new ArrayList<Callable<Void>>(capacity);
                    
                }
                
                // create a task for the subquery.
                tasks.add(new SubqueryTask<Void>(orderIndex + 1, bset));

                if (tasks.size() == maxParallelSubqueries) {

                    try {
                        joinHelper.submitTasks(tasks);
                    } catch(ExecutionException ex) {
                        throw new RuntimeException(ex);
                    }

                    tasks = null;
                    
                }

            }

            /*
             * Note: one more element from the chunk has been consumed. This is
             * true regardless of whether the binding was allowed and a subquery
             * resulted.
             */
            nremaining--;
            
        }

        if (tasks != null) {

            // submit any remaining tasks.
            try {
                joinHelper.submitTasks(tasks);
            } catch(ExecutionException ex) {
                throw new RuntimeException(ex);
            }
            
        }

    }
    
    /**
     * This class is used when we want to evaluate the subqueries in parallel.
     * The inner task uses
     * {@link NestedSubqueryWithJoinThreadsTask#apply(int, IBindingSet, IBuffer)}
     * to evaluate a subquery.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class SubqueryTask<E> implements Callable<E> {
        
        protected final int orderIndex;
        protected final IBindingSet bindingSet;
        
        public SubqueryTask(final int orderIndex, final IBindingSet bindingSet) {
            
            this.orderIndex = orderIndex;
            
            this.bindingSet = bindingSet;
            
        }
        
        /**
         * Run the subquery - it will use a private {@link IBuffer} for better
         * throughput and thread safety.
         */
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
     * @param buffer
     *            A buffer onto which the solutions will be written - this
     *            object is NOT thread-safe.
     */
    protected void emitSolutions(final int orderIndex, final Object[] chunk,
            final IBindingSet bindingSet, final IBuffer<ISolution> buffer)
            throws InterruptedException {

        final int tailIndex = getTailIndex(orderIndex);

        for (Object e : chunk) {

            if (DEBUG) {
                log.debug("Considering: " + e.toString() + ", orderIndex="
                        + orderIndex + ", rule=" + rule.getName());
            }

            ruleStats.elementCount[tailIndex]++;

            // bind variables from the current element.
            if (joinNexus.bind(rule, tailIndex, e, bindingSet)) {

                /*
                 * emit entailment
                 */

                if (DEBUG) {
                    log.debug("solution: " + bindingSet);
                }

                final ISolution solution = joinNexus.newSolution(rule,
                        bindingSet);

                final long solutionCount = ruleStats.solutionCount.incrementAndGet();
                
                if (last > 0 && solutionCount > last) {

                    // done generating solutions.
                    return;
                    
                }

                if (solutionCount > offset) {

                    // add the solution to the buffer.
                    buffer.add(solution);
                    
                }

            }

        }

    }

//    /**
//     * Evaluation of an {@link IRule} using nested subquery and an {@link Advancer}
//     * pattern to reduce the #of subqueries to one per chunk materialized for the
//     * left-hand side of a join.
//     * <p>
//     * This (a) unrolls the inner loop of the join; and (b) if the index for the
//     * right-hand side of the join is key-range partitioned, then partitions the
//     * join such that the outer loop is split across the {@link DataService}s where
//     * the index partition resides and runs each split in parallel. We do not
//     * parallelize subqueries that target the same index partition since a single
//     * reader on that index partition using an {@link Advancer} pattern will be very
//     * efficient (it will use an ordered read).
//     * <p>
//     * This makes the outer loop extremely efficient, distributes the computation
//     * over the cluster, and parallelizes the inner loop so that we do at most N
//     * queries - one query per index partition spanned by the queries framed for the
//     * inner loop by the bindings selected in the outer loop for a given chunk. A
//     * large chunk size for the outer loop is also effective since the reads are
//     * local and this reduces the #of times that we will unroll the inner loop.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    protected class JoinAdvancer<E> extends Advancer<E> {
//
//        private final E[] chunk;
//        private final int orderIndex;
//        private final IBindingSet bindingSet;
//
//        /**
//         * Create a filter that will perform subquery joins.
//         *  
//         * @param orderIndex
//         *            The index of the current join dimension in the evaluation
//         *            order.
//         * @param bindingSet
//         *            The set of bindings for the subquery joins.
//         * @param chunk
//         *            A chunk of elements materialized for the current join
//         *            dimension.
//         */
//        public JoinAdvancer(final int orderIndex, final IBindingSet bindingSet,
//                final E[] chunk) {
//
//            if (bindingSet == null) {
//
//                throw new IllegalArgumentException();
//                
//            }
//            
//            if (chunk == null || chunk.length <= 1) {
//                
//                throw new IllegalArgumentException();
//
//            }
//            
//            this.orderIndex = orderIndex;
//
//            this.bindingSet = bindingSet;
//            
//            this.chunk = chunk;
//            
//        }
//        
//        @Override
//        protected void advance(ITuple tuple) {
//            
//            final int tailIndex = getTailIndex(orderIndex);
//
//            for (Object e : chunk) {
//
//                if (DEBUG) {
//                    log.debug("Considering: " + e.toString()
//                            + ", tailIndex=" + orderIndex + ", rule="
//                            + rule.getName());
//                }
//
//                ruleStats.elementCount[tailIndex]++;
//
//                final IBindingSet bset = bindingSet.clone();
//                
//                if (ruleState.bind(tailIndex, e, bset)) {
//
//                    // run the subquery.
//                    
//                    ruleStats.subqueryCount[tailIndex]++;
//
//                    try {
//
//                        apply(orderIndex + 1, bset); // vs buffer?
//                        
//                    } catch(InterruptedException ex) {
//
//                        /*
//                         * Stop processing if the task is interrupted.
//                         */
//                        
//                        if(DEBUG)
//                            log.debug("Interrupted: "+ex);
//                        
//                        // stop processing.
//                        break;
//                        
//                    }
//                    
//                }
//
//            }
//
//        }
//        
//    };

}
