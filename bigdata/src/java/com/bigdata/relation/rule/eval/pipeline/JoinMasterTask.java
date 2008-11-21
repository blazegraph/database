/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Oct 16, 2008
 */

package com.bigdata.relation.rule.eval.pipeline;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.ITx;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.BufferClosedException;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;
import com.bigdata.relation.accesspath.UnsynchronizedArrayBuffer;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.relation.rule.eval.IRuleState;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.relation.rule.eval.IStepTask;
import com.bigdata.relation.rule.eval.NestedSubqueryWithJoinThreadsTask;
import com.bigdata.relation.rule.eval.RuleState;
import com.bigdata.relation.rule.eval.RuleStats;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.striterator.IKeyOrder;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.ExecutionExceptions;

/**
 * Master providing efficient distributed evaluation of {@link IRule}s. For
 * query, this task should be run by the client that wishes to materialize the
 * query results. For mutation, this task may be run by any client or service
 * since the data does not flow through the master for mutation.
 * <p>
 * For the first join dimension, the {@link JoinMasterTask} creates a
 * {@link JoinTask} per index partition that will be spanned by the
 * {@link IAccessPath} for the first {@link IPredicate} in the evaluation order
 * and feeds each {@link JoinTask}(s) in the first join dimension with an
 * {@link IAsynchronousIterator} reading on a buffer containing single empty
 * {@link IBindingSet}.
 * <p>
 * Each {@link JoinTask} consumes {@link IBindingSet} chunks read from the
 * previous join dimension. For each {@link IBindingSet} chunk read, a new
 * {@link IAccessPath} is obtained. Elements are then read from than
 * {@link IAccessPath} in chunks. Given the {@link IBindingSet} used to obtain
 * the {@link IAccessPath}, a new {@link IBindingSet} is created for each
 * element in each chunk read from the {@link IAccessPath}. If the new
 * {@link IBindingSet} satisifies the constraint(s) on the {@link IRule} then it
 * will be output to the next join dimension. An {@link IBindingSet} is output
 * by placing it onto the {@link UnsynchronizedArrayBuffer} for the join
 * dimension. Periodically that {@link UnsynchronizedArrayBuffer} will overflow,
 * and a chunk of {@link IBindingSet}s will be placed onto the
 * {@link IBlockingBuffer} from which the next join dimension will read its
 * {@link IBindingSet} chunks.
 * <p>
 * The last join dimension is slightly different. Its
 * {@link UnsynchronizedArrayBuffer} writes onto the
 * {@link IJoinNexus#newQueryBuffer()},
 * {@link IJoinNexus#newInsertBuffer(com.bigdata.relation.IMutableRelation)},
 * or {@link IJoinNexus#newDeleteBuffer(com.bigdata.relation.IMutableRelation)}
 * depending on the {@link ActionEnum}.
 * <p>
 * For each {@link JoinTask}, once its source iterator(s) have been exhausted
 * and the {@link IAccessPath} reading from the last source {@link IBindingSet}
 * has been exhausted, then the {@link JoinTask} for that join dimension is done
 * and it will flush its {@link UnsynchronizedArrayBuffer} and close its output
 * {@link IBuffer} and wait for the downstream {@link JoinTask}s to report
 * their {@link RuleStats}. Those {@link RuleStats} are aggregated and passed
 * back to its caller in turn.
 * <p>
 * Each join dimension is single-threaded. Coordination of resources is achieved
 * using the output buffer for each join dimension. This allows a source join
 * dimension to read ahead and forces the sink join dimension to process chunks
 * of {@link IBindingSet}s at a time.
 * <p>
 * The {@link JoinMasterTask} is responsible for the {@link JoinTask}s for the
 * first join dimension. Each {@link JoinTask} is responsible for the downstream
 * {@link JoinTask}s. If the {@link JoinMasterTask} is interrupted or
 * cancelled, then it interrupts or cancels the {@link JoinTask}s for the first
 * join dimension. If {@link JoinTask} is interrupted or cancelled then it must
 * cancel any {@link JoinTask}s which it has created for the next join
 * dimension.
 * 
 * <h2>Choosing the view</h2>
 * 
 * Rules SHOULD be evaluated against a read-historical state.
 * <p>
 * This is a hard requirement when computing the fix point closure of a rule
 * (set). Each round of closure MUST be evaluated against the commit time
 * reported by {@link IBigdataFederation#getLastCommitTime()} and is applied for
 * all rules in that round. This allows unisolated tasks to write on the
 * generated solutions onto the indices. This is a strong requirement since the
 * {@link JoinTask}s will otherwise wind up holding an exclusive lock on the
 * {@link ITx#UNISOLATED} index partitions, which would cause a deadlock when
 * attempting to write the generated solutions onto the index partitions. At the
 * start of the next round of closure, simply update the read-historical
 * timestamp to the then current value of
 * {@link IBigdataFederation#getLastCommitTime()}.
 * <p>
 * Queries that use {@link ITx#READ_COMMITTED} or {@link ITx#UNISOLATED} will
 * not generate deadlocks, but they are subject to abort from the
 * split/join/move of index partition(s) during query evaluation. This problem
 * WILL NOT arise if you read instead from the
 * {@link IBigdataFederation#getLastCommitTime()}.
 * 
 * <h2>Key-range partitioned joins</h2>
 * 
 * In order to scale-out efficiently, the {@link JoinMasterTask} must distribute
 * the {@link JoinTask}s such that they run inside of the
 * {@link ConcurrencyManager} on the various {@link DataService}s on which the
 * index partitions reside from which the {@link IAccessPath}s must read. This
 * allows the {@link IAccessPath} to read on the local index object and reduces
 * the message traffic to pulling chunks of {@link IBindingSet}s from the
 * source {@link JoinTask}s.
 * <p>
 * For the {@link JoinMasterTask} and for each {@link JoinTask}, the fan out of
 * {@link JoinTask}s is determined by the #of index partitions that are spanned
 * by the {@link IAccessPath}s required to evaluate the {@link IBindingSet}s
 * for the next join dimension. The {@link IAccessPath} will not be used by the
 * source join dimension to read on the index, merely to discover the index
 * partitions to which the generating {@link IBindingSet}s must be assigned.
 * The index partition spanned for a given {@link IBindingSet} is determined by
 * generating an as bound {@link IPredicate} for the next join dimension,
 * instantiating the {@link IAccessPath} on the source join dimension that will
 * be used by the target join dimension, and then using a locator scan for the
 * <i>fromKey</i> and <i>toKey</i> for that {@link IAccessPath}. In the case
 * where the {@link IPredicate} is fully bound, the {@link IAccessPath} will be
 * restricted to a single index partition, but we still need to know which index
 * partition.
 * <p>
 * The {@link IBindingSet} is written on an {@link UnsynchronizedArrayBuffer}
 * corresponding to the target index partition. The
 * {@link UnsynchronizedArrayBuffer} (together with the output {@link IBuffer}
 * for the {@link IBindingSet} chunks and the {@link Future} for the
 * {@link JoinTask} for that index partition) for the target index partition
 * exists in an LRU. If it falls off of the end of the LRU, then the
 * {@link UnsynchronizedArrayBuffer} is flushed and the output {@link IBuffer}
 * is closed. The downstream {@link JoinTask} will eventually exhaust the
 * corresponding {@link IAsynchronousIterator} source.
 * <p>
 * When the source join dimension and the sink join dimension have the same
 * {@link IKeyOrder} there will be an orderly progression through the indices
 * and each sink {@link JoinTask} can be safely closed once a {@link JoinTask}
 * is created on the {@link DataService} for the next index partition. However,
 * the {@link IKeyOrder}s offer differ, which can lead to more scattered
 * assignment of output {@link IBindingSet}s to index partitions. The LRU helps
 * to manage this fan out.
 * <p>
 * Fan out means that there may be N>1 {@link JoinTask}s for each join
 * dimension. For this reason, a QUERY SLICE must be applied by the client
 * reading on the {@link IAsynchronousIterator} returned by the
 * {@link JoinMasterTask}.
 * <p>
 * Fan out also implies a requirement for fan-in in order to reduce the scatter
 * of {@link JoinTask}s. Fan-in must aggregate the source {@link JoinTask} such
 * that they target the same sink {@link JoinTask} instance for the same rule
 * execution instance, the same orderIndex (hence the same {@link IPredicate}),
 * and the same index partition. This means that a factory mechanism must be
 * used to either create a new {@link JoinTask} or return the existing
 * {@link JoinTask} on the {@link DataService} based on those identifying
 * properties. This must be done in a thread-safe manner, but contention should
 * be restricted to the case where the identifying properties are the same. The
 * factory must be given the {@link IAsynchronousIterator} reading
 * {@link IBindingSet} chunks from the source join dimension and the
 * {@link JoinTask} must not close (unless interrupted or cancelled) until all
 * of its source {@link IAsynchronousIterator}s have been exhausted.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo fold these comments into the javadoc.
 *       <p>
 *       The goal is to have no more than one {@link JoinTask} per index
 *       partition per rule execution. If the #of index partitions is very large
 *       then we may have to use an LRU cache in an attempt to release
 *       {@link JoinTask}s that are not being written on by a given source
 *       {@link JoinTask}.
 *       <p>
 *       There is a strong requirement for closure to get back the
 *       mutationCount. That would require us to keep alive a source
 *       {@link JoinTask} until all downstream {@link JoinTask}s complete.
 *       
 * @todo Slice should be enforced by the solution buffer for query. This change
 *       could be made to the {@link NestedSubqueryWithJoinThreadsTask}.
 * 
 * @todo We are not seeing the totals when a SLICE is used. I believe that the
 *       test harness is simply exiting once it gets its N results and the
 *       daemon threads for the workers are not keeping the JVM alive. Ideally
 *       either the JoinMasterTask the last JoinTask would notice that the
 *       solution buffer was closed and would use that information to halt the
 *       ongoing {@link JoinTask}s.
 */
abstract public class JoinMasterTask implements IStepTask, IJoinMaster {

    protected static final Logger log = Logger.getLogger(JoinMasterTask.class);

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

    protected final IJoinNexusFactory joinNexusFactory;
    
    /**
     * From the ctor. This will be the {@link IBuffer} on which the last join
     * dimension writes the computed {@link ISolution}s.
     * <p>
     * Note: {@link LocalJoinMasterTask} always passes this along to the last
     * {@link LocalJoinTask}.
     * <p>
     * Note: For a {@link DistributedJoinMasterTask} running a Query this gets
     * proxied and the {@link DistributedJoinTask}s all write on the proxy.
     * However, the {@link DistributedJoinMasterTask} DOES NOT proxy this for
     * mutation in order to keep all data from flowing through the master.
     */
    protected final IBuffer<ISolution[]> solutionBuffer;

    protected final int tailCount;

    protected final IRuleState ruleState;

    /**
     * The evaluation order.
     */
    protected final int[] order;
    
    protected final RuleStats ruleStats;

    /**
     * Statistics on {@link JoinTask} behavior for each {@link IPredicate} in
     * the tail of the rule. These statistics are reported by each
     * {@link JoinTask} and then aggregated for each join dimension.
     * <p>
     * Note: The index into this array is the evaluation order of the predicate.
     */
    protected final JoinStats[] joinStats;

    /**
     * The unique identifier for this {@link JoinMasterTask} instance.
     */
    protected final UUID uuid;
    
    /**
     * 
     * @param rule
     *            The rule to be executed.
     * @param joinNexus
     *            The {@link IJoinNexus}.
     * @param solutionBuffer
     *            The {@link ISolution} buffer.
     */
    protected JoinMasterTask(final IRule rule, final IJoinNexus joinNexus,
            final IBuffer<ISolution[]> solutionBuffer) {

        if (rule == null)
            throw new IllegalArgumentException();

        if (joinNexus == null)
            throw new IllegalArgumentException();

        this.rule = rule;

        this.joinNexus = joinNexus;

        this.joinNexusFactory = joinNexus.getJoinNexusFactory();

        this.tailCount = rule.getTailCount();

        this.uuid = UUID.randomUUID();

        // computes the eval order.
        this.ruleState = new RuleState(rule, joinNexus);

        // the evaluation order.
        this.order = ruleState.getPlan().getOrder();

        // note: evaluation order is fixed by now.
        this.ruleStats = joinNexus.getRuleStatisticsFactory().newInstance(
                ruleState);

        {

            this.joinStats = new JoinStats[tailCount];

            for (int orderIndex = 0; orderIndex < tailCount; orderIndex++) {

                this.joinStats[orderIndex] = new JoinStats(orderIndex);

            }

        }
        
        this.solutionBuffer = solutionBuffer;
        
    }
    
    final public UUID getUUID() {        
        
        return uuid;
        
    }
    
    /**
     * Evaluate the rule.
     */
    public RuleStats call() throws Exception {

        if (ruleState.getPlan().isEmpty()) {

            if (INFO)
                log.info("Rule proven to have no solutions.");

            return ruleStats;

        }

        final long begin = System.currentTimeMillis();

        final List<Future<Void>> futures = start();
        
        try {

            awaitAll(futures, Long.MAX_VALUE, TimeUnit.SECONDS);
            
        } catch(InterruptedException ex) {
            
            /*
             * The master itself was interrupted.
             * 
             * Note: The most common reason for this exception is a SLICE. When
             * the query consumer decides that it has satisified the SLICE it
             * will close the iterator consuming the query and that will cause
             * the the query buffer to be closed and the task (this
             * JoinMasterTask) that is writing on that query buffer to be
             * interrupted.
             * 
             * Note: This can also happen if you shutdown the service on which
             * the master is running or deliberately interrupt the master.
             */

            if(INFO)
                log.info("Interrupted");
            
            /*
             * Fall through!
             * 
             * Note: We fall through so that the rule evaluation appears to
             * complete normally for the common case where a SLICE causes the
             * master to be interrupted. For this case the query buffer will
             * already contain at least those solutions that satisified the
             * slice and we need do nothing more.
             * 
             * Note: The JoinStats information may be incomplete as one or more
             * JoinTask(s) may still be running.
             */
            
            if (INFO) {
                
                /*
                 * Give the join tasks a chance to complete so that the join
                 * stats will get reported to the master so that the master can
                 * report out the correct stats to its caller.
                 * 
                 * Note: This is completely optional. You DO NOT need to wait
                 * here. Whether or not you wait here depends mainly on whether
                 * the potential additional latency or the potential of having
                 * the join stats on hand is more important for you.
                 */

                try {

                    awaitAll(futures, 1L, TimeUnit.SECONDS);

                } catch (Throwable t) {

                    // ignore.

                }
                
            }
            
        } catch(ExecutionExceptions ex) {
            
            // something unexpected
            log.error(ex, ex);
            
            throw new RuntimeException(ex);
            
        }

        ruleStats.elapsed += System.currentTimeMillis() - begin;

        /*
         * Aggregate statistics from each join dimension and log anything that
         * is interesting.
         */ 
        
        combineJoinStats();

        if (DEBUG)
            log.debug("Done");
        
        return ruleStats;

    }

    /**
     * Start one or more {@link JoinTask}s for the rule.
     * 
     * @return The {@link Future}s for those {@link JoinTask}s.
     */
    abstract List<Future<Void>> start() throws Exception;

    /**
     * Make sure that each {@link JoinTask} completed successfully.
     * <p>
     * Note: This waits until all {@link JoinTask}s complete, regardless of
     * their outcome (or until the timeout expires), so that all
     * {@link JoinTask} have the opportunity to report their {@link JoinStats}
     * to the {@link JoinMasterTask}.
     * 
     * @param futures
     *            The {@link Future} for each {@link JoinTask} that was created
     *            by the {@link JoinMasterTask}.
     * @param timeout
     *            The timeout for awaiting those futures.
     * @param unit
     *            The unit for that timeout.
     * 
     * @throws ExecutionExceptions
     *             if one or more {@link JoinTask}s fail.
     * @throws InterruptedException
     *             if the {@link JoinMasterTask} itself was interrupted while
     *             awaiting its {@link JoinTask}s.
     * @throws TimeoutException
     *             if the timeout expires first.
     */
    protected void awaitAll(final List<Future<Void>> futures,
            final long timeout, final TimeUnit unit) throws ExecutionExceptions,
            InterruptedException, TimeoutException {

        final long begin = System.nanoTime();
        
        long remaining = unit.toNanos(timeout);
        
        // errors.
        final List<ExecutionException> errors = new LinkedList<ExecutionException>();

        for (Future<Void> f : futures) {

            if (remaining < 0L) {

                int ncancelled = 0;

                for (Future<Void> x : futures) {

                    if (x.cancel(true/* mayInterruptIfRunning */)) {

                        ncancelled++;

                    }
                    
                }
                
                log.warn("Cancelled " + ncancelled + " futures due to timeout");
                
                throw new TimeoutException();
                
            }
            
            try {

                f.get(remaining, TimeUnit.NANOSECONDS);
                
            } catch (CancellationException ex) {

                /*
                 * A JoinTask will be cancelled if any of its output buffers
                 * are asynchronously closed. This will occur if a
                 * downstream JoinTask discovers that it has satisifed a
                 * SLICE or encountered an error during processing. Either
                 * way, we treat the CancellationException as a "info" NOT
                 * an error.
                 */

                if (INFO)
                    log.info(ex.getLocalizedMessage(), ex);
                
            } catch (ExecutionException ex) {

                if (InnerCause.isInnerCause(ex, InterruptedException.class)||
                    InnerCause.isInnerCause(ex, ClosedByInterruptException.class)||
                    InnerCause.isInnerCause(ex, BufferClosedException.class)) {

                    /*
                     * The root cause was the asynchronous close of the
                     * buffer that is the overflow() target for the
                     * unsynchronized buffer. This will occur if the
                     * high-level iterator was closed() while join thread(s)
                     * are still executing.
                     * 
                     * Note: InterruptedException will be thrown during
                     * query if the BlockingBuffer on which the query
                     * solutions are being written is closed, e.g., because
                     * someone closed a high-level iterator reading
                     * solutions from the BlockingBuffer. Closing the
                     * BlockingBuffer causes the Future that is writing on
                     * the BlockingBuffer to be interrupted in order to
                     * eagerly terminate processing.
                     * 
                     * Note: ClosedByInterruptException will be the cause if
                     * the interrupt was noticed during an IO by the thread
                     * in which this exception was thrown.
                     * 
                     * Note: AsynchronousCloseException will be the cause if
                     * the interrupt was noticed during an IO by a different
                     * thread resulting in the asynchronous close of the
                     * backing channel. However, the
                     * AsynchronousCloseException is trapped by
                     * DiskOnlyStrategy and results in the transparent
                     * re-opening of the backing channel. Since the target
                     * buffer will be closed, the AsynchronousCloseException
                     * should be swiftly followed by an BlockingBuffer#add()
                     * throwing an IllegalStateException if there is an
                     * attempt to write on a closed buffer.
                     * 
                     * Note: Using Thread#interrupt() to halt asynchronous
                     * processing for query is NOT ideal as it will
                     * typically force the FileChannel to be closed
                     * asynchronously. You are better off using a SLICE.
                     * However, when the query has a FILTER as well as a
                     * SLICE and the filter can not be evaluated inside of
                     * the the JOINs then the caller must pull solutions
                     * through the filter and close the iterator once the
                     * slice is satisified. That will trigger an interrupt
                     * of join thread(s) unless join processing is already
                     * complete.
                     */

                    if (INFO)
                        log.info(ex.getLocalizedMessage(), ex);

                } else {

                    /*
                     * Something unexpected.
                     * 
                     * Note: We add the orderIndex to the stack trace so
                     * that we can figure out which JoinTask failed.
                     */

                    errors.add(new ExecutionException(ex));

                    log.error(ex);
                    
                }

            }

            // subtract out the elapsed time so far.
            remaining -= (System.nanoTime() - begin);
            
        }

        if (!errors.isEmpty()) {

            /*
             * Throw exception containing all failures.
             */
            
            throw new ExecutionExceptions(errors);
            
        }
        
    }
    
    /**
     * Return an {@link IAsynchronousIterator} that will read a single
     * {@link IBindingSet}.
     * 
     * @param bindingSet
     *            the binding set.
     */
    protected ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
            final IBindingSet bindingSet) {

        return new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { new IBindingSet[] { bindingSet } });
        
    }
    
    /**
     * Aggregates statistics each {@link JoinTask} onto {@link #ruleStats}.
     * There are N {@link JoinTask}s per {@link IPredicate} in the tail of the
     * rule, where N is the #of index partitions on which we must read to
     * evaluate the {@link IRule} for a given {@link IPredicate} in the tail (N
     * is per {@link IPredicate}, not the same for each {@link IPredicate}).
     */
    protected void combineJoinStats() {
        
        /*
         * Get the #of solutions produced.
         */
        final long solutionCount;
        if (!joinNexus.getAction().isMutation()) {

            /*
             * For query all solutions flow through the master so we get to see
             * the solution count.
             */

            solutionCount = ((BlockingBuffer) solutionBuffer).getElementCount();

        } else {

            /*
             * The #of binding sets output from the last join dimension is
             * another way to get the solution count.
             * 
             * Note: This should work regardless of whether we are evaluating a
             * rule for mutation or query.
             */

            solutionCount = joinStats[order[tailCount - 1]].bindingSetsOut;

        }
        ruleStats.solutionCount.addAndGet(solutionCount);
        
        /*
         * The mutation count is taken from the last join dimension.
         */
        ruleStats.mutationCount
                .addAndGet(joinStats[order[tailCount - 1]].mutationCount.get());
        
        final int[] order = ruleState.getPlan().getOrder();
        
        for (int tailIndex = 0; tailIndex < tailCount; tailIndex++) {

            final JoinStats o = joinStats[order[tailIndex]];
            
            ruleStats.chunkCount[tailIndex] += o.chunkCount;
            
            ruleStats.elementCount[tailIndex] += o.elementCount;
            
        }
    
        if(INFO) {
            
            log.info("\n" + ruleState);

            log.info("\n" + ruleStats);
            
            /*
             * Note: This provides more detail on this join algorithm than the
             * RuleStats view, however the per-predicate pre-index partition
             * details are not available since these data aggregate across all
             * index partitions for a given tail predicate.
             */
            
            log.info("\n"+JoinStats.toString(rule, order, joinStats));
        
        }
        
    }
    
    /**
     * Aggregates the statistics for some join dimension.
     * 
     * @param joinStats
     *            Statistics for an index partition of some join dimension.
     */
    public void report(final JoinStats joinStats) {

        if (DEBUG) {

            log.debug("\n"+joinStats.toString());
            
        }
        
        final JoinStats total = this.joinStats[joinStats.orderIndex];

        total.add(joinStats);

    }

    /**
     * Returns the buffer specified to the ctor (overridden for distributed
     * joins).
     */
    public IBuffer<ISolution[]> getSolutionBuffer() throws IOException {

        return solutionBuffer;
        
    }
    
}
