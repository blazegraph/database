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

package com.bigdata.relation.rule.eval;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.UnisolatedReadWriteIndex;
import com.bigdata.concurrent.NamedLock;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IIndexStore;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.IResourceLockService;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.accesspath.AbstractAccessPath;
import com.bigdata.relation.accesspath.AbstractUnsynchronizedArrayBuffer;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.BufferClosedException;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;
import com.bigdata.relation.accesspath.UnsynchronizedArrayBuffer;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.ISolutionExpander;
import com.bigdata.relation.rule.eval.JoinMasterTask.JoinTask.AccessPathTask;
import com.bigdata.resources.IndexManager;
import com.bigdata.resources.StoreManager.ManagedJournal;
import com.bigdata.service.AbstractDistributedFederation;
import com.bigdata.service.AbstractScaleOutFederation;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IClientIndex;
import com.bigdata.service.IDataService;
import com.bigdata.service.IDataServiceAwareProcedure;
import com.bigdata.service.LocalDataServiceFederation;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.DaemonThreadFactory;
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

        final List<Future<?extends Object>> futures = start();
        
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
    abstract List<Future<? extends Object>> start() throws Exception;

    /**
     * Make sure that each {@link JoinTask} completed successfully.
     * <p>
     * Note: This waits until all {@link JoinTask}s complete, regardless of
     * their outcome, so that all {@link JoinTask} have the opportunity to
     * report their {@link JoinStats} to the {@link JoinMasterTask}.
     * 
     * @param futures
     *            The {@link Future} for each {@link JoinTask}.
     * @param timeout
     * @param unit
     * 
     * @throws ExecutionExceptions
     *             if one or more {@link JoinTask}s fail.
     * @throws InterruptedException
     *             if the {@link JoinMasterTask} itself was interrupted while
     *             awaiting its {@link JoinTask}s.
     * @throws TimeoutException
     *             if the timeout expires first.
     */
    protected void awaitAll(final List<Future<? extends Object>> futures,
            final long timeout, final TimeUnit unit) throws ExecutionExceptions,
            InterruptedException, TimeoutException {

        final long begin = System.nanoTime();
        
        long remaining = unit.toNanos(timeout);
        
        int orderIndex = 0;

        // errors.
        final List<ExecutionException> errors = new LinkedList<ExecutionException>();

        for (Future<? extends Object> f : futures) {

            if (remaining < 0L) {

                int ncancelled = 0;

                for (Future x : futures) {

                    if (x.cancel(true/* mayInterruptIfRunning */)) {

                        ncancelled++;

                    }
                    
                }
                
                log.warn("Cancelled "+ncancelled+" futures due to timeout");
                
                throw new TimeoutException();
                
            }
            
            try {

                f.get(remaining,TimeUnit.NANOSECONDS);
                
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
                    log.info("orderIndex=" + orderIndex, ex);
                
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
                        log.info("orderIndex=" + orderIndex, ex);

                } else {

                    /*
                     * Something unexpected.
                     * 
                     * Note: We add the orderIndex to the stack trace so
                     * that we can figure out which JoinTask failed.
                     */

                    errors.add(new ExecutionException("orderIndex="
                            + orderIndex, ex));

                    log.error("orderIndex=" + orderIndex, ex);
                    
                }

            }

            orderIndex++;

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
    
    /**
     * Implementation for local join execution on a {@link Journal}.
     * <p>
     * Note: Just like a nested subquery join, when used for mutation this must
     * read and write on the {@link ITx#UNISOLATED} indices and an
     * {@link UnisolatedReadWriteIndex} will be used to serialize exclusive
     * access to the unisolated index for writers while allowing readers
     * concurrent access.
     * 
     * FIXME Write an implementation for local join execution on a
     * {@link DataService}. It will be similar to this implementation, but
     * since it is already running in the {@link ConcurrencyManager}, it must
     * run its own join tasks on a normal thread pool. Therefore
     * {@link JoinTask} should NOT extend {@link AbstractTask} but rather be a
     * {@link Callable} that can be run on the {@link ConcurrencyManager} using
     * a delegation pattern.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class LocalJoinMasterTask extends JoinMasterTask {

        /**
         * @param rule
         * @param joinNexus
         * @param buffer
         */
        protected LocalJoinMasterTask(IRule rule, IJoinNexus joinNexus,
                IBuffer<ISolution[]> buffer) {

            super(rule, joinNexus, buffer);

            if ((joinNexus.getIndexManager() instanceof IBigdataFederation)
                    && (((IBigdataFederation) joinNexus.getIndexManager())
                            .isScaleOut())) {
                
                /*
                 * This implementation can not be used with a scale-out
                 * federation.
                 */
                
                throw new UnsupportedOperationException();
                
            }
            
        }

        /**
         * Applies an initial {@link IBindingSet} to the first join dimension.
         * Intermediate {@link IBindingSet}s will propagate to each join
         * dimension. The final {@link IBindingSet}s will be generated by the
         * last join dimension and written on the {@link #getSolutionBuffer()}.
         * 
         * @return The {@link Future} for the {@link LocalJoinTask} for each
         *         join dimension.
         */
        @Override
        protected List<Future<? extends Object>> start() throws Exception {

            // source for each join dimension.
            final IAsynchronousIterator<IBindingSet[]>[] sources = new IAsynchronousIterator[tailCount];

            // source for the 1st join dimension.
            sources[0] = newBindingSetIterator(joinNexus.newBindingSet(rule));

            // Future for each JoinTask.
            final List<Future<? extends Object>> futures = new ArrayList<Future<? extends Object>>(tailCount); 
            
//            // FIXME hardwired for the Journal (vs LDS).
//            final ConcurrencyManager concurrencyManager = ((Journal) joinNexus
//                    .getIndexManager()).getConcurrencyManager();

            // The JoinTasks will be run on this service.
            final ExecutorService executorService = joinNexus.getIndexManager().getExecutorService();
            
            // the previous JoinTask and null iff this is the first join dimension.
            LocalJoinTask priorJoinTask = null;

            // for each predicate in the evaluate order.
            for (int orderIndex = 0; orderIndex < tailCount; orderIndex++) {

                // true iff this is the last JOIN in the evaluation order.
                final boolean lastJoin = orderIndex + 1 == tailCount;
                
//                // name of the index on which this task will read.
//                final String indexName = joinNexus.getTailAccessPath(
//                        rule.getTail(orderIndex)).getIndex().getIndexMetadata()
//                        .getName();
                
                final IPredicate predicate = rule.getTail(orderIndex);
                
                // the index on which this predicate must read.
                final String indexName = predicate.getOnlyRelationName()
                        + ruleState.getKeyOrder()[order[orderIndex]];

                // source for this join dimension.
                final IAsynchronousIterator<IBindingSet[]> src = sources[orderIndex];
                
                assert src != null : "No source: orderIndex=" + orderIndex
                        + ", tailCount=" + tailCount + ", rule=" + rule;
                
                // create the local join task.
                final LocalJoinTask joinTask = new LocalJoinTask(
                        indexName, rule, joinNexus,
                        order, orderIndex, this/* master */, src, getSolutionBuffer());

                if (!lastJoin) {

                    // source for the next join dimension.
                    sources[orderIndex + 1] = joinTask.syncBuffer.iterator();

                }

                /*
                 * Submit the JoinTask.
                 * 
                 * When the JoinTask for the 1st join dimension executes it will
                 * consume the [initialBindingSet]. That bindingSet will be used to
                 * obtain the first access path and merged with the elements drawn
                 * from that access path. Intermediate bindingSets will be
                 * propagated to the JoinTask for the next predicate in the
                 * evaluation order.
                 */

                // Submit the JoinTask for execution. @todo Future<Void>
                final Future<? extends Object> future = executorService
                        .submit(joinTask);

                // Save reference to the Future.
                futures.add( future );
                
                // Set the Future on the BlockingBuffer.
                if (!lastJoin) {

                    joinTask.syncBuffer.setFuture(future);
                    
                }

                // Set the Future on the JoinTask for the previous join dimension.
                if (priorJoinTask != null) {
                    
                    priorJoinTask.sinkFuture = future;
                    
                }
                
                priorJoinTask = joinTask;
                
            }            

            return futures;
            
        }
        
    }
    
    /**
     * Implementation for distributed join execution.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class DistributedJoinMasterTask extends JoinMasterTask {

        private final IBuffer<ISolution[]> solutionBufferProxy;
        private final IJoinMaster masterProxy;
        
        /**
         * @param rule
         * @param joinNexus
         * @param buffer
         *            The buffer on which the last {@link DistributedJoinTask}
         *            will write query {@link ISolution}s. However, it is
         *            ignored for mutation operations as each
         *            {@link DistributedJoinTask} for the last join dimension
         *            (there can be more than one if the index partition has
         *            more than one partition) will obtain and write on its own
         *            solution buffer in order to avoid moving all data through
         *            the master.
         * 
         * @throws UnsupportedOperationException
         *             unless {@link IJoinNexus#getIndexManager()} reports an
         *             {@link AbstractScaleOutFederation}.
         */
        protected DistributedJoinMasterTask(IRule rule, IJoinNexus joinNexus,
                IBuffer<ISolution[]> buffer) {

            super(rule, joinNexus, buffer);
         
            if (!(joinNexus.getIndexManager() instanceof IBigdataFederation)
                    || !(((IBigdataFederation) joinNexus.getIndexManager())
                            .isScaleOut())) {
                
                /*
                 * Either not running in a scale-out deployment or executed in a
                 * context (such as within the ConcurrencyManager) where the
                 * joinNexus will not report the federation as the index manager
                 * object.
                 */
                
                throw new UnsupportedOperationException();
                
            }

            if(joinNexus.getAction().isMutation()) {
                
                /*
                 * Check constraints on executing mutation operations.
                 * 
                 * Note: These constraints arise from (a) the need to flush
                 * solutions onto relations that may also be in the body of the
                 * fule; and (b) the need to avoid stale locators when writing
                 * on those relations.
                 */
                
                if(!TimestampUtility.isHistoricalRead(joinNexus.getReadTimestamp())) {
                    
                    /*
                     * Must use a read-consistent view and advance the
                     * readTimestamp before each mutation operation.
                     */

                    throw new UnsupportedOperationException();
                    
                }
                
            } else {
                
                if (joinNexus.getReadTimestamp() == ITx.UNISOLATED) {
                    
                    /*
                     * Note: While you probably can run a query against the
                     * unisolated indices it will prevent overflow processing
                     * since there will exclusive locks and is a bad idea.
                     */

                    log.warn("Unisolated scale-out query");

                }
                
            }
            
            // @todo JDS export proxy for the master.
            masterProxy = this;

            if (joinNexus.getAction().isMutation()) {
                
                // mutation - no proxy.
                solutionBufferProxy = null;
                
            } else {
                
                // query - @todo JDS export proxy for the solution buffer.
                solutionBufferProxy = solutionBuffer;
                
            }
            
        }
        
        @Override
        public IBuffer<ISolution[]> getSolutionBuffer() throws IOException {
            
            if (joinNexus.getAction().isMutation()) {
                
                /*
                 * Note: access is not permitted for mutation to keep data from
                 * distributed join tasks from flowing through the master.
                 */
                
                throw new UnsupportedOperationException();
                
            }

            return solutionBufferProxy;
            
        }
        
        /**
         * Create and run the {@link JoinTask}(s) that will evaluate the first
         * join dimension.
         * <p>
         * A {@link JoinTask} is created on the {@link DataService} for each
         * index partition that is spanned by the {@link IAccessPath} for the
         * first {@link IPredicate} in the evaluation order. Those
         * {@link JoinTask} are run in parallel, so the actual parallelism for
         * the first {@link IPredicate} is the #of index partitions spanned by
         * its {@link IAccessPath}.
         * 
         * @return The {@link Future} for each {@link DistributedJoinTask}
         *         created for the first join dimension (one per index
         *         partitions spanned by the predicate that is first in the
         *         evaluation order given the initial bindingSet for the rule).
         */
        @Override
        final protected List<Future<? extends Object>> start() throws Exception {

            /*
             * The initial bindingSet.
             * 
             * Note: This bindingSet might not be empty since constants can be
             * bound before the rule is evaluated.
             */
            final IBindingSet initialBindingSet = joinNexus.newBindingSet(rule);

            final List<Future> factoryTaskFutures = mapBindingSet(initialBindingSet);

            // await futures for the factory tasks.
            final List<Future<? extends Object>> joinTaskFutures = awaitFactoryFutures(factoryTaskFutures);

            return joinTaskFutures;

        }

        /**
         * Map the given {@link IBindingSet} over the {@link JoinTask}(s) for
         * the index partition(s) the span the {@link IAccessPath} for that
         * {@link IBindingSet}.
         * 
         * @param bindingSet
         *            The binding set.
         * 
         * @return A list of {@link Future}s for the
         *         {@link JoinTaskFactoryTask} that will create the
         *         {@link DistributedJoinTask}s for the first join dimension.
         * 
         * @throws Exception
         * 
         * FIXME If a predicate defines an {@link ISolutionExpander} then we DO
         * NOT map the predicate. Instead, we use
         * {@link IJoinNexus#getTailAccessPath(IPredicate)} and evaluate the
         * {@link IAccessPath} with the layered {@link ISolutionExpander} in
         * process. If the {@link ISolutionExpander} touches the index, it will
         * be using an {@link IClientIndex}. While the {@link IClientIndex} is
         * not nearly as efficient as using a local index partition, it will
         * provide a view of the total key-range partitioned index.
         * <p>
         * do this for each join dimension for which an
         * {@link ISolutionExpander} is defined, including not only the first N
         * join dimensions (handles free text search) but also an intermediate
         * join dimension (requires that all source join tasks target a join
         * task having a view of the scale-out index rather than mapping the
         * task across the index partitions).
         */
        protected List<Future> mapBindingSet(final IBindingSet bindingSet)
                throws Exception {

            /*
             * The first predicate in the evaluation order with the initial
             * bindings applied.
             */
            final IPredicate predicate = rule.getTail(order[0]).asBound(
                    bindingSet);

            // scale-out index manager.
            final AbstractScaleOutFederation fed = (AbstractScaleOutFederation) joinNexus
                    .getIndexManager();

            // the scale out index on which this predicate must read.
            final String scaleOutIndexName = predicate.getOnlyRelationName()
                    + ruleState.getKeyOrder()[order[0]];

            final Iterator<PartitionLocator> itr = joinNexus.locatorScan(fed,
                    predicate);

            final List<Future> futures = new LinkedList<Future>();

            while (itr.hasNext()) {

                final PartitionLocator locator = itr.next();

                final int partitionId = locator.getPartitionId();

                if (DEBUG)
                    log.debug("Will submit JoinTask: partitionId="
                            + partitionId);

                /*
                 * Note: Since there is only a single binding set, we send a
                 * serializable thick iterator to the client.
                 */
                final ThickAsynchronousIterator<IBindingSet[]> sourceItr = newBindingSetIterator(bindingSet);

                final JoinTaskFactoryTask factoryTask = new JoinTaskFactoryTask(
                        scaleOutIndexName, rule, joinNexusFactory, order,
                        0/* orderIndex */, partitionId, masterProxy,
                        sourceItr, ruleState.getKeyOrder());

                final IDataService dataService = fed.getDataService(locator
                        .getDataServices()[0]);

                /*
                 * Submit the JoinTask. It will begin to execute when it is
                 * scheduled by the ConcurrencyManager. When it executes it will
                 * consume the [initialBindingSet]. We wait on its future to
                 * complete below.
                 */
                final Future f;
                
                try {
                 
                    f = dataService.submit(factoryTask);
                    
                } catch (Exception ex) {
                    
                    throw new ExecutionException("Could not submit: task="
                            + factoryTask, ex);
                    
                }

                /*
                 * Add to the list of futures that we need to await.
                 */
                futures.add(f);

            }
            
            return futures;

        }

        /**
         * Await the {@link JoinTaskFactoryTask} {@link Future}s.
         * <p>
         * Note: the result for a {@link JoinTaskFactoryTask} {@link Future} is
         * a {@link DistributedJoinTask} {@link Future}.
         * 
         * @param factoryTaskFutures
         *            A list of {@link Future}s, with one {@link Future} for
         *            each index partition that is spanned by the
         *            {@link IAccessPath} for the first {@link IPredicate} in
         *            the evaluation order.
         * 
         * @return A list of {@link DistributedJoinTask} {@link Future}s. There
         *         will be one element in the list for each
         *         {@link JoinTaskFactoryTask} {@link Future} in the caller's
         *         list. The elements will be in the same order.
         * 
         * @throws InterruptedException
         *             if the master itself was interrupted.
         * @throws ExecutionExceptions
         *             if any of the factory tasks fail.
         */
        protected List<Future<? extends Object>> awaitFactoryFutures(
                final List<Future> factoryTaskFutures) throws InterruptedException,
                ExecutionExceptions {

            final int size = factoryTaskFutures.size();

            if (DEBUG)
                log.debug("#futures=" + size);
            
            int ndone = 0;

            /*
             * A list containing any join tasks that were successfully created.
             * Since we process the factory task futures in order the list will
             * be in the same order as the factory task futures.
             */
            final List<Future<? extends Object>> joinTaskFutures = new ArrayList<Future<? extends Object>>(
                    size);
            
            final Iterator<Future> itr = factoryTaskFutures.iterator();

            /*
             * Initially empty. Populated with an errors encountered when trying
             * to execute the _factory_ tasks.
             */
            final List<ExecutionException> causes = new LinkedList<ExecutionException>();
            
            /*
             * Process all factory tasks.
             * 
             * Note: if an error occurs for any factory task, then we cancel the
             * remaining factory tasks and also cancel any join task that was
             * already started.
             */
            while (itr.hasNext()) {

                /*
                 * Note: The Future of the JoinFactoryTask returns the Future of
                 * the JoinTask.
                 */

                // future for the JoinTaskFactoryTask.
                final Future factoryTaskFuture = itr.next();

                if (DEBUG)
                    log.debug("Waiting for factoryTask");

                // wait for the JoinTaskFactoryTask to finish.
                final Future joinTaskFuture;
                
                try {

                    if(!causes.isEmpty()) {
                        
                        /*
                         * We have to abort, so cancel the factory task in case
                         * it is still running but fall through and try to get
                         * its future in case it has already created the join
                         * task.
                         */

                        factoryTaskFuture
                                .cancel(true/* mayInterruptIfRunning */);
                        
                    }
                    
                    joinTaskFuture = (Future) factoryTaskFuture.get();
                    
                } catch (ExecutionException ex) {
                    
                    causes.add(ex);
                    
                    /*
                     * Note: This is here because the ExecutionExceptions that
                     * we throw does not print out all of its stack traces.
                     * 
                     * @todo log iff unexpected exception class or get all
                     * traces from ExecutionExceptions class.
                     */
                    log.error(ex, ex);
                    
                    continue;
                
                }

                if(causes.isEmpty()) {

                    // no errors yet, so remeber the future for the join task.
                    joinTaskFutures.add(joinTaskFuture);
                    
                } else {

                    // cancel the join task since we have to abort anyway.
                    joinTaskFuture.cancel(true/* mayInterruptIfRunning */);
                    
                }
                
                ndone++;
                
                if (DEBUG)
                    log.debug("ndone=" + ndone + " of " + size);
                
            }

            if(!causes.isEmpty()) {
                
                for(Future f : joinTaskFutures) {
                    
                    // cancel since we have to abort anyway.
                    f.cancel(true/* mayInterruptIfRunning */);

                }
                
                throw new ExecutionExceptions(causes);
                
            }
            
            if (DEBUG)
                log.debug("All factory tasks done: #futures=" + size);

            return joinTaskFutures;
            
        }

    }
    
    /**
     * Statistics about processing for a single join dimension as reported by a
     * single {@link JoinTask}. Each {@link JoinTask} handles a single index
     * partition, so the {@link JoinStats} for those index partitions need to be
     * aggregated by the {@link JoinMasterTask}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class JoinStats implements Serializable {

        /**
         * 
         */
        private static final long serialVersionUID = 9028650921831777131L;

        /**
         * The index partition for which these statistics were collected or -1
         * if the statistics are aggregated across index partitions.
         */
        public final int partitionId;

        /**
         * The index in the evaluation order whose statistics are reported here.
         */
        public final int orderIndex;

        /** #of join tasks writing on this join task. */
        public int fanIn;

        /**
         * #of join tasks written on by this join task (zero if last in eval
         * order).
         */
        public int fanOut;

        /**
         * The #of binding set chunks read from all source {@link JoinTask}s.
         */
        public long bindingSetChunksIn;

        /** The #of binding sets read from all source {@link JoinTask}s. */
        public long bindingSetsIn;

        /**
         * The #of {@link IAccessPath}s read. This will differ from
         * {@link #bindingSetIn} iff the same {@link IBindingSet} is read from
         * more than one source and the {@link JoinTask} is able to recognize
         * the duplication and collapse it by removing the duplicate(s).
         */
        public long accessPathCount;
        
        /**
         * The #of duplicate {@link IAccessPath}s that were eliminated by a
         * {@link JoinTask}. Duplicate {@link IAccessPath}s arise when the
         * source {@link JoinTask}(s) generate the bindings on the
         * {@link IPredicate} for a join dimension. Duplicates are detected by a
         * {@link JoinTask} when it generates chunk of distinct
         * {@link AccessPathTask}s from a chunk of {@link IBindingSet}s read
         * from its source(s) {@link JoinTask}s.
         * <p>
         * Note: While the {@link IPredicate}s for those tasks may have the
         * same bindings, the source {@link IBindingSet}s typically (always?)
         * have variety not represented in the bound {@link IPredicate} and
         * therefore are combined under a single {@link AccessPathTask}. This
         * reduces redundent reads on an {@link IAccessPath} while producing
         * exactly the same output {@link IBindingSet}s that would have been
         * produced if we did not identify the duplicate {@link IAccessPath}s.
         */
        public long accessPathDups;

        /** #of chunks visited over all access paths. */
        public long chunkCount;

        /** #of elements visited over all chunks. */
        public long elementCount;

        /**
         * The #of {@link IBindingSet}s written onto the next join dimension
         * (aka the #of solutions written iff this is the last join dimension).
         * <p>
         * Note: An {@link IBindingSet} can be written onto more than one index
         * partition for the next join dimension, so one generated
         * {@link IBindingSet} MAY result in N GTE ONE "binding sets out". This
         * occurs when the {@link IAccessPath} required to read on the next
         * {@link IPredicate} in the evaluation order spans more than one index
         * partition.
         */
        public long bindingSetsOut;

        /**
         * The #of {@link IBindingSet} chunks written onto the next join
         * dimension (aka the #of solutions written iff this is the last join
         * dimension in the evaluation order).
         */
        public long bindingSetChunksOut;

        /**
         * The mutationCount is the #of solutions output by a {@link JoinTask}(s)
         * for the last join dimension of a mutation operation that were not
         * already present in the target relation. This value is always zero
         * (0L) for query.
         * <p>
         * Note: The mutationCount MUST be obtained from {@link IBuffer#flush()}
         * for the buffer on which the {@link JoinTask}(s) for the last join
         * dimension write their solutions. For mutation, this buffer is
         * obligated to report the #of elements whose state was changed in the
         * target relation. Failure to correctly obey this contract can result
         * in non-termination of fix point closure operations.
         * 
         * @see RuleStats#mutationCount
         */
        public AtomicLong mutationCount = new AtomicLong();
        
        /**
         * Ctor variant used by the {@link JoinMasterTask} to aggregate
         * statistics across the index partitions for a given join dimension.
         * 
         * @param orderIndex
         *            The index in the evaluation order.
         */
        public JoinStats(final int orderIndex) {

            this(-1, orderIndex);

        }

        /**
         * Ctor variant used by a {@link JoinTask} to self-report.
         * 
         * @param partitionId
         *            The index partition.
         * @param orderIndex
         *            The index in the evaluation order.
         */
        public JoinStats(final int partitionId, final int orderIndex) {

            this.partitionId = partitionId;

            this.orderIndex = orderIndex;

            fanIn = fanOut = 0;

            bindingSetChunksIn = bindingSetsIn = 0L;
            
            accessPathCount = accessPathDups = 0L;

            chunkCount = elementCount = bindingSetsOut = 0L;

            bindingSetChunksOut = 0L;

        }

        synchronized void add(JoinStats o) {

            if (this.orderIndex != o.orderIndex)
                throw new IllegalArgumentException();

            this.fanIn += o.fanIn;
            this.fanOut += o.fanOut;
            this.bindingSetChunksIn += o.bindingSetChunksIn;
            this.bindingSetsIn += o.bindingSetsIn;
            this.accessPathCount += o.accessPathCount;
            this.accessPathDups += o.accessPathDups;
            this.chunkCount += o.chunkCount;
            this.elementCount += o.elementCount;
            this.bindingSetsOut += o.bindingSetsOut;
            this.bindingSetChunksOut += o.bindingSetChunksOut;
            this.mutationCount.addAndGet(o.mutationCount.get());

        }

        public String toString() {
            
            final StringBuilder sb = new StringBuilder("JoinStats");
            
            sb.append("{ orderIndex="+orderIndex);
            
            sb.append(", partitionId="+partitionId);
            
            sb.append(", fanIn="+fanIn);
            
            sb.append(", fanOut="+fanOut);
            
            sb.append(", bindingSetChunksIn="+bindingSetChunksIn);
            
            sb.append(", bindingSetsIn="+bindingSetsIn);
            
            sb.append(", accessPathCount="+accessPathCount);
            
            sb.append(", accessPathDups="+accessPathDups);
            
            sb.append(", chunkCount="+chunkCount);
            
            sb.append(", elementCount="+elementCount);
            
            sb.append(", bindingSetsOut="+bindingSetsOut);

            sb.append(", bindingSetChunksOut="+bindingSetChunksOut);

            sb.append(", mutationCount="+mutationCount);
            
            sb.append("}");
            
            return sb.toString();
            
        }

        /**
         * Formats the array of {@link JoinStats} into a CSV table view.
         * 
         * @param rule
         *            The {@link IRule} whose {@link JoinStats} are being
         *            reported.
         * @param order
         *            The execution order for the {@link IPredicate}s in the
         *            tail of the <i>rule</i>.
         * @param a
         *            The {@link JoinStats}.
         * 
         * @return The table view.
         */
        public static StringBuilder toString(final IRule rule,
                final int[] order, final JoinStats[] a) {
            
            final StringBuilder sb = new StringBuilder();
            
            sb.append("rule, orderIndex, partitionId, fanIn, fanOut, bindingSetChunksIn, bindingSetsIn, accessPathCount, accessPathDups, chunkCount, elementCount, bindingSetsOut, bindingSetChunksOut, mutationCount, tailIndex, tailPredicate");
            
            sb.append("\n");
            
            int i = 0;
            for(JoinStats s : a) {

                final int tailIndex = order[i++];

                sb.append(rule.getName().replace(',', ' ')+", ");
                sb.append(Integer.toString(s.orderIndex)+", ");
                sb.append(Integer.toString(s.partitionId)+", ");
                sb.append(Integer.toString(s.fanIn)+", ");
                sb.append(Integer.toString(s.fanOut)+", ");
                sb.append(Long.toString(s.bindingSetChunksIn)+", ");
                sb.append(Long.toString(s.bindingSetsIn)+", ");
                sb.append(Long.toString(s.accessPathCount)+", ");
                sb.append(Long.toString(s.accessPathDups)+", ");
                sb.append(Long.toString(s.chunkCount)+", ");
                sb.append(Long.toString(s.elementCount)+", ");
                sb.append(Long.toString(s.bindingSetsOut)+", ");
                sb.append(Long.toString(s.bindingSetChunksOut)+", ");
                sb.append(Long.toString(s.mutationCount.get())+", ");
                sb.append(Integer.toString(tailIndex)+", ");
                sb.append(rule.getTail(tailIndex).toString().replace(",", "")+"\n");
                
            }
            
            return sb;
            
        }
        
    }

    /**
     * An object used by a {@link JoinTask} to write on another {@link JoinTask}
     * providing a sink for a specific index partition.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class JoinTaskSink {

        protected static final Logger log = Logger.getLogger(JoinTaskSink.class);

        /**
         * True iff the {@link #log} level is INFO or less.
         */
        protected static final boolean INFO = log.isInfoEnabled();

        /**
         * True iff the {@link #log} level is DEBUG or less.
         */
        protected static final boolean DEBUG = log.isDebugEnabled();

        /**
         * The future may be used to cancel or interrupt the downstream
         * {@link JoinTask}.
         */
        private Future future;
        
        /**
         * The {@link Future} of the downstream {@link JoinTask}. This may be
         * used to cancel or interrupt that {@link JoinTask}.
         */
        public Future getFuture() {
            
            if (future == null)
                throw new IllegalStateException();
            
            return future;
            
        }
        
        protected void setFuture(Future f) {
            
            if (future != null)
                throw new IllegalStateException();
            
            this.future = f;
            
            if(DEBUG)
                log.debug("sinkOrderIndex=" + sinkOrderIndex
                        + ", sinkPartitionId=" + locator.getPartitionId());
            
        }

        /**
         * The orderIndex for the sink {@link JoinTask}.
         */
        final int sinkOrderIndex;
        
        /**
         * The index partition that is served by the sink.
         */
        final PartitionLocator locator;

        /**
         * The individual {@link IBindingSet}s are written onto this
         * unsynchronized buffer. The buffer gathers those {@link IBindingSet}s
         * into chunks and writes those chunks onto the {@link #blockingBuffer}.
         */
        final UnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer;

        /**
         * This buffer provides {@link IBindingSet} chunks to the downstream
         * {@link JoinTask}. That join task reads those chunks from a proxy for
         * the {@link BlockingBuffer#iterator()}.
         */
        final BlockingBuffer<IBindingSet[]> blockingBuffer;

        public String toString() {
            
            return "JoinSinkTask{ sinkOrderIndex=" + sinkOrderIndex
                    + ", sinkPartitionId=" + locator.getPartitionId() + "}";
            
        }
        
        /**
         * Setups up the local buffers for a downstream {@link JoinTask}.
         * <p>
         * Note: The caller MUST create the task using a factory pattern on the
         * target data service and assign its future to the returned object
         * using {@link #setFuture(Future)}.
         * 
         * @param fed
         *            The federation.
         * @param locator
         *            The locator for the index partition.
         * @param sourceJoinTask
         *            The current join dimension.
         */
        public JoinTaskSink(final IBigdataFederation fed,
                final PartitionLocator locator, final JoinTask sourceJoinTask) {

            if (fed == null)
                throw new IllegalArgumentException();
            
            if (locator == null)
                throw new IllegalArgumentException();
            
            if (sourceJoinTask == null)
                throw new IllegalArgumentException();
            
            this.locator = locator;

            final IJoinNexus joinNexus = sourceJoinTask.joinNexus;

            this.sinkOrderIndex = sourceJoinTask.orderIndex + 1;
            
            /*
             * The sink JoinTask will read from the asynchronous iterator
             * drawing on the [blockingBuffer]. When we first create the sink
             * JoinTask, the [blockingBuffer] will be empty, but the JoinTask
             * will simply wait until there is something to be read from the
             * asynchronous iterator.
             */
            this.blockingBuffer = new BlockingBuffer<IBindingSet[]>(joinNexus
                    .getChunkOfChunksCapacity());

            /*
             * The JoinTask adds bindingSets to this buffer. On overflow, the
             * binding sets are added as a chunk to the [blockingBuffer]. Once
             * on the [blockingBuffer] they are available to be read by the sink
             * JoinTask.
             */
            this.unsyncBuffer = new UnsynchronizedArrayBuffer<IBindingSet>(
                    blockingBuffer, joinNexus.getChunkCapacity());

            /*
             * Note: The caller MUST create the task using a factory pattern on
             * the target data service and assign its future.
             */
            this.future = null;

        }

    }

    /**
     * Abstract base class that keeps track of the chunks of binding sets that
     * are generated by a {@link JoinTask}. This information is updated on the
     * {@link JoinStats}s for that {@link JoinTask}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    abstract static class UnsynchronizedOutputBuffer<E extends IBindingSet>
            extends AbstractUnsynchronizedArrayBuffer<E> {

        protected final JoinTask joinTask;

        protected UnsynchronizedOutputBuffer(final JoinTask joinTask,
                final int capacity) {

            super(capacity);

            if (joinTask == null)
                throw new IllegalArgumentException();

            this.joinTask = joinTask;

        }
        
    }
    
    /**
     * Implementation used to write on the {@link JoinTask#getSolutionBuffer()}
     * for the last join dimension. The solution buffer is either an
     * {@link IBlockingBuffer} (for query) or a buffer that writes on the head
     * relation for the rule (for mutation).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    static class UnsynchronizedSolutionBuffer<E extends IBindingSet> extends
            UnsynchronizedOutputBuffer<E> {

        private final IJoinNexus joinNexus;
        
        public UnsynchronizedSolutionBuffer(final JoinTask joinTask,
                final IJoinNexus joinNexus, final int capacity) {

            super(joinTask, capacity);

            this.joinNexus = joinNexus;
            
        }
        
        /**
         * Generate a chunk of {@link ISolution}s for the accepted
         * {@link IBindingSet}s and add those those {@link ISolution}s to the
         * {@link JoinTask#getSolutionBuffer()}. For query, that will be a
         * (proxy for) the {@link IJoinNexus#newQueryBuffer()} created by the
         * {@link JoinMasterTask}. For mutation, that will be a buffer created
         * for the {@link JoinTask} instance (this avoids have all data for
         * mutation flow through the master).
         * 
         * @throws BufferClosedException
         *             If the {@link IBuffer} returned by
         *             {@link JoinTask#getSolutionBuffer()} is an
         *             {@link IBlockingBuffer} which has been closed. This will
         *             occur for query if the query specifies a SLICE and the
         *             SLICE has been satisified. Under these conditions the
         *             {@link IBlockingBuffer} will be closed asynchronously by
         *             the query consumer and {@link BufferClosedException} will
         *             be thown by {@link IBlockingBuffer#add(Object)}.
         */
        protected void handleChunk(final E[] chunk) {

            final IBuffer<ISolution[]> solutionBuffer = joinTask
                    .getSolutionBuffer();

            final IRule rule = joinTask.rule;

            final int naccepted = chunk.length;
            
            final ISolution[] a = new ISolution[naccepted];
            
            for (int i = 0; i < naccepted; i++) {

                // an accepted binding set.
                final IBindingSet bindingSet = chunk[i];

                /*
                 * Note: The [joinNexus] MUST have access to the global and
                 * mutable index views. For the federation, this means that it
                 * is not the same instance that you are using to read on the
                 * access path!
                 */
                final ISolution solution = joinNexus.newSolution(rule,
                        bindingSet);

                a[i] = solution;
                
            }

            /*
             * Add the chunk to the [solutionBuffer].
             * 
             * Note: This can throw a BufferClosedException. In particular, this
             * exception will be thrown if the [solutionBuffer] is a query
             * buffer and a SLICE been satisified causing the [solutionBuffer]
             * to be asynchronously closed by the query consumer.
             */
            solutionBuffer.add(a);

            joinTask.stats.bindingSetChunksOut++;
            joinTask.stats.bindingSetsOut += naccepted;

        }

    }
    
    /**
     * Consumes {@link IBindingSet} chunks from the previous join dimension.
     * <p>
     * Note: Instances of this class MUST be created on the {@link IDataService}
     * that is host to the index partition on the task will read and they MUST
     * run inside of an {@link AbstractTask} on the {@link ConcurrencyManager}
     * in order to have access to the local index object for the index
     * partition.
     * <p>
     * This class is NOT serializable.
     * <p>
     * For a rule with 2 predicates, there will be two {@link JoinTask}s. The
     * {@link #orderIndex} is ZERO (0) for the first {@link JoinTask} and ONE
     * (1) for the second {@link JoinTask}. The first {@link JoinTask} will
     * have a single initialBinding from the {@link JoinMasterTask} and will
     * read on the {@link IAccessPath} for the first {@link IPredicate} in the
     * evaluation {@link #order}. The second {@link JoinTask} will read chunks
     * of {@link IBindingSet}s containing partial solutions from the first
     * {@link JoinTask} and will obtain and read on an {@link IAccessPath} for
     * the second predicate in the evaluation order for every partial solution.
     * Since there are only two {@link IPredicate}s in the {@link IRule}, the
     * second and last {@link JoinTask} will write on the {@link ISolution}
     * buffer obtained from {@link JoinMasterTask#getSolutionBuffer()}. Each
     * {@link JoinTask} will report its {@link JoinStats} to the master, which
     * aggregates those statistics.
     * <p>
     * Note: {@link ITx#UNISOLATED} requests will deadlock if the same query
     * uses the same access path for two predicates! This is because the first
     * such join dimension in the evaluation order will obtain an exclusive lock
     * on an index partition making it impossible for another {@link JoinTask}
     * to obtain an exclusive lock on the same index partition. This is not a
     * problem if you are using read-consistent timestamps!
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo Allow the access paths to be consumed in parallel. this would let
     *       us use more threads for join dimensions that had to test more
     *       source binding sets.
     *       <p>
     *       Parallel {@link AccessPathTask} processing is useful when each
     *       {@link AccessPathTask} consumes only a small chunk and there are a
     *       large #of source binding sets to be processed. In this case,
     *       parallelism reduces the overall latency by allowing threads to
     *       progress as soon as the data can be materialized from the index.
     *       {@link AccessPathTask} parallelism is realized by submitting each
     *       {@link AccessPathTask} to a service imposing a parallelism limit on
     *       the shared {@link IIndexStore#getExecutorService()}. Since the
     *       {@link AccessPathTask}s are concurrent, each one requires its own
     *       {@link UnsynchronizedOutputBuffer} on which it will place any
     *       accepted {@link IBindingSet}s. Once an {@link AccessPathTask}
     *       completes, its buffer may be reused by the next
     *       {@link AccessPathTask} assigned to a worker thread (this reduces
     *       heap churn and allows us to assemble full chunks when each
     *       {@link IAccessPath} realizes only a few accepted
     *       {@link IBindingSet}s). For an {@link ExecutorService} with a
     *       parallelism limit of N, there are therefore N
     *       {@link UnsynchronizedOutputBuffer}s. Those buffers must be flushed
     *       when the {@link JoinTask} exhausts its source(s).
     *       <p>
     *       Parallel {@link ChunkTask} processing may be useful when an
     *       {@link AccessPathTask} will consume a large #of chunks. Since the
     *       {@link IAccessPath#iterator()} is NOT thread-safe, reads on the
     *       {@link IAccessPath} must be sequential, but the chunks read from
     *       the {@link IAccessPath} can be placed onto a queue and parallel
     *       {@link ChunkTask}s can drain that queue, consuming the chunks.
     *       This can help by reducing the latency to materialize any given
     *       chunk.
     *       <p>
     *       the required change is to make have a per-thread object
     *       {@link UnsynchronizedArrayBuffer} feeding a thread-safe
     *       {@link UnsyncDistributedOutputBuffer} (potentially via a queue)
     *       which maps each generated binding set across the index partition(s)
     *       for the sink {@link JoinTask}s.
     */
    static abstract public class JoinTask implements Callable<Void> {

        static protected final Logger log = Logger.getLogger(JoinTask.class);

        /**
         * True iff the {@link #log} level is INFO or less.
         */
        static final protected boolean INFO = log.isInfoEnabled();

        /**
         * True iff the {@link #log} level is DEBUG or less.
         */
        static final protected boolean DEBUG = log.isDebugEnabled();
        
        /** The rule that is being evaluated. */
        final protected IRule rule;

        /**
         * The #of predicates in the tail of that rule.
         */
        final protected int tailCount;

        /**
         * The index partition on which this {@link JoinTask} is reading -or-
         * <code>-1</code> if the deployment does not support key-range
         * partitioned indices.
         */
        final protected int partitionId;

        /**
         * The tail index in the rule for the predicate on which we are reading
         * for this join dimension.
         */
        final protected int tailIndex;

        /**
         * The {@link IPredicate} on which we are reading for this join
         * dimension.
         */
        final protected IPredicate predicate;
        
        /**
         * The index into the evaluation {@link #order} for the predicate on
         * which we are reading for this join dimension.
         */
        final protected int orderIndex;

        /**
         * <code>true</code> iff this is the last join dimension in the
         * evaluation order.
         */
        final protected boolean lastJoin;

        /**
         * A proxy for the remote {@link JoinMasterTask}.
         */
        final protected IJoinMaster masterProxy;

        /**
         * The {@link IJoinNexus} for the local {@link IIndexManager}, which
         * will be the live {@link IJournal}. This {@link IJoinNexus} MUST have
         * access to the local index objects, which means that class MUST be run
         * inside of the {@link ConcurrencyManager}. The {@link #joinNexus} is
         * created from the {@link #joinNexusFactory} once the task begins to
         * execute.
         * 
         * @todo javadoc
         */
        protected IJoinNexus joinNexus;

        /**
         * Volatile flag is set <code>true</code> if the {@link JoinTask}
         * (including any tasks executing on its behalf) should halt. This flag
         * is monitored by the {@link BindingSetConsumerTask}, the
         * {@link AccessPathTask}, and the {@link ChunkTask}. It is set by any
         * of those tasks if they are interrupted or error out.
         * 
         * @todo review handling of this flag. Should an exception always be
         *       thrown if the flag is set wrapping the {@link #firstCause}?
         *       Are there any cases where the behavior should be different?
         *       If not, then replace tests with halt() and encapsulate the
         *       logic in that method.
         */
        volatile protected boolean halt = false;

        /**
         * Set by {@link BindingSetConsumerTask}, {@link AccessPathTask}, and
         * {@link ChunkTask} if they throw an error. Tasks are required to use
         * an {@link AtomicReference#compareAndSet(Object, Object)} and must
         * specify <code>null</code> as the expected value. This ensures that
         * only the first cause is recorded by this field.
         */
        final protected AtomicReference<Throwable> firstCause = new AtomicReference<Throwable>(
                null);
        
        /**
         * Indicate that join processing should halt.  This method is written
         * defensively and will not throw anything.
         * 
         * @param cause
         *            The cause.
         */
        protected void halt(final Throwable cause) {
            
            halt = true;
            
            firstCause.compareAndSet(null/*expect*/, cause);
            
            if (INFO)
                
                try {
                
                    if (!InnerCause.isInnerCause(cause, InterruptedException.class) &&
                        !InnerCause.isInnerCause(cause, CancellationException.class) &&
                        !InnerCause.isInnerCause(cause, ClosedByInterruptException.class) &&
                        !InnerCause.isInnerCause(cause, RejectedExecutionException.class) &&
                        !InnerCause.isInnerCause(cause, BufferClosedException.class)) {
                        
                        log.info("" + cause, cause);
                        
                    }

                } catch (Throwable ex) {

                    // error in logging system - ignore.
                    
                }

        }
        
        /**
         * The evaluation order. {@link #orderIndex} is the index into this
         * array. The {@link #orderIndex} is zero (0) for the first join
         * dimension and is incremented by one for each subsequent join
         * dimension. The value at <code>order[orderIndex]</code> is the index
         * of the tail predicate that will be evaluated at a given
         * {@link #orderIndex}.
         */
        final int[] order;

        /**
         * The statistics for this {@link JoinTask}.
         */
        final JoinStats stats;

        /**
         * This list is used to accumulate the references to the per-{@link Thread}
         * unsynchronized output buffers. The list is processed by either
         * {@link #flushUnsyncBuffers()} or {@link #resetUnsyncBuffers()}
         * depending on whether the {@link JoinTask} completes successfully or
         * not.
         * 
         * FIXME Examine in the debugger to see whether we are accumulating a
         * bunch of {@link ThreadLocal}s with buffers per thread or if their
         * life cycle is in fact scoped to the {@link JoinTask}.
         * <p>
         * Note: The real danger is that old buffers might hang around with
         * partial results which would cause {@link IBindingSet}s from other
         * {@link JoinTask}s to be emitted by the current {@link JoinTask}.
         */
        final private List<AbstractUnsynchronizedArrayBuffer<IBindingSet>> unsyncBufferList = new LinkedList<AbstractUnsynchronizedArrayBuffer<IBindingSet>>(); 
        
        /**
         * A factory for the per-{@link Thread} buffers used to accumulate
         * chunks of output {@link IBindingSet}s across the
         * {@link AccessPathTask}s for this {@link JoinTask}.
         * <p>
         * Note: This is not static because access is required to
         * {@link JoinTask#newUnsyncOutputBuffer()}.
         */
        final protected ThreadLocal<AbstractUnsynchronizedArrayBuffer<IBindingSet>> 
            threadLocalBufferFactory = new ThreadLocal<AbstractUnsynchronizedArrayBuffer<IBindingSet>>() {

            protected synchronized AbstractUnsynchronizedArrayBuffer<IBindingSet> initialValue() {

                // new buffer created by the concrete JoinClass impl.
                final AbstractUnsynchronizedArrayBuffer<IBindingSet> buffer = newUnsyncOutputBuffer();

                /*
                 * Note: List#add() is safe since initialValue() is
                 * synchronized.
                 */
                
                unsyncBufferList.add(buffer);

                return buffer;

            }

        };

        /**
         * A method used by the {@link #threadLocalBufferFactory} to create new
         * output buffer as required. The output buffer will be used to
         * aggregate {@link IBindingSet}s generated by this {@link JoinTask}.
         * <p>
         * Note: A different implementation class must be used depending on
         * whether or not this is the last join dimension for the query (when it
         * is, then we write on the solution buffer) and whether or not the
         * target join index is key-range partitioned (when it is, each binding
         * set is mapped across the sink {@link JoinTask}(s)).
         */
        abstract protected AbstractUnsynchronizedArrayBuffer<IBindingSet> newUnsyncOutputBuffer();

        /**
         * The buffer on which the last predicate in the evaluation order will
         * write its {@link ISolution}s.
         * 
         * @return The buffer.
         * 
         * @throws IllegalStateException
         *             unless {@link #lastJoin} is <code>true</code>.
         */
        abstract protected IBuffer<ISolution[]> getSolutionBuffer();

        /**
         * Return the index of the tail predicate to be evaluated at the given
         * index in the evaluation order.
         * 
         * @param orderIndex
         *            The evaluation order index.
         * 
         * @return The tail index to be evaluated at that index in the
         *         evaluation order.
         */
        final protected int getTailIndex(final int orderIndex) {

            assert order != null;
            
            final int tailIndex = order[orderIndex];

            assert orderIndex >= 0 && orderIndex < tailCount : "orderIndex="
                    + orderIndex + ", rule=" + rule;

            return tailIndex;

        }

        public String toString() {
            
            return getClass().getName() + "{ orderIndex=" + orderIndex
                    + ", partitionId=" + partitionId + ", lastJoin=" + lastJoin
                    + "}";
            
        }
        
        /**
         * Instances of this class MUST be created in the appropriate execution
         * context of the target {@link DataService} so that the federation and
         * the joinNexus references are both correct and so that it has access
         * to the local index object for the specified index partition.
         * 
         * @param concurrencyManager
         * @param indexName
         * @param rule
         * @param joinNexus
         * @param order
         * @param orderIndex
         * @param partitionId
         *            The index partition identifier and <code>-1</code> if
         *            the deployment does not support key-range partitioned
         *            indices.
         * @param master
         * 
         * @see JoinTaskFactoryTask
         */
        public JoinTask(
                final String indexName, final IRule rule,
                final IJoinNexus joinNexus, final int[] order,
                final int orderIndex, final int partitionId,
                final IJoinMaster master) {
            
            if (rule == null)
                throw new IllegalArgumentException();
            if (joinNexus == null)
                throw new IllegalArgumentException();
            final int tailCount = rule.getTailCount();
            if (order == null)
                throw new IllegalArgumentException();
            if (order.length != tailCount)
                throw new IllegalArgumentException();
            if (orderIndex < 0 || orderIndex >= tailCount)
                throw new IllegalArgumentException();
            if (master == null)
                throw new IllegalArgumentException();

            this.rule = rule;
            this.partitionId = partitionId;
            this.tailCount = tailCount;
            this.orderIndex = orderIndex;
            this.joinNexus = joinNexus;
            this.order = order; // note: assign before using getTailIndex()
            this.tailIndex = getTailIndex(orderIndex);
            this.lastJoin = ((orderIndex + 1) == tailCount);
            this.predicate = rule.getTail(tailIndex);
            this.stats = new JoinStats(partitionId, orderIndex);
            this.masterProxy = master;
            
            if (DEBUG)
                log.debug("orderIndex=" + orderIndex + ", partitionId="
                        + partitionId);

        }

        /**
         * Runs the {@link JoinTask}.
         * 
         * @return <code>null</code>.
         */
        public Void call() throws Exception {

            if (DEBUG)
                log.debug("orderIndex=" + orderIndex + ", partitionId="
                        + partitionId);

            try {

                /*
                 * Consume bindingSet chunks from the source JoinTask(s).
                 */
                consumeSources();
                
                /*
                 * Flush and close output buffers and wait for all sink
                 * JoinTasks to complete.
                 */

                // flush the unsync buffers.
                flushUnsyncBuffers();

                // flush the sync buffer and await the sink JoinTasks
                flushAndCloseBuffersAndAwaitSinks();
                
                if (DEBUG)
                    log.debug("JoinTask done: orderIndex=" + orderIndex
                            + ", partitionId=" + partitionId + ", halt=" + halt
                            + "firstCause=" + firstCause.get());
                if (halt)
                    throw new RuntimeException(firstCause.get());

                return null;

            } catch (Throwable t) {

                /*
                 * This is used for processing errors and also if this task is
                 * interrupted (because a SLICE has been satisified).
                 * 
                 * @todo For a SLICE, consider that the query solution buffer
                 * proxy could return the #of solutions added so far so that we
                 * can halt each join task on the last join dimension in a
                 * relatively timely manner producing no more than one chunk too
                 * many (actually, it might not be that timely since some index
                 * partitions might not produce any solutions; this suggests
                 * that the master might need a fatter API than a Future for the
                 * JoinTask so that it can directly notify the JoinTasks for the
                 * first predicate and they can propagate that notice downstream
                 * to their sinks). This will be an issue when fanOut GT ONE.
                 */

                halt(t);
                
                // reset the unsync buffers.
                resetUnsyncBuffers();
                
                // reset the sync buffer and cancel the sink JoinTasks.
                cancelSinks();

                // report join stats _before_ we close our source(s).
                reportOnce();
                
                /*
                 * Close source iterators, which will cause any source JoinTasks
                 * that are still executing to throw a CancellationException
                 * when the Future associated with the source iterator is
                 * cancelled.
                 */
                closeSources();
                
                throw new RuntimeException(t);

            } finally {

                // report join stats iff they have not already been reported.
                reportOnce();
                
            }

        }

        /**
         * Method reports {@link JoinStats} to the {@link JoinMasterTask}, but
         * only if they have not already been reported. This "report once"
         * constraint is used to make it safe to invoke during error handling
         * before actions which could cause the source {@link JoinTask}s (and
         * hence the {@link JoinMasterTask}) to terminate.
         */
        protected void reportOnce() {
            
            if(!didReport) {

                didReport = true;
                
                try {

                    // report statistics to the master.
                    masterProxy.report(stats);

                } catch(IOException ex) {
                    
                    log.warn("Could not report statistics to the master", ex);
                    
                }

            }
            
        }
        private boolean didReport = false;
        
        /**
         * Consume {@link IBindingSet} chunks from source(s). The first join
         * dimension always has a single source - the initialBindingSet
         * established by the {@link JoinMasterTask}. Downstream join
         * dimensions read from {@link IAsynchronousIterator}(s) from the
         * upstream join dimension. When the {@link IIndexManager} allows
         * key-range partitions, then the fan-in for the sources may be larger
         * than one as there will be one {@link JoinTask} for each index
         * partition touched by each join dimension.
         * 
         * @throws Exception
         * @throws BufferClosedException
         *             if there is an attempt to output a chunk of
         *             {@link IBindingSet}s or {@link ISolution}s and the
         *             output buffer is an {@link IBlockingBuffer} (true for all
         *             join dimensions exception the lastJoin and also true for
         *             query on the lastJoin) and that {@link IBlockingBuffer}
         *             has been closed.
         */
        protected void consumeSources() throws Exception {
            
            if(INFO)
                log.info(toString());

            /*
             * The maximum parallelism with which the {@link JoinTask} will
             * consume the source {@link IBindingSet}s.
             * 
             * Note: When ZERO (0), everything will run in the caller's
             * {@link Thread}. When GT ZERO (0), tasks will run on an
             * {@link ExecutorService} with the specified maximum parallelism.
             * 
             * Note: even when maxParallel is zero there will be one thread per
             * join dimension. For many queries that may be just fine.
             */
            final int maxParallel;
            maxParallel = joinNexus.getMaxParallelSubqueries();
//            maxParallel = 10;
            
            /*
             * Note: There is little reason for parallelism in the first join
             * dimension as there will be only a single source bindingSet so the
             * thread pool is just overhead.
             */
            if (orderIndex > 0 && maxParallel > 0) {
                
                /*
                 * Setup parallelism limitedService that will be used to run the
                 * access path tasks. Note that this is layered over the shared
                 * ExecutorService.
                 * 
                 * FIXME The parallelism limited executor service is clearly
                 * broken. When enabled here it will fail to progress. However
                 * the code runs just fine if you use a standard executor
                 * service in its place.
                 */

//                // the sharedService.
//                final ExecutorService sharedService = joinNexus
//                        .getIndexManager().getExecutorService();
//
//                final ParallelismLimitedExecutorService limitedService = new ParallelismLimitedExecutorService(//
//                        sharedService, //
//                        maxParallel, //
//                        joinNexus.getChunkCapacity() * 2// workQueueCapacity
//                );

                final ExecutorService limitedService = Executors
                        .newFixedThreadPool(maxParallel, DaemonThreadFactory
                                .defaultThreadFactory());
                
                try {

                    /*
                     * consume chunks until done (using caller's thread to
                     * consume and service to run subtasks).
                     */
                    new BindingSetConsumerTask(limitedService).call();
                    
                    // normal shutdown.
                    limitedService.shutdown();

                    // wait for AccessPathTasks to complete.
                    limitedService.awaitTermination(Long.MAX_VALUE,
                            TimeUnit.SECONDS);

//                    if (limitedService.getErrorCount() > 0) {
//
//                        // at least one AccessPathTask failed.
//
//                        if (INFO)
//                            log.info("Task failure(s): " + limitedService);
//
//                        throw new RuntimeException(
//                                "Join failure(s): errorCount="
//                                        + limitedService.getErrorCount());
//
//                    }
                    if(halt)
                        throw new RuntimeException(firstCause.get());
                    
                } finally {

                    if (!limitedService.isTerminated()) {

                        // shutdown the parallelism limitedService.
                        limitedService.shutdownNow();

                    }
                    
                }

            } else {
                
                /*
                 * consume chunks until done using the caller's thread and run
                 * subtasks in the caller's thread as well.
                 */
                new BindingSetConsumerTask(null/* noService */).call();

            }

        }

        /**
         * Close any source {@link IAsynchronousIterator}(s). This method is
         * invoked when a {@link JoinTask} fails.
         */
        abstract void closeSources();
        
        /**
         * Flush the per-{@link Thread} unsynchronized output buffers (they
         * write onto the thread-safe output buffer).
         */
        protected void flushUnsyncBuffers() {

            if(INFO) 
                log.info("Flushing " + unsyncBufferList.size()
                        + " unsynchronized buffers");
            
            for (AbstractUnsynchronizedArrayBuffer<IBindingSet> b : unsyncBufferList) {

                // unless halted
                if (halt)
                    throw new RuntimeException(firstCause.get());
                
                // #of elements to be flushed.
                final int size = b.size();
                
                // flush, returning total #of elements written onto this buffer.
                final long counter = b.flush();
                
                if (DEBUG)
                    log.debug("Flushed buffer: size=" + size + ", counter="
                            + counter);

            }

        }
        
        /**
         * Reset the per-{@link Thread} unsynchronized output buffers (used as
         * part of error handling for the {@link JoinTask}).
         */
        protected void resetUnsyncBuffers() {

            if(INFO) 
                log.info("Resetting "+unsyncBufferList.size()+" unsynchronized buffers");
            
            for (AbstractUnsynchronizedArrayBuffer<IBindingSet> b : unsyncBufferList) {

                // #of elements in the buffer before reset().
                final int size = b.size();
                
                // flush the buffer.
                b.reset();
                
                if (DEBUG)
                    log.debug("Reset buffer: size=" + size);

            }

        }
        
        /**
         * Flush and close all output buffers and await sink {@link JoinTask}(s).
         * <p>
         * Note: You MUST close the {@link BlockingBuffer} from which each sink
         * reads <em>before</em> invoking thise method in order for those
         * sinks to terminate. Otherwise the source
         * {@link IAsynchronousIterator}(s) on which the sink is reading will
         * remain open and the sink will never decide that it has exhausted its
         * source(s).
         * 
         * @throws InterruptedException
         * @throws ExecutionException
         */
        abstract protected void flushAndCloseBuffersAndAwaitSinks()
                throws InterruptedException, ExecutionException;

        /**
         * Cancel sink {@link JoinTask}(s).
         */
        abstract protected void cancelSinks();
        
        /**
         * Return a chunk of {@link IBindingSet}s from the
         * {@link IAsynchronousIterator}s. The 1st join dimension is always fed
         * by the {@link JoinMasterTask}. The nth+1 join dimension is always
         * fed by the nth {@link JoinTask}(s).
         * 
         * @return The next available chunk of {@link IBindingSet}s -or-
         *         <code>null</code> IFF all known source(s) are exhausted.
         */
        abstract protected IBindingSet[] nextChunk();
        
        /**
         * Class consumes chunks from the source(s) until cancelled,
         * interrupted, or all source(s) are exhausted. For each
         * {@link IBindingSet} in each chunk, an {@link AccessPathTask} is
         * created which will consume that {@link IBindingSet}. The
         * {@link AccessPathTask} for a given source chunk are sorted based on
         * their <code>fromKey</code> so as to order the execution of those
         * tasks in a manner that will maximize the efficiency of index reads.
         * The ordered {@link AccessPathTask}s are then submitted to the
         * caller's {@link Executor}.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        class BindingSetConsumerTask implements Callable {

            private final ExecutorService executor;

            /**
             * 
             * @param executor
             *            The service that will execute the generated
             *            {@link AccessPathTask}s -or- <code>null</code> IFF
             *            you want the {@link AccessPathTask}s to be executed
             *            in the caller's thread.
             */
            public BindingSetConsumerTask(final ExecutorService executor) {

                this.executor = executor;

            }
            
            /**
             * Read chunks from one or more sources until cancelled,
             * interrupted, or all sources are exhausted and submits
             * {@link AccessPathTask}s to the caller's {@link ExecutorService}
             * -or- executes those tasks in the caller's thread if no
             * {@link ExecutorService} was provided to the ctor.
             * <p>
             * Note: When running with an {@link ExecutorService}, the caller
             * is responsible for waiting on that {@link ExecutorService} until
             * the {@link AccessPathTask}s to complete and must verify all
             * tasks completed successfully.
             * 
             * @return <code>null</code>
             * 
             * @throws BufferClosedException
             *             if there is an attempt to output a chunk of
             *             {@link IBindingSet}s or {@link ISolution}s and the
             *             output buffer is an {@link IBlockingBuffer} (true for
             *             all join dimensions exception the lastJoin and also
             *             true for query on the lastJoin) and that
             *             {@link IBlockingBuffer} has been closed.
             */
            public Object call() throws Exception {

                try {

                    if (DEBUG)
                        log.debug("begin: orderIndex=" + orderIndex
                                + ", partitionId=" + partitionId);

                    IBindingSet[] chunk;

                    while (!halt && (chunk = nextChunk()) != null) {
                        // @todo ChunkTrace for bindingSet chunks in as well as access path chunks consumed 
                        if (DEBUG)
                            log.debug("Read chunk of bindings: chunkSize="
                                    + chunk.length + ", orderIndex="
                                    + orderIndex + ", partitionId="
                                    + partitionId);

                        /*
                         * Aggregate the source bindingSets that license the
                         * same asBound predicate.
                         */
                        final Map<IPredicate, Collection<IBindingSet>> map = combineBindingSets(chunk);

                        /*
                         * Generate an AbstractPathTask from each distinct
                         * asBound predicate that will consume all of the source
                         * bindingSets in the chunk which resulted in the same
                         * asBound predicate.
                         */
                        final AccessPathTask[] tasks = getAccessPathTasks(map);

                        /*
                         * Reorder those tasks for better index read
                         * performance.
                         */
                        reorderTasks(tasks);

                        /*
                         * Execute the tasks (either in the caller's thread or
                         * on the supplied service).
                         */
                        executeTasks(tasks);
                        
                    }

                    if(halt)
                        throw new RuntimeException(firstCause.get());
                    
                    if (DEBUG)
                        log.debug("done: orderIndex=" + orderIndex
                                + ", partitionId=" + partitionId);
                    
                    return null;

                } catch (Throwable t) {

                    halt(t);
                    
                    throw new RuntimeException(t);

                }

            }

            /**
             * Populates a map of asBound predicates paired to a set of
             * bindingSets.
             * <p>
             * Note: The {@link AccessPathTask} will apply each bindingSet to
             * each element visited by the {@link IAccessPath} obtained for the
             * asBound {@link IPredicate}. This has the natural consequence of
             * eliminating subqueries within the chunk.
             * 
             * @param chunk
             *            A chunk of bindingSets from the source join dimension.
             * 
             * @return A map which pairs the distinct asBound predicates to the
             *         bindingSets in the chunk from which the predicate was
             *         generated.
             */
            protected Map<IPredicate, Collection<IBindingSet>> combineBindingSets(
                    final IBindingSet[] chunk) {
           
                if (DEBUG)
                    log.debug("chunkSize=" + chunk.length);

                final int tailIndex = getTailIndex(orderIndex);

                final Map<IPredicate, Collection<IBindingSet>> map = new LinkedHashMap<IPredicate, Collection<IBindingSet>>(
                        chunk.length);
                
                for( IBindingSet bindingSet : chunk ) {
                    
                    if (halt)
                        throw new RuntimeException(firstCause.get());

                    // constrain the predicate to the given bindings.
                    IPredicate predicate = rule.getTail(tailIndex).asBound(
                            bindingSet);

                    if (partitionId != -1) {

                        /*
                         * Constrain the predicate to the desired index partition.
                         * 
                         * Note: we do this for scale-out joins since the access
                         * path will be evaluated by a JoinTask dedicated to this
                         * index partition, which is part of how we give the
                         * JoinTask to gain access to the local index object for an
                         * index partition.
                         */

                        predicate = predicate.setPartitionId(partitionId);

                    }

                    // lookup the asBound predicate in the map.
                    Collection<IBindingSet> values = map.get(predicate);

                    if (values == null) {

                        /*
                         * This is the first bindingSet for this asBound
                         * predicate. We create a collection of bindingSets to
                         * be paired with that predicate and put the collection
                         * into the map using that predicate as the key.
                         */
                        
                        values = new LinkedList<IBindingSet>();
                     
                        map.put(predicate, values);
                        
                    } else {
                        
                        // more than one bindingSet will use the same access path.
                        stats.accessPathDups++;
                        
                    }
                    
                    /*
                     * Add the bindingSet to the collection of bindingSets
                     * paired with the asBound predicate.
                     */

                    values.add(bindingSet);
                    
                }

                if (DEBUG)
                    log.debug("chunkSize=" + chunk.length
                            + ", #distinct predicates=" + map.size());
                
                return map;
                
            }
            
            /**
             * Creates an {@link AccessPathTask} for each {@link IBindingSet} in
             * the given chunk.
             * 
             * @param chunk
             *            A chunk of {@link IBindingSet}s from one or more
             *            source {@link JoinTask}s.
             * 
             * @return A chunk of {@link AccessPathTask} in a desirable
             *         execution order.
             * 
             * @throws Exception
             */
            protected AccessPathTask[] getAccessPathTasks(
                    final Map<IPredicate, Collection<IBindingSet>> map) {

                final int n = map.size();
                
                if (DEBUG)
                    log.debug("#distinct predicates=" + n);

                final AccessPathTask[] tasks = new AccessPathTask[n];

                final Iterator<Map.Entry<IPredicate, Collection<IBindingSet>>> itr = map
                        .entrySet().iterator();

                int i = 0;

                while (itr.hasNext()) {

                    if (halt)
                        throw new RuntimeException(firstCause.get());
                    
                    final Map.Entry<IPredicate, Collection<IBindingSet>> entry = itr
                            .next();
                    
                    final IPredicate predicate = entry.getKey();
                    
                    final Collection<IBindingSet> bindingSets = entry.getValue();
                    
                    tasks[i++] = new AccessPathTask(predicate, bindingSets);
                    
                }
                
                return tasks;

            }

            /**
             * The tasks are ordered based on the <i>fromKey</i> for the
             * associated {@link IAccessPath} as licensed by each
             * {@link IBindingSet}. This order tends to focus the reads on the
             * same parts of the index partitions with a steady progression in
             * the <i>fromKey</i> as we process a chunk of {@link IBindingSet}s.
             * 
             * @param tasks
             *            The tasks.
             */
            protected void reorderTasks(final AccessPathTask[] tasks) {
                
                // @todo layered access paths do not expose a fromKey.
                if (tasks[0].accessPath instanceof AbstractAccessPath) {

                    // reorder the tasks.
                    Arrays.sort(tasks);

                }
                
            }

            /**
             * Either execute the tasks in the caller's thread or schedule them
             * for execution on the supplied service.
             * 
             * @param tasks
             *            The tasks.
             * 
             * @throws Exception
             */
            protected void executeTasks(final AccessPathTask[] tasks)
                    throws Exception {
                
                int i=0;
                for (AccessPathTask task : tasks) {

                    if (halt)
                        throw new RuntimeException(firstCause.get());

                    if (executor != null) {

                        /*
                         * Queue the AccessPathTask for execution.
                         * 
                         * Note: The caller MUST verify that no tasks submitted
                         * to the [executor] result in errors. This is done by
                         * checking the errorCount for that [executor], so you
                         * need to run the tasks on a service which exposes that
                         * information.
                         */
                        
                        executor.submit(task);

                    } else {

                        /*
                         * Execute the AccessPathTask in the caller's thread.
                         */

                        task.call();

                    }

                    i++;
                    
                } // next task.

            }
            
        }
        
        /**
         * Accepts an asBound {@link IPredicate} and a (non-empty) collection of
         * {@link IBindingSet}s each of which licenses the same asBound
         * predicate for the current join dimension. The task obtains the
         * corresponding {@link IAccessPath} and delegates each chunk visited on
         * that {@link IAccessPath} to a {@link ChunkTask}. Note that optionals
         * are also handled by this task.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        class AccessPathTask implements Callable, Comparable<AccessPathTask> {

            /**
             * The {@link IBindingSet}s from the source join dimension to be
             * combined with each element visited on the {@link #accessPath}.
             * If there is only a single source {@link IBindingSet} in a given
             * chunk of source {@link IBindingSet}s that results in the same
             * asBound {@link IPredicate} then this will be a collection with a
             * single member. However, if multiple source {@link IBindingSet}s
             * result in the same asBound {@link IPredicate} within the same
             * chunk then those are aggregated and appear together in this
             * collection.
             * <p>
             * Note: An array is used for thread-safe traversal.
             */
            final private IBindingSet[] bindingSets;

            /**
             * The {@link IAccessPath} corresponding to the asBound
             * {@link IPredicate} for this join dimension. The asBound
             * {@link IPredicate} is {@link IAccessPath#getPredicate()}.
             */
            final private IAccessPath accessPath;

            /**
             * Return the <em>fromKey</em> for the {@link IAccessPath}
             * generated from the {@link IBindingSet} for this task.
             * 
             * @todo layered access paths do not expose a fromKey.
             */
            protected byte[] getFromKey() {

                return ((AbstractAccessPath)accessPath).getFromKey();
                
            }

            /**
             * Return <code>true</code> iff the tasks are equivalent (same as
             * bound predicate). This test may be used to eliminate duplicates
             * that arise when different source {@link JoinTask}s generate the
             * same {@link IBindingSet}.
             * 
             * @param o
             *            Another task.
             * 
             * @return if the as bound predicate is equals().
             */
            public boolean equals(AccessPathTask o) {

                return accessPath.getPredicate().equals(
                        o.accessPath.getPredicate());

            }
            
            /**
             * Evaluate an {@link IBindingSet} for the join dimension. When the
             * task runs, it will pair each element visited on the
             * {@link IAccessPath} with the asBound {@link IPredicate}. For
             * each element visited, if the binding is acceptable for the
             * constraints on the asBound {@link IPredicate}, then the task
             * will emit one {@link IBindingSet} for each source
             * {@link IBindingSet}.
             * 
             * @param predicate
             *            The asBound {@link IPredicate}.
             * @param bindingSets
             *            A collection of {@link IBindingSet}s from the source
             *            join dimension that all result in the same asBound
             *            {@link IPredicate}.
             */
            public AccessPathTask(final IPredicate predicate,
                    final Collection<IBindingSet> bindingSets) {

                if (predicate == null)
                    throw new IllegalArgumentException();

                if (bindingSets == null)
                    throw new IllegalArgumentException();
                
                /*
                 * Note: this needs to be the access path for the local index
                 * partition. We handle this by (a) constraining the predicate
                 * to the desired index partition; (b) using an IJoinNexus that
                 * is initialized once the JoinTask starts to execute inside of
                 * the ConcurrencyManager; (c) declaring; and (d) using the
                 * index partition name NOT the scale-out index name.
                 */

                final int n = bindingSets.size();
                
                if (n == 0)
                    throw new IllegalArgumentException();
                
                this.accessPath = joinNexus.getTailAccessPath(predicate);

                if (DEBUG)
                    log.debug("orderIndex=" + orderIndex + ", tailIndex="
                            + tailIndex + ", tail=" + rule.getTail(tailIndex)
                            + ", #bindingSets=" + n + ", accessPath="
                            + accessPath);

                // convert to array for thread-safe traversal.
                this.bindingSets = bindingSets.toArray(new IBindingSet[n]);

            }

            public String toString() {

                return getClass().getSimpleName() + "{ orderIndex="
                        + orderIndex + ", partitionId=" + partitionId
                        + ", #bindingSets=" + bindingSets.length+ "}";
                
            }
            
            /**
             * Evaluate the {@link #accessPath} against the {@link #bindingSets}.
             * If nothing is accepted and {@link IPredicate#isOptional()} then
             * the {@link #bindingSets} is output anyway (this implements the
             * semantics of OPTIONAL).
             * 
             * @return <code>null</code>.
             * 
             * @throws BufferClosedException
             *             if there is an attempt to output a chunk of
             *             {@link IBindingSet}s or {@link ISolution}s and the
             *             output buffer is an {@link IBlockingBuffer} (true for
             *             all join dimensions exception the lastJoin and also
             *             true for query on the lastJoin) and that
             *             {@link IBlockingBuffer} has been closed.
             */
            public Object call() throws Exception {

                if (halt)
                    throw new RuntimeException(firstCause.get());
                
                final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer = threadLocalBufferFactory
                        .get();
                
                boolean nothingAccepted = true;
                
                stats.accessPathCount++;
                
                // Obtain the iterator for the current join dimension.
                final IChunkedOrderedIterator itr = accessPath.iterator();
                
                try {

                    while (itr.hasNext()) {

                        final Object[] chunk = itr.nextChunk();
                        
                        stats.chunkCount++;

                        // process the chunk in the caller's thread.
                        if (new ChunkTask(bindingSets, unsyncBuffer, chunk)
                                .call()) {

                            nothingAccepted = false;
                            
                        }

                    } // next chunk.

                    if (nothingAccepted && predicate.isOptional()) {

                        /*
                         * Note: when NO binding sets were accepted AND the
                         * predicate is OPTIONAL then we output the _original_
                         * binding set(s) to the sink join task(s).
                         */

                        for(IBindingSet bs : this.bindingSets) {
                        
                            unsyncBuffer.add(bs);
                            
                        }
                        
                    }
                    
                    return null;
                    
                } catch(Throwable t) {
                    
                    halt(t);
                    
                    throw new RuntimeException(t);
                    
                } finally {

                    itr.close();

                }

            }

            /**
             * Imposes an order based on the <em>fromKey</em> for the
             * {@link IAccessPath} associated with the task.
             * 
             * @param o
             * 
             * @return
             */
            public int compareTo(AccessPathTask o) {
                
                return BytesUtil.compareBytes(getFromKey(), o.getFromKey());
                
            }

        }

        /**
         * Task processes a chunk of elements read from the {@link IAccessPath}
         * for a join dimension. Each element in the chunk in paired with a copy
         * of the given bindings. If that {@link IBindingSet} is accepted by the
         * {@link IRule}, then the {@link IBindingSet} will be output. The
         * {@link IBindingSet}s to be output are buffered into chunks and the
         * chunks added to the {@link JoinPipelineTask#bindingSetBuffers} for
         * the corresponding predicate.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        class ChunkTask implements Callable {

            /**
             * The index of the predicate for the access path that is being
             * consumed.
             */
            private final int tailIndex;

            /**
             * The {@link IBindingSet}s which the each element in the chunk
             * will be paired to create {@link IBindingSet}s for the downstream
             * join dimension.
             */
            private final IBindingSet[] bindingSets;

            /**
             * A per-{@link Thread} buffer that is used to collect
             * {@link IBindingSet}s into chunks before handing them off to the
             * next join dimension. The hand-off occurs no later than when the
             * current join dimension finishes consuming its source(s).
             */
            private final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer;
            
            /**
             * A chunk of elements read from the {@link IAccessPath} for the
             * current join dimension.
             */
            private final Object[] chunk;
            
            /**
             * 
             * @param bindingSet
             *            The bindings with which the each element in the chunk
             *            will be paired to create the bindings for the
             *            downstream join dimension.
             * @param unsyncBuffer
             *            A per-{@link Thread} buffer used to accumulate chunks
             *            of generated {@link IBindingSet}s.
             * @param chunk
             *            A chunk of elements read from the {@link IAccessPath}
             *            for the current join dimension.
             */
            public ChunkTask(final IBindingSet[] bindingSet,
                    final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer,
                    final Object[] chunk) {

                if (bindingSet == null)
                    throw new IllegalArgumentException();
                
                if (unsyncBuffer == null)
                    throw new IllegalArgumentException();

                if (chunk == null)
                    throw new IllegalArgumentException();
                
                this.tailIndex = getTailIndex(orderIndex);

                this.bindingSets = bindingSet;

                this.chunk = chunk;

                this.unsyncBuffer = unsyncBuffer;

            }

            /**
             * @return <code>true</code> iff NO elements in the chunk (as read
             *         from the access path by the caller) were accepted when
             *         combined with the {@link #bindingSets} from the source
             *         {@link JoinTask}.
             * 
             * @throws BufferClosedException
             *             if there is an attempt to output a chunk of
             *             {@link IBindingSet}s or {@link ISolution}s and the
             *             output buffer is an {@link IBlockingBuffer} (true for
             *             all join dimensions exception the lastJoin and also
             *             true for query on the lastJoin) and that
             *             {@link IBlockingBuffer} has been closed.
             */
            public Boolean call() throws Exception {

                try {

                    ChunkTrace.chunk(orderIndex, chunk);

                    boolean nothingAccepted = true;

                    for (Object e : chunk) {

                        if (halt)
                            return nothingAccepted;

                        // naccepted for the current element (trace only).
                        int naccepted = 0;

                        stats.elementCount++;

                        for (IBindingSet bset : bindingSets) {

                            /*
                             * Clone the binding set since it is tested for each
                             * element visited.
                             */ 
                            bset = bset.clone();

                            // propagate bindings from the visited element.
                            if (joinNexus.bind(rule, tailIndex, e, bset)) {

                                // Accept this binding set.
                                unsyncBuffer.add(bset);

                                naccepted++;

                                nothingAccepted = false;

                            }

                        }

                        if (DEBUG)
                            log.debug("Accepted element for " + naccepted
                                    + " of " + bindingSets.length
                                    + " possible bindingSet combinations: "
                                    + e.toString() + ", orderIndex="
                                    + orderIndex + ", lastJoin=" + lastJoin
                                    + ", rule=" + rule.getName());
                    }

                    return nothingAccepted ? Boolean.TRUE : Boolean.FALSE;

                } catch (Throwable t) {

                    halt( t );

                    throw new RuntimeException(t);

                }

            }
            
        }
        
    }
    
    /**
     * {@link JoinTask} implementation for a {@link Journal} or
     * {@link LocalDataServiceFederation}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class LocalJoinTask extends JoinTask {
        
        /**
         * The asynchronous iterator that is the source for this
         * {@link LocalJoinTask}.
         */
        final private IAsynchronousIterator<IBindingSet[]> source;

        /**
         * <code>null</code> unless this is the last join dimension.
         */
        final private IBuffer<ISolution[]> solutionBuffer;
        
        /**
         * @param concurrencyManager
         * @param indexName
         * @param rule
         * @param joinNexusFactory
         * @param order
         * @param orderIndex
         * @param master
         * @param source
         * @param solutionBuffer
         */
        public LocalJoinTask(
                final String indexName, final IRule rule,
                final IJoinNexus joinNexus, final int[] order,
                final int orderIndex, 
                final IJoinMaster master,
                final IAsynchronousIterator<IBindingSet[]> source,
                final IBuffer<ISolution[]> solutionBuffer) {

            super(indexName, rule, joinNexus, order,
                    orderIndex, -1/* partitionId */, master);

            if (source == null)
                throw new IllegalArgumentException();
            
            if (lastJoin && solutionBuffer == null)
                throw new IllegalArgumentException();

            this.source = source;
            
            /*
             * The fanIn is always one.
             * 
             * Note: There is one source for the first LocalJoinTask. It is the
             * async iterator containing the initial bindingSet from the
             * LocalJoinMaster.
             */
            stats.fanIn = 1;
            
            if (lastJoin) {

                /*
                 * Accepted binding sets are flushed to the solution buffer.
                 */

                // not used
                this.syncBuffer = null;
                
                // solutionBuffer.
                this.solutionBuffer = solutionBuffer;
                
            } else {

                /*
                 * The index is not key-range partitioned. This means that there
                 * is ONE (1) JoinTask per predicate in the rule. Chunks of
                 * bindingSets are written pre-Thread buffers by ChunkTasks.
                 * Those unsynchronized buffers overflow onto the per-JoinTask
                 * [syncBuffer], which is a BlockingBuffer. The sink
                 * LocalJoinTask drains that BlockingBuffer using its
                 * iterator(). When the BlockingBuffer is closed and everything
                 * in the buffer has been drained, then the sink LocalJoinTask
                 * will conclude that no more bindingSets are available and it
                 * will terminate.
                 */

                this.syncBuffer = new BlockingBuffer<IBindingSet[]>(joinNexus
                        .getChunkOfChunksCapacity());
                
                // not used.
                this.solutionBuffer = null;

                stats.fanOut = 1;
                
            }

        }

        @Override
        protected IBuffer<ISolution[]> getSolutionBuffer() {
            
            if (!lastJoin)
                throw new IllegalStateException();
            
            return solutionBuffer;
            
        }

        /**
         * Closes the {@link #source} specified to the ctor.
         */
        protected void closeSources() {
            
            if(INFO)
                log.info(toString());
            
            source.close();
            
        }
        
        /**
         * Note: The target buffer on which the unsynchronized buffer writes
         * depends on whether or not there is a downstream sink for this
         * {@link LocalJoinTask}. When this is the {@link JoinTask#lastJoin},
         * the unsynchronized buffer returned by this method will write on the
         * solution buffer. Otherwise it will write on {@link #syncBuffer},
         * which is drained by the sink {@link LocalJoinTask}.
         */
        final protected AbstractUnsynchronizedArrayBuffer<IBindingSet> newUnsyncOutputBuffer() {

            if (lastJoin) {

                /*
                 * Accepted binding sets are flushed to the solution buffer.
                 */

                // flushes to the solution buffer.
                return new UnsynchronizedSolutionBuffer<IBindingSet>(
                        this, joinNexus, joinNexus.getChunkCapacity());
                
            } else {

                /*
                 * The index is not key-range partitioned. This means that there
                 * is ONE (1) JoinTask per predicate in the rule. The
                 * bindingSets are aggregated into chunks by this buffer. On
                 * overflow, the buffer writes onto a BlockingBuffer. The sink
                 * JoinTask reads from that BlockingBuffer's iterator.
                 */

                // flushes to the syncBuffer.
                return new UnsyncLocalOutputBuffer<IBindingSet>(
                        this, joinNexus.getChunkCapacity(), syncBuffer);

            }

        }

        /**
         * The {@link BlockingBuffer} whose queue will be drained by the
         * downstream {@link LocalJoinTask} -or- <code>null</code> IFF
         * [lastJoin == true].
         */
        private final BlockingBuffer<IBindingSet[]> syncBuffer;

        /**
         * The {@link Future} for the sink for this {@link LocalJoinTask} and
         * <code>null</code> iff this is {@link JoinTask#lastJoin}. This
         * field is set by the {@link LocalJoinMasterTask} so it can be
         * <code>null</code> if things error out before it gets set or 
         * perhaps if they complete too quickly.
         */
        protected Future<? extends Object> sinkFuture;
        
        @Override
        protected void flushAndCloseBuffersAndAwaitSinks()
                throws InterruptedException, ExecutionException {

            if (DEBUG)
                log.debug("orderIndex=" + orderIndex + ", partitionId="
                        + partitionId);
            
            if (halt)
                throw new RuntimeException(firstCause.get());

            if(lastJoin) {

                /*
                 * Flush the solutionBuffer.
                 * 
                 * Note: For the last JOIN, the buffer is either the query
                 * solution buffer or the mutation buffer.
                 * 
                 * DO NOT close() the solutionBuffer for the last join since
                 * (for query) the buffer is shared by all rules in the program.
                 * 
                 * Closing the solutionBuffer on the last join is BAD BAD BAD.
                 */

                final long counter = getSolutionBuffer().flush();

                if (joinNexus.getAction().isMutation()) {

                    /*
                     * For mutation operations, the solutionBuffer for the last
                     * join dimension writes solutions onto the target relation.
                     * When that buffer is flushed it returns the #of solutions
                     * that resulted in a state change in the target relation.
                     * This is the mutationCount. We report it here to the
                     * JoinStats and it will be aggregated by the
                     * JoinMasterTask.
                     */

                    stats.mutationCount.addAndGet(counter);
                    
                }
                
            } else {

                /*
                 * Close the thread-safe output buffer. For any JOIN except the
                 * last, this buffer will be the source for one or more sink
                 * JoinTasks for the next join dimension. Once this buffer is
                 * closed, the asynchronous iterator draining the buffer will
                 * eventually report that there is nothing left for it to
                 * process.
                 * 
                 * Note: This is a BlockingBuffer. BlockingBuffer#flush() is a
                 * NOP.
                 */

                syncBuffer.close();
                
                assert !syncBuffer.isOpen();

                if (halt)
                    throw new RuntimeException(firstCause.get());

                if (sinkFuture == null) {

                    // @todo should we wait for the Future to be assigned?
                    log.warn("sinkFuture not assigned yet: orderIndex="
                            + orderIndex);
                    
                } else {
                    
                    try {

                        sinkFuture.get();

                    } catch (Throwable t) {

                        halt(t);

                    }
                
                }
                
            }
            
        }

        @Override
        protected void cancelSinks() {

            if (DEBUG)
                log.debug("orderIndex=" + orderIndex + ", partitionId="
                        + partitionId );

            if (!lastJoin) {

                syncBuffer.reset();

                if (sinkFuture != null) {

                    sinkFuture.cancel(true/* mayInterruptIfRunning */);

                }

            }

        }
        
        /**
         * Return the next chunk of {@link IBindingSet}s the source
         * {@link JoinTask}.
         * 
         * @return The next chunk -or- <code>null</code> iff the source is
         *         exhausted.
         */
        protected IBindingSet[] nextChunk() {

            if (DEBUG)
                log.debug("orderIndex=" + orderIndex);

            // @todo use timeout and test halt to avoid long waits?
            if(source.hasNext()) {

                if (halt)
                    throw new RuntimeException(firstCause.get());

                // read the chunk.
                final IBindingSet[] chunk = source.next();

                stats.bindingSetChunksIn++;
                stats.bindingSetsIn += chunk.length;

                if (DEBUG)
                    log.debug("Read chunk from source: chunkSize="
                            + chunk.length + ", orderIndex=" + orderIndex);

                return chunk;

            }

            /*
             * Termination condition: the source is exhausted.
             */

            if (DEBUG)
                log.debug("Source exhausted: orderIndex=" + orderIndex);

            return null;

        }

    }

    /**
     * Implementation used for {@link LocalJoinTask}s.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    static class UnsyncLocalOutputBuffer<E extends IBindingSet> extends
            UnsynchronizedOutputBuffer<E> {

        private final IBlockingBuffer<E[]> syncBuffer;

        /**
         * @param joinTask
         *            The task that is writing on this buffer.
         * @param capacity
         *            The capacity of this buffer.
         * @param syncBuffer
         *            The thread-safe buffer onto which this buffer writes when
         *            it overflows.
         */
        protected UnsyncLocalOutputBuffer(final LocalJoinTask joinTask,
                final int capacity, final IBlockingBuffer<E[]> syncBuffer) {

            super(joinTask, capacity);

            this.syncBuffer = syncBuffer;

        }

        /**
         * Adds the chunk to the {@link #syncBuffer} and updated the
         * {@link JoinStats} to reflect the #of {@link IBindingSet} chunks that
         * will be output and the #of {@link IBindingSet}s in those chunks.
         * 
         * @param chunk
         *            A chunk of {@link IBindingSet}s to be output.
         */
        @Override
        protected void handleChunk(final E[] chunk) {

            syncBuffer.add(chunk);

            joinTask.stats.bindingSetChunksOut++;
            joinTask.stats.bindingSetsOut += chunk.length;

        }
        
    }
    
    /**
     * Implementation used by scale-out deployments. There will be one instance
     * of this task per index partition on which the rule will read. Those
     * instances will be in-process on the {@link DataService} hosting that
     * index partition. Instances are created on the {@link DataService} using
     * the {@link JoinTaskFactoryTask} helper class.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class DistributedJoinTask extends JoinTask {
        
        /**
         * The federation is used to obtain locator scans for the access paths.
         */
        final private AbstractScaleOutFederation fed;
        
        /**
         * The {@link IJoinNexus} for the {@link IBigdataFederation}. This is
         * mainly used to setup the {@link #solutionBuffer} since it needs to
         * write on the scale-out index while the {@link AccessPathTask} will
         * read on the local index partition view.
         */
        final protected IJoinNexus fedJoinNexus;

        /**
         * @see IRuleState#getKeyOrder()
         */
        final private IKeyOrder[] keyOrders;
        
        /**
         * The name of the scale-out index associated with the next
         * {@link IPredicate} in the evaluation order and <code>null</code>
         * iff this is the last {@link IPredicate} in the evaluation order.
         */
        final private String nextScaleOutIndexName;
        
        /**
         * Sources for {@link IBindingSet} chunks that will be processed by this
         * {@link JoinTask}. There will be one such source for each upstream
         * {@link JoinTask} that targets this {@link JoinTask}.
         * <p>
         * Note: This is a thread-safe collection since new sources may be added
         * asynchronously during processing.
         */
        final private Vector<IAsynchronousIterator<IBindingSet[]>> sources = new Vector<IAsynchronousIterator<IBindingSet[]>>();

        /**
         * The {@link JoinTaskSink}s for the downstream
         * {@link DistributedJoinTask}s onto which the generated
         * {@link IBindingSet}s will be written. This is <code>null</code>
         * for the last join since we will write solutions onto the
         * {@link #getSolutionBuffer()} instead.
         * 
         * @todo configure capacity based on expectations of index partition
         *       fan-out for this join dimension
         */
        final private Map<PartitionLocator, JoinTaskSink> sinkCache;

        public DistributedJoinTask(//final ConcurrencyManager concurrencyManager,
                final String scaleOutIndexName, final IRule rule,
                final IJoinNexus joinNexus, final int[] order,
                final int orderIndex, final int partitionId,
                final AbstractScaleOutFederation fed, final IJoinMaster master,
                final IAsynchronousIterator<IBindingSet[]> src, 
                final IKeyOrder[] keyOrders) {

            super(//concurrencyManager,
                    DataService.getIndexPartitionName(
                    scaleOutIndexName, partitionId), rule, joinNexus,
                    order, orderIndex, partitionId, master);

            if (fed == null)
                throw new IllegalArgumentException();

            if (src == null)
                throw new IllegalArgumentException();

            this.fed = fed;
            
            this.keyOrders = keyOrders;
            
            this.fedJoinNexus = joinNexus.getJoinNexusFactory().newInstance(fed);

            if(lastJoin) {

                sinkCache = null;
                
                nextScaleOutIndexName = null;

                final ActionEnum action = fedJoinNexus.getAction();
                
                if(action.isMutation()) {
                    
                    /*
                     * Note: The solution buffer for mutation operations
                     * is obtained locally from a joinNexus that is
                     * backed by the federation NOT the local index
                     * manager. (This is because the solution buffer
                     * needs to write on the scale-out indices.)
                     */
                    
                    final IJoinNexus tmp = fedJoinNexus;
                    
                    /*
                     * The view of the mutable relation for the _head_ of the
                     * rule.
                     */
                    
                    final IMutableRelation relation = (IMutableRelation) tmp
                            .getHeadRelationView(rule.getHead());

                    switch (action) {

                    case Insert: {

                        solutionBuffer = tmp.newInsertBuffer(relation);

                        break;

                    }

                    case Delete: {

                        solutionBuffer = tmp.newDeleteBuffer(relation);

                        break;

                    }

                    default:
                        throw new AssertionError();

                    }

                } else {

                    /*
                     * The solution buffer for queries is obtained from the
                     * master.
                     */

                    try {

                        solutionBuffer = masterProxy.getSolutionBuffer();

                    } catch (IOException ex) {

                        throw new RuntimeException(ex);

                    }

                }
                
            } else {
                
                final IPredicate nextPredicate = rule.getTail(order[orderIndex + 1]);

                final String namespace = nextPredicate.getOnlyRelationName();

                nextScaleOutIndexName = namespace + keyOrders[order[orderIndex + 1]]; 

                solutionBuffer = null;

                sinkCache = new LinkedHashMap<PartitionLocator, JoinTaskSink>();
                
//                System.err.println("orderIndex=" + orderIndex + ", resources="
//                        + Arrays.toString(getResource()) + ", nextPredicate="
//                        + nextPredicate + ", nextScaleOutIndexName="
//                        + nextScaleOutIndexName);
                
            }
            
            addSource( src );

        }

        /**
         * Adds a source from which this {@link DistributedJoinTask} will read
         * {@link IBindingSet} chunks. 
         * 
         * @param source
         *            The source.
         * 
         * @throws IllegalArgumentException
         *             if the <i>source</i> is <code>null</code>.
         * @throws IllegalStateException
         *             if {@link #closeSources()} has already been invoked.
         */
        public void addSource(final IAsynchronousIterator<IBindingSet[]> source) {
            
            if (source == null)
                throw new IllegalArgumentException();
            
            if (closedSources) {
             
                // new source declarations are rejected.
                throw new IllegalStateException();
                
            }
            
            sources.add(source);
            
            stats.fanIn++;

            if (DEBUG)
                log.debug("orderIndex=" + orderIndex + ", partitionId="
                        + partitionId + ", fanIn=" + stats.fanIn + ", fanOut="
                        + stats.fanOut);
            
        }

        final protected IBuffer<ISolution[]> getSolutionBuffer() {

            return solutionBuffer;

        }
        private final IBuffer<ISolution[]> solutionBuffer;
        
        /**
         * Sets a flag preventing new sources from being declared and closes all
         * known {@link #sources}.
         */
        protected void closeSources() {
            
            if(INFO)
                log.info(toString());
            
            closedSources = true;
            
            final IAsynchronousIterator[] a = sources
                    .toArray(new IAsynchronousIterator[] {});
            
            for( IAsynchronousIterator source : a ) {
                
                source.close();
                
            }
            
        }
        private volatile boolean closedSources = false;
        
        /**
         * Returns a chunk of {@link IBindingSet}s by combining chunks from the
         * various source {@link JoinTask}s.
         * 
         * @return A chunk assembled from one or more chunks from one or more of
         *         the source {@link JoinTask}s.
         */
        protected IBindingSet[] nextChunk() {

            if(DEBUG)
                log.debug("Reading chunk of bindings from source(s): orderIndex="
                                + orderIndex + ", partitionId=" + partitionId);
            
            // #of elements in the combined chunk(s)
            int bindingSetCount = 0;

            // source chunks read so far.
            final List<IBindingSet[]> chunks = new LinkedList<IBindingSet[]>();

            /*
             * Assemble a chunk of suitable size
             * 
             * @todo don't wait too long. if we have some data then it is
             * probably better to process that data rather than waiting beyond a
             * timeout for a full chunk. also, make sure that we are neither
             * yielding nor spinning too long in this loop. However, the loop
             * must wait if there is nothing available and the sources are not
             * yet exhausted.
             * 
             * @todo we need a different capacity here than the one used for
             * batch index operations. on the order of 100 should work well.
             */

            final int chunkCapacity = 100;// joinNexus.getChunkCapacity();

            System.err.print("\norderIndex="+orderIndex);
            
            while (!halt && !sources.isEmpty()
                    && bindingSetCount < chunkCapacity) {
                
                System.err.print(": reading");
                
//                if (DEBUG)
//                    log.debug("Testing " + nsources + " sources: orderIndex="
//                            + orderIndex + ", partitionId=" + partitionId);
                
                // clone to avoid concurrent modification of sources during traversal.
                final IAsynchronousIterator<IBindingSet[]>[] sources = 
                    (IAsynchronousIterator<IBindingSet[]>[]) this.sources.toArray(new IAsynchronousIterator[]{});

                // #of sources that are exhausted.
                int nexhausted = 0;

                for (int i = 0; i < sources.length
                        && bindingSetCount < chunkCapacity; i++) {

                    System.err.print(" <<"+i);

                    final IAsynchronousIterator<IBindingSet[]> src = sources[i];

                    // if there is something to read on that source.
                    if (src.hasNext(1L, TimeUnit.MILLISECONDS)) {
                        
                        /*
                         * Read the chunk, waiting up to the timeout for
                         * additional chunks from this source which can be
                         * combined together by the iterator into a single
                         * chunk.
                         */
                        final IBindingSet[] chunk = src.next(10L,
                                TimeUnit.MILLISECONDS);

                        /*
                         * Note: Since hasNext() returned [true] for this source
                         * we SHOULD get a chunk back since it is known to be
                         * there waiting for us. The timeout should only give
                         * the iterator an opportunity to combine multiple
                         * chunks together if they are already in the iterator's
                         * queue (or if they arrive in a timely manner).
                         */
                        assert chunk != null;
                            
                        chunks.add(chunk);

                        bindingSetCount += chunk.length;

                        System.err.print("["+chunk.length+"]");
                        
                        if (DEBUG)
                            log.debug("Read chunk from source: sources[" + i
                                    + "], chunkSize=" + chunk.length
                                    + ", orderIndex=" + orderIndex
                                    + ", partitionId=" + partitionId);
                        
                    } else if (src.isExhausted()) {

                        nexhausted++;

                        System.err.print("X{"+nexhausted+"}");
                        
                        if (DEBUG)
                            log.debug("Source is exhausted: nexhausted="+nexhausted);
                        
                        // no longer consider an exhausted source : @todo define src.equals()?
                        this.sources.remove(src);
                        
                    }

                }
                
                if (nexhausted == sources.length) {

                    /*
                     * All sources are exhausted, but we may have buffered some
                     * data, which is checked below.
                     */

                    break;
                    
                }

            }

            if (halt)
                throw new RuntimeException(firstCause.get());
            
            /*
             * Combine the chunks.
             */

            final int chunkCount = chunks.size();

            if( chunkCount == 0) {
                
                /*
                 * Termination condition: we did not get any data from any
                 * source.
                 * 
                 * Note: This implies that all sources are exhausted per the
                 * logic above.
                 */

                if (DEBUG)
                    log.debug("Sources are exhausted: orderIndex=" + orderIndex
                            + ", partitionId=" + partitionId);
                
                System.err.print(" exhausted");
                
                return null;
                
            }

            final IBindingSet[] chunk;

            if (chunkCount == 1) {

                // Only one chunk is available.

                chunk = chunks.get(0);

            } else {

                // Combine 2 or more chunks.

                chunk = new IBindingSet[bindingSetCount];

                final Iterator<IBindingSet[]> itr = chunks.iterator();

                int offset = 0;

                while (itr.hasNext()) {

                    final IBindingSet[] a = itr.next();

                    System.arraycopy(a, 0, chunk, offset, a.length);

                    offset += a.length;
                    
                }

            }
            
            if (halt)
                throw new RuntimeException(firstCause.get());

            if(DEBUG) {
            
                log.debug("Read chunk(s): nchunks=" + chunkCount
                        + ", #bindingSets=" + chunk.length + ", orderIndex="
                        + orderIndex + ", partitionId=" + partitionId);
            }

            stats.bindingSetChunksIn += chunkCount;
            stats.bindingSetsIn += bindingSetCount;

            System.err.print(" chunk["+chunk.length+"]");

            return chunk;

        }

        protected AbstractUnsynchronizedArrayBuffer<IBindingSet> newUnsyncOutputBuffer() {

            final AbstractUnsynchronizedArrayBuffer<IBindingSet> unsyncOutputBuffer;

            /*
             * On overflow, the generated binding sets are mapped across the
             * JoinTaskSink(s) for the target index partition(s).
             */

            final int chunkCapacity = fedJoinNexus.getChunkCapacity();

            if (lastJoin) {

                /*
                 * Accepted binding sets are flushed to the solution buffer.
                 */

                unsyncOutputBuffer = new UnsynchronizedSolutionBuffer<IBindingSet>(
                        this, fedJoinNexus, chunkCapacity);

            } else {

                /*
                 * Accepted binding sets are flushed to the next join dimension.
                 * 
                 * Note: The index is key-range partitioned. Each bindingSet
                 * will be mapped across the index partition(s) on which the
                 * generated access path for that bindingSet will have to read.
                 * There will be a JoinTask associated with each such index
                 * partition. That JoinTask will execute locally on the
                 * DataService which hosts that index partition.
                 */

                unsyncOutputBuffer = new UnsyncDistributedOutputBuffer<IBindingSet>(
                        fed, this, chunkCapacity);

            }

            return unsyncOutputBuffer;

        }
        
        /**
         * Notifies each sink that this {@link DistributedJoinTask} will no
         * longer generate new {@link IBindingSet} chunks and then waits for the
         * sink task(s) to complete.
         * <p>
         * Note: Closing the {@link BlockingBuffer} from which a sink
         * {@link JoinTask} is reading will cause the source iterator for that
         * sink task to eventually return <code>false</code> indicating that
         * it is exhausted (assuming that the sink keeps reading on the
         * iterator).
         * 
         * @throws InterruptedException
         *             if interrupted while awaiting the future for a sink.
         */
        @Override
        protected void flushAndCloseBuffersAndAwaitSinks() throws InterruptedException,
                ExecutionException {
            
            if (DEBUG)
                log.debug("orderIndex="
                        + orderIndex
                        + ", partitionId="
                        + partitionId
                        + (lastJoin ? ", lastJoin" : ", sinkCount="
                                + sinkCache.size()));

            /*
             * For the last join dimension the JoinTask instead writes onto the
             * [solutionBuffer]. For query, that is the shared solution buffer
             * and will be a proxied object. For mutation, that is a per
             * JoinTask buffer that writes onto the target relation. In the
             * latter case we MUST report the mutationCount returned by flushing
             * the solutionBuffer via JoinStats to the master.
             * 
             * Note: JoinTask#flushUnsyncBuffers() will already have been
             * invoked so all generated binding sets will already be in the sync
             * buffer ready for output.
             */
            if (lastJoin) {

                assert sinkCache == null;

                if (DEBUG)
                    log.debug("\nWill flush buffer containing "
                            + getSolutionBuffer().size() + " solutions.");
                
                final long counter = getSolutionBuffer().flush();

                if (DEBUG)
                    log.debug("\nFlushed buffer: mutationCount=" + counter);
                
                if (joinNexus.getAction().isMutation()) {

                    /*
                     * Apply mutationCount to the JoinStats so that it will be
                     * reported back to the JoinMasterTask.
                     */

                    stats.mutationCount.addAndGet(counter);

                }

            } else {

                /*
                 * Close sinks.
                 * 
                 * For all but the lastJoin, the buffers are writing onto the
                 * per-sink buffers. We flush and close those buffers now. The
                 * sink JoinTasks drain those buffers. Once the buffers are
                 * closed, the sink JoinTasks will eventually exhaust the
                 * buffers.
                 * 
                 * FIXME This should flush the buffers using a thread pool with
                 * one thread per sink. This will give better throughput when
                 * the fanOut is GT ONE (1).
                 */

                {

                    final Iterator<JoinTaskSink> itr = sinkCache.values()
                            .iterator();

                    while (itr.hasNext()) {

                        if (halt)
                            throw new RuntimeException(firstCause.get());

                        final JoinTaskSink sink = itr.next();

                        if (DEBUG)
                            log.debug("Closing sink: sink=" + sink
                                    + ", unsyncBufferSize="
                                    + sink.unsyncBuffer.size()
                                    + ", blockingBufferSize="
                                    + sink.blockingBuffer.size());

                        // flush to the blockingBuffer.
                        sink.unsyncBuffer.flush();

                        // close the blockingBuffer.
                        sink.blockingBuffer.close();

                    }
                
                }

                // Await sinks.
                {
                    
                    final Iterator<JoinTaskSink> itr = sinkCache.values()
                            .iterator();

                    while (itr.hasNext()) {

                        if (halt)
                            throw new RuntimeException(firstCause.get());

                        final JoinTaskSink sink = itr.next();

                        final Future f = sink.future;

                        if (DEBUG)
                            log.debug("Waiting for Future: sink=" + sink);

                        // will throw any exception from the sink's Future.
                        f.get();

                    }

                }

            } // else (lastJoin)

            if (DEBUG)
                log.debug("Done: orderIndex="
                        + orderIndex
                        + ", partitionId="
                        + partitionId
                        + (lastJoin ? "lastJoin" : ", sinkCount="
                                + sinkCache.size()));

        }
        
        /**
         * Cancel all {@link DistributedJoinTask}s that are sinks for this
         * {@link DistributedJoinTask}.
         */
        @Override
        protected void cancelSinks() {

            // no sinks.
            if(lastJoin) return;
            
            if (DEBUG)
                log.debug("orderIndex=" + orderIndex + ", partitionId="
                        + partitionId + ", sinkCount=" + sinkCache.size());

            final Iterator<JoinTaskSink> itr = sinkCache.values().iterator();

            while (itr.hasNext()) {

                final JoinTaskSink sink = itr.next();

                sink.unsyncBuffer.reset();

                sink.blockingBuffer.reset();

                sink.blockingBuffer.close();

                sink.future.cancel(true/* mayInterruptIfRunning */);
                
            }
            
            if (DEBUG)
                log.debug("Done: orderIndex=" + orderIndex + ", partitionId="
                        + partitionId + ", sinkCount=" + sinkCache.size());

        }

        /**
         * Return the sink on which we will write {@link IBindingSet} for the
         * index partition associated with the specified locator. The sink will
         * be backed by a {@link DistributedJoinTask} running on the
         * {@link IDataService} that is host to that index partition. The
         * scale-out index will be the scale-out index for the next
         * {@link IPredicate} in the evaluation order.
         * 
         * @param locator
         *            The locator for the index partition.
         * 
         * @return The sink.
         * 
         * @throws ExecutionException
         *             If the {@link JoinTaskFactoryTask} fails.
         * @throws InterruptedException
         *             If the {@link JoinTaskFactoryTask} is interrupted.
         * 
         * @todo Review this as a possible concurrency bottleneck. The operation
         *       can have significant latency since RMI is required on a cache
         *       miss to lookup or create the {@link JoinTask} on the target
         *       dataService. Therefore we should probably allow concurrent
         *       callers and establish a {@link NamedLock} that serializes
         *       callers seeking the {@link JoinTaskSink} for the same index
         *       partition identifier.
         */
        synchronized
        protected JoinTaskSink getSink(final PartitionLocator locator)
                throws InterruptedException, ExecutionException {

            JoinTaskSink sink = sinkCache.get(locator);

            if (sink == null) {

                /*
                 * Allocate JoinTask on the target data service and obtain a
                 * sink reference for its future and buffers.
                 * 
                 * Note: The JoinMasterTask uses very similar logic to setup the
                 * first join dimension.
                 */

                final int nextOrderIndex = orderIndex + 1;
                
                if (DEBUG)
                    log.debug("Creating join task: nextOrderIndex=" + nextOrderIndex
                            + ", indexName=" + nextScaleOutIndexName
                            + ", partitionId=" + locator.getPartitionId());
               
                final IDataService dataService = fed.getDataService(locator
                        .getDataServices()[0]);
                
                sink = new JoinTaskSink(fed, locator, this);
                
                // @todo JDS export async iterator proxy.
                final IAsynchronousIterator<IBindingSet[]> sourceItr = sink.blockingBuffer
                        .iterator();

                // the future for the factory task (not the JoinTask).
                final Future factoryFuture;
                try {
                    
                    // submit the factory task, obtain its future.
                    factoryFuture = dataService.submit(new JoinTaskFactoryTask(
                            nextScaleOutIndexName, rule, joinNexus.getJoinNexusFactory(),
                            order, nextOrderIndex, locator.getPartitionId(),
                            masterProxy, sourceItr, keyOrders));
                    
                } catch (IOException ex) {
                    
                    // RMI problem.
                    throw new RuntimeException(ex);
                    
                }

                /*
                 * Obtain the future for the JoinTask from the factory task's
                 * Future.
                 */

                sink.setFuture( (Future) factoryFuture.get() );
                
                stats.fanOut++;
                
                sinkCache.put(locator, sink);
               
            }

            return sink;

        }

    }

    /**
     * Unsynchronized buffer maps the {@link IBindingSet}s across the index
     * partition(s) for the target scale-out index when it overflows.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     *            The generic type of the elements in the buffer.
     */
    static class UnsyncDistributedOutputBuffer<E extends IBindingSet> extends
            AbstractUnsynchronizedArrayBuffer<E> {
        
        private final DistributedJoinTask joinTask;
        
        /** The evaluation order of the next predicate. */
        private final int nextOrderIndex;
        
        /** The tailIndex of the next predicate to be evaluated. */
        final int nextTailIndex;
        
        final IBigdataFederation fed;

        /**
         * 
         * @param fed
         * @param joinTask
         * @param capacity
         */
        public UnsyncDistributedOutputBuffer(final AbstractScaleOutFederation fed,
                final DistributedJoinTask joinTask, final int capacity) {

            super(capacity);

            if (fed == null)
                throw new IllegalArgumentException();

            if (joinTask == null)
                throw new IllegalArgumentException();
            
            this.fed = fed;
            
            this.joinTask = joinTask;
            
            this.nextOrderIndex = joinTask.orderIndex + 1;
            
            this.nextTailIndex = joinTask.getTailIndex(nextOrderIndex);

        }
        
        /**
         * Maps the chunk of {@link IBindingSet}s across the index partition(s)
         * for the sink join dimension.
         * 
         * @param a
         *            A chunk of {@link IBindingSet}s.
         * 
         * FIXME optimize locator lookup.
         * <p>
         * Note: We always use a read-consistent view for the join evaluation so
         * we are permitted to cache the locators just as much as we like.
         * <p>
         * When the buffer overflow()s, we generate the asBound() predicates,
         * SORT them by their [fromKey] (or its predicate level equivalence),
         * and process the sorted asBound() predicates. Since they are sorted
         * and since they are all for the same predicate pattern (optionals will
         * leave some variables unbound - does that cause a problem?) we know
         * that the first partitionId is GE to the last partitionId of the last
         * asBound predicate. We can test the rightSeparatorKey on the
         * PartitionLocator and immediately determine whether the asBound
         * predicate in fact starts and (and possibly ends) within the same
         * index partition. We only need to do a locatorScan when the asBound
         * predicate actually crosses into the next index partition, which could
         * also be handled by an MDI#find(key).
         */
        protected void handleChunk(final E[] chunk) {

            if (DEBUG)
                log.debug("chunkSize=" + chunk.length);
            
            int bindingSetsOut = 0;

            // the next predicate to be evaluated.
            final IPredicate nextPred = joinTask.rule.getTail(nextTailIndex);

            final IJoinNexus joinNexus = joinTask.joinNexus;

            final JoinStats stats = joinTask.stats;

            final int naccepted = chunk.length;

            for (int i = 0; i < naccepted; i++) {

                // an accepted binding set.
                final IBindingSet bindingSet = chunk[i];

                /*
                 * Locator scan for the index partitions for that predicate as
                 * bound.
                 */
                final Iterator<PartitionLocator> itr = joinNexus.locatorScan(
                        joinTask.fed, nextPred.asBound(bindingSet));

                while (itr.hasNext()) {

                    final PartitionLocator locator = itr.next();

                    if (DEBUG)
                        log
                                .debug("adding bindingSet to buffer: nextOrderIndex="
                                        + nextOrderIndex
                                        + ", partitionId="
                                        + locator.getPartitionId()
                                        + ", bindingSet=" + bindingSet);

                    // obtain sink JoinTask from cache or dataService.
                    final JoinTaskSink sink;
                    try {
                        sink = joinTask.getSink(locator);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    } catch (ExecutionException ex) {
                        throw new RuntimeException(ex);
                    }

                    // add binding set to the sink.
                    if (sink.unsyncBuffer.add2(bindingSet)) {

                        // another chunk out to this sink.
                        stats.bindingSetChunksOut++;

                    }

                    // #of bindingSets out across all sinks for this join task.
                    bindingSetsOut++;

                }

            }

            stats.bindingSetsOut += bindingSetsOut;

        }

    }

    /**
     * A factory for {@link DistributedJoinTask}s. The factory either creates a
     * new {@link DistributedJoinTask} or returns the pre-existing
     * {@link DistributedJoinTask} for the given {@link JoinMasterTask}
     * instance, orderIndex, and partitionId. The use of a factory pattern
     * allows us to concentrate all {@link DistributedJoinTask}s which target
     * the same tail predicate and index partition for the same rule execution
     * instance onto the same {@link DistributedJoinTask}. The concentrator
     * effect achieved by the factory only matters when the fan-out is GT ONE
     * (1). When the fan-out from the source join dimension is GT ONE(1), then
     * factory achieves an idential fan-in for the sink.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * FIXME The factory semantics requires something like a "session" concept
     * on the {@link DataService}. Whenever a {@link DistributedJoinTask} is
     * interrupted or errors it must make sure that the entry is removed from
     * the session. This could also interupt/cancel the remaining
     * {@link DistributedJoinTask}s for the same {masterInstance}, but we are
     * already doing that in a different way.
     * <p>
     * This should not be a problem for a single index partition since fan-in ==
     * fan-out == 1, but it will be a problem for larger fan-in/outs.
     * <p>
     * When the desired join task pre-exists, factory will need to invoke
     * {@link DistributedJoinTask#addSource(IAsynchronousIterator)} and specify
     * the {@link #sourceItrProxy} as another source for that join task.
     */
    public static class JoinTaskFactoryTask implements Callable<Future>,
            IDataServiceAwareProcedure, Serializable {

        /**
         * 
         */
        private static final long serialVersionUID = -2637166803787195001L;
        
        protected static final Logger log = Logger.getLogger(JoinTaskFactoryTask.class);

        /**
         * True iff the {@link #log} level is INFO or less.
         */
        protected static final boolean INFO = log.isInfoEnabled();

        /**
         * True iff the {@link #log} level is DEBUG or less.
         */
        protected static final boolean DEBUG = log.isDebugEnabled();

        final String scaleOutIndexName;
        
        final IRule rule;

        final IJoinNexusFactory joinNexusFactory;

        final int[] order;

        final int orderIndex;

        final int partitionId;

        final IJoinMaster masterProxy;

        final IAsynchronousIterator<IBindingSet[]> sourceItrProxy;
        
        final IKeyOrder[] keyOrders;

        /**
         * Set by the {@link DataService} which recognized that this class
         * implements the {@link IDataServiceAwareProcedure}.
         */
        private transient DataService dataService;
        
        public void setDataService(DataService dataService) {
            
            this.dataService = dataService;
            
        }

        public String toString() {

            return getClass().getSimpleName() + "{ orderIndex=" + orderIndex
                    + ", partitionId=" + partitionId + "}";
            
        }
        
        /**
         * 
         * @param scaleOutIndexName
         * @param rule
         * @param joinNexusFactory
         * @param order
         * @param orderIndex
         * @param partitionId
         * @param masterProxy
         * @param sourceItrProxy
         * @param nextScaleOutIndexName
         * 
         * @todo JDS There is no [sourceItrProxy] right now. Instead there is
         *       the local {@link IAsynchronousIterator} and there is the remote
         *       chunked iterator, which does not actually implement Iterator or
         *       {@link IAsynchronousIterator}. Perhaps this should be
         *       simplified into a think proxy object that does implement
         *       {@link IAsynchronousIterator} and magics the protocol
         *       underneath.
         */
        public JoinTaskFactoryTask(final String scaleOutIndexName,
                final IRule rule, final IJoinNexusFactory joinNexusFactory,
                final int[] order, final int orderIndex, final int partitionId,
                final IJoinMaster masterProxy,
                final IAsynchronousIterator<IBindingSet[]> sourceItrProxy,
                final IKeyOrder[] keyOrders) {
            
            if (scaleOutIndexName == null)
                throw new IllegalArgumentException();
            if (rule == null)
                throw new IllegalArgumentException();
            final int tailCount = rule.getTailCount();
            if (joinNexusFactory == null)
                throw new IllegalArgumentException();
            if (order == null)
                throw new IllegalArgumentException();
            if (order.length != tailCount)
                throw new IllegalArgumentException();
            if (orderIndex < 0 || orderIndex >= tailCount)
                throw new IllegalArgumentException();
            if (partitionId < 0)
                throw new IllegalArgumentException();
            if (sourceItrProxy == null)
                throw new IllegalArgumentException();
            if (keyOrders == null || keyOrders.length != order.length)
                throw new IllegalArgumentException();

            this.scaleOutIndexName = scaleOutIndexName;
            this.rule = rule;
            this.joinNexusFactory = joinNexusFactory;
            this.order = order;
            this.orderIndex = orderIndex;
            this.partitionId = partitionId;
            this.masterProxy = masterProxy;
            this.sourceItrProxy = sourceItrProxy;
            this.keyOrders = keyOrders;
            
        }
        
        public Future call() throws Exception {
            
            if (dataService == null)
                throw new IllegalStateException();

            final AbstractScaleOutFederation fed = (AbstractScaleOutFederation) dataService
                    .getFederation();

            /*
             * Note: This wrapper class passes getIndex(name,timestamp) to the
             * IndexManager for the DataService, which is the class that knows
             * how to assemble the index partition view.
             */
            final IIndexManager indexManager = new DelegateIndexManager(
                    dataService);
            
            final DistributedJoinTask task = new DistributedJoinTask(
                    scaleOutIndexName, rule, joinNexusFactory
                            .newInstance(indexManager), order, orderIndex,
                    partitionId, fed, masterProxy, sourceItrProxy, keyOrders);

            if (DEBUG) // @todo new vs locating existing JoinTask in session.
                log.debug("Submitting new JoinTask: orderIndex=" + orderIndex
                        + ", partitionId=" + partitionId + ", indexName="
                        + scaleOutIndexName);

            final Future f = dataService.getFederation().getExecutorService().submit(task);

            if (fed.isDistributed()) {

                // return a proxy for the future.
                return ((AbstractDistributedFederation) fed).getProxy(f);

            }

            // just return the future.
            return f;
            
        }

    }

    /**
     * The index view that we need for the {@link DistributedJoinTask} is on the
     * {@link IndexManager} class, not the live {@link ManagedJournal}. Looking
     * on the live journal we will only see the mutable {@link BTree} and not
     * the entire index partition view. However, {@link IndexManager} does not
     * implement {@link IIndexManager} or even {@link IIndexStore}. Therefore
     * this class was introduced. It passes most of the methods on to the
     * {@link IBigdataFederation} but {@link #getIndex(String, long)} is
     * delegated to {@link IndexManager#getIndex(String, long)} which is the
     * method that knows how to create the index partition view.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo While this class solves our problem I do not know whether or not
     *       this class should this class have more visibility? The downside is
     *       that it is a bit incoherent how it passes along one method to the
     *       {@link IndexManager}, most methods to the
     *       {@link IBigdataFederation} and disallows {@link #dropIndex(String)}
     *       and {@link #registerIndex(IndexMetadata)} in an attempt to stay out
     *       of trouble. That may be enough reason to keep it private.
     */
    private static class DelegateIndexManager implements IIndexManager {
        
        private final DataService dataService;
        
        public DelegateIndexManager(DataService dataService) {
            
            if (dataService == null)
                throw new IllegalArgumentException();
            
            this.dataService = dataService;
            
        }
        
        /**
         * Delegates to the {@link IndexManager}.
         */
        public IIndex getIndex(String name, long timestamp) {

            return dataService.getResourceManager().getIndex(name, timestamp);
            
        }

        /**
         * Not allowed.
         */
        public void dropIndex(String name) {
            
            throw new UnsupportedOperationException();
            
        }

        /**
         * Not allowed.
         */
        public void registerIndex(IndexMetadata indexMetadata) {

            throw new UnsupportedOperationException();
            
        }

        public void destroy() {
            
            throw new UnsupportedOperationException();
            
        }

        public ExecutorService getExecutorService() {
            
            return dataService.getFederation().getExecutorService();
            
        }

        public BigdataFileSystem getGlobalFileSystem() {

            return dataService.getFederation().getGlobalFileSystem();
            
        }

        public SparseRowStore getGlobalRowStore() {

            return dataService.getFederation().getGlobalRowStore();
            
        }

        public long getLastCommitTime() {

            return dataService.getFederation().getLastCommitTime();
            
        }

        public IResourceLocator getResourceLocator() {

            return dataService.getFederation().getResourceLocator();
            
        }

        public IResourceLockService getResourceLockService() {

            return dataService.getFederation().getResourceLockService();
            
        }

        public TemporaryStore getTempStore() {

            return dataService.getFederation().getTempStore();
            
        }
        
    }
    
}
