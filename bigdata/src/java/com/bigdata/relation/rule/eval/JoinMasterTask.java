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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.UnsynchronizedArrayBuffer;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.service.AbstractScaleOutFederation;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;
import com.bigdata.util.InnerCause;

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
 *       <p>
 *       The {@link JoinMasterTask} will wind up holding an exclusive lock on
 *       all index partitions when used for an unisolated operation such as
 *       computing the fix point of some set of rules. This is not really a
 *       problem, but it is worth noting.
 */
public class JoinMasterTask implements IStepTask, IJoinMaster {

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

    protected final int tailCount;

    protected final RuleState ruleState;

    protected final RuleStats ruleStats;

    /**
     * Per join dimension statistics aggregated across the {@link JoinTask}s
     * for each join dimension.
     * <p>
     * Note: index is the evaluation order of the predicates.
     */
    protected final JoinStats[] joinStats;

    /**
     * 
     * @param rule
     *            The rule to be executed.
     * @param joinNexus
     *            The {@link IJoinNexus}.
     * @param buffer
     *            The {@link ISolution} buffer. This is exported as a proxy
     *            object for query. However, it is ignored for mutation
     *            operations as each {@link JoinTask} for the last join
     *            dimension will obtain and write on its own solution buffer in
     *            order to avoid moving all data through the master.
     * 
     * @throws UnsupportedOperationException
     *             unless you are using a scale-out deployment (EDS or JDS) AND
     *             the execution context causes
     *             {@link IJoinNexus#getIndexManager()} to report the
     *             {@link AbstractScaleOutFederation} as the
     *             {@link IIndexManager}.
     */
    public JoinMasterTask(final IRule rule, final IJoinNexus joinNexus,
            final IBuffer<ISolution[]> buffer) {

        if (rule == null)
            throw new IllegalArgumentException();

        if (joinNexus == null)
            throw new IllegalArgumentException();

        this.rule = rule;

        this.joinNexus = joinNexus;

        this.tailCount = rule.getTailCount();

        if (!(joinNexus.getIndexManager() instanceof AbstractScaleOutFederation)) {
            
            /*
             * Either not running in a scale-out deployment or executed in a
             * context (such as within the ConcurrencyManager) where the
             * joinNexus will not report the federation as the index manager
             * object.
             */
            
            throw new UnsupportedOperationException();
            
        }
        
        // computes the eval order.
        this.ruleState = new RuleState(rule, joinNexus);

        // note: evaluation order is fixed by now.
        this.ruleStats = joinNexus.getRuleStatisticsFactory().newInstance(rule,
                ruleState.plan, ruleState.keyOrder);

        this.joinStats = new JoinStats[tailCount - 1];

        for (int orderIndex = 0; orderIndex < tailCount - 1; orderIndex++) {

            this.joinStats[orderIndex] = new JoinStats(orderIndex);

        }

        // @todo JDS export proxy for the master.
        masterProxy = this;
        
        if (joinNexus.getAction().isMutation()) {

            solutionBufferProxy = null;

        } else {

            /*
             * @todo JDS export proxy for the solution buffer. There will be
             * some API pain here for RMI IOExceptions.
             */

            solutionBufferProxy = joinNexus.newQueryBuffer();
            
        }

    }
    private final IBuffer<ISolution[]> solutionBufferProxy;
    private final IJoinMaster masterProxy;

    /**
     * Evaluate the rule.
     */
    public RuleStats call() throws Exception {

        if (ruleState.plan.isEmpty()) {

            if (INFO)
                log.info("Rule proven to have no solutions.");

            return ruleStats;

        }

        final long begin = System.currentTimeMillis();

        try {

            apply();

        } catch (Throwable t) {

            if (InnerCause.isInnerCause(t, InterruptedException.class) ||
                InnerCause.isInnerCause(t, ClosedByInterruptException.class)
                ) {

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

        // aggregate statistics from each join dimension. 
        combineJoinStats();
        
        if (INFO) {

            log.info("done:" + "\nruleState=" + ruleState + ruleStats);

        }

        return ruleStats;

    }
    
    /**
     * Create and run the {@link JoinTask}(s) that will evaluate the first join
     * dimension.
     * <p>
     * A {@link JoinTask} is created on the {@link DataService} for each index
     * partition that is spanned by the {@link IAccessPath} for the first
     * {@link IPredicate} in the evaluation order.
     */
    final protected void apply() throws Exception {
        
        // the initial bindings (might not be empty since constants can be bound).
        final IBindingSet initialBindings = joinNexus.newBindingSet(rule);
        
        /*
         * The first predicate in the evaluation order with the initial
         * bindings applied.
         */
        final IPredicate predicate = rule.getTail(ruleState.order[0])
                .asBound(initialBindings);

        if (predicate.isOptional()) {

            /*
             * @todo OPTIONAL is not supported yet for this JOIN technique.
             */

            throw new UnsupportedOperationException();

        }

        final Iterator<PartitionLocator> itr = joinNexus.locatorScan(
                (AbstractScaleOutFederation) joinNexus.getIndexManager(),
                predicate);

        while (itr.hasNext()) {

            final PartitionLocator locator = itr.next();

            final int partitionId = locator.getPartitionId();

            // the join task will read from this buffer.
            final BlockingBuffer<IBindingSet[]> buffer = new BlockingBuffer<IBindingSet[]>(
                    1/* capacity */);

            // add the initial binding set to the buffer.
            buffer.add(new IBindingSet[] { initialBindings });

            /*
             * FIXME create on target data service using factory.
             * 
             * @todo JDS export buffer.iterator() or just send a serializable
             * thick object since there is only a single binding set!
             */
            new JoinTask(rule, joinNexus, ruleState.order, ruleState.keyOrder,
                    0/* orderIndex */, partitionId,
                    (AbstractScaleOutFederation) joinNexus.getIndexManager(),
                    masterProxy, buffer.iterator()).call();

        }

    }

    /**
     * Aggregates statistics from each join dimension onto {@link #ruleStats}.
     */
    protected void combineJoinStats() {
        
        for (int tailIndex = 0; tailIndex < tailCount; tailIndex++) {

            final JoinStats o = joinStats[ruleState.order[tailIndex]];
            
            ruleStats.chunkCount[tailIndex] += o.chunkCount;
            
            ruleStats.elementCount[tailIndex] += o.elementCount;
            
        }
        
    }
    
    /**
     * Aggregates the statistics for some join dimension.
     * 
     * @param joinStats
     *            Statistics for an index partition of some join dimension.
     */
    public void report(final JoinStats joinStats) {

        if (INFO) {

            log.info("joinStats: orderIndex=" + joinStats.orderIndex
                    + ", partitionId=" + joinStats.partitionId);
            
        }
        
        final JoinStats total = this.joinStats[joinStats.orderIndex];

        total.add(joinStats);

    }
    
    public IBuffer<ISolution[]> getSolutionBuffer() throws IOException {
        
        if (joinNexus.getAction().isMutation()) {

            throw new UnsupportedOperationException();
            
        }

        return solutionBufferProxy;
        
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

        /** #of chunks visited over all access paths. */
        public long chunkCount;

        /** #of elements visited over all chunks. */
        public long elementCount;

        /**
         * The #of {@link IBindingSet}s written onto the next join dimension
         * (aka the #of solutions written iff this is the last join dimension).
         */
        public long bindingSetsOut;

        /**
         * The #of {@link IBindingSet} chunks written onto the next join
         * dimension (aka the #of solutions written iff this is the last join
         * dimension in the evaluation order).
         */
        public long bindingSetChunksOut;

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

            bindingSetChunksIn = bindingSetsIn = accessPathCount = 0L;

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
            this.chunkCount += o.chunkCount;
            this.elementCount += o.elementCount;
            this.bindingSetsOut += o.bindingSetsOut;
            this.bindingSetChunksOut += o.bindingSetChunksOut;

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

        /**
         * The future may be used to cancel or interrupt the downstream
         * {@link JoinTask}.
         */
        final Future future;

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

        /**
         * Obtains the {@link Future} for a downstream {@link JoinTask} running
         * in the {@link ConcurrencyManager} for the specified index partition.
         * 
         * @param fed
         * @param locator
         * @param name
         *            The name of the index on which the join will read.
         * @param self
         *            The current join dimension.
         */
        public JoinTaskSink(IBigdataFederation fed, PartitionLocator locator,
                String namespace, JoinTask self) {

            this.locator = locator;

            final IJoinNexus joinNexus = self.joinNexus;

            this.blockingBuffer = new BlockingBuffer<IBindingSet[]>(joinNexus
                    .getChunkOfChunksCapacity());

            this.unsyncBuffer = new UnsynchronizedArrayBuffer<IBindingSet>(
                    blockingBuffer, joinNexus.getChunkCapacity());

            /*
             * FIXME must create the task using a factory pattern on the target
             * data service and return its future. The JoinTask needs to be able
             * to do the same thing. Maybe the AbstractStepTask can be reused fo
             * this? Basically, we need to submit a task that will be run by the
             * data service (and is data service aware) and that creates an
             * AbstractTask that is then submitted to the ConcurrencyManager.
             */
            this.future = null;

            throw new UnsupportedOperationException();

        }

    }

    /**
     * Consumes {@link IBindingSet} chunks from the previous join dimension.
     * <p>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo must be created inside of the execution context on the server. task
     *       is NOT serializable since {@link IJoinNexus} is not serializable.
     */
    static public class JoinTask implements Callable {

        /** The rule that is being evaluated. */
        final IRule rule;

        /**
         * The #of predicates in the tail of that rule.
         */
        final int tailCount;

        /**
         * The index partition on which this {@link JoinTask} is reading.
         */
        final int partitionId;

        /**
         * The tail index in the rule for the predicate on which we are reading
         * for this join dimension.
         */
        final int tailIndex;

        /**
         * The {@link IPredicate} on which we are reading for this join
         * dimension.
         */
        final IPredicate predicate;
        
        /**
         * The evaluation order for the predicate on which we are reading for
         * this join dimension.
         */
        final int orderIndex;

        /**
         * <code>true</code> iff this is the last join dimension in the
         * evaluation order.
         */
        final boolean lastJoin;

        /**
         * A proxy for the remote {@link JoinMasterTask}.
         */
        final IJoinMaster master;
        
        /**
         * The federation is used to obtain locator scans for the access paths.
         */
        final AbstractScaleOutFederation fed;

        /**
         * The {@link IJoinNexus}. The {@link IIndexManager} MUST have access
         * to the local index objects (this class MUST be run inside of the
         * {@link ConcurrencyManager}).
         */
        final transient IJoinNexus joinNexus;

        /**
         * The evaluation order for the rule.
         * 
         * @todo we only need the evaluation order for this join dimension and
         *       the next.
         */
        final int[] order;

        /**
         * The key order for each predicate in the rule.
         * 
         * @todo we only need the key order for this join dimension and the
         *       next, and in fact we may not need this at all.
         */
        final IKeyOrder[] keyOrder;

        /**
         * The statistics for this {@link JoinTask}.
         */
        final JoinStats stats;

        /**
         * Sources for {@link IBindingSet} chunks that will be processed by this
         * {@link JoinTask}. There will be one such source for each upstream
         * {@link JoinTask} that targets this {@link JoinTask}.
         * 
         * @todo The {@link IBindingSet}s from the different sources should be
         *       collected into chunks, ordered, and duplicates should be
         *       eliminated.
         *       <p>
         *       The {@link IAccessPath}s that are generated from these
         *       {@link IBindingSet} should be collected into chunks, ordered by
         *       their fromKey, and processed in that order. This will improve
         *       index read performance since we will tend to do ordered reads
         *       even when the #of elements visited by any given
         *       {@link IAccessPath} is very small.
         *       <p>
         *       Note: Duplicate elimination is probably not that important as
         *       long as we are processing the {@link IAccessPath}s in order.
         */
        final Vector<IAsynchronousIterator<IBindingSet[]>> sources = new Vector<IAsynchronousIterator<IBindingSet[]>>();

        /**
         * The {@link JoinTaskSink}s for the downstream {@link JoinTask}s onto
         * which the generated {@link IBindingSet}s will be written.
         * 
         * @todo configure capacity based on expectations of index partition
         *       fan-out for this join dimension
         */
        final private Map<PartitionLocator, JoinTaskSink> sinkCache = new LinkedHashMap<PartitionLocator, JoinTaskSink>();

        /**
         * Return the sink on which we will write {@link IBindingSet} for the
         * index partition associated with the specified locator. The sink will
         * be backed by a {@link JoinTask} running on the {@link IDataService}
         * that is host to that index partition.
         * 
         * @param locator
         *            The locator for the index partition.
         *            
         * @return The sink.
         */
        protected JoinTaskSink getSink(PartitionLocator locator) {

            JoinTaskSink sink = sinkCache.get(locator);
            
            if(sink == null) {
                
                /*
                 * FIXME allocate JoinTask on the target data service and obtain
                 * a sink reference for its future and buffers. The master also
                 * needs to use the same logic. Look at reuse of the
                 * AbstractStepTask for this purpose.
                 */

                stats.fanOut++;
               
            }

            assert sink != null;
            
            return sink;

        }

        /**
         * The buffer on which the last predicate in the evaluation order will
         * write its {@link ISolution}s.
         * 
         * @return The buffer.
         * 
         * @throws IllegalStateException
         *             unless {@link #lastJoin} is <code>true</code>.
         */
        final IBuffer<ISolution[]> getSolutionBuffer() {

            if (!lastJoin)
                throw new IllegalStateException();
            
            if (solutionBuffer == null) {

                switch (joinNexus.getAction()) {

                case Insert: {

                    final IMutableRelation relation = (IMutableRelation) joinNexus
                            .getTailRelationView(predicate);

                    solutionBuffer = joinNexus.newInsertBuffer(relation);

                    break;

                }

                case Delete: {

                    final IMutableRelation relation = (IMutableRelation) joinNexus
                            .getTailRelationView(predicate);

                    solutionBuffer = joinNexus.newDeleteBuffer(relation);

                    break;

                }

                case Query:

                    try {
                        
                        solutionBuffer = master.getSolutionBuffer();
                        
                    } catch(IOException ex) {
                    
                        throw new RuntimeException(ex);

                    }
                    
                }

            }

            return solutionBuffer;

        }

        private IBuffer<ISolution[]> solutionBuffer;
        
        /**
         * Return the index of the tail predicate to be evaluated at the given
         * index in the evaluation order.
         * 
         * @param orderIndex
         *            The evaluation order index.
         * @return The tail index to be evaluated at that index in the
         *         evaluation order.
         */
        final protected int getTailIndex(int orderIndex) {

            final int tailIndex = order[orderIndex];

            assert orderIndex >= 0 && orderIndex < tailCount : "orderIndex="
                    + orderIndex + ", rule=" + rule;

            return tailIndex;

        }

        /**
         * 
         * @param rule
         * @param joinNexus
         * @param order
         * @param keyOrder
         * @param orderIndex
         * @param src
         * 
         * FIXME Instances of this class MUST be created in the appropriate
         * execution context of the target {@link DataService} so that the
         * federation and the joinNexus references are both correct and so that
         * it has access to the local index object for the specified index
         * partition.
         */
        public JoinTask(final IRule rule, final IJoinNexus joinNexus,
                final int[] order, final IKeyOrder[] keyOrder,
                final int orderIndex, final int partitionId,
                final AbstractScaleOutFederation fed, 
                final IJoinMaster master,
                final IAsynchronousIterator<IBindingSet[]> src) {

            // @todo test args.

            this.rule = rule;
            this.partitionId = partitionId;
            this.tailCount = rule.getTailCount();
            this.orderIndex = orderIndex;
            this.tailIndex = getTailIndex(orderIndex);
            this.lastJoin = ((orderIndex + 1) == tailCount);
            this.predicate = rule.getTail(tailIndex);
            this.joinNexus = joinNexus;
            this.order = order;
            this.keyOrder = keyOrder;
            this.stats = new JoinStats(partitionId, orderIndex);
            this.fed = fed;
            this.master = master;
            this.sources.add(src);

        }

        public Object call() throws Exception {

            try {

                // until closed or interrupted.
                if(true)
                    while (true/*FIXME termination condition?*/) {

                    final IAsynchronousIterator<IBindingSet[]>[] sources = (IAsynchronousIterator<IBindingSet[]>[]) this.sources
                            .toArray();

                    for (IAsynchronousIterator<IBindingSet[]> src : sources) {

                        // if there is something read on that source.
                        if (src.hasNext(1L, TimeUnit.MILLISECONDS)) {

                            // read the chunk.
                            final IBindingSet[] chunk = src.next();

                            for (IBindingSet bindingSet : chunk) {

                                new AccessPathTask(bindingSet);

                            }

                        }

                    }

                }

                /*
                 * Flush all buffers, close them and wait for the sinks to
                 * complete.
                 * 
                 * FIXME The above loop does not terminate. We need a way to
                 * make it terminate, eg., when the task is interrupted.
                 * 
                 * However we also need to differentiate between an interrupt
                 * and a cancel if we would like to halt processing after a
                 * SLICE has been satisified (or may be the query solution
                 * buffer proxy can return the #of solutions added so far so
                 * that we can halt each join task on the last join dimension in
                 * a relatively timely manner producing no more than one chunk
                 * too many).
                 */ 
                closeBuffersAndAwaitSinks();
                
                return null;

            } catch(Throwable t) {
                
                // cancel any downstream tasks.
                cancelSinks();
                
                throw new RuntimeException(t);
                
            } finally {

                master.report(stats);

            }

        }

        /**
         * Flush all buffers, close them and wait for the sinks to complete.
         * <p>
         * Note: Closing the {@link BlockingBuffer} will cause its iterator to
         * eventually return false indicating that it is exhausted (assuming
         * that the sink keeps reading on the iterator).
         */
        protected void closeBuffersAndAwaitSinks() {

            final Iterator<JoinTaskSink> itr = sinkCache.values().iterator();

            while (itr.hasNext()) {

                final JoinTaskSink sink = itr.next();

                sink.blockingBuffer.close();
                
            }

            // FIXME await completion of all tasks!
            throw new UnsupportedOperationException();
            
        }
        
        protected void cancelSinks() {

            final Iterator<JoinTaskSink> itr = sinkCache.values().iterator();

            while (itr.hasNext()) {

                final JoinTaskSink sink = itr.next();

                sink.future.cancel(true/* mayInterruptIfRunning */);
                
            }
            
        }
        
        /**
         * Return the {@link IAccessPath} for the tail predicate to be evaluated
         * at the given index in the evaluation order.
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

            final IPredicate predicate = rule.getTail(tailIndex).asBound(
                    bindingSet);

            if (predicate.isOptional()) {

                /*
                 * @todo OPTIONAL is not supported yet for this JOIN technique.
                 */

                throw new UnsupportedOperationException();

            }

            final IAccessPath accessPath = joinNexus
                    .getTailAccessPath(predicate);

            if (DEBUG) {

                log.debug("orderIndex=" + orderIndex + ", tailIndex="
                        + tailIndex + ", tail=" + rule.getTail(tailIndex)
                        + ", bindingSet=" + bindingSet + ", accessPath="
                        + accessPath);

            }

            return accessPath;

        }

        /**
         * Accepts an {@link IBindingSet}, obtains the corresponding
         * {@link IAccessPath} and pairs the {@link IBindingSet} in turn with
         * each element visited by that {@link IAccessPath}, generating a new
         * {@link IBindingSet} each time. If the new {@link IBindingSet} is
         * consistent with the {@link IRule}, then it is added to the
         * {@link JoinTaskSink}(s) for the index partition(s) on which the next
         * join dimension will have to read for the new {@link IBindingSet}.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        class AccessPathTask implements Callable {

            final private IBindingSet bindingSet;

            final private IAccessPath accessPath;

            public AccessPathTask(IBindingSet bindingSet) {

                this.bindingSet = bindingSet;

                this.accessPath = getAccessPath(orderIndex, bindingSet);

            }

            public Object call() throws Exception {

                apply(orderIndex, bindingSet);

                return null;

            }

            /**
             * Evaluate a join dimension.
             * 
             * @param orderIndex
             *            The current index in the evaluation order[] that is
             *            being scanned.
             * @param bindingSet
             *            The bindings from the prior join(s) (if any).
             * 
             * @todo is there any advantage to re-order the chunk into the key
             *       order for the next join dimension? What we really need is
             *       to re-order the generated bindings and the next join
             *       dimension should be doing that, not this one.
             *       <p>
             *       Review for {@link NestedSubqueryWithJoinThreadsTask} and
             *       drop the IKeyOrder[] if we do not need it here.
             */
            final protected void apply(final int orderIndex,
                    final IBindingSet bindingSet) throws InterruptedException {

                // Obtain the iterator for the current join dimension.
                final IChunkedOrderedIterator itr = accessPath.iterator();

                try {

                    while (itr.hasNext()) {

                        final Object[] chunk = itr.nextChunk();

                        stats.chunkCount++;

                        // process the chunk.
                        new ChunkTask(bindingSet, chunk).run();

                    } // while

                } finally {

                    itr.close();

                }

            }

        }

        /**
         * Task processes a chunk of elements read from the access path for a
         * join dimension. Each element in the chunk in paired with a copy of
         * the given bindings and the resulting bindings are buffered into
         * chunks and the chunks added to the
         * {@link JoinPipelineTask#bindingSetBuffers} for the corresponding
         * predicate.
         * 
         * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
         *         Thompson</a>
         * @version $Id$
         */
        class ChunkTask implements Runnable {

            /**
             * The index of the predicate for the access path that is being
             * consumed.
             */
            private final int tailIndex;

            /**
             * The bindings with which the each element in the chunk will be
             * paired to create the bindings for the downstream join dimension.
             */
            private final IBindingSet bindingSet;

            /**
             * A chunk of elements read from the {@link IAccessPath} for the
             * current join dimension.
             */
            private final Object[] chunk;

            /**
             * A buffer that is used to collect {@link IBindingSet}s into
             * chunks before handing them off to the next join dimension. This
             * is sized to the chunk that we are processing.
             */
            private final IBindingSet[] accepted;

            /**
             * The #of {@link IBindingSet}s that have been accepted so far for
             * the current chunk.
             */
            private int naccepted;

            /**
             * 
             * @param bindingSet
             *            The bindings with which the each element in the chunk
             *            will be paired to create the bindings for the
             *            downstream join dimension.
             * @param chunk
             *            A chunk of elements read from the {@link IAccessPath}
             *            for the current join dimension.
             */
            public ChunkTask(final IBindingSet bindingSet, final Object[] chunk) {

                this.tailIndex = getTailIndex(orderIndex);

                this.bindingSet = bindingSet;

                this.chunk = chunk;

                this.accepted = new IBindingSet[chunk.length];

            }

            public void run() {

                for (Object e : chunk) {

                    if (DEBUG) {
                        log.debug("Considering: " + e.toString()
                                + ", tailIndex=" + orderIndex + ", rule="
                                + rule.getName());
                    }

                    stats.elementCount++;

                    // clone the binding set.
                    final IBindingSet bset = bindingSet.clone();

                    // propagate bindings from the visited element.
                    if (joinNexus.bind(rule, tailIndex, e, bset)) {

                        // accept this binding set.
                        accepted[naccepted++] = bset;

                    }

                }

                if (lastJoin) {

                    /*
                     * Generate solutions for accepted binding sets and flush
                     * them to the solution buffer for the rule evaluation as a
                     * whole.
                     */

                    flushToSolutionBuffer();

                } else {

                    /*
                     * Identify the target JoinTask for each binding set and add
                     * the binding set to the JoinTaskSink for that JoinTask.
                     */

                    flushToJoinTaskSinks();

                }

            }

            /**
             * Generate {@link ISolution}s for the accepted {@link IBindingSet}s
             * and add those those {@link ISolution}s to an
             * {@link UnsynchronizedArrayBuffer} for the {@link JoinTask}. This
             * gives us nice sized chunks in the
             * {@link UnsynchronizedArrayBuffer}. The
             * {@link UnsynchronizedArrayBuffer} will overflow onto the
             * {@link ISolution} buffer for the rule. For query, that will be an
             * (exported proxy for) the {@link IJoinNexus#newQueryBuffer()}
             * created by the {@link JoinMasterTask}. For mutation, that will
             * be a buffer created for the {@link JoinTask} instance (this
             * avoids have all data for mutation flow through the master).
             */
            protected void flushToSolutionBuffer() {

                final UnsynchronizedArrayBuffer<ISolution> buffer = new UnsynchronizedArrayBuffer<ISolution>(
                        getSolutionBuffer(), naccepted);

                for (int i = 0; i < naccepted; i++) {

                    final ISolution solution = joinNexus.newSolution(rule,
                            bindingSet);

                    buffer.add(solution);

                }

                buffer.flush();

                stats.bindingSetChunksOut++;
                stats.bindingSetsOut += naccepted;

            }

            /**
             * Add each accepted {@link IBindingSet} to the input buffer for the
             * {@link JoinTaskSink}(s) target index partition(s) for the
             * {@link IAccessPath} that the downstream {@link JoinTask}(s) will
             * use to process those bindings.
             * <p>
             * Note: The caller is assumed to be single-threaded!
             */
            protected void flushToJoinTaskSinks() {

                // the tailIndex of the next predicate to be evaluated.
                final int nextTailIndex = getTailIndex(orderIndex + 1);

                // the next predicate to be evaluated.
                final IPredicate nextPred = rule.getTail(nextTailIndex);

                for (int i = 0; i < naccepted; i++) {

                    // an accepted binding set.
                    final IBindingSet bindingSet = accepted[i];

                    // locator scan for the index partitions for that predicate as bound.
                    final Iterator<PartitionLocator> itr = joinNexus
                            .locatorScan(fed, nextPred.asBound(bindingSet));

                    while (itr.hasNext()) {

                        final PartitionLocator locator = itr.next();

                        // add binding set to the sink.
                        getSink(locator).unsyncBuffer.add(bindingSet);

                    }

                }

                stats.bindingSetsOut += naccepted;

            }

        }

    }

}
