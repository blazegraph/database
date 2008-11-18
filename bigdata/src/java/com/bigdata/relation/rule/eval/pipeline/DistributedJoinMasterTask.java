package com.bigdata.relation.rule.eval.pipeline;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.ISolutionExpander;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.service.AbstractDistributedFederation;
import com.bigdata.service.AbstractScaleOutFederation;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IClientIndex;
import com.bigdata.service.IDataService;
import com.bigdata.service.proxy.RemoteBuffer;
import com.bigdata.util.concurrent.ExecutionExceptions;

/**
 * Implementation for distributed join execution.
 * <p>
 * Note: For query, this object MUST be executed locally on the client. This
 * ensures that all data flows back to the client directly. For mutation, it
 * is possible to submit this object to any service in the federation and
 * each {@link DistributedJoinTask} will write directly on the scale-out
 * view of the target {@link IMutableRelation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DistributedJoinMasterTask extends JoinMasterTask implements
        Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 7096223893807015958L;

    /**
     * The proxy for this {@link DistributedJoinMasterTask}.
     */
    private final IJoinMaster masterProxy;

    /**
     * The proxy for the solution buffer (query only).
     * <p>
     * Note: The query buffer is always an {@link IBlockingBuffer}. The
     * client has the {@link IAsynchronousIterator} that drains the
     * {@link BlockingBuffer}. The master is local to the client so that
     * data from the distributed join tasks flows directly to the client.
     * <p>
     * Note: The reason why we do not use a {@link RemoteBuffer} for
     * mutation is that it would cause all data to flow through the master!
     * Instead each {@link JoinTask} for the last join dimension uses its
     * own buffer to aggregate and write on the target
     * {@link IMutableRelation}.
     */
    private final IBuffer<ISolution[]> solutionBufferProxy;

    /**
     * For queries, the master MUST execute locally to the client. If the
     * master were to be executed on a remote {@link DataService} then that
     * would cause the {@link #getSolutionBuffer()} to be created on the
     * remote service and all query results would be forced through that
     * remote JVM before being streamed back to the client.
     * <p>
     * This is not a problem when the rule is a mutation operation since
     * the individual join tasks will each allocate their own buffer that
     * writes on the target {@link IMutableRelation}.
     * 
     * @throws UnsupportedOperationException
     *             if the operation is a query.
     */
    private void writeObject(java.io.ObjectOutputStream out) throws IOException {

        if (!joinNexus.getAction().isMutation()) {

            throw new UnsupportedOperationException(
                    "Join master may not be executed remotely for query.");

        }

        out.defaultWriteObject();

    }

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
    public DistributedJoinMasterTask(IRule rule, IJoinNexus joinNexus,
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

        if (joinNexus.getAction().isMutation()) {

            /*
             * Check constraints on executing mutation operations.
             * 
             * Note: These constraints arise from (a) the need to flush
             * solutions onto relations that may also be in the body of the
             * fule; and (b) the need to avoid stale locators when writing
             * on those relations.
             */

            if (!TimestampUtility
                    .isHistoricalRead(joinNexus.getReadTimestamp())) {

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

                JoinMasterTask.log.warn("Unisolated scale-out query");

            }

        }

        /*
         * Export proxies?
         * 
         * Note: We need proxies if the federation is really distributed and
         * using RMI to communicate.
         * 
         * @todo do we need distributed garbarge collection for these
         * proxies?
         */
        if (joinNexus.getIndexManager() instanceof AbstractDistributedFederation) {

            final AbstractDistributedFederation fed = (AbstractDistributedFederation) joinNexus
                    .getIndexManager();

            masterProxy = (IJoinMaster) fed
                    .getProxy(this, true/* enableDGC */);

            if (joinNexus.getAction().isMutation()) {

                // mutation.
                solutionBufferProxy = null;

            } else {

                // query - export proxy for the solution buffer.
                solutionBufferProxy = fed.getProxy(solutionBuffer);

            }

        } else {

            /*
             * Not really distributed, so just use the actual reference.
             */

            masterProxy = this;

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
        final IPredicate predicate = rule.getTail(order[0]).asBound(bindingSet);

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

            if (JoinMasterTask.DEBUG)
                JoinMasterTask.log.debug("Will submit JoinTask: partitionId="
                        + partitionId);

            /*
             * Note: Since there is only a single binding set, we send a
             * serializable thick iterator to the client.
             */
            final ThickAsynchronousIterator<IBindingSet[]> sourceItr = newBindingSetIterator(bindingSet);

            final JoinTaskFactoryTask factoryTask = new JoinTaskFactoryTask(
                    scaleOutIndexName, rule, joinNexusFactory, order,
                    0/* orderIndex */, partitionId, masterProxy, sourceItr,
                    ruleState.getKeyOrder());

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

        if (JoinMasterTask.DEBUG)
            JoinMasterTask.log.debug("#futures=" + size);

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

            if (JoinMasterTask.DEBUG)
                JoinMasterTask.log.debug("Waiting for factoryTask");

            // wait for the JoinTaskFactoryTask to finish.
            final Future<? extends Object> joinTaskFuture;

            try {

                if (!causes.isEmpty()) {

                    /*
                     * We have to abort, so cancel the factory task in case
                     * it is still running but fall through and try to get
                     * its future in case it has already created the join
                     * task.
                     */

                    factoryTaskFuture.cancel(true/* mayInterruptIfRunning */);

                }

                //                    log.fatal("\nWaiting on factoryTaskFuture: "+factoryTaskFuture);
                joinTaskFuture = (Future<? extends Object>) factoryTaskFuture
                        .get();
                //                    log.fatal("\nHave joinTaskFuture: "+joinTaskFuture);

            } catch (ExecutionException ex) {

                causes.add(ex);

                /*
                 * Note: This is here because the ExecutionExceptions that
                 * we throw does not print out all of its stack traces.
                 * 
                 * @todo log iff unexpected exception class or get all
                 * traces from ExecutionExceptions class.
                 */
                JoinMasterTask.log.error(ex, ex);

                continue;

            }

            if (causes.isEmpty()) {

                // no errors yet, so remeber the future for the join task.
                joinTaskFutures.add(joinTaskFuture);

            } else {

                // cancel the join task since we have to abort anyway.
                joinTaskFuture.cancel(true/* mayInterruptIfRunning */);

            }

            ndone++;

            if (JoinMasterTask.DEBUG)
                JoinMasterTask.log.debug("ndone=" + ndone + " of " + size);

        }

        if (!causes.isEmpty()) {

            for (Future f : joinTaskFutures) {

                // cancel since we have to abort anyway.
                f.cancel(true/* mayInterruptIfRunning */);

            }

            throw new ExecutionExceptions(causes);

        }

        if (JoinMasterTask.DEBUG)
            JoinMasterTask.log
                    .debug("All factory tasks done: #futures=" + size);

        return joinTaskFutures;

    }

}
