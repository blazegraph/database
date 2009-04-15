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
 * Created on Apr 1, 2009
 */

package com.bigdata.service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.AbstractKeyRangeIndexProcedure;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.btree.proc.IKeyArrayIndexProcedure;
import com.bigdata.btree.proc.IKeyRangeIndexProcedure;
import com.bigdata.btree.proc.IParallelizableIndexProcedure;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.btree.proc.ISimpleIndexProcedure;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.TaskCounters;

/**
 * Abstract class encapsulating MOST of the logic for executing tasks
 * corresponding to client index operations. {@link StaleLocatorException}s are
 * handled by the recursive application of the various <code>submit()</code>
 * methods.
 * <p>
 * A concrete subclass must implement {@link #runTasks(boolean, ArrayList)}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractScaleOutClientIndexView2 extends
        AbstractScaleOutClientIndexView {

    /**
     * Create a view on a scale-out index.
     * 
     * @param fed
     *            The federation containing the index.
     * @param name
     *            The index name.
     * @param timestamp
     *            A transaction identifier, {@link ITx#UNISOLATED} for the
     *            unisolated index view, {@link ITx#READ_COMMITTED}, or
     *            <code>timestamp</code> for a historical view no later than
     *            the specified timestamp.
     * @param metadataIndex
     *            The {@link IMetadataIndex} for the named scale-out index as of
     *            that timestamp. Note that the {@link IndexMetadata} on this
     *            object contains the template {@link IndexMetadata} for the
     *            scale-out index partitions.
     */
    public AbstractScaleOutClientIndexView2(AbstractScaleOutFederation fed,
            String name, long timestamp, IMetadataIndex metadataIndex) {

        super(fed, name, timestamp, metadataIndex);
        
    }

    /**
     * 
     * @see #getRecursionDepth()
     */
    final private ThreadLocal<AtomicInteger> recursionDepth = new ThreadLocal<AtomicInteger>() {
   
        protected synchronized AtomicInteger initialValue() {
        
            return new AtomicInteger();
            
        }
        
    };

    /**
     * Return a {@link ThreadLocal} {@link AtomicInteger} whose value is the
     * recursion depth of the current {@link Thread}. This is initially zero
     * when the task is submitted by the application. The value incremented when
     * a task results in a {@link StaleLocatorException} and is decremented when
     * returning from the recursive handling of the
     * {@link StaleLocatorException}.
     * <p>
     * The recursion depth MAY be used to:
     * <ol>
     * <li>Limit the #of retries due to {@link StaleLocatorException}s for a
     * split of a task submitted by the application</li>
     * <li>Force execution of retried tasks in the caller's thread.</li>
     * </ol>
     * When the thread pool size si bounded, the latter point becomes critical.
     * Under that condition, if the retry tasks are run in the client
     * {@link #getThreadPool() thread pool} then all threads in the pool can
     * rapidly become busy awaiting retry tasks with the result that the client
     * is essentially deadlocked.
     * 
     * @return The recursion depth.
     */
    final protected AtomicInteger getRecursionDepth() {

        return recursionDepth.get();
        
    }

    /**
     * Runs set of tasks.
     * 
     * @param parallel
     *            <code>true</code> iff the tasks MAY be run in parallel.
     * @param tasks
     *            The tasks to be executed.
     */
    abstract protected void runTasks(final boolean parallel,
            final ArrayList<AbstractDataServiceProcedureTask> tasks);

    /**
     * Variant uses the caller's timestamp.
     * 
     * @param ts
     * @param key
     * @param proc
     * @return
     */
    protected Object submit(final long ts, final byte[] key,
            final ISimpleIndexProcedure proc) {

        // Find the index partition spanning that key.
        final PartitionLocator locator = fed.getMetadataIndex(name, ts).find(
                key);

        /*
         * Submit procedure to that data service.
         */
        try {

            if (INFO) {

                log.info("Submitting " + proc.getClass() + " to partition"
                        + locator);

            }

            // required to get the result back from the procedure.
            final IResultHandler resultHandler = new IdentityHandler();

            final SimpleDataServiceProcedureTask task = new SimpleDataServiceProcedureTask(
                    key, ts, new Split(locator, 0, 0), proc, resultHandler);

            // submit procedure and await completion.
            getThreadPool().submit(task).get(taskTimeout, TimeUnit.MILLISECONDS);

            // the singleton result.
            final Object result = resultHandler.getResult();

            return result;

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }

    /**
     * Variant uses the caller's timestamp.
     * 
     * @param ts
     * @param fromKey
     * @param toKey
     * @param proc
     * @param resultHandler
     */
    protected void submit(final long ts, final byte[] fromKey,
            final byte[] toKey, final IKeyRangeIndexProcedure proc,
            final IResultHandler resultHandler) {

        // true iff the procedure is known to be parallelizable.
        final boolean parallel = proc instanceof IParallelizableIndexProcedure;

        if (INFO)
            log.info("Procedure " + proc.getClass().getName()
                    + " will be mapped across index partitions in "
                    + (parallel ? "parallel" : "sequence"));

        final int poolSize = ((ThreadPoolExecutor) getThreadPool())
                .getCorePoolSize();

        final int maxTasksPerRequest = fed.getClient()
                .getMaxParallelTasksPerRequest();

        // max #of tasks to queue at once.
        final int maxTasks = poolSize == 0 ? maxTasksPerRequest : Math.min(
                poolSize, maxTasksPerRequest);

        // verify positive or the loop below will fail to progress.
        assert maxTasks > 0 : "maxTasks=" + maxTasks + ", poolSize=" + poolSize
                + ", maxTasksPerRequest=" + maxTasksPerRequest;

        /*
         * Scan visits index partition locators in key order.
         * 
         * Note: We are using the caller's timestamp.
         */
        final Iterator<PartitionLocator> itr = locatorScan(ts, fromKey, toKey,
                false/* reverseScan */);

        long nparts = 0;

        while (itr.hasNext()) {

            /*
             * Process the remaining locators a "chunk" at a time. The chunk
             * size is choosen to be the configured size of the client thread
             * pool. This lets us avoid overwhelming the thread pool queue when
             * mapping a procedure across a very large #of index partitions.
             * 
             * The result is an ordered list of the tasks to be executed. The
             * order of the tasks is determined by the natural order of the
             * index partitions - that is, we submit the tasks in key order so
             * that a non-parallelizable procedure will be mapped in the correct
             * sequence.
             */

            final ArrayList<AbstractDataServiceProcedureTask> tasks = new ArrayList<AbstractDataServiceProcedureTask>(
                    maxTasks);

            for (int i = 0; i < maxTasks && itr.hasNext(); i++) {

                final PartitionLocator locator = itr.next();

                final Split split = new Split(locator, 0/* fromIndex */, 0/* toIndex */);

                tasks.add(new KeyRangeDataServiceProcedureTask(fromKey, toKey,
                        ts, split, proc, resultHandler));

                nparts++;

            }

            runTasks(parallel, tasks);

        } // next (chunk of) locators.

        if (INFO)
            log.info("Procedure " + proc.getClass().getName()
                    + " mapped across " + nparts + " index partitions in "
                    + (parallel ? "parallel" : "sequence"));

    }

    /**
     * Variant uses the caller's timestamp.
     * 
     * @param ts
     * @param fromIndex
     * @param toIndex
     * @param keys
     * @param vals
     * @param ctor
     * @param aggregator
     */
    protected void submit(final long ts, final int fromIndex, final int toIndex,
            final byte[][] keys, final byte[][] vals,
            final AbstractKeyArrayIndexProcedureConstructor ctor,
            final IResultHandler aggregator) {

        /*
         * Break down the data into a series of "splits", each of which will be
         * applied to a different index partition.
         * 
         * Note: We are using the caller's timestamp here so this will have
         * read-consistent semantics!
         */

        final LinkedList<Split> splits = splitKeys(ts, fromIndex, toIndex, keys);

        final int nsplits = splits.size();

        /*
         * Create the instances of the procedure for each split.
         */

        final ArrayList<AbstractDataServiceProcedureTask> tasks = new ArrayList<AbstractDataServiceProcedureTask>(
                nsplits);

        final Iterator<Split> itr = splits.iterator();

        boolean parallel = false;
        while (itr.hasNext()) {

            final Split split = itr.next();

            final IKeyArrayIndexProcedure proc = ctor.newInstance(this,
                    split.fromIndex, split.toIndex, keys, vals);

            if (proc instanceof IParallelizableIndexProcedure) {

                parallel = true;

            }

            tasks.add(new KeyArrayDataServiceProcedureTask(keys, vals, ts,
                    split, proc, aggregator, ctor));

        }

        if (INFO)
            log.info("Procedures created by " + ctor.getClass().getName()
                    + " will run on " + nsplits + " index partitions in "
                    + (parallel ? "parallel" : "sequence"));

        runTasks(parallel, tasks);

    }

    /**
     * Helper class for submitting an {@link IIndexProcedure} to run on an
     * {@link IDataService}. The class traps {@link StaleLocatorException}s
     * and handles the redirection of requests to the appropriate
     * {@link IDataService}. When necessary, the data for an
     * {@link IKeyArrayIndexProcedure} will be re-split in order to distribute
     * the requests to the new index partitions following a split of the target
     * index partition.
     * <p>
     * Note: If an index partition is moved then the key range is unchanged.
     * <p>
     * Note: If an index partition is split, then the key range for each new
     * index partition is a sub-range of the original key range and the total
     * key range for the new index partitions is exactly the original key range.
     * <p>
     * Note: If an index partition is joined with another index partition then
     * the key range of the new index partition is increased. However, we will
     * wind up submitting one request to the new index partition for each index
     * partition that we knew about at the time that the procedure was first
     * mapped across the index partitions.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected abstract class AbstractDataServiceProcedureTask implements
            Callable<Void> {

        /**
         * The timestamp for the operation. This will be the timestamp for the
         * view (the outer class) unless the operation is read-only, in which
         * case a different timestamp may be choosen either to improve
         * concurrency or to provide globally read-consistent operations (in the
         * latter cases this will be a read-only transaction identifier).
         */
        protected final long ts;
        /**
         * The split assigned to this instance of the procedure.
         */
        protected final Split split;
        /**
         * The procedure to be executed against that {@link #split}.
         */
        protected final IIndexProcedure proc;
        /**
         * Used to aggregate results when a procedure can span more than one
         * index partition.
         */
        protected final IResultHandler resultHandler;
        private final TaskCounters taskCounters;
//        protected final TaskCounters taskCountersByProc;
//        protected final TaskCounters taskCountersByIndex;
        
        private long nanoTime_submitTask;
        private long nanoTime_beginWork;
        private long nanoTime_finishedWork;
        
        /**
         * If the task fails then this will be populated with an ordered list of
         * the exceptions. There will be one exception per-retry of the task.
         * For some kinds of failure this list MAY remain unbound.
         */
        protected List<Throwable> causes = null;
        
        /**
         * A human friendly representation.
         */
        public String toString() {

            return "Index=" + AbstractScaleOutClientIndexView2.this.getName()
                    + ", Procedure " + proc.getClass().getName() + " : "
                    + split;
            
        }

        /**
         * Variant used for procedures that are NOT instances of
         * {@link IKeyArrayIndexProcedure}.
         * 
         * @param ts
         * @param split
         * @param proc
         * @param resultHandler
         */
        public AbstractDataServiceProcedureTask(final long ts,
                final Split split, final IIndexProcedure proc,
                final IResultHandler resultHandler) {
            
            if (split.pmd == null)
                throw new IllegalArgumentException();
            
            if(!(split.pmd instanceof PartitionLocator)) {
                
                throw new IllegalArgumentException("Split does not have a locator");
                
            }

            if (proc == null)
                throw new IllegalArgumentException();

            this.ts = ts;
            
            this.split = split;
            
            this.proc = proc;

            this.resultHandler = resultHandler;

            this.taskCounters = fed.getTaskCounters();
            
//            this.taskCountersByProc = fed.getTaskCounters(proc);
//            
//            this.taskCountersByIndex = fed.getTaskCounters(ClientIndexView.this);

            nanoTime_submitTask = System.nanoTime();
            
        }
        
        final public Void call() throws Exception {

            // the index partition locator.
            final PartitionLocator locator = (PartitionLocator) split.pmd;

            taskCounters.taskSubmitCount.incrementAndGet();
//            taskCountersByProc.taskSubmitCount.incrementAndGet();
//            taskCountersByIndex.taskSubmitCount.incrementAndGet();

            nanoTime_beginWork = System.nanoTime();
            final long queueWaitingTime = nanoTime_beginWork - nanoTime_submitTask;
            taskCounters.queueWaitingNanoTime.addAndGet(queueWaitingTime);
//            taskCountersByProc.queueWaitingTime.addAndGet(queueWaitingTime);
//            taskCountersByIndex.queueWaitingTime.addAndGet(queueWaitingTime);
            
            try {

                submit(locator);
                
                taskCounters.taskSuccessCount.incrementAndGet();
//                taskCountersByProc.taskSuccessCount.incrementAndGet();
//                taskCountersByIndex.taskSuccessCount.incrementAndGet();

            } catch(Exception ex) {

                taskCounters.taskFailCount.incrementAndGet();
//                taskCountersByProc.taskFailCount.incrementAndGet();
//                taskCountersByIndex.taskFailCount.incrementAndGet();

                throw ex;
                
            } finally {

                nanoTime_finishedWork = System.nanoTime();

                taskCounters.taskCompleteCount.incrementAndGet();
//                taskCountersByProc.taskCompleteCount.incrementAndGet();
//                taskCountersByIndex.taskCompleteCount.incrementAndGet();

                // increment by the amount of time that the task was executing.
                final long serviceNanoTime = nanoTime_finishedWork - nanoTime_beginWork;
                taskCounters.serviceNanoTime.addAndGet(serviceNanoTime);
//                taskCountersByProc.serviceNanoTime.addAndGet(serviceNanoTime);
//                taskCountersByIndex.serviceNanoTime.addAndGet(serviceNanoTime);

                // increment by the total time from submit to completion.
                final long queuingNanoTime = nanoTime_finishedWork - nanoTime_submitTask;
                taskCounters.queuingNanoTime.addAndGet(queuingNanoTime);
//                taskCountersByProc.queuingNanoTime.addAndGet(queuingNanoTime);
//                taskCountersByIndex.queuingNanoTime.addAndGet(queuingNanoTime);

            }
            
            return null;

        }

        /**
         * Submit the procedure to the {@link IDataService} identified by the
         * locator.
         * 
         * @param locator
         *            An index partition locator.
         *            
         * @throws Exception
         */
        final protected void submit(final PartitionLocator locator)
                throws Exception {
            
            if (locator == null)
                throw new IllegalArgumentException();
            
            if (Thread.interrupted())
                throw new InterruptedException();
            
            // resolve service UUID to data service.
            final IDataService dataService = getDataService(locator);

            if (dataService == null)
                throw new RuntimeException("DataService not found: " + locator);
            
            // the name of the index partition.
            final String name = DataService.getIndexPartitionName(//
                    AbstractScaleOutClientIndexView2.this.name, // the name of the scale-out index.
                    split.pmd.getPartitionId() // the index partition identifier.
                    );

            if (INFO)
                log.info("Submitting task=" + this + " on " + dataService);

            try {

                submit(dataService, name);

            } catch(Exception ex) {

                if (causes == null)
                    causes = new LinkedList<Throwable>();
                
                causes.add(ex);
                
                final StaleLocatorException cause = (StaleLocatorException) InnerCause
                        .getInnerCause(ex, StaleLocatorException.class);
                
                if(cause != null) {

                    // notify the client so that it can refresh its cache.
                    staleLocator(ts, locator, cause);
                    
                    // retry the operation.
                    retry();
                    
                } else {
                    
                    throw ex;
                    
                }
                
            }
            
        }
        
        /**
         * Submit the procedure to the {@link IDataService} and aggregate the
         * result with the caller's {@link IResultHandler} (if specified).
         * 
         * @param dataService
         *            The data service on which the procedure will be executed.
         * @param name
         *            The name of the index partition on that data service.
         * 
         * @todo do not require aggregator for {@link ISimpleIndexProcedure} so
         *       make this abstract in the base class or just override for
         *       {@link ISimpleIndexProcedure}. This would also mean that
         *       {@link #call()} would return the value for
         *       {@link ISimpleIndexProcedure}.
         */
        @SuppressWarnings("unchecked")
        final private void submit(final IDataService dataService,
                final String name) throws Exception {

            /*
             * Note: The timestamp here is the one specified for the task. This
             * allows us to realize read-consistent procedures by choosing a
             * read-only transaction based on the lastCommitTime of the
             * federation for the procedure.
             */

            final Object result = dataService.submit(ts, name, proc).get();

            if (resultHandler != null) {

                resultHandler.aggregate(result, split);

            }

        }
        
        /**
         * Invoked when {@link StaleLocatorException} was thrown. Since the
         * procedure was being run against an index partition of some scale-out
         * index this exception indicates that the index partition locator was
         * stale. We re-cache the locator(s) for the same key range as the index
         * partition which we thought we were addressing and then re-map the
         * operation against those updated locator(s). Note that a split will go
         * from one locator to N locators for a key range while a join will go
         * from N locators for a key range to 1. A move does not change the #of
         * locators for the key range, just where that index partition is living
         * at this time.
         * 
         * @throws Exception
         */
        abstract protected void retry() throws Exception;

    }

    /**
     * Class handles stale locators by finding the current locator for the
     * <i>key</i> and redirecting the request to execute the procedure on the
     * {@link IDataService} identified by that locator.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class SimpleDataServiceProcedureTask extends AbstractDataServiceProcedureTask {

        protected final byte[] key;
        
        private int ntries = 1;
        
        /**
         * @param key
         * @param split
         * @param proc
         * @param resultHandler
         */
        public SimpleDataServiceProcedureTask(final byte[] key, long ts,
                final Split split, final ISimpleIndexProcedure proc,
                final IResultHandler resultHandler) {

            super(ts, split, proc, resultHandler);
            
            if (key == null)
                throw new IllegalArgumentException();
        
            this.key = key;
            
        }

        /**
         * The locator is stale. We locate the index partition that spans the
         * {@link #key} and re-submit the request.
         */
        @Override
        protected void retry() throws Exception {
            
            if (ntries++ > fed.getClient().getMaxStaleLocatorRetries()) {

                throw new RuntimeException("Retry count exceeded: ntries="
                        + ntries);

            }

            /*
             * Note: uses the metadata index for the timestamp against which the
             * procedure is running.
             */
            final PartitionLocator locator = fed.getMetadataIndex(name, ts)
                    .find(key);

            if (INFO)
                log.info("Retrying: proc=" + proc.getClass().getName()
                        + ", locator=" + locator + ", ntries=" + ntries);

            /*
             * Note: In this case we do not recursively submit to the outer
             * interface on the client since all we need to do is fetch the
             * current locator for the key and re-submit the request to the data
             * service identified by that locator.
             */

            submit(locator);
            
        }
        
    }

    /**
     * Handles stale locators for {@link IKeyRangeIndexProcedure}s.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class KeyRangeDataServiceProcedureTask extends AbstractDataServiceProcedureTask {

        final byte[] fromKey;
        final byte[] toKey;
        
        /**
         * @param fromKey
         * @param toKey
         * @param split
         * @param proc
         * @param resultHandler
         */
        public KeyRangeDataServiceProcedureTask(final byte[] fromKey,
                final byte[] toKey, final long ts, final Split split,
                final IKeyRangeIndexProcedure proc,
                final IResultHandler resultHandler) {

            super(ts, split, proc, resultHandler);
            
            /*
             * Constrain the range to the index partition. This constraint will
             * be used if we discover that the locator data was stale in order
             * to discover the new locator(s).
             */
            
            this.fromKey = AbstractKeyRangeIndexProcedure.constrainFromKey(
                    fromKey, split.pmd);

            this.toKey = AbstractKeyRangeIndexProcedure.constrainFromKey(toKey,
                    split.pmd);
            
        }

        /**
         * The {@link IKeyRangeIndexProcedure} is re-mapped for the constrained
         * key range of the stale locator using
         * {@link ClientIndexViewRefactorResourceQueues#submit(byte[], byte[], IKeyRangeIndexProcedure, IResultHandler)}.
         */
        @Override
        protected void retry() throws Exception {

            /*
             * Note: recursive retries MUST run in the same thread in order to
             * avoid deadlock of the client's thread pool. The recursive depth
             * is used to enforce this constrain.
             */

            final int depth = getRecursionDepth().incrementAndGet();

            try {
            
                if (depth > fed.getClient().getMaxStaleLocatorRetries()) {

                    throw new RuntimeException("Retry count exceeded: ntries="
                            + depth);
                    
                }
                
                /*
                 * Note: This MUST use the timestamp already assigned for this
                 * operation but MUST compute new splits against the updated
                 * locators.
                 */
                AbstractScaleOutClientIndexView2.this.submit(ts, fromKey, toKey,
                        (IKeyRangeIndexProcedure) proc, resultHandler);
            
            } finally {
                
                final int tmp = getRecursionDepth().decrementAndGet();
                
                assert tmp >= 0 : "depth="+depth+", tmp="+tmp;
                
            }
            
        }
        
    }

    /**
     * Handles stale locators for {@link IKeyArrayIndexProcedure}s. When
     * necessary the procedure will be re-split.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class KeyArrayDataServiceProcedureTask extends AbstractDataServiceProcedureTask {

        protected final byte[][] keys;
        protected final byte[][] vals;
        protected final AbstractKeyArrayIndexProcedureConstructor ctor;
        
        /**
         * Variant used for {@link IKeyArrayIndexProcedure}s.
         * 
         * @param keys
         *            The original keys[][].
         * @param vals
         *            The original vals[][].
         * @param split
         *            The split identifies the subset of keys and values to be
         *            applied by this procedure.
         * @param proc
         *            The procedure instance.
         * @param resultHandler
         *            The result aggregator.
         * @param ctor
         *            The object used to create instances of the <i>proc</i>.
         *            This is used to re-split the data if necessary in response
         *            to stale locator information.
         */
        public KeyArrayDataServiceProcedureTask(final byte[][] keys,
                final byte[][] vals, final long ts, final Split split,
                final IKeyArrayIndexProcedure proc,
                final IResultHandler resultHandler,
                final AbstractKeyArrayIndexProcedureConstructor ctor) {
            
            super( ts, split, proc, resultHandler );
            
            if (ctor == null)
                throw new IllegalArgumentException();

            this.ctor = ctor;
            
            this.keys = keys;
            
            this.vals = vals;
            
        }

        /**
         * Submit using
         * {@link ClientIndexViewRefactorResourceQueues#submit(int, int, byte[][], byte[][], AbstractKeyArrayIndexProcedureConstructor, IResultHandler)}.
         * This will recompute the split points and re-map the procedure across
         * the newly determined split points.
         */
        @Override
        protected void retry() throws Exception {

            /*
             * Note: recursive retries MUST run in the same thread in order to
             * avoid deadlock of the client's thread pool. The recursive depth
             * is used to enforce this constraint.
             */

            final int depth = getRecursionDepth().incrementAndGet();

            try {
            
                if (depth > fed.getClient().getMaxStaleLocatorRetries()) {

                    throw new RuntimeException("Retry count exceeded: ntries="
                            + depth);
                    
                }
                
                /*
                 * Note: This MUST use the timestamp already assigned for this
                 * operation but MUST compute new splits against the updated
                 * locators.
                 */
                AbstractScaleOutClientIndexView2.this.submit(ts, split.fromIndex, split.toIndex,
                        keys, vals, ctor, resultHandler);
                
            } finally {
                
                final int tmp = getRecursionDepth().decrementAndGet();
                
                assert tmp >= 0 : "depth="+depth+", tmp="+tmp;
                
            }
            
        }
        
    }
    
}
