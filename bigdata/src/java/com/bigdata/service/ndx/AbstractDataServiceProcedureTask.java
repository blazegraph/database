package com.bigdata.service.ndx;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.btree.proc.IKeyArrayIndexProcedure;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.btree.proc.ISimpleIndexProcedure;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.service.DataService;
import com.bigdata.service.IDataService;
import com.bigdata.service.Split;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.TaskCounters;

/**
 * Helper class for submitting an {@link IIndexProcedure} to run on an
 * {@link IDataService}. The class traps {@link StaleLocatorException}s and
 * handles the redirection of requests to the appropriate {@link IDataService}.
 * When necessary, the data for an {@link IKeyArrayIndexProcedure} will be
 * re-split in order to distribute the requests to the new index partitions
 * following a split of the target index partition.
 * <p>
 * Note: If an index partition is moved then the key range is unchanged.
 * <p>
 * Note: If an index partition is split, then the key range for each new index
 * partition is a sub-range of the original key range and the total key range
 * for the new index partitions is exactly the original key range.
 * <p>
 * Note: If an index partition is joined with another index partition then the
 * key range of the new index partition is increased. However, we will wind up
 * submitting one request to the new index partition for each index partition
 * that we knew about at the time that the procedure was first mapped across the
 * index partitions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract class AbstractDataServiceProcedureTask implements Callable<Void> {

    /**
     * Note: Invocations of the non-batch API are logged at the WARN level since
     * they result in an application that can not scale-out efficiently.
     */
    protected static final transient Logger log = Logger
            .getLogger(AbstractDataServiceProcedureTask.class);

    /**
     * True iff the {@link #log} level is WARN or less.
     */
    final protected boolean WARN = log.getEffectiveLevel().toInt() <= Level.WARN
            .toInt();

    protected final IScaleOutClientIndex ndx;

    /**
     * The timestamp for the operation. This will be the timestamp for the view
     * (the outer class) unless the operation is read-only, in which case a
     * different timestamp may be chosen either to improve concurrency or to
     * provide globally read-consistent operations (in the latter cases this
     * will be a read-only transaction identifier).
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
    protected final IndexSyncRPCCounters taskCountersByIndex;

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

        return "index=" + ndx.getName() + ", ts=" + ts + ", procedure "
                + proc.getClass().getName() + " : " + split;

    }

    /**
     * Return the #of elements in the operation. This is used to update
     * {@link IndexSyncRPCCounters#elementsOut}.
     */
    abstract protected int getElementCount();
    
    /**
     * Variant used for procedures that are NOT instances of
     * {@link IKeyArrayIndexProcedure}.
     * 
     * @param ts
     * @param split
     * @param proc
     * @param resultHandler
     */
    public AbstractDataServiceProcedureTask(IScaleOutClientIndex ndx,
            final long ts, final Split split, final IIndexProcedure proc,
            final IResultHandler resultHandler) {

        if (ndx == null)
            throw new IllegalArgumentException();

        if (split.pmd == null)
            throw new IllegalArgumentException();

        if (!(split.pmd instanceof PartitionLocator)) {

            throw new IllegalArgumentException("Split does not have a locator");

        }

        if (proc == null)
            throw new IllegalArgumentException();

        this.ndx = ndx;

        this.ts = ts;

        this.split = split;

        this.proc = proc;

        this.resultHandler = resultHandler;

        this.taskCounters = ndx.getFederation().getTaskCounters();

        //            this.taskCountersByProc = fed.getTaskCounters(proc);
        //            
        this.taskCountersByIndex = ndx.getFederation().getIndexCounters(
                ndx.getName()).synchronousCounters;

        /*
         * This timestamp is set when the task is created since that is when the
         * request was submitted.
         */
        nanoTime_submitTask = System.nanoTime();

        if(proc.isReadOnly()) {
            synchronized(taskCountersByIndex) {
                taskCountersByIndex.readOnlyRequestCount++;
            }
        }
        
    }

    final public Void call() throws Exception {

        // the index partition locator.
        final PartitionLocator locator = (PartitionLocator) split.pmd;

        /*
         * Track the total inter-arrival time.
         */
        synchronized (taskCounters.lastArrivalNanoTime) {
            final long lastArrivalNanoTime = taskCounters.lastArrivalNanoTime
                    .get();
            final long now = System.nanoTime();
            final long delta = now - lastArrivalNanoTime;
            // cumulative inter-arrival time.
            taskCounters.interArrivalNanoTime.addAndGet(delta);
            // update timestamp of the last task arrival.
            taskCounters.lastArrivalNanoTime.set(now);
        }
        
        taskCounters.taskSubmitCount.incrementAndGet();
        //            taskCountersByProc.taskSubmitCount.incrementAndGet();
        taskCountersByIndex.taskSubmitCount.incrementAndGet();

        nanoTime_beginWork = System.nanoTime();
        final long queueWaitingTime = nanoTime_beginWork - nanoTime_submitTask;
        taskCounters.queueWaitingNanoTime.addAndGet(queueWaitingTime);
        //            taskCountersByProc.queueWaitingTime.addAndGet(queueWaitingTime);
        taskCountersByIndex.queueWaitingNanoTime.addAndGet(queueWaitingTime);
        synchronized (taskCountersByIndex) {
            taskCountersByIndex.elementsOut += getElementCount();
            taskCountersByIndex.requestCount++;
        }

        try {

            submit(locator);

            taskCounters.taskSuccessCount.incrementAndGet();
            //                taskCountersByProc.taskSuccessCount.incrementAndGet();
            taskCountersByIndex.taskSuccessCount.incrementAndGet();

        } catch (Exception ex) {

            taskCounters.taskFailCount.incrementAndGet();
            //                taskCountersByProc.taskFailCount.incrementAndGet();
            taskCountersByIndex.taskFailCount.incrementAndGet();

            throw ex;

        } finally {

            nanoTime_finishedWork = System.nanoTime();

            taskCounters.taskCompleteCount.incrementAndGet();
            //                taskCountersByProc.taskCompleteCount.incrementAndGet();
            taskCountersByIndex.taskCompleteCount.incrementAndGet();

            // increment by the amount of time that the task was executing.
            final long serviceNanoTime = nanoTime_finishedWork
                    - nanoTime_beginWork;
            taskCounters.serviceNanoTime.addAndGet(serviceNanoTime);
            //                taskCountersByProc.serviceNanoTime.addAndGet(serviceNanoTime);
            taskCountersByIndex.serviceNanoTime.addAndGet(serviceNanoTime);

            // increment by the total time from submit to completion.
            final long queuingNanoTime = nanoTime_finishedWork
                    - nanoTime_submitTask;
            taskCounters.queuingNanoTime.addAndGet(queuingNanoTime);
            //                taskCountersByProc.queuingNanoTime.addAndGet(queuingNanoTime);
            taskCountersByIndex.queuingNanoTime.addAndGet(queuingNanoTime);

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
        final IDataService dataService = ndx.getDataService(locator);

        if (dataService == null)
            throw new RuntimeException("DataService not found: " + locator);

        // the name of the index partition.
        final String name = DataService.getIndexPartitionName(//
                ndx.getName(), // the name of the scale-out index.
                split.pmd.getPartitionId() // the index partition identifier.
                );

        if (log.isInfoEnabled())
            log.info("Submitting task=" + this + " on " + dataService);

        try {

            submit(dataService, name);

        } catch (Exception ex) {

            if (causes == null)
                causes = new LinkedList<Throwable>();

            causes.add(ex);

            final StaleLocatorException cause = (StaleLocatorException) InnerCause
                    .getInnerCause(ex, StaleLocatorException.class);

            if (cause != null) {

                // notify the client so that it can refresh its cache.
                ndx.staleLocator(ts, locator, cause);

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
    final private void submit(final IDataService dataService, final String name)
            throws Exception {

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
