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

package com.bigdata.service.ndx;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.IKeyArrayIndexProcedure;
import com.bigdata.btree.proc.IKeyRangeIndexProcedure;
import com.bigdata.btree.proc.IParallelizableIndexProcedure;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.btree.proc.ISimpleIndexProcedure;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.service.AbstractScaleOutFederation;
import com.bigdata.service.Split;

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

    final public AtomicInteger getRecursionDepth() {

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
                    this, key, ts, new Split(locator, 0, 0), proc,
                    resultHandler);

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

                tasks.add(new KeyRangeDataServiceProcedureTask(this, fromKey, toKey,
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

            tasks.add(new KeyArrayDataServiceProcedureTask(this, keys, vals, ts,
                    split, proc, aggregator, ctor));

        }

        if (INFO)
            log.info("Procedures created by " + ctor.getClass().getName()
                    + " will run on " + nsplits + " index partitions in "
                    + (parallel ? "parallel" : "sequence"));

        runTasks(parallel, tasks);

    }
    
}
