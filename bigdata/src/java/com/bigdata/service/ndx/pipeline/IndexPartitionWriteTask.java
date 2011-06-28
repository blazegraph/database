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
 * Created on Apr 16, 2009
 */

package com.bigdata.service.ndx.pipeline;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.proc.IAsyncResultHandler;
import com.bigdata.btree.proc.IKeyArrayIndexProcedure;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.service.DataService;
import com.bigdata.service.IDataService;
import com.bigdata.service.Split;
import com.bigdata.util.InnerCause;

/**
 * Class drains a {@link BlockingBuffer} writing on a specific index
 * partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexPartitionWriteTask<//
H extends IndexAsyncWriteStats<L, HS>, //
O extends Object, //
E extends KVO<O>, //
L extends PartitionLocator, //
S extends IndexPartitionWriteTask, //
HS extends IndexPartitionWriteStats,//
M extends IndexWriteTask<H, O, E, S, L, HS, T, R, A>,//
T extends IKeyArrayIndexProcedure,//
R,//
A//
> extends AbstractSubtask<HS, M, E, L> {

    /**
     * The data service on which the index partition resides.
     */
    public final IDataService dataService;

    /**
     * The timestamp associated with the index view.
     */
    public final long timestamp;

    /**
     * The index partition identifier.
     */
    public final int partitionId;

    /**
     * The name of the index partition.
     */
    private final String indexPartitionName;

    public String toString() {

        return getClass().getName() + "{indexPartition=" + indexPartitionName
                + ", open=" + buffer.isOpen() + "}";

    }

    public IndexPartitionWriteTask(final M master,
            final L locator, final IDataService dataService,
            final BlockingBuffer<E[]> buffer) {

        super(master,locator,buffer);
        
        if (dataService == null)
            throw new IllegalArgumentException();

        this.dataService = dataService;

        this.timestamp = master.ndx.getTimestamp();

        this.partitionId = locator.getPartitionId();

        this.indexPartitionName = DataService.getIndexPartitionName(master.ndx
                .getName(), partitionId);

    }

    /**
     * {@inheritDoc}
     * 
     * Maps the chunk across the sinks for the index partitions on which the
     * element in that chunk must be written.
     * 
     * @param sourceChunk
     *            A chunk (in sorted order).
     * 
     * @return <code>true</code> iff a {@link StaleLocatorException} was
     *         handled, in which case the task should exit immediately.
     * 
     * @throws IOException
     *             RMI error.
     * @throws ExecutionException
     * @throws InterruptedException
     */
    protected boolean handleChunk(final E[] sourceChunk)
            throws ExecutionException, InterruptedException, IOException {

        /*
         * Remove duplicates in a caller specified manner (may be a NOP).
         * 
         * Note: Duplicate removal is applied in a single-threaded context by
         * the sink against an unchanging KVO[] chunk. Concurrency problems can
         * not arise from duplicate elimination.
         */
        final KVO<O>[] chunk;
        final int duplicateCount;
        if (master.duplicateRemover == null) {
            
            chunk = sourceChunk;
            
            duplicateCount = 0;
            
        } else {
            
            // filter out duplicates.
            chunk = master.duplicateRemover.filter(sourceChunk);
            
            // #of duplicates that were filtered out.
            duplicateCount = sourceChunk.length - chunk.length;

            if (duplicateCount > 0 && log.isDebugEnabled())
                log.debug("Filtered out " + duplicateCount
                        + " duplicates from " + chunk.length + " elements");
            
        }

        if (chunk.length == 0) {

            // empty chunk after duplicate elimination (keep reading).
            return false;

        }

        // size of the chunk to be processed.
        final int chunkSize = chunk.length;

        /*
         * Change the shape of the data for RMI.
         */

        final boolean sendValues = master.ctor.sendValues();

        final byte[][] keys = new byte[chunkSize][];

        final byte[][] vals = sendValues ? new byte[chunkSize][] : null;

        for (int i = 0; i < chunkSize; i++) {

            keys[i] = chunk[i].key;

            if (sendValues)
                vals[i] = chunk[i].val;

        }

        /*
         * Instantiate the procedure using the data from the chunk and submit it
         * to be executed on the DataService using an RMI call.
         */
        final long beginNanos = System.nanoTime();
        try {

            final T proc = master.ctor.newInstance(master.ndx,
                    0/* fromIndex */, chunkSize/* toIndex */, keys, vals);

            /*
             * @todo isolate this as a retry policy, but note that we need to be
             * able to indicate when the error is fatal, when the error was
             * handled by a redirect and hence the sink should close, and when
             * the error was handled by a successful retry.
             */
            R result = null;
            boolean done = false;
            final int maxtries = 3;
//            final long retryDelayNanos = TimeUnit.MILLISECONDS.toNanos(1000);
            for (int ntries = 0; ntries < maxtries; ntries++) {

                // submit and await Future
                try {

                    result = ((Future<R>) dataService.submit(timestamp,
                            indexPartitionName, proc)).get();
                    done = true;
                    break;
                } catch (ExecutionException ex) {

                    final StaleLocatorException cause = (StaleLocatorException) InnerCause
                            .getInnerCause(ex, StaleLocatorException.class);

                    if (cause != null) {

                        // Handle a stale locator.
                        handleRedirect((E[]) chunk, cause);

                        // done.
                        return true;

                    } else {

                        if (ntries + 1 < maxtries) {

                            log.error("Will retry (" + ntries + " of "
                                    + maxtries + "): " + this, ex);

                            continue;

                        }

                        log.fatal(this, ex);

                        throw ex;

                    }

                }
            }
            if (!done) {
                // should not reach this point.
                throw new AssertionError();
            }
             
            if (master.resultHandler != null) {

                // aggregate results.
                master.resultHandler.aggregate(result, new Split(locator,
                        0/* fromIndex */, chunkSize/* toIndex */));

                if(master.resultHandler instanceof IAsyncResultHandler) {

                    // aggregate results.
                    ((IAsyncResultHandler<R, A, O, KVO<O>>) master.resultHandler)
                            .aggregateAsync(chunk, result, new Split(locator,
                                    0/* fromIndex */, chunkSize/* toIndex */));

                }
                
            }

            if (log.isDebugEnabled())
                log.debug(stats);

            /*
             * Since the chunk was successfully written, we now rip through the
             * KVOs and invoke done() on each. If there are duplicates and
             * KVOList was used, then done() will be mapped over each duplicate
             * as well.
             */
            for (int i = 0; i < chunkSize; i++) {

                chunk[i].done();

            }

            // keep reading.
            return false;

        } finally {

            final long elapsedNanos = System.nanoTime() - beginNanos;

            // update the local statistics.
            synchronized (stats) {
                stats.chunksOut.incrementAndGet();
                stats.elementsOut.addAndGet(chunkSize);
                stats.elapsedChunkWritingNanos += elapsedNanos;
            }

            // update the master's statistics.
            synchronized (master.stats) {
                master.stats.chunksOut.incrementAndGet();
                master.stats.elementsOut.addAndGet(chunkSize);
                master.stats.duplicateCount.addAndGet(duplicateCount);
                master.stats.elapsedSinkChunkWritingNanos += elapsedNanos;
            }

        }

    }

    /**
     * Notifies the client that the locator is stale.
     */
    @Override
    protected void notifyClientOfRedirect(L locator, Throwable cause) {

        master.ndx.staleLocator(master.ndx.getTimestamp(), (L) locator,
                (StaleLocatorException) cause);

    }
    
}
