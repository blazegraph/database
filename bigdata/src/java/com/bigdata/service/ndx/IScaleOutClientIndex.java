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
 * Created on Mar 31, 2009
 */

package com.bigdata.service.ndx;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import com.bigdata.btree.ITuple;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.UnisolatedReadWriteIndex;
import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.IKeyArrayIndexProcedure;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.service.AbstractFederation;
import com.bigdata.service.AbstractScaleOutFederation;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.ILoadBalancerService;
import com.bigdata.service.Split;
import com.bigdata.service.ndx.pipeline.IDuplicateRemover;
import com.bigdata.service.ndx.pipeline.IndexAsyncWriteStats;

/**
 * A client-side view of a scale-out index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IScaleOutClientIndex extends IClientIndex {

    /**
     * Resolve the data service to which the index partition is mapped.
     * 
     * @param pmd
     *            The index partition locator.
     * 
     * @return The data service and never <code>null</code>.
     * 
     * @throws RuntimeException
     *             if none of the data services identified in the index
     *             partition locator record could be discovered.
     */
    IDataService getDataService(final PartitionLocator pmd);

    /**
     * Returns an iterator that will visit the {@link PartitionLocator}s for
     * the specified scale-out index key range.
     * 
     * @see AbstractScaleOutFederation#locatorScan(String, long, byte[], byte[],
     *      boolean)
     * 
     * @param ts
     *            The timestamp that will be used to visit the locators.
     * @param fromKey
     *            The scale-out index first key that will be visited
     *            (inclusive). When <code>null</code> there is no lower bound.
     * @param toKey
     *            The first scale-out index key that will NOT be visited
     *            (exclusive). When <code>null</code> there is no upper bound.
     * @param reverseScan
     *            <code>true</code> if you need to visit the index partitions
     *            in reverse key order (this is done when the partitioned
     *            iterator is scanning backwards).
     * 
     * @return The iterator. The value returned by {@link ITuple#getValue()}
     *         will be a serialized {@link PartitionLocator} object.
     */
    Iterator<PartitionLocator> locatorScan(final long ts, final byte[] fromKey,
            final byte[] toKey, final boolean reverseScan);

    /**
     * Notifies the client that a {@link StaleLocatorException} was received.
     * The client will use this information to refresh the
     * {@link IMetadataIndex}.
     * 
     * @param ts
     *            The timestamp of the metadata index view from which the
     *            locator was obtained.
     * @param locator
     *            The locator that was stale.
     * @param cause
     *            The reason why the locator became stale (split, join, or
     *            move).
     * 
     * @throws RuntimeException
     *             unless the timestamp given is {@link ITx#UNISOLATED} or
     *             {@link ITx#READ_COMMITTED} since stale locators do not occur
     *             for other views.
     */
    void staleLocator(final long ts, final PartitionLocator locator,
            final StaleLocatorException cause);

    /**
     * Return a {@link ThreadLocal} {@link AtomicInteger} whose value is the
     * recursion depth of the current {@link Thread}. This is initially zero
     * when the task is submitted by the application. The value incremented when
     * a task results in a {@link StaleLocatorException} and is decremented when
     * returning from the recursive handling of the
     * {@link StaleLocatorException}.
     * <p>
     * The recursion depth is used:
     * <ol>
     * <li>to limit the #of retries due to {@link StaleLocatorException}s for
     * a split of a task submitted by the application</li>
     * <li> to force execution of retried tasks in the caller's thread.</li>
     * </ol>
     * The latter point is critical - if the retry tasks are run in the client
     * {@link #getThreadPool() thread pool} then all threads in the pool can
     * rapidly become busy awaiting retry tasks with the result that the client
     * is essentially deadlocked.
     * 
     * @return The recursion depth.
     */
    AtomicInteger getRecursionDepth();
    
    /**
     * Return the object used to access the services in the connected
     * federation.
     */
    AbstractScaleOutFederation getFederation();

    /**
     * Identify the {@link Split}s for an ordered array of keys such that there
     * is one {@link Split} per index partition spanned by the data.
     * 
     * @param ts
     *            The timestamp for the {@link IMetadataIndex} view that will be
     *            applied to choose the {@link Split}s.
     * @param fromIndex
     *            The index of the first key in <i>keys</i> to be processed
     *            (inclusive).
     * @param toIndex
     *            The index of the last key in <i>keys</i> to be processed.
     * @param keys
     *            An array of keys. Each key is an interpreted as an unsigned
     *            byte[]. All keys must be non-null. The keys must be in sorted
     *            order.
     * 
     * @return The {@link Split}s that you can use to form requests based on
     *         the identified first/last key and partition identified by this
     *         process.
     */
    LinkedList<Split> splitKeys(final long ts, final int fromIndex,
            final int toIndex, final byte[][] keys);

    /**
     * Identify the {@link Split}s for an ordered {@link KVO}[] such that
     * there is one {@link Split} per index partition spanned by the data.
     * 
     * @param ts
     *            The timestamp for the {@link IMetadataIndex} view that will be
     *            applied to choose the {@link Split}s.
     * @param fromIndex
     *            The index of the first key in <i>keys</i> to be processed
     *            (inclusive).
     * @param toIndex
     *            The index of the last key in <i>keys</i> to be processed.
     * @param keys
     *            An array of keys. Each key is an interpreted as an unsigned
     *            byte[]. All keys must be non-null. The keys must be in sorted
     *            order.
     * 
     * @return The {@link Split}s that you can use to form requests based on
     *         the identified first/last key and partition identified by this
     *         process.
     */
    LinkedList<Split> splitKeys(final long ts, final int fromIndex,
            final int toIndex, final KVO[] a);
    
    /**
     * Asynchronous write API (streaming writes).
     * <p>
     * The returned buffer provides a streaming API which is highly efficient if
     * you do not need to have synchronous notification or directly consume the
     * returned values. The caller writes ordered {@link KVO}[] chunks onto the
     * {@link BlockingBuffer}. Those chunks are dynamically combined and then
     * split into per-index partition chunks which are written on internally
     * managed {@link BlockingBuffer}s for each index partition which will be
     * touched by a write operation. The splits are are slices of ordered chunks
     * for a specific index partition. The {@link BlockingBuffer} uses a merge
     * sort when it combines ordered chunks so that the combined chunks remain
     * fully ordered. Once a chunk is ready, it is re-shaped for the CTOR and
     * sent to the target data service using RMI.
     * <p>
     * {@link BlockingBuffer#getFuture()} may be used to obtain the
     * {@link Future} of the consumer. You can use {@link Future#get()} to await
     * the completion of the consumer, to cancel the consumer, etc. The
     * {@link Future} will not terminate (other than by error) until the buffer
     * has been {@link IBlockingBuffer#close() closed}. The {@link Future}
     * evaluates to an {@link IndexAsyncWriteStats} object. Those statistics are also
     * reported to the {@link ILoadBalancerService} via the
     * {@link IBigdataFederation}.
     * <p>
     * Note: Each buffer returned by this method is independent. However, all
     * the performance counters for all asynchronous write buffers for a given
     * client and scale-out index are aggregated by an {@link ScaleOutIndexCounters}.
     * 
     * @param <T>
     *            The generic type of the procedure used to write on the index.
     * @param <O>
     *            The generic type for unserialized value objects.
     * @param <R>
     *            The type of the result from applying the index procedure to a
     *            single {@link Split} of data.
     * @param <A>
     *            The type of the aggregated result.
     * 
     * @param resultHandler
     *            Used to aggregate results.
     * @param duplicateRemover
     *            Used to filter out duplicates in an application specified
     *            manner (optional).
     * @param ctor
     *            Used to create instances of the procedure that will execute a
     *            write on an individual index partition (this implies that
     *            insert and remove operations as well as custom index write
     *            operations must use separate buffers).
     * 
     * @return A buffer on which the producer may write their data.
     * 
     * @see IndexMetadata#getAsynchronousIndexWriteConfiguration()
     * 
     * @see AbstractFederation#getIndexCounters(String)
     * 
     * @todo The async API is only defined at this time for scale-out index
     *       views. An asynchronous write API could be defined for local
     *       B+Trees. It would have to ensure locks using the
     *       {@link UnisolatedReadWriteIndex}. It would not use the same
     *       layering since writes could not be scattered. It could be written
     *       as a single blocking buffer which was drained by the CTOR for the
     *       operation. If we combine the two buffer capacity parameters into a
     *       single parameter, then this method signature could be used for both
     *       local and scale-out index views. This method could then be moved to
     *       an IAsynchronousIndexWriter interface.
     */
    public <T extends IKeyArrayIndexProcedure, O, R, A> BlockingBuffer<KVO<O>[]> newWriteBuffer(
            final IResultHandler<R, A> resultHandler,
            final IDuplicateRemover<O> duplicateRemover,
            final AbstractKeyArrayIndexProcedureConstructor<T> ctor);

}
