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
 * Created on Jun 5, 2009
 */

package com.bigdata.service.ndx;

import java.util.concurrent.Future;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.UnisolatedReadWriteIndex;
import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.IKeyArrayIndexProcedure;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IRunnableBuffer;
import com.bigdata.service.AbstractFederation;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.ILoadBalancerService;
import com.bigdata.service.Split;
import com.bigdata.service.ndx.pipeline.IDuplicateRemover;
import com.bigdata.service.ndx.pipeline.IndexAsyncWriteStats;
import com.bigdata.service.ndx.pipeline.KVOC;
import com.bigdata.service.ndx.pipeline.KVOLatch;

/**
 * Interface for asynchronous writes on scale-out indices. 
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IAsynchronousWriteBufferFactory {

    /**
     * Asynchronous write API (streaming writes).
     * <p>
     * The returned buffer provides a streaming API which is highly efficient.
     * The caller writes ordered {@link KVO}[] chunks onto the thread-safe
     * {@link BlockingBuffer}. Those chunks are dynamically combined and then
     * split into per-index partition chunks which are written on internally
     * managed {@link BlockingBuffer}s for each index partition which will be
     * touched by a write operation. The splits are slices of ordered chunks for
     * a specific index partition. The {@link BlockingBuffer} uses a merge sort
     * when it combines ordered chunks so that the combined chunks remain fully
     * ordered. Once a chunk is ready, it is re-shaped for the CTOR and sent to
     * the target data service using RMI.
     * <p>
     * Since this API is asynchronous, you will not have synchronous access to
     * values returned by asynchronous writes. However, patterns can be created
     * using {@link KVOC} and {@link KVOLatch} which provide notification when
     * application defined sets of results have become available. Such patterns
     * are created by associated the {@link KVOLatch} with the set of results
     * and using {@link IResultHandler} and the object reference on the
     * {@link KVOC} to capture the side-effect of the write.
     * <p>
     * {@link BlockingBuffer#getFuture()} may be used to obtain the
     * {@link Future} of the consumer. You can use {@link Future#get()} to await
     * the completion of the consumer, to cancel the consumer, etc. The
     * {@link Future} will not terminate (other than by error) until the buffer
     * has been {@link IBlockingBuffer#close() closed}. The {@link Future}
     * evaluates to an {@link IndexAsyncWriteStats} object. Those statistics are
     * also reported to the {@link ILoadBalancerService} via the
     * {@link IBigdataFederation}.
     * <p>
     * Each buffer returned by this method is independent, and writes onto
     * independent sinks which write through to the index partitions. This is
     * necessary in order for the caller to retain control over the life cycle
     * of their write operations. The {@link BlockingBuffer} is thread-safe so
     * it may be the target for concurrent producers can be can utilized to
     * create very high throughput designs. While the returned buffers are
     * independent, the performance counters for all asynchronous write buffers
     * for a given client and scale-out index are aggregated by a single
     * {@link ScaleOutIndexCounters} instance.
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
     *            manner (optional). A duplicate remover MUST NOT be used to
     *            eliminate duplicates in combination with {@link KVOC} and
     *            {@link KVOLatch}. If a {@link KVOC} having the same key,
     *            value, and reference but a distinct {@link KVOLatch} reference
     *            is eliminated as a duplicate that would cause a
     *            {@link KVOLatch#dec()} invocation to be "lost" and would
     *            result in non-termination since the count for the "lost" latch
     *            would never reach zero.
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
    public <T extends IKeyArrayIndexProcedure, O, R, A> IRunnableBuffer<KVO<O>[]> newWriteBuffer(
            final IResultHandler<R, A> resultHandler,
            final IDuplicateRemover<O> duplicateRemover,
            final AbstractKeyArrayIndexProcedureConstructor<T> ctor);

}
