/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Sep 10, 2010
 */

package com.bigdata.bop.fed;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.engine.IChunkMessage;
import com.bigdata.bop.engine.IQueryClient;
import com.bigdata.io.DirectBufferPoolAllocator.IAllocation;
import com.bigdata.io.DirectBufferPoolAllocator.IAllocationContext;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.service.ResourceService;

/**
 * An {@link IChunkMessage} where the payload is made available to the receiving
 * service using an NIO transfer against the sender's {@link ResourceService}.
 * This is suitable for moving large blocks of data during query evaluation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ChunkMessageWithNIOPayload implements IChunkMessage, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Metadata about an allocation to be retrieved from the sender's
     * {@link ResourceService}.
     */
    private final class A implements Serializable {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        /**
         * The identifier of the resource on the sender's
         * {@link ResourceService}.
         */
        private final UUID bufferId;

        /**
         * The size of that resource in bytes.
         */
        private final int nbytes;

        /**
         * 
         * @param bufferId
         *            The identifier of the resource on the sender's
         *            {@link ResourceService}.
         * @param nbytes
         *            The size of that resource in bytes.
         */
        public A(final UUID bufferId, final int nbytes) {
            this.bufferId = bufferId;
            this.nbytes = nbytes;
        }
    }
    
    final private IQueryClient queryController;

    final private long queryId;

    final private int bopId;

    final private int partitionId;

    final private int nbytes;
    
    /**
     * Note: Even when we send one message per chunk, we can still have a list
     * of {@link IAllocation}s if the chunk did not get formatted onto a single
     * {@link IAllocation}.
     */
    final private A[] allocations;

    /**
     * The Internet address and port where the receiver can fetch the payload
     * using the sender's {@link ResourceService}.
     */
    final private InetSocketAddress addr;

    public IQueryClient getQueryController() {
        return queryController;
    }

    public long getQueryId() {
        return queryId;
    }

    public int getBOpId() {
        return bopId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    /** The #of bytes of data which are available for that operator. */
    public int getBytesAvailable() {
        return nbytes;
    }

    /**
     * The Internet address and port of a {@link ResourceService} from which the
     * receiver may demand the data.
     */
    public InetSocketAddress getServiceAddr() {
        return addr;
    }

    /**
     * 
     * @param queryController
     * @param queryId
     * @param sinkId
     * @param partitionId
     * @param allocations
     *            The ordered list of {@link IAllocation}s comprising the chunk.
     * @param addr
     *            The Internet address and port where the receiver can fetch the
     *            payload using the sender's {@link ResourceService}.
     */
    public ChunkMessageWithNIOPayload(final IQueryClient queryController,
            final long queryId, final int sinkId, final int partitionId,
            final List<IAllocation> allocations, final InetSocketAddress addr) {

        if (queryController == null)
            throw new IllegalArgumentException();

        if (allocations == null)
            throw new IllegalArgumentException();

        if (addr == null)
            throw new IllegalArgumentException();

        this.queryController = queryController;
        this.queryId = queryId;
        this.bopId = sinkId;
        this.partitionId = partitionId;
        final int n = allocations.size();
        this.allocations = new A[n];
        int i = 0;
        int nbytes = 0;
        final Iterator<IAllocation> itr = allocations.iterator();
        while (itr.hasNext()) {
            final IAllocation alloc = itr.next();
            final int len = alloc.getSlice().capacity();
            this.allocations[i++] = new A(alloc.getId(), len);
            nbytes += len;
        }
        this.nbytes = nbytes;
        this.addr = addr;

    }

    public boolean isMaterialized() {
        return materialized;
    }
    private volatile boolean materialized = false;

    /**
     * 
     * FIXME unit tests for materializing and visiting the chunk.
     */
    synchronized public void materialize(FederatedRunningQuery runningQuery) {
        
        if (materialized)
            return;

        final AllocationContextKey key = new ShardContext(queryId, bopId,
                partitionId);

        final IAllocationContext allocationContext = runningQuery
                .getAllocationContext(key);

        final ResourceService resourceService = runningQuery.getQueryEngine()
                .getResourceService();

//        for (A a : allocations) {
//
//            /*
//             * FIXME harmonize an IAllocation[] with a ByteBuffer for the {@link
//             * ResourceService}. The problem is that an object to be sent across
//             * the wire may span multiple ByteBuffers.
//             */ 
//            final ByteBuffer tmp = allocationContext.alloc(a.nbytes);
//
//            new ResourceService.ReadBufferTask(addr, a.bufferId, tmp);
//
//        }

        throw new UnsupportedOperationException();
        
    }

    public IAsynchronousIterator<IBindingSet[]> iterator() {

        if (!isMaterialized())
            throw new UnsupportedOperationException();
        
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
   
    }

}
