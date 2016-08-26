/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

import java.io.Externalizable;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.rmi.RemoteException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.bigdata.bop.engine.IChunkAccessor;
import com.bigdata.bop.engine.IChunkMessage;
import com.bigdata.bop.engine.IQueryClient;
import com.bigdata.io.DirectBufferPoolAllocator;
import com.bigdata.io.DirectBufferPoolAllocator.IAllocation;
import com.bigdata.io.DirectBufferPoolAllocator.IAllocationContext;
import com.bigdata.io.SerializerUtil;
import com.bigdata.rdf.internal.encoder.IVSolutionSetEncoder;
import com.bigdata.service.ManagedResourceService;
import com.bigdata.service.ResourceService;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * An {@link IChunkMessage} where the payload is made available to the receiving
 * service using an NIO transfer against the sender's {@link ResourceService}.
 * This is suitable for moving large blocks of data during query evaluation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/160">
 *      ResourceService should use NIO for file and buffer transfers</a>
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/486">Support
 *      NIO solution set interchange on the cluster</a>
 * 
 *      TODO Implement {@link Externalizable} for this class based on the
 *      {@link ThickChunkMessage} and {@link IVSolutionSetEncoder}.
 */
public class NIOChunkMessage<E> implements IChunkMessage<E>, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    final private IQueryClient queryController;
    
    final private UUID queryControllerId;

    final private UUID queryId;

    final private int bopId;

    final private int partitionId;

    final private int solutionCount;
    
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

    @Override
    public IQueryClient getQueryController() {
        return queryController;
    }

    @Override
    public UUID getQueryControllerId() {
        return queryControllerId;
    }
    
    @Override
    public UUID getQueryId() {
        return queryId;
    }

    @Override
    public int getBOpId() {
        return bopId;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public boolean isLastInvocation() {
        return false; // Never.
    }

    /**
     * The #of elements in this chunk.
     * 
     * @todo we could track this in total and in {@link A} on a per-slice basis.
     */
    @Override
    public int getSolutionCount() {
        return solutionCount;
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

    @Override
    public String toString() {

        return getClass().getName() + "{queryId=" + queryId + ",bopId=" + bopId
                + ",partitionId=" + partitionId + ", controller="
                + queryController + ",solutionCount=" + solutionCount
                + ", bytesAvailable=" + nbytes + ", nslices="
                + allocations.length + ", serviceAddr=" + addr + "}";

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
    public NIOChunkMessage(final IQueryClient queryController,
            final UUID queryId, final int sinkId, final int partitionId,
            final IAllocationContext allocationContext,
            final E[] source,
            final InetSocketAddress addr) {

        if (queryController == null)
            throw new IllegalArgumentException();

        if (queryId == null)
            throw new IllegalArgumentException();

        if (allocationContext == null)
            throw new IllegalArgumentException();

        if (source == null)
            throw new IllegalArgumentException();

        if (addr == null)
            throw new IllegalArgumentException();

        // format onto NIO buffers.
        final AtomicInteger nsolutions = new AtomicInteger();
        final List<IAllocation> allocations = moveToNIOBuffers(
                allocationContext, source, nsolutions);

        this.queryController = queryController;
        try {
            this.queryControllerId = queryController.getServiceUUID();
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        }
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
        this.solutionCount = nsolutions.get();
        this.nbytes = nbytes;
        this.addr = addr;

    }

    /**
     * Chunk-wise serialization of the data onto allocations.
     * 
     * @param allocationContext
     * @param source
     * @return
     */
    static private <E> List<IAllocation> moveToNIOBuffers(
            final IAllocationContext allocationContext,
            final E[] source,
            final AtomicInteger nsolutions) {

        int nbytes = 0;

        int n = 0;

        final List<IAllocation> allocations = new LinkedList<IAllocation>();

        // Next chunk to be serialized.
        final E[] chunk = source;// itr.next();

        // track #of solutions.
        n += chunk.length;

        // FIXME Replace with FAST/TIGHT SERIALIZATION
        // serialize the chunk of binding sets.
        final byte[] data = SerializerUtil.serialize(chunk);

        // track size of the allocations.
        nbytes += data.length;

        // allocate enough space for those data.
        final IAllocation[] tmp;
        try {
            tmp = allocationContext.alloc(data.length);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }

        // copy the data into the allocations.
        DirectBufferPoolAllocator.put(data, tmp);

        for (IAllocation a : tmp) {

            // prepare for reading.
            a.getSlice().flip();

            // append the allocation.
            allocations.add(a);

        }

        nsolutions.addAndGet(n);

        return allocations;

    }

    /**
     * Metadata about an allocation to be retrieved from the sender's
     * {@link ResourceService}.
     */
    private static final class A implements Serializable {

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

            if (bufferId == null)
                throw new IllegalArgumentException();

            if (nbytes <= 0)
                throw new IllegalArgumentException();
            
            this.bufferId = bufferId;
            
            this.nbytes = nbytes;
            
        }
        
    }
    
    @Override
    public boolean isMaterialized() {

        return materialized != null;
        
    }
    
    private volatile List<IAllocation> materialized = null;

    @Override
    public void materialize(final FederatedRunningQuery runningQuery) {

        final AllocationContextKey key = new ShardContext(queryId, bopId,
                partitionId);

        final IAllocationContext allocationContext = runningQuery
                .getAllocationContext(key);

        final ManagedResourceService resourceService = runningQuery
                .getQueryEngine().getResourceService();

        materialize(resourceService, allocationContext);
        
    }
    
    /**
     * Discard the materialized data.
     */
    @Override
    public void release() {

        if (chunkAccessor != null) {
         
            chunkAccessor.close();
            
        }
        
        final List<IAllocation> tmp = materialized;

        if (tmp != null) {

            boolean interrupted = false;
            for (IAllocation a : tmp) {
                while (true) {
                    try {
                        a.release();
                        break;
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
            }
            materialized = null;
        
            if(interrupted) {
                Thread.currentThread().interrupt();
            }
            
        }
        
    }

    /**
     * Core implementation. This is responsible receiving the data from the
     * remote {@link ResourceService} and assembling it into a set of
     * {@link IAllocation}s in the specified {@link IAllocationContext}.
     * 
     * @param resourceService
     * @param allocationContext
     * 
     * @todo The {@link ResourceService} does not currently use NIO and there is
     *       no benefit to passing in direct {@link ByteBuffer}s yet.
     */
    synchronized protected void materialize(
            final ManagedResourceService resourceService,
            final IAllocationContext allocationContext) {

        if (materialized != null)
            return;
        
        try {
            // list of the allocations created to receive the data.
            final List<IAllocation> received = new LinkedList<IAllocation>();

            for (A a : allocations) {

                final ByteBuffer buf = ByteBuffer.allocate(a.nbytes);

                new ResourceService.ReadBufferTask(addr, a.bufferId, buf)
                        .call();

                final IAllocation[] tmp = allocationContext.alloc(a.nbytes);

                DirectBufferPoolAllocator.put(buf, tmp);

                for (IAllocation alloc : tmp) {

                    // prepare for reading.
                    alloc.getSlice().flip();

                    // add to list of received slices.
                    received.add(alloc);

                }

            }

            materialized = received;

        } catch (Exception ex) {

            throw new RuntimeException(ex);
            
        }

    }

    @Override
    public IChunkAccessor<E> getChunkAccessor() {

        if (chunkAccessor == null) {

            chunkAccessor = new ChunkAccessor();
            
        }

        return chunkAccessor;
    
    }
    
    private volatile transient ChunkAccessor chunkAccessor = null;

    /**
     * FIXME Provide in place decompression and read out of the binding sets.
     * This should be factored out into classes similar to IRaba and IRabaCoder.
     * This stuff should be generic so it can handle elements and binding sets
     * and bats, but there should be specific coders for handling binding sets
     * which leverages the known set of variables in play as of the operator
     * which generated those intermediate results.
     * <p>
     * Note: Some similar work was done to improve the htree performance.
     * <p>
     * Note: Very small chunks (1-5 solutions) are common on a cluster and might
     * be optimized different than modest chunks (10-100+).
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/395">HTree
     *      performance tuning</a>
     */
    private class ChunkAccessor implements IChunkAccessor<E> {

        private final ICloseableIterator<E[]> source;
        
        public ChunkAccessor() {
            
            final List<IAllocation> tmp = materialized;
            
            if (tmp == null)
                throw new UnsupportedOperationException();

            source = new DeserializationIterator(materialized.iterator());

        }
        
        public ICloseableIterator<E[]> iterator() {
            
            return source;
            
        }
        
        public void close() {

            source.close();
            
        }

    }

    private class DeserializationIterator implements ICloseableIterator<E[]> {

        private final Iterator<IAllocation> src;
        private volatile boolean open = true;
        
        public DeserializationIterator(final Iterator<IAllocation> src) {
            
            this.src = src;
            
        }

        @Override
        public void close() {
        
            if(open) {
                
                open = false;
                
                // TODO Anything to discard?
                
            }
            
        }

        @Override
        public boolean hasNext() {

            if(open && src.hasNext())
                return true;
            
            close();
            
            return false;
            
        }

        @Override
        @SuppressWarnings("unchecked")
        public E[] next() {

            if (!hasNext())
                throw new NoSuchElementException();

            /*
             * Note: Deserialization from a direct ByteBuffer is very expensive.
             * First copy the data into a byte[] and then deserialize it.
             */
            final IAllocation a = src.next();
            // independent position, limit, etc. to avoid side effects
            final ByteBuffer b = a.getSlice().asReadOnlyBuffer();

            final byte[] c = new byte[b.remaining()];
            
            b.get(c);
            
            return (E[]) SerializerUtil.deserialize(c);
            
        }

        @Override
        public void remove() {
            
            throw new UnsupportedOperationException();
            
        }

    }

}
