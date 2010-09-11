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
 * Created on Sep 8, 2010
 */

package com.bigdata.io;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import com.bigdata.service.ResourceService;
import com.bigdata.util.concurrent.Haltable;

/**
 * An allocator for {@link ByteBuffer} slices backed by direct
 * {@link ByteBuffer}s allocated against a {@link DirectBufferPool}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Make the type of the identifier for the {@link IAllocation} generic
 *       using a factory pattern (we need {@link UUID} for scale-out, but the
 *       class could be reused for other purposes as well). [The allocation
 *       context identifier should continue to be an application specified
 *       object.]
 */
public class DirectBufferPoolAllocator {

    /**
     * The pool from which the direct {@link ByteBuffer}s are allocated.
     */
    private final DirectBufferPool directBufferPool;

    /**
     * The set of allocation contexts.
     */
    private final ConcurrentHashMap<Object/* key */, AllocationContext> allocationContexts = new ConcurrentHashMap<Object, AllocationContext>();

    /**
     * The set of {@link IAllocation} outstanding against the
     * {@link #directBufferPool}.
     */
    private final ConcurrentHashMap<UUID, Allocation> allocations = new ConcurrentHashMap<UUID, Allocation>();

    /**
     * @todo Maybe replace this with a private {@link Haltable} (or extend
     *       {@link Haltable}) so we can test {@link Haltable#halted()} in
     *       critical methods? If we expose the {@link Haltable} then the
     *       {@link ResourceService} can also check it to see whether all
     *       allocations have been invalidated. However, that will not help us
     *       to invalidate a specific {@link IAllocationContext}. For that
     *       purpose we would need to do pretty much the same thing recursively.
     */
    private final AtomicBoolean open = new AtomicBoolean(true);
    
    /**
     * 
     * @param pool
     *            The pool from which the direct {@link ByteBuffer}s are
     *            allocated.
     */
    public DirectBufferPoolAllocator(final DirectBufferPool pool) {
        
        this.directBufferPool = pool;
        
    }

    /**
     * Extended to {@link #close()} the allocator.
     */
    @Override
    protected void finalize() throws Throwable {
        
        close();
        
        super.finalize();
        
    }
    
    /**
     * Releases all {@link AllocationContext}s and all direct {@link ByteBuffer}
     * s which they are using.
     */
    public void close() {

        if (open.compareAndSet(true/* expect */, false/* update */)) {

            for (AllocationContext c : allocationContexts.values()) {

                c.release();

            }

        }

    }

    /**
     * The maximum #of bytes in a single {@link IAllocation}.
     */
    public int getMaxSlotSize() {

        return directBufferPool.getBufferCapacity();

    }
    
    /**
     * Return an allocation context for the key. If none exists for that key,
     * then one is atomically created and returned.
     * 
     * @param key
     *            A key which uniquely identifies that context. The key will be
     *            inserted into a hash table and therefore must have appropriate
     *            hashCode() and equals() methods.
     * 
     * @return The allocation context.
     */
    public IAllocationContext getAllocationContext(final Object key) {

        AllocationContext c = allocationContexts.get(key);

        if (c == null) {

            final AllocationContext t = allocationContexts.putIfAbsent(key,
                    c = new AllocationContext(key));

            if (t != null) {

                // lost the race to another thread.
                c = t;

            }

        }

        return c;
        
    }
    
    /**
     * Return the allocation associated with that id.
     * 
     * @param id
     *            The allocation identifier.
     * 
     * @return The allocation -or- <code>null</code> if there is no such
     *         allocation.
     */
    public IAllocation getAllocation(final UUID id) {

        return allocations.get(id);

    }
    
//    /**
//     * A direct {@link ByteBuffer} allocated from the {@link #directBufferPool}
//     * together with the identifier assigned to that {@link ByteBuffer} (we can
//     * not directly insert {@link ByteBuffer}s into the keys of a hash map since
//     * their hash code is a function of their content).
//     */
//    private class DirectBufferAllocation {
//
//        private final Long id;
//
//        private final ByteBuffer directBuffer;
//
//        public DirectBufferAllocation(final Long id,
//                final ByteBuffer directBuffer) {
//
//            if (id == null)
//                throw new IllegalArgumentException();
//            
//            if (directBuffer == null)
//                throw new IllegalArgumentException();
//            
//            this.id = id;
//
//            this.directBuffer = directBuffer;
//            
//        }
//
//    }
    
    /**
     * An allocation context links some application specified key with a list
     * of direct {@link ByteBuffer}s on which allocations have been made by
     * the application.
     */
    public interface IAllocationContext {

        /**
         * Allocate a series of {@link ByteBuffer} slices on which the
         * application may write data. The application is encouraged to maintain
         * the order of the allocations in the array in order to preserve the
         * ordering of data written onto those allocation.
         * 
         * @param nbytes
         *            The #of bytes required.
         * 
         * @return The {@link UUID}s of those allocations.
         * 
         * @throws InterruptedException
         */
        IAllocation[] alloc(int nbytes) throws InterruptedException;

        /**
         * Release all allocations made against this allocation context.
         */
        void release();

    }

    /**
     * An allocation against a direct {@link ByteBuffer}.
     */
    public interface IAllocation {
       
        /** The allocation identifier. */
        public UUID getId();

        /**
         * The allocated {@link ByteBuffer#slice()}.
         */
        public ByteBuffer getSlice();

        /**
         * Release this allocation.
         * <p>
         * Note: The implementation is encouraged to release the associated
         * direct {@link ByteBuffer} if there are no remaining allocations
         * against it and MAY made the slice of the buffer available for
         * reallocation.
         * <p>
         * Note: An {@link InterruptedException} MAY be thrown. This allows us
         * to handle cases where a concurrent process (such as a query) was
         * halted and its component threads were interrupted. By looking for the
         * interrupt, we can avoid attempts to release an allocation in some
         * thread where the entire {@link IAllocationContext} has already been
         * released by another thread.
         * 
         * @throws InterruptedException
         */
        public void release() throws InterruptedException;
        
    }
    
    /**
     * An allocation against a direct {@link ByteBuffer}.
     */
    // Note: package private for the unit tests.
    /*private*/ class Allocation implements IAllocation {

        private final AllocationContext allocationContext;
        
        /** The allocation identifier. */
        final private UUID id;

        /**
         * The direct {@link ByteBuffer} against which the allocation was made.
         * 
         * @todo Allow incremental recycling of allocations. To do this we would
         *       keep an allocation map
         * 
         * @todo Allow incremental release of direct buffers as allocations are
         *       released.
         */
        // Note: package private for the unit tests.
        final /*private*/ ByteBuffer nativeBuffer;

        /**
         * A {@link ByteBuffer#slice()} onto the allocated region of the
         * {@link #nativeBuffer}. The slice is cleared when the allocation is
         * released.
         */
        private volatile ByteBuffer allocatedSlice;

        public UUID getId() {
            return id;
        }

        public ByteBuffer getSlice() {

            if (!open.get()) {
                // allocator was closed.
                throw new IllegalStateException();
            }
            
            final ByteBuffer t = allocatedSlice;
            
            if(t == null) {
                // slice was released.
                throw new IllegalStateException();
            }
            
            return t;
            
        }

        private Allocation(final AllocationContext allocationContext,
                final ByteBuffer nativeBuffer, final ByteBuffer allocatedSlice) {

            if (allocationContext == null)
                throw new IllegalArgumentException();
            
            if (nativeBuffer == null)
                throw new IllegalArgumentException();
            
            if (allocatedSlice == null)
                throw new IllegalArgumentException();
            
            this.allocationContext = allocationContext;
            
            this.id = UUID.randomUUID();
            
            this.nativeBuffer = nativeBuffer;
            
            this.allocatedSlice = allocatedSlice;
            
        }

        public void release() throws InterruptedException {
            
            allocationContext.release(this);
            
            allocatedSlice = null;
            
        }

        public String toString() {
            return getClass().getName() + "{id=" + id + ",slice="
                    + allocatedSlice + ",context=" + allocationContext + "}";
        }
        
    } // class Allocation

    /**
     * An allocation context links some application specified key with a list of
     * direct {@link ByteBuffer}s on which allocations have been made by the
     * application.
     */
    // Note: package private for the unit tests.
    /*private*/ class AllocationContext implements IAllocationContext {

        /**
         * The application key for this allocation context.
         */
        private final Object key;

        /**
         * Lock guarding allocations made by this allocation context.
         */
        private final ReentrantLock lock = new ReentrantLock();

        /**
         * The set of native {@link ByteBuffer}s in use by this allocation
         * context. Last element in the list is the {@link ByteBuffer} against
         * which allocations are currently being made.
         */
        private final LinkedList<ByteBuffer> nativeBuffers = new LinkedList<ByteBuffer>();

        /**
         * The set of native {@link ByteBuffer}s in use by this allocation
         * context. The positive of in that buffer is the next byte available in
         * that buffer. The limit is the buffer capacity. Mutable views of this
         * buffer object are published by {@link #alloc(int)}. Those views have
         * independent position, offset, and limit from the original
         * {@link ByteBuffer}.
         */
        private final ConcurrentHashMap<UUID, Allocation> allocations = new ConcurrentHashMap<UUID, Allocation>();

        /**
         * Human readable summary (non-blocking).
         * <p>
         * Note: The #of native buffers is approximate since this method does
         * not acquire the lock guarding that field and thus can not ensure
         * timely visibility.
         */
        public String toString() {

            return getClass().getName() + "{key=" + key + ",#allocations="
                    + allocations.size() + ",#nativeBuffers="
                    + nativeBuffers.size() + "}";
            
        }
        
        public AllocationContext(final Object key) {

            if (key == null)
                throw new IllegalArgumentException();

            this.key = key;

        }

        public IAllocation[] alloc(int nbytes) throws InterruptedException {

            if (nbytes <= 0)
                throw new IllegalArgumentException();

            lock.lock();
            try {

                final LinkedList<IAllocation> allocations = new LinkedList<IAllocation>();

                while (nbytes > 0) {

                    ByteBuffer nativeBuffer = nativeBuffers.peekLast();

                    if (nativeBuffer == null) {

                        if (!open.get()) {
                            /*
                             * The allocation pool is closed.
                             * 
                             * @todo use a lock to serialize these decisions
                             * since otherwise allocations could be made
                             * concurrent with a shutdown and release of the
                             * buffers in the pool which would be a memory leak.
                             */
                            throw new IllegalStateException();
                        }
                        
                        nativeBuffers.add(nativeBuffer = directBufferPool
                                .acquire());

                    }

                    final int remaining = nativeBuffer.remaining();

                    final int allocSize = Math.min(remaining, nbytes);

                    final int start = nativeBuffer.position();
                    
                    final int limit = start + allocSize;

                    nativeBuffer.limit(limit);

                    // create the slice.
                    final Allocation a = new Allocation(this, nativeBuffer,
                            nativeBuffer.slice());
                    
                    // restore limit to the remaining capacity.
                    nativeBuffer.limit(nativeBuffer.capacity());
                    
                    // advance position over the allocated slice.
                    nativeBuffer.position(limit);

                    // remember allocation in the global scope.
                    DirectBufferPoolAllocator.this.allocations.put(a.id, a);

                    // remember allocation in the allocation context scope.
                    this.allocations.put(a.id, a);
                    
                    // add to list of allocations made for this request.
                    allocations.add(a);

                    // subtract out the #of bytes allocated.
                    nbytes -= allocSize;

                }

                return allocations.toArray(new IAllocation[allocations.size()]);

            } finally {

                lock.unlock();
                
            }

        }
        
        /**
         * Release all direct {@link ByteBuffer}s associated with this
         * allocation context back to the {@link #directBufferPool}. If the
         * current thread is interrupted, the interrupt will be trapped and the
         * thread will be interrupted on exit (after all allocations have been
         * released).
         */
        public void release() {

            boolean interrupted = false;

            // Note: lock is NOT interruptable.
            lock.lock();
            try {

                for(Allocation a : allocations.values()) {
                    
                    // clear the slice reference.
                    a.allocatedSlice = null;
                    
                }
                
                // clear collection.
                allocations.clear();

                // release backing buffers.
                for (ByteBuffer b : nativeBuffers) {

                    while (true) {
                        try {
                            directBufferPool.release(b);
                            break;
                        } catch (InterruptedException e) {
                            interrupted = true;
                        }
                    }

                }

            } finally {

                lock.unlock();

            }

            if (interrupted)
                Thread.currentThread().interrupt();

        }

        /**
         * Release the allocation.
         * 
         * @throws InterruptedException
         *             if the thread was interrupted
         */
        private void release(final Allocation a) throws InterruptedException {

            if (a == null)
                throw new IllegalArgumentException();
            
            lock.lockInterruptibly();
            
            try {
            
                if (!allocations.remove(a.id, a)) {
        
                    /*
                     * The allocation was not found in the map.
                     */
                    throw new RuntimeException();
                    
                }
                
            } finally {
                
                lock.unlock();
                
            }

        }
        
    }

    /**
     * Copy the caller's data onto the ordered array of allocations. The
     * position of each {@link IAllocation#getSlice() allocation} will be
     * advanced by the #of bytes copied into that allocation.
     * 
     * @param src
     *            The source data.
     * @param a
     *            The allocations.
     * 
     * @throws BufferOverflowException
     *             if there is not enough room in the allocations for the data
     *             to be copied.
     */
    static public void put(final byte[] src, final IAllocation[] a) {

        int offset = 0;
        
        int remaining = src.length;
        
        for (int i = 0; i < a.length && remaining > 0; i++) {

            final ByteBuffer slice = a[i].getSlice();

            final int length = Math.min(remaining, slice.remaining());

            slice.put(src, offset, length);

            offset += length;

            remaining -= length;

        }

        if (remaining > 0)
            throw new BufferOverflowException();

    }
    
    /**
     * Copy the caller's data onto the ordered array of allocations. The
     * position of each {@link IAllocation#getSlice() allocation} will be
     * advanced by the #of bytes copied into that allocation.
     * 
     * @param src
     *            The source data.
     * @param a
     *            The allocations.
     * 
     * @throws BufferOverflowException
     *             if there is not enough room in the allocations for the data
     *             to be copied.
     */
    static public void put(final ByteBuffer src, final IAllocation[] a) {

        int remaining = src.remaining();
        
        for (int i = 0; i < a.length && remaining > 0; i++) {

            final ByteBuffer slice = a[i].getSlice();

            final int length = Math.min(remaining, slice.remaining());

            slice.put(src);

            remaining -= length;

        }

        if (remaining > 0)
            throw new BufferOverflowException();

    }
    
}
