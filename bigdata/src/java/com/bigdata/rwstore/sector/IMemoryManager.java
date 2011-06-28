/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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

package com.bigdata.rwstore.sector;

import java.nio.ByteBuffer;

import com.bigdata.counters.ICounterSetAccess;
import com.bigdata.io.DirectBufferPool;

/**
 * Abstraction for managing data in {@link ByteBuffer}s. Typically those buffers
 * will be allocated on the native process heap.
 * <p>
 * <strong>CAUTION: The memory manager helps you manage direct storage. However,
 * it does not prevent you from doing something stupid with it.</strong> The
 * most likely error is one in which you {@link #get(long)} an address and hold
 * onto the returned {@link ByteBuffer} after the data at that address has been
 * {@link #free(long) freed}. This can leave you in a position where you are
 * reading on (or writing on!) someone else's data on the JVM native heap. The
 * memory manager works with {@link DirectBufferPool}s. This class provides the
 * efficient reuse of direct allocations, but does not release them back to the
 * JVM. For this reason, if you do stomp on someone's memory, it will not be
 * memory in use by the JVM but only memory in use by another part of your
 * application using the same {@link DirectBufferPool} instance.
 * 
 * @author martyncutcher
 */
public interface IMemoryManager extends ICounterSetAccess {

	/**
	 * Allocates space on the backing resource and copies the provided data.
	 * 
	 * @param data
	 *            The data will be copied to the backing resource. For each
	 *            buffer in this array, the position will be advanced to the
	 *            limit.
	 * @param blocks
	 *            When <code>true</code> the request will block until the memory
	 *            is available for the allocation.
	 * 
	 * @return the address to be passed to the get method to retrieve the data.
	 * 
	 * @throws IllegalArgumentException
	 *             if <i>data</i> is <code>null</code>.
	 * @throws IllegalArgumentException
	 *             if {@link ByteBuffer#remaining()} is ZERO (0).
	 * @throws MemoryManagerResourceError
	 *             If the memory is not available for the allocation and
	 *             <i>blocks:=false</i>. Whether or not this exception can be
	 *             thrown depends on whether the backing buffer pool has a
	 *             bounded capacity and whether the {@link IMemoryManager} using
	 *             that pool has a bounded capacity.
	 */
	public long allocate(ByteBuffer data, boolean blocks);

	/**
	 * Blocking version of {@link #allocate(ByteBuffer, boolean)}.
	 * 
	 * @param data
	 *            The data will be copied to the backing resource. For each
	 *            buffer in this array, the position will be advanced to the
	 *            limit.
	 * 
	 * @throws IllegalArgumentException
	 *             if <i>data</i> is <code>null</code>.
	 * @throws IllegalArgumentException
	 *             if {@link ByteBuffer#remaining()} is ZERO (0).
	 * @throws MemoryManagerResourceError
	 *             If the memory is not available for the allocation and
	 *             <i>blocks:=false</i>. Whether or not this exception can be
	 *             thrown depends on whether the backing buffer pool has a
	 *             bounded capacity and whether the {@link IMemoryManager} using
	 *             that pool has a bounded capacity.
	 */
	public long allocate(ByteBuffer data);

	/**
	 * Return the address of a new allocation sufficient to store the specified
	 * number of bytes of application data.
	 * 
	 * @param nbytes
	 *            The size of the allocation request.
	 * @param blocks
	 *            When <code>true</code> the method will block until the
	 *            allocation request can be satisfied. When <code>false</code> a
	 *            {@link MemoryManagerOutOfMemory} will be thrown.
	 * 
	 * @return The address of the allocation.
	 * 
	 * @throws IllegalArgumentException
	 *             if <i>nbytes</i> is non-positive.
	 * @throws MemoryManagerResourceError
	 *             If the memory is not available for the allocation and
	 *             <i>blocks:=false</i>. Whether or not this exception can be
	 *             thrown depends on whether the backing buffer pool has a
	 *             bounded capacity and whether the {@link IMemoryManager} using
	 *             that pool has a bounded capacity.
	 */
	public long allocate(int nbytes, boolean blocks);

	/**
	 * Return the address of a new allocation sufficient to store the specified
	 * number of bytes of application data. This is the blocking version of
	 * {@link #allocate(int)}.
	 * 
	 * @param nbytes
	 *            The size of the allocation request.
	 * @param blocks
	 *            When <code>true</code> the method will block until the
	 *            allocation request can be satisfied. When <code>false</code> a
	 *            {@link MemoryManagerOutOfMemory} will be thrown.
	 * 
	 * @return The address of the allocation.
	 * 
	 * @throws IllegalArgumentException
	 *             if <i>nbytes</i> is non-positive.
	 * @throws MemoryManagerResourceError
	 *             If the memory is not available for the allocation and
	 *             <i>blocks:=false</i>. Whether or not this exception can be
	 *             thrown depends on whether the backing buffer pool has a
	 *             bounded capacity and whether the {@link IMemoryManager} using
	 *             that pool has a bounded capacity.
	 */
	public long allocate(int nbytes);

	/**
	 * Return an array of {@link ByteBuffer}s providing an updatable view onto
	 * the backing allocation.
	 * <p>
	 * The ByteBuffer[] return enables the handling of blobs that span more than
	 * a single slot, without the need to create an intermediate ByteBuffer.
	 * This method is designed for use with zero-copy NIO. Furthermore, since
	 * the {@link ByteBuffer}s in the returned array are not read-only, they can
	 * be updated directly. In this way the {@link #allocate(int)} can be used
	 * in conjunction with get to provide more flexibility when storing data.
	 * <p>
	 * Using ByteBuffer:put the returned array can be efficiently copied to
	 * another ByteBuffer:
	 * 
	 * <pre>
	 * ByteBuffer mybb;
	 * ByteBuffer[] bufs = get(addr);
	 * for (ByteBuffer b : bufs) {
	 * 	mybb.put(b);
	 * }
	 * </pre>
	 * 
	 * <strong>CAUTION: Do not hold onto the {@link ByteBuffer} longer than is
	 * necessary.</strong> If the allocation is released by {@link #free(long)}
	 * or {@link #clear()}, then the memory backing the {@link ByteBuffer} could
	 * be reallocated by another {@link DirectBufferPool} consumer.
	 * 
	 * @param addr
	 *            An previously allocated address.
	 * 
	 * @return array of ByteBuffers
	 */
	public ByteBuffer[] get(long addr);

	/**
	 * Return a copy of the data stored at that address. This method is intended
	 * for use with patterns where the {@link IMemoryManager} is treated as a
	 * persistence store.
	 * 
	 * @param addr
	 *            The address.
	 * 
	 * @return A copy of the data stored at that address.
	 */
	public byte[] read(long addr);

	/**
	 * Frees the address and makes available for recycling
	 * 
	 * @param addr
	 *            to be freed
	 * 
	 * @throws IllegalArgumentException
	 *             If the address is known to be invalid (never written or
	 *             deleted). Note that the address 0L is always invalid as
	 *             is any address which encodes a 0 byte length.
	 */
	public void free(long addr);

	/**
	 * Clears all current allocations. Clearing an allocation context makes the
	 * backing heap storage available to immediate reallocation. Clearing the
	 * allocation context of the top-level {@link IMemoryManager} will release
	 * any direct {@link ByteBuffer}s back to the pool from which they were
	 * allocated.
	 * <p>
	 * <strong>CAUTION: Do not clear an allocation context until you know that
	 * all threads with access to that allocation context have either been
	 * terminated or released their reference to that allocation context.
	 * </strong>
	 */
	public void clear();

	/**
	 * Create a child allocation context within which the caller may make and
	 * release allocations.
	 */
	public IMemoryManager createAllocationContext();

	/**
	 * Return the size of the application data for the allocation with the given
	 * address.
	 * 
	 * @param addr
	 *            The address.
	 * 
	 * @return The #of bytes of in the applications allocation request.
	 */
	public int allocationSize(long addr);

	/**
	 * The #of allocation spanned by this allocation context (including any
	 * any child allocation contexts).
	 */
	public long getAllocationCount();

	/**
	 * Return the #of bytes of application data allocated against this
	 * {@link IMemoryManager} (including any child allocation contexts). Due to
	 * the overhead of the storage allocation scheme, this value may be smaller
	 * than {@link #getSlotBytes()}.
	 */
	public long getUserBytes();

	/**
	 * Return the #of bytes of consumed by allocation slots allocated against
	 * this {@link IMemoryManager} (including any child allocation contexts).
	 */
	public long getSlotBytes();
	
}
