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

/**
 * Abstraction for managing data in {@link ByteBuffer}s. Typically those buffers
 * will be allocated on the native process heap.
 * 
 * @author martyncutcher
 */
public interface IMemoryManager {

	/**
	 * Allocates space on the backing resource and copies the provided data.
	 * 
	 * @param data
	 *            The data will be copied to the backing resource
	 * 
	 * @return the address to be passed to the get method to retrieve the data
	 * 
	 * @throws IllegalArgumentException
	 *             if the argument is <code>null</code>.
	 *             
	 *             FIXME Replace with allocate(int nbytes):ByteBuffer[] so the
	 *             caller can do zero copy NIO.
	 */
	public long allocate(ByteBuffer data);

	/**
	 * To give more control to the caller by reserving the allocations without
	 * the requirement to supply a source ByteBuffer.
	 * 
	 * @param nbytes
	 * @return an address that will return a ByteBuffer[] using get
	 */
	public long allocate(int nbytes);

	/**
	 * The ByteBuffer[] return enables the handling of blobs that span more than
	 * a single slot, without the need to create an intermediate ByteBuffer.
	 * 
	 * This will support transfers directly to other direct ByteBuffers, for
	 * example for network IO.
	 * 
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
	 * Furthermore, since the ByteBuffers are not read-only, they can be updated
	 * directly.  In this way the {@link #allocate(int)} can be used
	 * in conjunction with get to provide more flexibility when storing data.
	 * 
	 * @param addr
	 *            An address previously returned by
	 *            {@link #allocate(ByteBuffer)} or {@link #allocate(int)}.
	 *            
	 * @return array of ByteBuffers
	 */
	public ByteBuffer[] get(long addr);
	
	/**
	 * Frees the address and makes available for recycling
	 * 
	 * @param addr to be freed
	 */
	public void free(long addr);
	
	/**
	 * Clears all current allocations
	 */
	public void clear();

	/**
	 * Create a child allocation context within which the caller may make and
	 * release allocations.
	 */
	public IMemoryManager createAllocationContext();

	/**
	 * @param addr
	 * @return the number of bytes allocated at the address
	 */
	public int allocationSize(long addr);
	
}
