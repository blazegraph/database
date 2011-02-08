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
import java.util.HashSet;

/**
 * The {@link AllocationContext} is used to maintain a handle on allocations
 * made within some specific environment (context).
 * 
 * In this way, clearing a context will return all allocations to the more
 * general pool.
 * 
 * There are two obvious implementation strategies:
 * <ol>
 * <li>Retaining set of addresses allocated</li>
 * <li>Retaining a copy of the allocated bits</li>
 * </ol>
 * 
 * If it was not for the BLOB implementations which require length data to
 * manage the freeing of an allocation, it would be efficient to maintain copies
 * of the allocation bits. This remains an option for the future but requires a
 * relatively complex callback protocol.
 * 
 * For this reason, the initial implementation maintains a set of allocated
 * addresses.
 * 
 * @author Martyn Cutcher
 */
public class AllocationContext implements IMemoryManager {
	
	private final IMemoryManager m_parent;
	
	private HashSet<Long> m_addresses = new HashSet<Long>();
	
	public AllocationContext(final IMemoryManager parent) {

		if(parent == null)
			throw new IllegalArgumentException();
		
		m_parent = parent;
		
	}

	synchronized
	public long allocate(final ByteBuffer data) {

		if (data == null)
			throw new IllegalArgumentException();
		
		final long addr = m_parent.allocate(data);
		
		// getSectorAllocation(addr).allocate(addr);		
		m_addresses.add(Long.valueOf(addr));
		
		return addr;
	}

	synchronized
	public void clear() {
		for (Long addr : m_addresses) {
			m_parent.free(addr);
		}
		
		m_addresses.clear();
	}

	synchronized
	public void free(final long addr) {
		// getSectorAllocation(addr).free(addr);
		m_addresses.remove(Long.valueOf(addr));
		
		m_parent.free(addr);
	}

	synchronized
	public ByteBuffer[] get(final long addr) {
		return m_parent.get(addr);
	}

	public IMemoryManager createAllocationContext() {
		return new AllocationContext(this);
	}

//	private SectorAllocation m_head = null;
//	
//	/**
//	 * Return the index of {@link SectorAllocator} for the given address.
//	 * 
//	 * @param addr
//	 *            The given address.
//	 * @return The index of the {@link SectorAllocator} containing that address.
//	 */
//	private int segmentID(final long addr) {
//		final int rwaddr = MemoryManager.getAllocationAddress(addr);
//		
//		return SectorAllocator.getSectorIndex(rwaddr);
//	}
//
//	/**
//	 * Return the bit offset into the bit map of {@link SectorAllocator} for the
//	 * given address.
//	 * 
//	 * @param addr
//	 *            The given address.
//	 * @return
//	 */
//	private int segmentOffset(final long addr) {
//		final int rwaddr = MemoryManager.getAllocationAddress(addr);
//		
//		return SectorAllocator.getSectorOffset(rwaddr);
//	}
//	
//	SectorAllocation getSectorAllocation(final long addr) {
//		final int index = segmentID(addr);
//		if (m_head == null) {
//			m_head = new SectorAllocation(index);
//		}
//		SectorAllocation sa = m_head;
//		while (sa.m_index != index) {
//			if (sa.m_next == null) {
//				sa.m_next = new SectorAllocation(index);
//			}
//			sa = sa.m_next;
//		}
//		
//		return sa;
//	}
//	
//	class SectorAllocation {
//		final int m_index;
//		final int[] m_bits = new int[SectorAllocator.NUM_ENTRIES];
//		SectorAllocation m_next = null;
//		
//		SectorAllocation(final int index) {
//			m_index = index;
//		}
//
//		public void allocate(long addr) {
//			assert !SectorAllocator.tstBit(m_bits, segmentOffset(addr));
//			
//			SectorAllocator.setBit(m_bits, segmentOffset(addr));
//		}
//
//		public void free(long addr) {
//			assert SectorAllocator.tstBit(m_bits, segmentOffset(addr));
//			
//			SectorAllocator.clrBit(m_bits, segmentOffset(addr));
//		}
//	}

}
