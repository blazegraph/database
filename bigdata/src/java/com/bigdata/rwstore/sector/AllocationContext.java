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
 * The AllocationContext is used to maintain a handle on allocations made
 * within some specific environment (context).
 * 
 * In this way, clearing a context will return all allocations to the more
 * general pool.
 * 
 * There are two obvious implementaiton strategies:
 * 1) Retaining set of addresses allocated
 * 2) Retaining a copy of the allocated bits
 * 
 * If it was not for the BLOB implementations which require length
 * data to manage the freeing of an allocation, it would be efficient to
 * maintain copies of the allocation bits.  This remoains an option for the
 * future but requires a relatively complex callback protocol.
 * 
 * For this reason, the initial implementation maintains a set of allocated
 * addresses.
 * 
 * @author Martyn Cutcher
 *
 */
public class AllocationContext implements IMemoryManager {
	
	final IMemoryManager m_parent;
	
	SectorAllocation m_head = null;
	
	HashSet<Long> m_addresses = new HashSet<Long>();
	
	public AllocationContext(IMemoryManager parent) {
		m_parent = parent;
	}

	public long allocate(final ByteBuffer data) {
		final long addr = m_parent.allocate(data);
		
		// getSectorAllocation(addr).allocate(addr);		
		m_addresses.add(Long.valueOf(addr));
		
		return addr;
	}

	/**
	 * The main reason for the AllocationContext is to be
	 * able to atomically release the associated allocations
	 */
	public void clear() {
		for (Long addr : m_addresses) {
			m_parent.free(addr);
		}
		
		m_addresses.clear();
	}

	public void free(final long addr) {
		// getSectorAllocation(addr).free(addr);
		m_addresses.remove(Long.valueOf(addr));
		
		m_parent.free(addr);
	}

	public ByteBuffer[] get(long addr) {
		return m_parent.get(addr);
	}

	public AllocationContext createAllocationContext() {
		return new AllocationContext(this);
	}
	
	private int segmentID(final long addr) {
		final int rwaddr = MemoryManager.getAllocationAddress(addr);
		
		return SectorAllocator.getSectorIndex(rwaddr);
	}
	
	private int segmentOffset(final long addr) {
		final int rwaddr = MemoryManager.getAllocationAddress(addr);
		
		return SectorAllocator.getSectorOffset(rwaddr);
	}
	
	SectorAllocation getSectorAllocation(final long addr) {
		final int index = segmentID(addr);
		if (m_head == null) {
			m_head = new SectorAllocation(index);
		}
		SectorAllocation sa = m_head;
		while (sa.m_index != index) {
			if (sa.m_next == null) {
				sa.m_next = new SectorAllocation(index);
			}
			sa = sa.m_next;
		}
		
		return sa;
	}
	
	class SectorAllocation {
		final int m_index;
		final int[] m_bits = new int[SectorAllocator.NUM_ENTRIES];
		SectorAllocation m_next = null;
		
		SectorAllocation(final int index) {
			m_index = index;
		}

		public void allocate(long addr) {
			assert !SectorAllocator.tstBit(m_bits, segmentOffset(addr));
			
			SectorAllocator.setBit(m_bits, segmentOffset(addr));
		}

		public void free(long addr) {
			assert SectorAllocator.tstBit(m_bits, segmentOffset(addr));
			
			SectorAllocator.clrBit(m_bits, segmentOffset(addr));
		}
	}

}
