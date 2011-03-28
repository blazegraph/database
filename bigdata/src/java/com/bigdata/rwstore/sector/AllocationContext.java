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
import java.util.LinkedHashSet;
import java.util.concurrent.atomic.AtomicLong;

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
	
	private final MemoryManager m_root;
	private final IMemoryManager m_parent;

	/**
	 * All addresses allocated either directly by this {@link AllocationContext}
	 * or recursively by any {@link AllocationContext} created within this
	 * {@link AllocationContext}.
	 */
	private final LinkedHashSet<Long> m_addresses = new LinkedHashSet<Long>();
	
	private final AtomicLong m_userBytes = new AtomicLong();
	private final AtomicLong m_slotBytes = new AtomicLong();

	public AllocationContext(final MemoryManager root) {
		
		if(root == null)
			throw new IllegalArgumentException();
		
		m_root = root;
		m_parent = root;

	}
	
	public AllocationContext(final AllocationContext parent) {

		if(parent == null)
			throw new IllegalArgumentException();
		
		m_root = parent.m_root;
		m_parent = parent;
		
	}

	synchronized
	public long allocate(final ByteBuffer data) {

		if (data == null)
			throw new IllegalArgumentException();
		
		final long addr = allocate(data.remaining());
		
		final ByteBuffer[] bufs = get(addr);

		MemoryManager.copyData(data, bufs);
	
		// getSectorAllocation(addr).allocate(addr);		
		m_addresses.add(Long.valueOf(addr));
		
		return addr;
	}

	/*
	 * Core impl.
	 */
	synchronized
	public long allocate(final int nbytes) {

		final long addr = m_parent.allocate(nbytes);
		
		final int rwaddr = MemoryManager.getAllocationAddress(addr);
//		final int size = getAllocationSize(addr);
		final SectorAllocator sector = m_root.getSector(rwaddr);

		m_userBytes.addAndGet(nbytes);
		m_slotBytes.addAndGet(sector.getPhysicalSize(SectorAllocator
				.getSectorOffset(rwaddr)));

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

		final int rwaddr = MemoryManager.getAllocationAddress(addr);
		final int size = MemoryManager.getAllocationSize(addr);
		final int offset = SectorAllocator.getSectorOffset(rwaddr);
		
		final SectorAllocator sector = m_root.getSector(rwaddr);
		
		m_parent.free(addr);
		
		m_addresses.remove(Long.valueOf(addr));
		
		m_userBytes.addAndGet(-size);
		m_slotBytes.addAndGet(-sector.getPhysicalSize(offset));

	}

	public ByteBuffer[] get(final long addr) {
		return m_root.get(addr);
	}

	public IMemoryManager createAllocationContext() {
		return new AllocationContext(this);
	}

	public int allocationSize(final long addr) {
		return m_root.allocationSize(addr);
	}

	public long getSlotBytes() {
		return m_slotBytes.get();
	}

	public long getUserBytes() {
		return m_userBytes.get();
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
