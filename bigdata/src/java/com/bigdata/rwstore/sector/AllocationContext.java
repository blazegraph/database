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
import java.util.concurrent.locks.ReentrantLock;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.OneShotInstrument;

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
	
	/**
	 * The top-level {@link MemoryManager}.
	 */
	private final MemoryManager m_root;
	
	/**
	 * The parent {@link IMemoryManager}.
	 */
	private final IMemoryManager m_parent;
	
	/**
	 * The lock used to serialize all all allocation/deallocation requests. This
	 * is shared by the top-level {@link MemoryManager} to avoid lock ordering
	 * problems.
	 */
	private final ReentrantLock lock; 

	/**
	 * All addresses allocated either directly by this {@link AllocationContext}
	 * or recursively by any {@link AllocationContext} created within this
	 * {@link AllocationContext}.
	 */
	private final LinkedHashSet<Long> m_addresses = new LinkedHashSet<Long>();
	
	private final AtomicLong m_allocCount = new AtomicLong();
	private final AtomicLong m_userBytes = new AtomicLong();
	private final AtomicLong m_slotBytes = new AtomicLong();

	public AllocationContext(final MemoryManager root) {
		
		if(root == null)
			throw new IllegalArgumentException();
		
		m_root = root;

		m_parent = root;
		
		lock = root.m_allocationLock;

	}
	
	public AllocationContext(final AllocationContext parent) {

		if(parent == null)
			throw new IllegalArgumentException();
		
		m_root = parent.m_root;

		m_parent = parent;
		
		lock = m_root.m_allocationLock;
		
	}

	public long allocate(final ByteBuffer data) {

		return allocate(data, true/* blocks */);

	}
	
	public long allocate(final ByteBuffer data, final boolean blocks) {

		if (data == null)
			throw new IllegalArgumentException();
		
		final long addr = allocate(data.remaining(), blocks);
		
		final ByteBuffer[] bufs = get(addr);

		MemoryManager.copyData(data, bufs);
		
		return addr;

	}

	public long allocate(final int nbytes) {

		return allocate(nbytes, true/*blocks*/);

	}
	
	/*
	 * Core impl.
	 */
	public long allocate(final int nbytes, final boolean blocks) {

		lock.lock();
		try {

			final long addr = m_parent.allocate(nbytes, blocks);

			final int rwaddr = MemoryManager.getAllocationAddress(addr);

			final SectorAllocator sector = m_root.getSector(rwaddr);

			m_addresses.add(Long.valueOf(addr));

			m_allocCount.incrementAndGet();
			m_userBytes.addAndGet(nbytes);
			m_slotBytes.addAndGet(sector.getPhysicalSize(SectorAllocator
					.getSectorOffset(rwaddr)));

			return addr;

		} finally {
			lock.unlock();
		}

	}

	public void clear() {

		lock.lock();
		try {

			for (Long addr : m_addresses) {

				m_parent.free(addr);

			}

			m_addresses.clear();

			m_allocCount.set(0);
			m_userBytes.set(0);
			m_slotBytes.set(0);

		} finally {
			lock.unlock();
		}

	}

	public void free(final long addr) {

		final int rwaddr = MemoryManager.getAllocationAddress(addr);
		final int size = MemoryManager.getAllocationSize(addr);
		final int offset = SectorAllocator.getSectorOffset(rwaddr);

		lock.lock();
		try {

			final SectorAllocator sector = m_root.getSector(rwaddr);

			m_parent.free(addr);

			m_addresses.remove(Long.valueOf(addr));

			m_allocCount.decrementAndGet();
			m_userBytes.addAndGet(-size);
			m_slotBytes.addAndGet(-sector.getPhysicalSize(offset));

		} finally {
			lock.unlock();
		}

	}

	public ByteBuffer[] get(final long addr) {

		return m_root.get(addr);

	}

	public byte[] read(final long addr) {

		return MemoryManager.read(this, addr);

	}

	public IMemoryManager createAllocationContext() {

		return new AllocationContext(this);

	}

	public int allocationSize(final long addr) {

		return m_root.allocationSize(addr);
		
	}

	public long getAllocationCount() {
		
		return m_allocCount.get();
		
	}
	
	public long getSlotBytes() {
	
		return m_slotBytes.get();
		
	}

	public long getUserBytes() {

		return m_userBytes.get();
		
	}

	public CounterSet getCounters() {
		
		final CounterSet root = new CounterSet();
		
		// #of allocation slot bytes.
		root.addCounter("slotBytes", new OneShotInstrument<Long>(
				getUserBytes()));
		
		// #of application data bytes.
		root.addCounter("userBytes", new OneShotInstrument<Long>(
				getUserBytes()));
	
		// #of allocations spanned by this context.
		root.addCounter("allocationCount", new OneShotInstrument<Long>(
				getAllocationCount()));
		
		return root;
		
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
