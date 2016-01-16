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

package com.bigdata.rwstore.sector;

import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedHashSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.ICommitter;
import com.bigdata.rawstore.IAllocationContext;
import com.bigdata.rawstore.IPSOutputStream;
import com.bigdata.rwstore.IRawTx;
import com.bigdata.rwstore.PSOutputStream;

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
public class AllocationContext implements IAllocationContext, IMemoryManager {//, IStore {
	
	private static final transient Logger log = Logger
			.getLogger(AllocationContext.class);
	
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
	private final Lock writeLock; 
	private final Lock readLock; 

	/**
	 * All addresses allocated either directly by this {@link AllocationContext}
	 * or recursively by any {@link AllocationContext} created within this
	 * {@link AllocationContext}.
	 */
	private final LinkedHashSet<Long> m_addresses = new LinkedHashSet<Long>();
	
	private final AtomicLong m_allocCount = new AtomicLong();
	private final AtomicLong m_userBytes = new AtomicLong();
	private final AtomicLong m_slotBytes = new AtomicLong();

	/**
	 * Note: Must be either atomic or volatile since accessed without a lock!
	 */
	private final AtomicBoolean m_active = new AtomicBoolean(true);
	
	final boolean m_isolated;

	@Override
	final public void checkActive() {
		if (!m_active.get()) {
			throw new IllegalStateException();
		}
	}
	
	public AllocationContext(final MemoryManager root, boolean isolated) {
		
		if(root == null)
			throw new IllegalArgumentException();
		
		m_isolated = isolated;
		
		m_root = root;

		m_parent = root;
		
		writeLock = root.m_allocationLock.writeLock();
		readLock = m_root.m_allocationLock.readLock();

	}
	
	public AllocationContext(final AllocationContext parent) {

		if(parent == null)
			throw new IllegalArgumentException();
		
		m_root = parent.m_root;

		m_parent = parent;
		
		writeLock = m_root.m_allocationLock.writeLock();
		readLock = m_root.m_allocationLock.readLock();
		
		m_isolated = parent.m_isolated;
		
	}
	
	@Override
	public boolean isIsolated() {
		return m_isolated;
	}

	@Override
	public long allocate(final ByteBuffer data) {

		return allocate(data, true/* blocks */);

	}
	
	@Override
	public long allocate(final ByteBuffer data, final boolean blocks) {

		if (data == null)
			throw new IllegalArgumentException();
		
		final long addr = allocate(data.remaining(), blocks);
		
		final ByteBuffer[] bufs = get(addr);

		MemoryManager.copyData(data, bufs);
		
		return addr;

	}

	@Override
	public long allocate(final int nbytes) {

		return allocate(nbytes, true/*blocks*/);

	}
	
	/*
	 * Core impl.
	 */
	@Override
	public long allocate(final int nbytes, final boolean blocks) {

		writeLock.lock();
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
			writeLock.unlock();
		}

	}

	@Override
	public void clear() {

		writeLock.lock();
		try {

			if(log.isDebugEnabled())
				log.debug("");
			
			for (Long addr : m_addresses) {

				m_parent.free(addr);

			}

			m_addresses.clear();

			m_allocCount.set(0);
			m_userBytes.set(0);
			m_slotBytes.set(0);

		} finally {
			writeLock.unlock();
		}

	}

	@Override
	public void free(final long addr) {

		final int rwaddr = MemoryManager.getAllocationAddress(addr);
		final int size = MemoryManager.getAllocationSize(addr);
		final int offset = SectorAllocator.getSectorOffset(rwaddr);

		writeLock.lock();
		try {

			final SectorAllocator sector = m_root.getSector(rwaddr);

			m_parent.free(addr);

			m_addresses.remove(Long.valueOf(addr));

			m_allocCount.decrementAndGet();
			m_userBytes.addAndGet(-size);
			m_slotBytes.addAndGet(-sector.getPhysicalSize(offset));

		} finally {
			writeLock.unlock();
		}

	}

	@Override
	public ByteBuffer[] get(final long addr) {

		return m_root.get(addr);

	}

	@Override
	public byte[] read(final long addr) {

		return MemoryManager.read(this, addr);

	}

	@Override
	public IMemoryManager createAllocationContext() {

		return new AllocationContext(this);

	}

	@Override
	public int allocationSize(final long addr) {

		return m_root.allocationSize(addr);
		
	}

	@Override
	public long getAllocationCount() {
		
		return m_allocCount.get();
		
	}
	
	@Override
	public long getSlotBytes() {
	
		return m_slotBytes.get();
		
	}

	@Override
	public long getUserBytes() {

		return m_userBytes.get();
		
	}

	@Override
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

	@Override
	public IPSOutputStream getOutputStream() {
		return PSOutputStream.getNew(this, SectorAllocator.BLOB_SIZE+4 /*no checksum*/, null);
	}

    @Override
    public IPSOutputStream getOutputStream(IAllocationContext context) {
        return PSOutputStream.getNew(this, SectorAllocator.BLOB_SIZE+4 /*no checksum*/, context);
    }

	@Override
	public InputStream getInputStream(long addr) {
		return new PSInputStream(this, addr);
	}

	@Override
	public long alloc(byte[] buf, int size, IAllocationContext context) {
		if (context != null)
			throw new IllegalArgumentException("Nested AllocationContexts are not supported");
		
		return MemoryManager.getAllocationAddress(allocate(ByteBuffer.wrap(buf, 0, size))); // return the rwaddr!
	}

	@Override
	public void close() {
		clear();
	}

	@Override
	public void free(long addr, int size) {
		free((addr << 32) + size);
	}

	@Override
	public int getAssociatedSlotSize(int addr) {
		return m_root.allocationSize(addr);
	}

	@Override
	public void getData(long l, byte[] buf) {
		/**
		 * TBD: this could probably be more efficient!
		 */
		readLock.lock();
		try {
			final ByteBuffer rbuf = m_root.getBuffer((int) l, buf.length);
			rbuf.get(buf);
		} finally {
			readLock.unlock();
		}
	}

	@Override
	public File getStoreFile() {
		throw new UnsupportedOperationException("The MemoryManager does not provdie a StoreFile");

	}

	@Override
	public int getSectorSize() {
		return m_root.getSectorSize();
	}

	@Override
	public int getMaxSectors() {
		return m_root.getMaxSectors();
	}

	@Override
	public int getSectorCount() {
		return m_root.getSectorCount();
	}

	@Override
	public void commit() {
		m_root.commit();
	}

    @Override
    public Lock getCommitLock() {
        return m_root.getCommitLock();
    }

	@Override
	public void postCommit() {
		m_root.postCommit();
	}

	@Override
	public void registerExternalCache(
			ConcurrentWeakValueCache<Long, ICommitter> historicalIndexCache,
			int byteCount) {
		m_root.registerExternalCache(historicalIndexCache, byteCount);
	}

	@Override
	public int checkDeferredFrees(AbstractJournal abstractJournal) {
		return m_root.checkDeferredFrees(abstractJournal);
	}

	@Override
	public IRawTx newTx() {
		return m_root.newTx();
	}

	@Override
	public long saveDeferrals() {
		return m_root.saveDeferrals();
	}

	@Override
	public long getLastReleaseTime() {
		return m_root.getLastReleaseTime();
	}

	@Override
	public void abortContext(final IAllocationContext context) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void detachContext(final IAllocationContext context) {
		throw new UnsupportedOperationException();
	}

//	@Override
//	public void registerContext(final IAllocationContext context) {
//		throw new UnsupportedOperationException();
//	}

//	@Override
//	public void setRetention(final long parseLong) {
//		throw new UnsupportedOperationException();
//	}

	@Override
	public boolean isCommitted(final long addr) {
		return m_root.isCommitted(addr);
	}

	@Override
	public long getPhysicalAddress(final long addr) {
		return m_root.getPhysicalAddress(addr);
	}

	@Override
	public long allocate(ByteBuffer data, IAllocationContext context) {
		throw new UnsupportedOperationException();
	}

    @Override
    public long write(ByteBuffer data, IAllocationContext context) {
        return allocate(data,context);
    }

	@Override
	public void free(long addr, IAllocationContext context) {
		throw new UnsupportedOperationException();
	}

    @Override
    public void delete(long addr, IAllocationContext context) {
        free(addr,context);
    }

	@Override
	public IAllocationContext newAllocationContext(final boolean isolated) {
		return this;
	}

	@Override
	public void release() {
		checkActive();
		
		m_active.set(false);
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
