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
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSetAccess;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.io.DirectBufferPool;

/**
 * The MemoryManager manages an off-heap Direct {@link ByteBuffer}. It uses the
 * new SectorAllocator to allocate slots within the address range.
 * 
 * The interface is designed to support efficient transfer between NIO buffers.
 * 
 * The most complex aspect of the implementation is the BLOB representation,
 * requiring a mapping across multiple allocation slots. This is managed using
 * recursive calls in the main three methods: allocate, free and get.
 * 
 * @author Martyn Cutcher
 */
public class MemoryManager implements IMemoryManager, ISectorManager,
		ICounterSetAccess {

	private static final Logger log = Logger.getLogger(MemoryManager.class);

	/**
	 * The backing pool from which direct {@link ByteBuffer}s are recruited as
	 * necessary and returned when possible.
	 */
    private final DirectBufferPool m_pool;

	/**
	 * The set of direct {@link ByteBuffer} which are currently being managed by
	 * this {@link MemoryManager} instance.
	 */
    private final ByteBuffer[] m_resources;
	
	/**
	 * The lock used to serialize all allocation/deallocation requests. This is
	 * shared across all allocation contexts to avoid lock ordering problems.
	 */
    final /*private*/ ReentrantLock m_allocationLock = new ReentrantLock();

	/**
	 * Condition signalled when a sector is added to {@link #m_free}.
	 */
	final private Condition m_sectorFree = m_allocationLock.newCondition();

	/**
	 * The size of a backing buffer.
	 */
    private final int m_sectorSize;
	
	private final ArrayList<SectorAllocator> m_sectors = new ArrayList<SectorAllocator>();
	
	/**
	 * The free list (of sectors).
	 */
	private final ArrayList<SectorAllocator> m_free = new ArrayList<SectorAllocator>();

	/**
	 * The #of bytes in the backing buffers.
	 */
	private final AtomicLong m_allocation = new AtomicLong();
	
	/** The #of allocations. */
	private final AtomicLong m_allocCount = new AtomicLong();

	/** The #of application data bytes in current allocations. */
	private final AtomicLong m_userBytes = new AtomicLong();
	
	/** The #of slot bytes in current allocations. */
	private final AtomicLong m_slotBytes = new AtomicLong();

	/**
	 * Create a new {@link MemoryManager}.
	 * <p>
	 * The backing {@link DirectBufferPool} may be either bounded or
	 * (effectively) unbounded. The {@link MemoryManager} may also be bounded or
	 * (effectively) unbounded. If either the pool or the memory manager is
	 * bounded, then <em>blocking</em> allocation requests may block. Neither
	 * non-blocking allocation requests nor allocation requests made against an
	 * unbounded memory manager backed by an unbounded pool will block. The
	 * preferred method for bounding the memory manager is to specify a maximum
	 * #of buffers which it may consume from the pool.
	 * <p>
	 * The garbage collection of direct {@link ByteBuffer}s depends on a full GC
	 * pass. In an application which managers its heap pressure well, full GC
	 * passes are rare. Therefore, the best practice is to share an unbounded
	 * pool across multiple purposes. Since there are typically multiple users
	 * of the pool, the demand can not always be predicated and deadlocks can
	 * arise with a bounded pool.
	 * <p>
	 * Individual buffers will be allocated as necessary and released if they
	 * become empty. However, since allocation patterns may cause the in use
	 * data to be scattered across the allocated buffers, the backing buffers
	 * may not be returned to the backing pool until the top-level allocation
	 * context is cleared.
	 * <p>
	 * Any storage allocated by this instance will be released no later than
	 * when the instance is {@link #finalize() finalized}. Storage may be
	 * returned to the pool within the life cycle of the {@link MemoryManager}
	 * using {@link #clear()}. Nested allocation contexts may be created and
	 * managed using {@link #createAllocationContext()}.
	 * 
	 * @param pool
	 *            The pool from which the {@link MemoryManager} will allocate
	 *            its buffers (each "sector" is one buffer).
	 * @param sectors
	 *            The maximum #of buffers which the {@link MemoryManager} will
	 *            allocate from that pool (each "sector" is one buffer). This
	 *            may be {@link Integer#MAX_VALUE} for an effectively unbounded
	 *            capacity.
	 *            
	 * @throws IllegalArgumentException
	 *             if <i>pool</i> is <code>null</code>.
	 * @throws IllegalArgumentException
	 *             if <i>sectors</i> is non-positive.
	 */
	public MemoryManager(final DirectBufferPool pool, final int sectors) {
		
		if (pool == null)
			throw new IllegalArgumentException();
		
		if (sectors <= 0)
			throw new IllegalArgumentException();
		
		m_pool = pool;
		
		m_resources = new ByteBuffer[sectors];
		
		m_sectorSize = pool.getBufferCapacity();
		
	}

	protected void finalize() throws Throwable {
		// release to pool.
		releaseDirectBuffers();
	}

	/**
	 * Releases the backing direct buffers.
	 * <p>
	 * Note: Other than {@link #finalize()}, the caller MUST hold the
	 * {@link #m_allocationLock}.
	 * 
	 * @throws Throwable
	 */
	private void releaseDirectBuffers() {
		// release to pool.
		for (int i = 0; i < m_resources.length; i++) {
			final ByteBuffer buf = m_resources[i];
			if (buf != null) {
				try {
					DirectBufferPool.INSTANCE.release(buf);
				} catch (InterruptedException e) {
					log.error("Unable to release direct buffers", e);
				} finally {
					m_resources[i] = null;
				}
			}
		}
	}

	/**
	 * Return the maximum #of sectors which may be allocated.
	 */
	public int getMaxSectors() {
		return m_resources.length;
	}
	
	/**
	 * Return the #of sectors which are currently in use. 
	 */
	public int getSectorCount() {
		m_allocationLock.lock();
		try {
			return m_sectors.size();
		} finally {
			m_allocationLock.unlock();
		}
	}
	
	/**
	 * The size in bytes of the backing sector. This is the upper bound on the
	 * #of bytes which may be stored against a single sector.
	 */
	public int getSectorSize() {
		
		return m_sectorSize;
		
	}

	/**
	 * The maximum capacity in bytes of the managed memory.
	 */
	public long getMaxMemoryCapacity() {

		return m_sectorSize * (long) m_resources.length;
		
	}
	
	public long allocate(final ByteBuffer data) {

		return allocate(data, true/* blocks */);

	}

	public long allocate(final ByteBuffer data, final boolean blocks) {

		if (data == null)
			throw new IllegalArgumentException();

		final int nbytes = data.remaining();

		if (nbytes == 0)
			throw new IllegalArgumentException();
		
		final long retaddr = allocate(nbytes, blocks);

		final ByteBuffer[] bufs = get(retaddr);

		copyData(data, bufs);

		return retaddr;
	}

	/**
	 * Copy the data from the source buffer to the target buffers.
	 * <p>
	 * Note: Per the API (and for consistency with the IRawStore API), this
	 * method has a side-effect on the position of each source buffer.
	 */
	static void copyData(final ByteBuffer src, final ByteBuffer[] dst) {

//		final ByteBuffer src = data;//data.duplicate();
		int pos = 0;
		for (int i = 0; i < dst.length; i++) {
			final int tsize = dst[i].remaining();
			src.limit(pos + tsize);
			src.position(pos);
			dst[i].put(src);
			pos += tsize;
		}

	}
	
	public long allocate(final int nbytes) {

		return allocate(nbytes, true/*blocks*/);

	}

	/**
	 * Create a new sector and drop it on the free list.
	 */
	private SectorAllocator getSectorFromFreeList(final boolean blocks) {
		
		while(m_free.isEmpty()) {

			if (m_sectors.size() < m_resources.length) {

				/*
				 * We are under capacity, so allocate a new sector and add it to
				 * the free list.
				 */

				// Allocate new buffer (blocking request).
				final ByteBuffer nbuf;
				try {
					if (blocks) {
						nbuf = m_pool.acquire();
					} else {
						nbuf = m_pool.acquire(1L, TimeUnit.NANOSECONDS);
					}
				} catch (InterruptedException ex) {
					throw new RuntimeException(ex);
				} catch (TimeoutException ex) {
					throw new RuntimeException(ex);
				}

				m_resources[m_sectors.size()] = nbuf;

				final SectorAllocator sector = new SectorAllocator(this, null);
				// Note: The sector will add itself to the free list.
				sector.setSectorAddress(m_allocation.get(), m_sectorSize);
				sector.setIndex(m_sectors.size());

				m_sectors.add(sector);

				m_allocation.addAndGet(m_sectorSize);

			} else {

				if (blocks) {
					
					/*
					 * We are at the maximum #of sectors, so wait for something
					 * to get freed. Once enough data is freed from some sector,
					 * that sector will be placed back onto the free list.
					 */

					if(log.isDebugEnabled())
						log.debug("Blocking...");
					
					try {
						m_sectorFree.await();
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
					
				} else {
					
					throw new MemoryManagerOutOfMemory();
					
				}
				
			}
			
		}
		
		return m_free.get(0);

	}

	/*
	 * Core allocation method.
	 */
	public long allocate(final int nbytes, final boolean blocks) {

		if (nbytes <= 0)
			throw new IllegalArgumentException();
		
		m_allocationLock.lock();
		try {
			if (nbytes <= SectorAllocator.BLOB_SIZE) {

				final SectorAllocator sector = getSectorFromFreeList(blocks); 
				
				final int rwaddr = sector.alloc(nbytes);
				
				if (SectorAllocator.getSectorIndex(rwaddr) >= m_sectors.size()) {
					throw new IllegalStateException("Address: " + rwaddr + " yields index: " + SectorAllocator.getSectorIndex(rwaddr));
				}
				
				if (log.isTraceEnabled())
					log.trace("allocating bit: " + SectorAllocator.getSectorOffset(rwaddr));

				// Now copy the data to the backing resource
//				final long paddr = sector.getPhysicalAddress(SectorAllocator.getSectorOffset(rwaddr));
//				final ByteBuffer dest = m_resources[sector.m_index].duplicate();
//				final int bufferAddr = (int) (paddr - (sector.m_index * m_sectorSize));
//				dest.limit(bufferAddr + nbytes);
//				dest.position(bufferAddr);
//				dest.put(data);
				
				m_allocCount.incrementAndGet();
				m_userBytes.addAndGet(nbytes);
				m_slotBytes.addAndGet(sector.getPhysicalSize(SectorAllocator
						.getSectorOffset(rwaddr)));

				return makeAddr(rwaddr, nbytes);
				
			} else {

				/**
				 * For Blob allocation call the normal allocate and retrieve
				 * the allocation address to store in the blob header.
				 */
				final int nblocks = SectorAllocator.getBlobBlockCount(nbytes);
				final ByteBuffer hdrbuf = ByteBuffer.allocate(nblocks * 4);
				for (int i = 0; i < nblocks; i++) {
					final int pos = SectorAllocator.BLOB_SIZE * i;
					final int bsize = i < (nblocks-1) ? SectorAllocator.BLOB_SIZE : nbytes - pos;

					/*
					 * BLOB RECURSION
					 */
					final long bpaddr = allocate(bsize, blocks);
					final int bprwaddr = getAllocationAddress(bpaddr);
					hdrbuf.putInt(bprwaddr);
				}
				
				// now allocate the blob header and fix the return address size
				hdrbuf.flip();
				final int retaddr = getAllocationAddress(allocate(hdrbuf,blocks));
				
				return makeAddr(retaddr, nbytes);
			}
			
		} finally {
			m_allocationLock.unlock();
		}
	}
	
	public ByteBuffer[] get(final long addr) {

		if (addr == 0L)
			throw new IllegalArgumentException();
		
		final int rwaddr = getAllocationAddress(addr);
		final int size = getAllocationSize(addr);

		if (size <= 0)
			throw new IllegalArgumentException();

		if (size <= SectorAllocator.BLOB_SIZE) {

			/*
			 * This is a simple allocation.
			 */
			
			return new ByteBuffer[] { getBuffer(rwaddr, size) };
			
		} else {
			
			/*
			 * This will be a BLOB, so retrieve the header, then parse to
			 * retrieve components and assign to ByteBuffer[].
			 */
			
			final ByteBuffer hdrbuf = getBlobHdr(addr);
			
			final int nblocks = hdrbuf.remaining() / 4;
			
			final ByteBuffer[] blobbufs = new ByteBuffer[nblocks];
			int remaining = size;
			for (int i = 0; i < nblocks; i++) {
				int blockSize = remaining <= SectorAllocator.BLOB_SIZE ? remaining : SectorAllocator.BLOB_SIZE;
				blobbufs[i] = getBuffer(hdrbuf.getInt(), blockSize);
				
				remaining -= blockSize;
			}
			
			return blobbufs;
		}
	}

	public byte[] read(final long addr) {

		return MemoryManager.read(this, addr);
		
	}

	/**
	 * Utility method to read and return the application data stored at a given
	 * address.
	 * 
	 * @param mmgr
	 *            The allocation context.
	 * @param addr
	 *            The allocation address.
	 *            
	 * @return A copy of the data stored at that address.
	 */
	static byte[] read(IMemoryManager mmgr, final long addr) {

		final int nbytes = getAllocationSize(addr);
		
		final byte[] a = new byte[nbytes];
		
		final ByteBuffer mybb = ByteBuffer.wrap(a);
		
		final ByteBuffer[] bufs = mmgr.get(addr);
		
		for (ByteBuffer b : bufs) {
			
			mybb.put(b);
			
		}
	
		return a;

	}

	/**
	 * Given an address of a blob, determine the size of the header and
	 * create an address to support direct retrieval of the header.
	 * 
	 * This is required to support get and free.
	 * 
	 * @param addr of blob header
	 * @return the ByteBuffer containing the header
	 */
	private ByteBuffer getBlobHdr(final long addr) {
		int size = getAllocationSize(addr);
		
		final int nblocks = SectorAllocator.getBlobBlockCount(size);
		final int hdrsize = 4*nblocks;
		
		// Mockup hdraddr with header size to retrieve the ByteBuffer
		final long hdraddr = (addr & 0xFFFFFFFF00000000L) | hdrsize;
		
		return get(hdraddr)[0];
	}

	/**
	 * Return a mutable view of the user allocation.
	 * 
	 * @param rwaddr
	 *            The address.
	 * @param size
	 *            The byte length of the user application.
	 *            
	 * @return The mutable view.
	 */
	private ByteBuffer getBuffer(final int rwaddr, final int size) {

		final SectorAllocator sector = getSector(rwaddr);
		
		final int offset = SectorAllocator.getSectorOffset(rwaddr);
		
		final long paddr = sector.getPhysicalAddress(offset);
		
		// Duplicate the buffer to avoid side effects to position and limit.
		final ByteBuffer ret = m_resources[sector.m_index].duplicate();
		
		final int bufferAddr = (int) (paddr - (sector.m_index * m_sectorSize));
		
		// Set position and limit of the view onto the backing buffer.
		ret.limit(bufferAddr + size);
		ret.position(bufferAddr);
		
		// Take a slice to fix the view of the buffer we return to the caller.
		return ret.slice();
	}

	/**
	 * Return the sector for the address.
	 * 
	 * @param rwaddr
	 *            The address.
	 *            
	 * @return The sector.
	 */
	SectorAllocator getSector(final int rwaddr) {
	
		final int index = SectorAllocator.getSectorIndex(rwaddr);

		if (index >= m_sectors.size())
			throw new IllegalStateException("Address: " + rwaddr
					+ " yields index: " + index + " >= sector:size(): "
					+ m_sectors.size());

		return m_sectors.get(index);

	}

	static int getAllocationAddress(final long addr) {
		return (int) (addr >> 32L);
	}

	/**
	 * Return the size of the application data for the allocation with the given
	 * address.
	 * 
	 * @param addr
	 *            The address.
	 * 
	 * @return The #of bytes of in the applications allocation request.
	 */
	static int getAllocationSize(final long addr) {
		return (int) (addr & 0xFFFFFFFFL);
	}

	public void free(final long addr) {

		if (addr == 0L)
			throw new IllegalArgumentException();

		final int rwaddr = getAllocationAddress(addr);
		
		final int size = getAllocationSize(addr);
		
		if (size == 0)
			throw new IllegalArgumentException();
		
		m_allocationLock.lock();
		try {

			if (size <= SectorAllocator.BLOB_SIZE) {
				
				final int offset = SectorAllocator.getSectorOffset(rwaddr);
				
				// free of a simple allocation.
				final SectorAllocator sector = getSector(rwaddr);
				sector.free(offset);
				
				m_allocCount.decrementAndGet();
				m_userBytes.addAndGet(-size);
				m_slotBytes.addAndGet(-sector.getPhysicalSize(offset));

				/*
				 * TODO if the sector is empty, release it back to the pool.
				 * 
				 * Note: Sectors can have allocator metadata so they may not be
				 * empty in terms of slot bytes even though no user data is
				 * allocated against the sector. Such sectors may be released
				 * back to the pool.
				 */
				
			} else {

				// free of a blob.
				final ByteBuffer hdrbuf = getBlobHdr(addr);
				final int spos = hdrbuf.position();
				final int hdrsize = hdrbuf.limit() - spos;
				final int nblocks = hdrsize / 4;
				
				// free each block
				int remaining = size;
				for (int i = 0; i < nblocks; i++) {
					int blockSize = remaining <= SectorAllocator.BLOB_SIZE ? remaining : SectorAllocator.BLOB_SIZE;
					final long mkaddr = makeAddr(hdrbuf.getInt(), blockSize);
					/*
					 * BLOB RECURSION
					 */
					free(mkaddr);
				}
				hdrbuf.position(spos);
				// now free the header
				free(makeAddr(rwaddr, hdrsize));
			}
		} finally {
			m_allocationLock.unlock();
		}
	}
	
	private long makeAddr(final int rwaddr, final int size) {

		long addr = rwaddr;
		addr <<= 32;
		addr += size;
		
		assert rwaddr == getAllocationAddress(addr);
		assert size == getAllocationSize(addr);
		return addr;
	}

	public void clear() {
		m_allocationLock.lock();
		try {
			m_sectors.clear();
			m_free.clear();
			m_allocation.set(0L);
			m_allocCount.set(0L);
			m_userBytes.set(0L);
			m_slotBytes.set(0L);
			releaseDirectBuffers(); // release buffers back to the pool.
			m_sectorFree.signalAll();
		} finally {
			m_allocationLock.unlock();
		}
	}
	
	public void addToFreeList(final SectorAllocator sector) {
		m_allocationLock.lock();
		try {
			m_free.add(sector);
			m_sectorFree.signalAll();
		} finally {
			m_allocationLock.unlock();
		}
	}

	public void removeFromFreeList(final SectorAllocator sector) {
		m_allocationLock.lock();
		try {
			assert m_free.get(0) == sector;
			m_free.remove(sector);
		} finally {
			m_allocationLock.unlock();
		}
	}

	public void trimSector(final long trim, final SectorAllocator sector) {
		// Do not trim when using buffer pool
	}

	public IMemoryManager createAllocationContext() {
		return new AllocationContext(this);
	}

	public int allocationSize(final long addr) {
		return getAllocationSize(addr);
	}

	/**
	 * The maximum #of bytes which are available to the memory manager.
	 */
	public long getCapacity() {

		return m_pool.getBufferCapacity() * (long) m_resources.length;

	}
	
	/**
	 * The total #of bytes in the backing buffers currently attached to the
	 * {@link MemoryManager}.
	 */
	public long getExtent() {
		return m_allocation.get();
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

		// maximum #of buffers which may be allocated.
		root.addCounter("bufferCapacity", new OneShotInstrument<Integer>(
				m_resources.length));

		// current #of buffers which are allocated.
		root.addCounter("bufferCount", new OneShotInstrument<Integer>(
				getSectorCount()));

		// current backing storage in bytes.
		root.addCounter("extent", new OneShotInstrument<Long>(m_allocation
				.get()));

		// the current #of allocation.
		root.addCounter("allocationCount", new OneShotInstrument<Long>(
				getAllocationCount()));
		
		// #of allocation slot bytes.
		root.addCounter("slotBytes", new OneShotInstrument<Long>(
				getUserBytes()));
		
		// #of application data bytes.
		root.addCounter("userBytes", new OneShotInstrument<Long>(
				getUserBytes()));
		
		return root;
		
	}

	/*
	 * TODO The constructor must be able to accept nsectors := Integer.MAX_VALUE
	 * in order to indicate that the #of backing buffers is (conceptually)
	 * unbounded. This will require m_resources to be a data structure other
	 * than a simple array.
	 * 
	 * TODO Release empty buffers back to the pool. may require m_sectors to be
	 * a proper array and explicitly track which elements of the array are
	 * non-null.
	 * 
	 * TODO Give capacity to allocation context and have it throw an exception
	 * if the capacity would be exceeded?
	 * 
	 * TODO Should all allocation contexts share the reference to the allocation
	 * lock of the memory manager? (I think so. B)
	 */
	
}
