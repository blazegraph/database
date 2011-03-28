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
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

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
public class MemoryManager implements IMemoryManager, ISectorManager {

    private static final Logger log = Logger
    .getLogger(MemoryManager.class);

    private final DirectBufferPool m_pool;
    private final ByteBuffer[] m_resources;
	
    final private ReentrantLock m_allocationLock = new ReentrantLock();

    int m_allocation = 0;
    private final int m_sectorSize;
	
	private final ArrayList<SectorAllocator> m_sectors = new ArrayList<SectorAllocator>();
	private final ArrayList<SectorAllocator> m_free = new ArrayList<SectorAllocator>();

	private final AtomicLong m_userBytes = new AtomicLong();
	private final AtomicLong m_slotBytes = new AtomicLong();

	/**
	 * Create a new {@link MemoryManager}. Any storage allocated by this
	 * instance will be released no later than when the instance is
	 * {@link #finalize() finalized}. Storage may be returned to the pool within
	 * the life cycle of the {@link MemoryManager} using {@link #clear()}.
	 * Nested allocation contexts may be created and managed using
	 * {@link #createAllocationContext()}.
	 * 
	 * @param pool
	 *            The pool from which the {@link MemoryManager} will allocate
	 *            its buffers.
	 * @param sectors
	 *            The maximum #of buffers which the {@link MemoryManager} will
	 *            allocate from that pool.
	 */
	public MemoryManager(final DirectBufferPool pool, final int sectors) {
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
	private void releaseDirectBuffers() throws InterruptedException {
		// release to pool.
		for (int i = 0; i < m_resources.length; i++) {
			final ByteBuffer buf = m_resources[i];
			if (buf != null) {
				DirectBufferPool.INSTANCE.release(buf);
				m_resources[i] = null;
			}
		}
	}

	public long allocate(final ByteBuffer data) {

		try {

			return allocate(data, Long.MAX_VALUE, TimeUnit.MILLISECONDS);

		} catch (InterruptedException e) {

			throw new RuntimeException(e);

		} catch (TimeoutException e) {

			throw new RuntimeException(e);

		}

	}

	/**
	 * The memory manager can handle the ByteBuffer allocation and copying
	 * directly.
	 * 
	 * @throws TimeoutException 
	 * @throws InterruptedException 
	 */
	public long allocate(final ByteBuffer data, final long timeout,
			TimeUnit unit) throws InterruptedException, TimeoutException {

		if (data == null)
			throw new IllegalArgumentException();

		final long retaddr = allocate(data.remaining(), timeout, unit);
		
		final ByteBuffer[] bufs = get(retaddr);

		copyData(data, bufs);
		
		return retaddr;
	}

	static void copyData(final ByteBuffer data, final ByteBuffer[] bufs) {

		final ByteBuffer src = data.duplicate();
		int pos = 0;
		for (int i = 0; i < bufs.length; i++) {
			final int tsize = bufs[i].remaining();
			src.position(pos);
			src.limit(pos + tsize);
			bufs[i].put(src);
			pos += tsize;
		}

	}
	
	public long allocate(final int nbytes) {

		try {

			return allocate(nbytes, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
			
		} catch (InterruptedException e) {

			throw new RuntimeException(e);
			
		} catch (TimeoutException e) {
			
			throw new RuntimeException(e);
			
		}

	}

	/**
	 * Create a new sector and drop it on the free list.
	 * 
	 * @throws TimeoutException
	 * @throws InterruptedException
	 */
	private void addSectorToFreeList(final long timeout, final TimeUnit unit)
			throws InterruptedException, TimeoutException {

		if (!(m_sectors.size() < m_resources.length)) {
			throw new MemoryManagerResourceError();
		}
		// Allocate new buffer (blocking request). 
		final ByteBuffer nbuf = m_pool.acquire(timeout, unit);
		
		m_resources[m_sectors.size()] = nbuf;
		
		final SectorAllocator sector = new SectorAllocator(this, null);
		sector.setSectorAddress(m_allocation, m_sectorSize);
		sector.setIndex(m_sectors.size());
		
		m_sectors.add(sector);
		
		m_allocation += m_sectorSize;
		
	}
	
	/*
	 * Core allocation method.
	 */
	public long allocate(final int nbytes, final long timeout,
			final TimeUnit unit) throws InterruptedException, TimeoutException {

		m_allocationLock.lock();
		try {
			if (nbytes <= SectorAllocator.BLOB_SIZE) {

				if (m_free.isEmpty()) {
					
					addSectorToFreeList(timeout,unit);
					
				}
				
				final SectorAllocator sector = m_free.get(0);
				
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
//				dest.position(bufferAddr);
//				dest.limit(bufferAddr + nbytes);
//				dest.put(data);
				
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
					final long bpaddr = allocate(bsize, timeout, unit);
					hdrbuf.putInt(getAllocationAddress(bpaddr));
				}
				
				// now allocate the blob header and fix the return address size
				hdrbuf.flip();
				final int retaddr = getAllocationAddress(allocate(hdrbuf));
				
				return makeAddr(retaddr, nbytes);
			}
			
		} finally {
			m_allocationLock.unlock();
		}
	}
	
	public ByteBuffer[] get(final long addr) {
		final int rwaddr = getAllocationAddress(addr);
		final int size = getAllocationSize(addr);

		if (size <= SectorAllocator.BLOB_SIZE) {
			return new ByteBuffer[] { getBuffer(rwaddr, size) };
		} else {
			// This will be a BLOB, so retrieve the header, then parse
			// to retrieve components and assign to ByteBuffer[]
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
		final ByteBuffer ret = m_resources[sector.m_index].duplicate();
		final int bufferAddr = (int) (paddr - (sector.m_index * m_sectorSize));
		ret.position(bufferAddr);
		ret.limit(bufferAddr + size);
		
		return ret;
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
	 * Return the size of the application data for the allocation the allocation
	 * with the given address.
	 * 
	 * @param addr
	 *            The address.
	 *            
	 * @return The #of bytes of in the applications allocation request.
	 */
	static int getAllocationSize(final long addr) {
		return (int) (addr & 0xFFFFL);
	}

	public void free(final long addr) {
		m_allocationLock.lock();
		try {

			final int rwaddr = getAllocationAddress(addr);
			final int size = getAllocationSize(addr);
			
			if (size <= SectorAllocator.BLOB_SIZE) {
				
				final int offset = SectorAllocator.getSectorOffset(rwaddr);
				
				// free of a simple allocation.
				final SectorAllocator sector = getSector(rwaddr);
				sector.free(offset);
				
				m_userBytes.addAndGet(-size);
				m_slotBytes.addAndGet(-sector.getPhysicalSize(offset));

				// TODO if the sector is empty, release it back to the pool.
				
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
		
		return addr;
	}
	
	public void clear() {
		m_allocationLock.lock();
		try {
			m_sectors.clear();
			m_free.clear();
			m_allocation = 0;
			try {
				releaseDirectBuffers();
			} catch (Throwable e) {
				log.warn("Unable to release direct buffers", e);
			}
		} finally {
			m_allocationLock.unlock();
		}
	}
	
//	public void releaseResources() throws InterruptedException {
//		DirectBufferPool.INSTANCE.release(m_resource);
//	}
	
	public void addToFreeList(final SectorAllocator sector) {
		m_free.add(sector);
	}

	public void removeFromFreeList(final SectorAllocator sector) {
		assert m_free.get(0) == sector;
		
		m_free.remove(sector);
	}

	public void trimSector(final long trim, final SectorAllocator sector) {
		assert m_free.get(0) == sector;
		
		// Do not trim when using buffer pool
		// m_allocation -= trim;		
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
	
	public long getSlotBytes() {
		return m_slotBytes.get();
	}

	public long getUserBytes() {
		return m_userBytes.get();
	}

	/*
	 * @todo timeouts must be adjusted for the time already elapsed....
	 * 
	 * @todo release empty buffers back to the pool. may require m_sectors to
	 * be a proper array and explicitly track which elements of the array are
	 * non-null.
	 * 
	 * @todo block if no free capacity is available.
	 * 
	 * @todo track counters for sector so we can know when to release the sector.
	 * 
	 * @todo give capacity to allocation context and have it throw an exception
	 * if the capacity would be exceeded.
	 * 
	 * @todo should all allocation contexts share the reference to the
	 * allocation lock of the memory manager?
	 */
	
}
