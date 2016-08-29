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

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.IBufferAccess;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.CommitRecordIndex;
import com.bigdata.journal.CommitRecordSerializer;
import com.bigdata.journal.ICommitRecord;
import com.bigdata.journal.ICommitter;
import com.bigdata.rawstore.IAllocationContext;
import com.bigdata.rawstore.IPSOutputStream;
import com.bigdata.rwstore.IRawTx;
import com.bigdata.rwstore.PSOutputStream;
import com.bigdata.service.AbstractTransactionService;

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
		// , ICounterSetAccess, IStore {

	private static final Logger log = Logger.getLogger(MemoryManager.class);

    private static final Logger txLog = Logger.getLogger("com.bigdata.txLog");

    /**
     * Debug array. Should be [null] unless actively debugging this code.
     */
//  private int[] m_debugAddrs = new int[100000]; // 100K alloc/frees
    private int[] m_debugAddrs = null;
    private int m_debugCurs = 0;
  
    /**
     * true iff the {@link MemoryManager} is open.
     * 
     * @see BLZG-1658 MemoryManager should know when it has been closed
     */
    private final AtomicBoolean open = new AtomicBoolean(true);

    /**
     * Assert that the {@link MemoryManager} is open.
     * 
     * @throws MemoryManagerClosedException
     *             unless the {@link MemoryManager} is open.
     * 
     * @see BLZG-1658 MemoryManager should know when it has been closed
     */
    private void assertOpen() {
        if (!open.get())
            throw new MemoryManagerClosedException();
    }

    /**
	 * The backing pool from which direct {@link ByteBuffer}s are recruited as
	 * necessary and returned when possible.
	 */
    private final DirectBufferPool m_pool;

	/**
	 * The set of direct {@link ByteBuffer} which are currently being managed by
	 * this {@link MemoryManager} instance.
	 */
    private final ArrayList<IBufferAccess> m_resources;
	
	/**
     * The lock used to serialize all allocation/deallocation requests. This is
     * shared across all allocation contexts to avoid lock ordering problems.
     * 
     * @see BLZG-1658 MemoryManager should know when it has been closed
     * @see BLZG-1660 MemoryManager should use read/write allocation lock to
     *      protect readers against concurrent close()
     */
    final /*private*/ ReentrantReadWriteLock m_allocationLock = new ReentrantReadWriteLock();

	/**
	 * Condition signalled when a sector is added to {@link #m_free}.
	 */
	final private Condition m_sectorFree = m_allocationLock.writeLock().newCondition();

	/**
	 * The size of a backing buffer.
	 */
    private final int m_sectorSize;
    
    /**
     * The maximum #of sectors that may be allocated.
     */
    private final int m_maxSectors;
	
    /**
     * When <code>true</code> allocations will block if the memory pool is
     * exhausted. Otherwise an allocation request that occurs when there is no
     * memory available within the pool will immediately throw a
     * {@link MemoryManagerOutOfMemory} exception.
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-42" > Per query
     *      memory limit for analytic query mode. </a>
     */
    private final boolean m_blocks;
    
	private final ArrayList<SectorAllocator> m_sectors = new ArrayList<SectorAllocator>();
	
	/**
	 * The free list (of sectors).
	 */
	private final ArrayList<SectorAllocator> m_free = new ArrayList<SectorAllocator>();

	/**
	 * The #of bytes in the backing buffers.
	 */
	private final AtomicLong m_extent = new AtomicLong();
	
	/** The #of allocations. */
	private final AtomicLong m_allocCount = new AtomicLong();

	/** The #of application data bytes in current allocations. */
	private final AtomicLong m_userBytes = new AtomicLong();
	
	/** The #of slot bytes in current allocations. */
	private final AtomicLong m_slotBytes = new AtomicLong();
	
    /**
     * The #of open transactions (read-only or read-write).
     * 
     * This is guarded by the {@link #m_allocationLock}.
     */
    private int m_activeTxCount = 0;
    
	private final PSOutputStream m_deferredFreeOut;

    /**
     * Create a new {@link MemoryManager}.
     * <p>
     * The backing {@link DirectBufferPool} may be either bounded or
     * (effectively) unbounded. The {@link MemoryManager} will be (effectively)
     * unbounded. If either the pool is bounded, then <em>blocking</em>
     * allocation requests may block, otherwise allocation requests will be
     * non-blocking.
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
     * 
     * @throws IllegalArgumentException
     *             if <i>pool</i> is <code>null</code>.
     */
    public MemoryManager(final DirectBufferPool pool) {

        this(pool, Integer.MAX_VALUE);
        
    }

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

        this(pool, sectors, true/* blocking */, null/* properties */);

    }

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
     *            @param blocks When <code>true</code> an allocation request 
     *            will block until it can be satisfied. When <code>false</code>
     *            and allocation request that can not be satisfied immediately
     *            will result in a {@link MemoryManagerOutOfMemory}.
     * @param properties
     *            Used to communicate various configuration properties,
     *            including
     *            {@link AbstractTransactionService.Options#MIN_RELEASE_AGE}
     *            (optional).
     *            
     * @throws IllegalArgumentException
     *             if <i>pool</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <i>sectors</i> is non-positive.
     *             
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-42" > Per query
     * memory limit for analytic query mode. </a>
     */
    public MemoryManager(final DirectBufferPool pool, final int sectors,
            final boolean blocks, final Properties properties) {
            
		if (pool == null)
			throw new IllegalArgumentException();
		
		if (sectors <= 0)
			throw new IllegalArgumentException();
		
		m_pool = pool;
		
		m_maxSectors = sectors;

		m_blocks = blocks;
		
		m_resources = new ArrayList<IBufferAccess>();
		
		m_sectorSize = pool.getBufferCapacity();
		
		m_deferredFreeOut = PSOutputStream.getNew(this, SectorAllocator.BLOB_SIZE+4 /*allow for checksum*/, null);
		
        // store minimum release age
        if (properties != null) {
            m_retention = Long.parseLong(properties.getProperty(
                    AbstractTransactionService.Options.MIN_RELEASE_AGE,
                    AbstractTransactionService.Options.DEFAULT_MIN_RELEASE_AGE));
        }

	}

    @Override
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
		for (int i = 0; i < m_resources.size(); i++) {
			final IBufferAccess buf = m_resources.get(i);
			if (buf != null) {
				try {
					buf.release();
				} catch (InterruptedException e) {
					log.error("Unable to release direct buffers", e);
				} finally {
					m_resources.set(i, null);
				}
			}
		}
		m_resources.clear();
	}

	/**
	 * Return the maximum #of sectors which may be allocated.
	 */
	@Override
	public int getMaxSectors() {
		return m_maxSectors;
	}
	
    /**
     * Return <code>true</code> iff the default policy of this
     * {@link MemoryManager} instance is to block if an allocation can not be
     * made (due to exhaustion of the maximum number of backing buffers for the
     * {@link MemoryManager} instance). Return <code>false</code> iff the
     * allocation will throw a {@link MemoryManagerOutOfMemory} exception if the
     * backing memory pool is exhausted.
     * 
     * @see <a href="http://jira.blazegraph.com/browse/BLZG-42" > Per query
     *      memory limit for analytic query mode. </a>
     */
	public boolean isBlocking() {
	    return m_blocks;
	}
	
	/**
	 * Return the #of sectors which are currently in use. 
	 */
	@Override
	public int getSectorCount() {
		final Lock lock = m_allocationLock.writeLock();
    	lock.lock();
		try {
			return m_sectors.size();
		} finally {
			lock.unlock();
		}
	}
	
	/**
	 * The size in bytes of the backing sector. This is the upper bound on the
	 * #of bytes which may be stored against a single sector.
	 */
	@Override
	public int getSectorSize() {
		
		return m_sectorSize;
		
	}

	/**
	 * The maximum capacity in bytes of the managed memory.
	 */
    public long getMaxMemoryCapacity() {
        // BLZG-1658 Protect against concurrent modification of [m_resources].
    	final Lock lock = m_allocationLock.writeLock();
    	lock.lock();
        try {
            assertOpen(); // BLZG-1658 MemoryManager should know when it has been closed
            return m_sectorSize * (long) m_resources.size();
        } finally {
        	lock.unlock();
        }
	}
	
	@Override
	public long allocate(final ByteBuffer data) {

		return allocate(data, m_blocks);

	}

	@Override
	public long allocate(final ByteBuffer data, final boolean blocks) {

		if (data == null)
			throw new IllegalArgumentException();

		final int nbytes = data.remaining();

		if (nbytes == 0)
			throw new IllegalArgumentException();

		// BLZG-1658 Protect against current close between allocate() and get().
		final Lock lock = m_allocationLock.writeLock();
    	lock.lock();
        try {

            final long retaddr = allocate(nbytes, blocks);

            final ByteBuffer[] bufs = get(retaddr);

            copyData(data, bufs);

            return retaddr;
        
        } finally {
        
        	lock.unlock();
            
        }
        
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
	
	@Override
	public long allocate(final int nbytes) {

		return allocate(nbytes, m_blocks);

	}
	
	/**
	 * Scan the sectors not on the free list and see if we can locate one which
	 * could service this allocation request.
	 */
	private SectorAllocator scanForSectorWithFreeSpace(final int nbytes) {

		final byte tag = SectorAllocator.getTag(nbytes);

		for (SectorAllocator s : m_sectors) {

			if (s.m_free[tag] > 0) {

				if (log.isDebugEnabled())
					log.debug("Can allocate from sector: " + s);
				
				return s;
				
			}
			
		}

		return null;

	}

	/**
	 * Either create a new sector and drop it on the free list, find a sector
	 * with free space which could be used to make the specified allocation, or
	 * block and wait for a sector to become available on the free list.
	 */
	private SectorAllocator getSectorFromFreeList(final boolean blocks,
			final int nbytes) {
		
		while(m_free.isEmpty()) {

			if (m_sectors.size() < m_maxSectors) {

				SectorAllocator sector = scanForSectorWithFreeSpace(nbytes);

				if (sector != null) {

					return sector;

				}
				
				/*
				 * We are under capacity, so allocate a new sector and add it to
				 * the free list.
				 */

				// Allocate new buffer (blocking request).
				final IBufferAccess nbuf;
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

				m_resources.add(nbuf);

				// Wrap buffer as a new sector.
				sector = new SectorAllocator(this, null);
				
				// Note: The sector will add itself to the free list.
				sector.setSectorAddress(m_extent.get(), m_sectorSize);
				sector.setIndex(m_sectors.size());
				
				if (m_activeTxCount > 0 || !m_contexts.isEmpty())
					sector.preserveSessionData();

				m_sectors.add(sector);

				m_extent.addAndGet(m_sectorSize);

			} else {

				if (blocks) {
					
					/*
					 * We are at the maximum #of sectors.
					 */

					SectorAllocator sector = scanForSectorWithFreeSpace(nbytes);

					if (sector != null) {

						return sector;

					}

					/*
					 * Wait for something to get freed. Once enough data is
					 * freed from some sector, that sector will be placed back
					 * onto the free list.
					 */

					if(log.isDebugEnabled())
						log.debug("Blocking...");
					
					try {
						m_sectorFree.await();
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
					
					if (log.isDebugEnabled())
						log.debug("Resuming...");

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
	@Override
	public long allocate(final int nbytes, final boolean blocks) {

		if (nbytes <= 0)
			throw new IllegalArgumentException();
		
		final Lock lock = m_allocationLock.writeLock();
    	lock.lock();
		try {
            
		    assertOpen(); // BLZG-1658 MemoryManager should know when it has been closed

            if (nbytes <= SectorAllocator.BLOB_SIZE) {

				final SectorAllocator sector = getSectorFromFreeList(blocks,
						nbytes);

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

				if (m_debugAddrs != null) {
					m_debugAddrs[m_debugCurs++] = rwaddr;
					if (m_debugCurs == m_debugAddrs.length) {
						m_debugCurs = 0;
					}
				}

				return makeAddr(rwaddr, nbytes);
				
			} else {

				/**
				 * For Blob allocation call the normal allocate and retrieve
				 * the allocation address to store in the blob header.
				 */
				int nblocks = 0;
				ByteBuffer hdrbuf = null;
				int[] addrs = null;
				try {
					nblocks = SectorAllocator.getBlobBlockCount(nbytes);
					hdrbuf = ByteBuffer.allocate(nblocks * 4 + 4); // include block count
					hdrbuf.putInt(nblocks);
					addrs = new int[nblocks];
					
					for (int i = 0; i < nblocks; i++) {
						final int pos = SectorAllocator.BLOB_SIZE * i;
						final int bsize = i < (nblocks-1) ? SectorAllocator.BLOB_SIZE : nbytes - pos;
	
						/*
						 * BLOB RECURSION
						 */
						final long bpaddr = allocate(bsize, blocks);
						final int bprwaddr = getAllocationAddress(bpaddr);
						hdrbuf.putInt(bprwaddr);
						addrs[i] = bprwaddr;
					}
					
					// now allocate the blob header and fix the return address size
					hdrbuf.flip();
					final int retaddr = getAllocationAddress(allocate(hdrbuf,blocks));
					
					if (log.isTraceEnabled())
						log.trace("Allocation BLOB at: " + retaddr);
					
					return makeAddr(retaddr, nbytes);
				} catch (MemoryManagerOutOfMemory oom) {
					// We could have failed to allocate any of the blob parts or the header
					try {
						hdrbuf.position(0);
						hdrbuf.limit(nblocks*4+4);
						final int rblocks = hdrbuf.getInt();
						assert(nblocks == rblocks);
						for (int i = 0; i < nblocks; i++) {
							int addr = hdrbuf.getInt();
							if (addr == 0) {
								break;
							} else {
								long laddr = makeAddr(addr, SectorAllocator.BLOB_SIZE);
								
								free(laddr);
							}
						}
					} catch (Throwable t) {
						log.warn("Problem trying to release partial allocations after MemoryManagerOutOfMemory", t);
					}
					
					throw oom;
				}

			}
			
		} finally {
			lock.unlock();
		}
	}
	
    @Override
	public ByteBuffer[] get(final long addr) {

		if (addr == 0L)
			throw new IllegalArgumentException();
		
		final int rwaddr = getAllocationAddress(addr);
		final int size = getAllocationSize(addr);

		if (size <= 0)
			throw new IllegalArgumentException();

		// BLZG-1658 We need at least a readLock() on the allocation lock to protect against a concurrent close().
        final Lock lock = m_allocationLock.readLock();
    	lock.lock();
		try {
			assertOpen(); // BLZG-1658 MemoryManager should know when it has
							// been closed

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

				final ByteBuffer[] hdrbuf = getBlobHdr(addr);
				int hdrIndex = 0;
				final int nblocks = hdrbuf[hdrIndex].getInt();

				final ByteBuffer[] blobbufs = new ByteBuffer[nblocks];
				int remaining = size;
				for (int i = 0; i < nblocks; i++) {
					if (hdrbuf[hdrIndex].remaining() == 0) {
						hdrIndex++;
					}
					int blockSize = remaining <= SectorAllocator.BLOB_SIZE ? remaining
							: SectorAllocator.BLOB_SIZE;
					blobbufs[i] = getBuffer(hdrbuf[hdrIndex].getInt(), blockSize);

					remaining -= blockSize;
				}

				return blobbufs;
			}
		} finally {
        	lock.unlock();
        }
	}

	@Override
	public byte[] read(final long addr) {

	    assertOpen(); // BLZG-1658 MemoryManager should know when it has been closed

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
	static byte[] read(final IMemoryManager mmgr, final long addr) {

		final int nbytes = getAllocationSize(addr);
		
		final byte[] a = new byte[nbytes];
		
		final ByteBuffer mybb = ByteBuffer.wrap(a);
		
		// BLZG-1658 read() should be protected against concurrent close using read/write lock in get.
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
	private ByteBuffer[] getBlobHdr(final long addr) {
		int size = getAllocationSize(addr);
		
		final int nblocks = SectorAllocator.getBlobBlockCount(size);
		final int hdrsize = 4*nblocks+4; // include count entry
		
		// Mockup hdraddr with header size to retrieve the ByteBuffer
		final long hdraddr = (addr & 0xFFFFFFFF00000000L) | hdrsize;
		
		return get(hdraddr);
	}
	
	String debugInfo(int rwaddr) {
		StringBuilder out = new StringBuilder("Debug: " + rwaddr);
		for (int i = 0; i < this.m_debugCurs; i++) {
			if (m_debugAddrs[i] == rwaddr) {
				out.append("A");
			} else if (m_debugAddrs[i] == -rwaddr) {
				out.append("X");
			}
		}
		
		return out.toString();
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
	ByteBuffer getBuffer(final int rwaddr, final int size) {

	    // BLZG-1658 I think we need to assert that the allocation lock (or a readLock() on it) is held by the caller.
        final Lock lock = m_allocationLock.readLock();
    	lock.lock();
		try {
			assertOpen(); // BLZG-1658 MemoryManager should know when it has
							// been closed

			final SectorAllocator sector = getSector(rwaddr);

			final int offset = SectorAllocator.getSectorOffset(rwaddr);

			if (!sector.isGettable(offset)) {
				throw new IllegalArgumentException("Address not gettable: "
						+ rwaddr);
			}

			final long paddr = sector.getPhysicalAddress(offset);

			// Duplicate the buffer to avoid side effects to position and limit.
			final IBufferAccess ba = m_resources.get(sector.m_index);
			if (ba == null) {
				throw new IllegalArgumentException();
			}
			final ByteBuffer ret = ba.buffer().duplicate();

			final int bufferAddr = (int) (paddr - sector.m_sectorAddress);

			// Set position and limit of the view onto the backing buffer.
			final int nlimit = bufferAddr + size;
			if (nlimit > ret.capacity() || nlimit < 0) {
				throw new IllegalStateException(
						"Buffer Limit Error - Capacity: " + ret.capacity()
								+ ", new limit: " + nlimit);
			}
			ret.limit(nlimit);
			ret.position(bufferAddr);

			// Take a slice to fix the view of the buffer we return to the
			// caller.
			return ret.slice();
		} finally {
        	lock.unlock();
        }
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

    @Override
	public void free(final long addr) {

		if (addr == 0L)
			throw new IllegalArgumentException();

		final int rwaddr = getAllocationAddress(addr);
		
		final int size = getAllocationSize(addr);
		
		if (size == 0)
			throw new IllegalArgumentException();
		
		if (log.isTraceEnabled())
			log.trace("Releasing allocation at: " + rwaddr + "[" + size + "]");
		
		final Lock lock = m_allocationLock.writeLock();
    	lock.lock();
		try {
		    assertOpen(); // BLZG-1658 MemoryManager should know when it has been closed
			if (m_retention > 0) {
				deferFree(rwaddr, size);
			} else {
				immediateFree(addr);
			}
		} finally {
			lock.unlock();
		}
	}
	
	private void immediateFree(final long addr) {

		if (addr == 0L)
			throw new IllegalArgumentException();

		final int rwaddr = getAllocationAddress(addr);
		
		final int size = getAllocationSize(addr);
		
		if (size == 0)
			throw new IllegalArgumentException();
		
		if (log.isTraceEnabled())
			log.trace("Releasing allocation at: " + rwaddr + "[" + size + "]");
		
		final Lock lock = m_allocationLock.writeLock();
    	lock.lock();
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
				
				if (m_debugAddrs != null) {
					m_debugAddrs[m_debugCurs++] = -rwaddr;
					if (m_debugCurs == m_debugAddrs.length) {
						m_debugCurs = 0;
					}
				}

				removeFromExternalCache(getPhysicalAddress(addr),
                        sector.getPhysicalSize(offset));

			} else {

	            // free of a blob.
				final ByteBuffer[] hdrbuf = getBlobHdr(addr);
				int hdrIndex = 0;
				final int spos = hdrbuf[hdrIndex].position();
				int hdrsize = hdrbuf[hdrIndex].limit() - spos;
				final int nblocks = hdrbuf[hdrIndex].getInt(); // read # of blocks
				
				// free each block
				int remaining = size;
				
				for (int i = 0; i < nblocks; i++) {
					
					// check for blob of blobs header
					if (hdrbuf[hdrIndex].remaining() == 0) {
						hdrIndex++;
						hdrsize += hdrbuf[hdrIndex].limit();
					}

					final int blockSize = remaining <= SectorAllocator.BLOB_SIZE ? remaining
							: SectorAllocator.BLOB_SIZE;
				
					final long mkaddr = makeAddr(hdrbuf[hdrIndex].getInt(), blockSize);
					
					/*
					 * BLOB RECURSION
					 */

					immediateFree(mkaddr);
					
				}
				
				// now free the header
				immediateFree(makeAddr(rwaddr, hdrsize));
				
			}
			
//			removeFromExternalCache(getPhysicalAddress(addr), 0);
		} finally {
			lock.unlock();
		}
	}
	
    @Override
	public long getPhysicalAddress(final long addr) {
		// Should this compute based on sector index and sector size?
		final int rwaddr = getAllocationAddress(addr);
		final int sector = SectorAllocator.getSectorIndex(rwaddr);
		final int soffset = SectorAllocator.getSectorOffset(rwaddr);
		final long paddr = getSectorSize();
		
		return (paddr * sector) + soffset;
	}
	
	private long makeAddr(final int rwaddr, final int size) {

		long addr = rwaddr;
		addr <<= 32;
		addr += size;
		
		assert rwaddr == getAllocationAddress(addr);
		assert size == getAllocationSize(addr);
		return addr;
	}

    @Override
	public void clear() {
    	final Lock lock = m_allocationLock.writeLock();
    	lock.lock();
		try {
			if(log.isDebugEnabled())
				log.debug("");
			m_sectors.clear();
			m_free.clear();
			m_extent.set(0L);
			m_allocCount.set(0L);
			m_userBytes.set(0L);
			m_slotBytes.set(0L);
			releaseDirectBuffers(); // release buffers back to the pool.
			m_sectorFree.signalAll();
		} finally {
			lock.unlock();
		}
	}
	
    @Override
	public void addToFreeList(final SectorAllocator sector) {
    	final Lock lock = m_allocationLock.writeLock();
    	lock.lock();
		try {
			m_free.add(sector);
			m_sectorFree.signalAll();
		} finally {
			lock.unlock();
		}
	}

    @Override
	public void removeFromFreeList(final SectorAllocator sector) {
    	final Lock lock = m_allocationLock.writeLock();
    	lock.lock();
		try {
			assert m_free.get(0) == sector;
			m_free.remove(sector);
		} finally {
			lock.unlock();
		}
	}

    @Override
	public void trimSector(final long trim, final SectorAllocator sector) {
		// Do not trim when using buffer pool
	}

	/**
	 * Maintain allocationContext to check for session protection
	 */
    @Override
	public IMemoryManager createAllocationContext() {
        assertOpen(); // BLZG-1658 MemoryManager should know when it has been closed
		return new AllocationContext(this, true /*isolated*/);
	}

    @Override
	public int allocationSize(final long addr) {
		return getAllocationSize(addr);
	}

	/**
	 * The maximum #of bytes which are available to the memory manager.
	 */
	public long getCapacity() {

        // BLZG-1658 Protect against concurrent modification of [m_resources].
		final Lock lock = m_allocationLock.writeLock();
    	lock.lock();
        try {
            assertOpen(); // BLZG-1658 MemoryManager should know when it has been closed
            return m_pool.getBufferCapacity() * (long) m_resources.size();
        } finally {
        	lock.unlock();
        }

	}
	
	/**
	 * The total #of bytes in the backing buffers currently attached to the
	 * {@link MemoryManager}.
	 */
	public long getExtent() {
		return m_extent.get();
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

		// maximum #of buffers which may be allocated.
		root.addCounter("bufferCapacity", new OneShotInstrument<Integer>(
				m_maxSectors));

		// current #of buffers which are allocated.
		root.addCounter("bufferCount", new OneShotInstrument<Integer>(
				getSectorCount()));

		// current backing storage in bytes.
		root.addCounter("extent", new OneShotInstrument<Long>(m_extent
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
		
        // report whether or not the allocation policy is blocking.
        root.addCounter("blocking", new OneShotInstrument<Boolean>(
                isBlocking()));
        
		return root;
		
	}

    @Override
	public String toString() {

		return getClass().getName() + "{counters=" + getCounters() + "}";

	}

	@Override
	public long alloc(final byte[] buf, final int size, final IAllocationContext context) {
		if (context != null)
			throw new IllegalArgumentException("The MemoryManager does not support AllocationContexts");
		
		return getAllocationAddress(allocate(ByteBuffer.wrap(buf, 0, size))); // return the rwaddr!
	}

	// @see BLZG-1658 MemoryManager should know when it has been closed
	@Override
	public void close() {
		final Lock lock = m_allocationLock.writeLock();
    	lock.lock();
        try {
            if (open.compareAndSet(true/* expect */, false/* update */)) {
                if(log.isDebugEnabled())
                    log.debug("MemoryManager is closed.");
                clear(); // releasing direct buffers
            }
        } finally {
        	lock.unlock();
        }
	}

	@Override
	public void free(final long addr, final int size) {

	    free((addr << 32) + size);
	    
	}

	@Override
	public int getAssociatedSlotSize(final int addr) {
	    // BLZG-1658 MemoryManager should know when it has been closed (operation is not protected against concurrent close()).
        final Lock lock = m_allocationLock.readLock();
    	lock.lock();
		try {
			assertOpen();
			
			final SectorAllocator sector = getSector(addr);

			final int offset = SectorAllocator.getSectorOffset(addr);

			return sector.getPhysicalSize(offset);
		} finally {
        	lock.unlock();
        }
	}

	@Override
	public void getData(final long l, final byte[] buf) {
		/**
		 * TBD: this could probably be more efficient!
		 */
		final Lock lock = m_allocationLock.readLock();
		lock.lock();
		try {
			final ByteBuffer rbuf = getBuffer((int) l, buf.length);
			rbuf.get(buf);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public File getStoreFile() {
		throw new UnsupportedOperationException("The MemoryManager does not provdie a StoreFile");
	}

	@Override
	public IPSOutputStream getOutputStream() {
        assertOpen(); // BLZG-1658 MemoryManager should know when it has been closed
		return getOutputStream(null/*AllocationContext*/);
	}

	public IPSOutputStream getOutputStream(final IAllocationContext context) {
        assertOpen(); // BLZG-1658 MemoryManager should know when it has been closed
		return PSOutputStream.getNew(this, SectorAllocator.BLOB_SIZE+4 /*no checksum*/, context);
	}

	@Override
	public InputStream getInputStream(final long addr) {
        assertOpen(); // BLZG-1658 MemoryManager should know when it has been closed
		return new PSInputStream(this, addr);
	}

	@Override
	public void commit() {
        
	    assertOpen(); // BLZG-1658 MemoryManager should know when it has been closed

        // NOP, the commit of the transient objects is now in postCommit
	    
	}

	@Override
	public Lock getCommitLock() {
	    return m_allocationLock.writeLock();
	}
	
	@Override
	public void postCommit() {
	    if(!m_allocationLock.isWriteLockedByCurrentThread())
	        throw new IllegalMonitorStateException();
        assertOpen(); // BLZG-1658 MemoryManager should know when it has been closed
//		try {
        final Iterator<SectorAllocator> sectors = m_sectors.iterator();
        while (sectors.hasNext()) {
            sectors.next().commit();
        }
//		} finally {
//			m_allocationLock.unlock();
//		}
	}

	private ConcurrentWeakValueCache<Long, ICommitter> m_externalCache = null;
	private int m_cachedDatasize = 0;

//	private long m_lastReleaseTime;

	private long m_lastDeferredReleaseTime = 0;
//	/**
//	 * Call made from AbstractJournal to register the cache used.  This can then
//	 * be accessed to clear entries when storage is made availabel for re-cycling.
//	 * 
//	 * It is not safe to clear at the point of the delete request since the data
//	 * could still be loaded if the data is retained for a period due to a non-zero
//	 * retention period or session protection.
//	 * 
//	 * @param externalCache - used by the Journal to cache historical BTree references
//	 * @param dataSize - the size of the checkpoint data (fixed for any version)
//	 */
    @Override
	public void registerExternalCache(
			final ConcurrentWeakValueCache<Long, ICommitter> externalCache, final int dataSize) {
    	final Lock lock = m_allocationLock.writeLock();
    	lock.lock();
		try {
	        assertOpen(); // BLZG-1658 MemoryManager should know when it has been closed
			m_externalCache = externalCache;
			m_cachedDatasize = getSlotSize(dataSize);
		} finally {
			lock.unlock();
		}
	}

	/**
     * We need to remove entries from the historicalIndexCache for checkpoint
     * records when the allocations associated with those checkpoint records are
     * freed.
     * 
     * @param clr
     *            The physical address that is being deleted.
     * @param slotSize
     *            The size of the allocator slot for that physical address.
	 */
	void removeFromExternalCache(final long clr, final int slotSize) {
		
	    assert m_allocationLock.isWriteLocked();
		
	    if (m_externalCache == null)
		    return;
		
        if (slotSize == 0 || slotSize == m_cachedDatasize) {

            final Object rem = m_externalCache.remove(clr);

            if (rem != null && log.isTraceEnabled()) {

                log.trace("ExternalCache, removed: " + rem.getClass().getName()
                        + " with addr: " + clr);
            
            }

        }

    }

	private int getSlotSize(final int size) {
		return SectorAllocator.getBlockForSize(size);
	}

	@Override
	public long saveDeferrals() {
		final Lock lock = m_allocationLock.writeLock();
    	lock.lock();
		try {
	        assertOpen(); // BLZG-1658 MemoryManager should know when it has been closed
			if (m_deferredFreeOut.getBytesWritten() == 0) {
				return 0;
			}
			m_deferredFreeOut.writeInt(0); // terminate!
			final int outlen = m_deferredFreeOut.getBytesWritten();
			
			long addr = m_deferredFreeOut.save();
			
			addr <<= 32;
			addr += outlen;
			
			m_deferredFreeOut.reset();
			return addr;			
		} catch (IOException e) {
			throw new RuntimeException("Cannot write to deferred free", e);
		} finally {
			lock.unlock();
		}
	}

	public void deferFree(final int rwaddr, final int sze) {
		assert rwaddr != 0;
		final Lock lock = m_allocationLock.writeLock();
    	lock.lock();
		try {
	        assertOpen(); // BLZG-1658 MemoryManager should know when it has been closed
			if (sze > (SectorAllocator.BLOB_SIZE)) {
				m_deferredFreeOut.writeInt(-rwaddr);
				m_deferredFreeOut.writeInt(sze);
			} else {
				m_deferredFreeOut.writeInt(rwaddr);				
			}
		} catch (IOException e) {
            throw new RuntimeException("Could not free: rwaddr=" + rwaddr
                    + ", size=" + sze, e);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public int checkDeferredFrees(final AbstractJournal journal) {
		final AbstractTransactionService transactionService = (AbstractTransactionService) journal
				.getLocalTransactionManager().getTransactionService();

		// the previous commit point.
		final long lastCommitTime = journal.getLastCommitTime();

		if (lastCommitTime == 0L) {
			// Nothing committed.
			return 0;
		}

		/*
		 * The timestamp for which we may release commit state.
		 */
		final long latestReleasableTime = transactionService.getReleaseTime();
		
        return freeDeferrals(journal, m_lastDeferredReleaseTime  + 1,
                latestReleasableTime);
	}

	/**
	 * Provided with the address of a block of addresses to be freed
	 * @param blockAddr
	 * @return the total number of addresses freed
	 */
	private int freeDeferrals(final long blockAddr, final long lastReleaseTime) {
        int totalFreed = 0;
        DataInputStream strBuf = null;      
        final Lock lock = m_allocationLock.writeLock();
    	lock.lock();
		try {
	        assertOpen(); // BLZG-1658 MemoryManager should know when it has been closed
			strBuf = new DataInputStream(getInputStream(blockAddr));		
			int nxtAddr = strBuf.readInt();
			
			while (nxtAddr != 0) { // while (false && addrs-- > 0) {
				
				if (nxtAddr > 0) { // Blob
					final int bloblen = strBuf.readInt();
					assert bloblen > 0; // a Blob address MUST have a size

					immediateFree(makeAddr(-nxtAddr, bloblen));
				} else {
					// The lack of size messes with the stats
					immediateFree(makeAddr(nxtAddr, 1)); // size ignored for FixedAllocators
				}
				
				totalFreed++;
				
				nxtAddr = strBuf.readInt();
			}
			// now free delete block
			immediateFree(blockAddr);
            m_lastDeferredReleaseTime = lastReleaseTime;
            if (log.isTraceEnabled())
                log.trace("Updated m_lastDeferredReleaseTime="
                        + m_lastDeferredReleaseTime);
		} catch (IOException e) {
			throw new RuntimeException("Problem freeing deferrals", e);
		} finally {
			lock.unlock();
			if (strBuf != null) {
				try {
					strBuf.close();
				} catch (IOException e) {
					log.error(e,e);
				}
			}
		}
		
		return totalFreed;
	}

    /**
     * Provided with an iterator of CommitRecords, process each and free any
     * deferred deletes associated with each.
     * 
     * @param journal
     * @param fromTime
     *            The inclusive lower bound.
     * @param toTime
     *            The exclusive upper bound.
     */
    private int freeDeferrals(final AbstractJournal journal,
            final long fromTime,
            final long toTime) {

		/*
		 * Commit can be called prior to Journal initialisation, in which case
		 * the commitRecordIndex will not be set.
		 */
		final IIndex commitRecordIndex = journal.getReadOnlyCommitRecordIndex();
		if (commitRecordIndex == null) { // TODO Why is this here?
			return 0;
		}

		final IndexMetadata metadata = commitRecordIndex.getIndexMetadata();

		final byte[] fromKey = metadata.getTupleSerializer().serializeKey(
				fromTime);

		final byte[] toKey = metadata.getTupleSerializer().serializeKey(toTime);

		final ITupleIterator<CommitRecordIndex.Entry> commitRecords = commitRecordIndex
				.rangeIterator(fromKey, toKey);

		int totalFreed = 0;
		int commitPointsRecycled = 0;

		while (commitRecords.hasNext()) {

			final ITuple<CommitRecordIndex.Entry> tuple = commitRecords.next();

			final CommitRecordIndex.Entry entry = tuple.getObject();

			try {

				final ICommitRecord record = CommitRecordSerializer.INSTANCE
						.deserialize(journal.read(entry.addr));

				final long blockAddr = record
						.getRootAddr(AbstractJournal.DELETEBLOCK);

				if (blockAddr != 0) {

					totalFreed += freeDeferrals(blockAddr, record
							.getTimestamp());

				}

				// Note: This is releasing the ICommitRecord itself. I've moved
				// the responsibilty
				// for that into AbstractJournal#removeCommitRecordEntries()
				// (invoked below).
				//              
				// immediateFree((int) (entry.addr >> 32), (int) entry.addr);

				commitPointsRecycled++;

			} catch (RuntimeException re) {

				throw new RuntimeException("Problem with entry at "
						+ entry.addr, re);

			}

		}

		/*
		 * 
		 * 
		 * @see https://sourceforge.net/apps/trac/bigdata/ticket/440
		 */

		// Now remove the commit record entries from the commit record index.
		final int commitPointsRemoved = journal.removeCommitRecordEntries(
				fromKey, toKey);

		if (txLog.isInfoEnabled())
			txLog.info("fromTime=" + fromTime + ", toTime=" + toTime
					+ ", totalFreed=" + totalFreed + ", commitPointsRecycled="
					+ commitPointsRecycled + ", commitPointsRemoved="
					+ commitPointsRemoved);

		if (commitPointsRecycled != commitPointsRemoved)
			throw new AssertionError("commitPointsRecycled="
					+ commitPointsRecycled + " != commitPointsRemoved="
					+ commitPointsRemoved);

		return totalFreed;
	}
    
    @Override
	public IRawTx newTx() {
		activateTx();

		return new IRawTx() {
		    private final AtomicBoolean m_open = new AtomicBoolean(true);
			
		    @Override
			public void close() {
				if (m_open.compareAndSet(true/*expect*/, false/*update*/)) {
					deactivateTx();
				}
			}
		};
	}
	
    private void activateTx() {
    	final Lock lock = m_allocationLock.writeLock();
    	lock.lock();
        try {
            assertOpen(); // BLZG-1658 MemoryManager should know when it has been closed
            m_activeTxCount++;
            if(log.isInfoEnabled())
                log.info("#activeTx="+m_activeTxCount);
            
            // check for new session protection
            if (m_activeTxCount == 1 && m_contexts.isEmpty()) {
            	acquireSessions();
            }
        } finally {
        	lock.unlock();
        }
    }
    
    private void deactivateTx() {
    	final Lock lock = m_allocationLock.writeLock();
    	lock.lock();
        try {
            assertOpen(); // BLZG-1658 MemoryManager should know when it has been closed
        	if (m_activeTxCount == 0) {
        		throw new IllegalStateException("Tx count must be positive!");
        	}
            m_activeTxCount--;
            if(log.isInfoEnabled())
                log.info("#activeTx="+m_activeTxCount);
            
            if (m_activeTxCount == 0 /* FIXME && m_contexts.isEmpty()*/) {
            	releaseSessions();
            }
        } finally {
        	lock.unlock();
        }
    }

	private void releaseSessions() {
		for (SectorAllocator sector: m_sectors) {
			sector.releaseSession(null);
		}
	}

	private void acquireSessions() {
		for (SectorAllocator sector: m_sectors) {
			sector.preserveSessionData();
		}
	}

	@Override
	public long getLastReleaseTime() {
		return m_lastDeferredReleaseTime;
	}

	@Override
	public void abortContext(final IAllocationContext context) {
		final Lock lock = m_allocationLock.writeLock();
    	lock.lock();
		try {
            assertOpen(); // BLZG-1658 MemoryManager should know when it has been closed
			context.release();
			
        	if (context.isIsolated()) {
			final AllocationContext alloc = m_contexts.remove(context);
			
			if (alloc != null) {
				m_contextRemovals++;
				alloc.clear();
				
				if (m_activeTxCount == 0 && m_contexts.isEmpty())
					releaseSessions();
			}
        	}
			
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void detachContext(final IAllocationContext context) {
		final Lock lock = m_allocationLock.writeLock();
    	lock.lock();
		try {
            assertOpen(); // BLZG-1658 MemoryManager should know when it has been closed
			context.release();
			
        	if (context.isIsolated()) {
			final AllocationContext alloc = m_contexts.remove(context);
			
			if (alloc != null) {
				m_contextRemovals++;
				alloc.commit();			
			} else {
				throw new IllegalStateException("Multiple call to detachContext");
			}
			
			if (m_contexts.isEmpty() && this.m_activeTxCount == 0) {
				releaseSessions();
			}
        	}
		} finally {
			lock.unlock();
		}
	}

    private void checkContext(final IAllocationContext context) {
		if (context != null) {
			context.checkActive();
		}
	}

//	@Override
//	public void registerContext(final IAllocationContext context) {
//		takeReadLock();
//		try {
//			establishContextAllocation(context);
//		} finally {
//			releaseWriteLock();
//		}
//	}

	private final Map<IAllocationContext, AllocationContext> m_contexts = 
		new ConcurrentHashMap<IAllocationContext, AllocationContext>();
	
	private int m_contextRequests = 0;
	private int m_contextRemovals = 0;

	private long m_retention = 0; // retention period
	
    private AllocationContext getContextAllocation(
            final IAllocationContext context) {

        /*
         * The allocation lock MUST be held to make changes in the membership of
         * m_contexts atomic with respect to free().
         */
        assert m_allocationLock.isWriteLockedByCurrentThread();
        
        checkContext(context);
        
        AllocationContext ret = m_contexts.get(context);
        
        if (ret == null) {
        	
            throw new IllegalStateException("No associated Context found");
            
        }

        return ret;
    
    }
    /*
	 * DONE: The constructor must be able to accept nsectors :=
	 * Integer.MAX_VALUE in order to indicate that the #of backing buffers is
	 * (conceptually) unbounded. This will require m_resources to be a data
	 * structure other than a simple array.
	 * 
	 * FIXME Release empty buffers back to the pool. may require m_sectors to be
	 * a proper array and explicitly track which elements of the array are
	 * non-null.
	 * 
	 * TODO Give capacity to allocation context and have it throw an exception
	 * if the capacity would be exceeded?
	 * 
	 * TODO Should all allocation contexts share the reference to the allocation
	 * lock of the memory manager? (I think so. B)
	 * 
	 * FIXME There are patterns of usage where the memory manager can deadlock
	 * (for blocking requests) due to an inability to repurposed memory which
	 * had already been provisioned for a given allocation size.
	 * 
	 * We could do blob style allocators if the address space is too fragmented
	 * to do right sized allocators.
	 * 
	 * Maintain a free list per size and do not pre-provision allocators of any
	 * given size against a new sector.
	 */

//	@Override
//	public void setRetention(final long retention) {
//		m_retention = retention;
//	}

	@Override
	public boolean isCommitted(final long addr) {
		
		final Lock lock = m_allocationLock.writeLock();
    	lock.lock();
		try {
            assertOpen(); // BLZG-1658 MemoryManager should know when it has been closed
			final int rwaddr = getAllocationAddress(addr);

			final int offset = SectorAllocator.getSectorOffset(rwaddr);
				
			// free of a simple allocation.
			final SectorAllocator sector = getSector(rwaddr);
			
			return sector.isCommitted(offset);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public long allocate(ByteBuffer data, IAllocationContext context) {
		final Lock lock = m_allocationLock.writeLock();
    	lock.lock();
		try {
            assertOpen(); // BLZG-1658 MemoryManager should know when it has been closed
			return getContextAllocation(context).allocate(data);
		} finally {
			lock.unlock();
		}
	}

    @Override
    public long write(ByteBuffer data, IAllocationContext context) {
        return allocate(data, context);
    }

	@Override
	public void free(long addr, IAllocationContext context) {
		final Lock lock = m_allocationLock.writeLock();
    	lock.lock();
		try {
            assertOpen(); // BLZG-1658 MemoryManager should know when it has been closed
			getContextAllocation(context).free(addr);
		} finally {
			lock.unlock();
		}
	}

    @Override
    public void delete(long addr, IAllocationContext context) {
        free(addr,context);
    }

	public IAllocationContext newAllocationContext(final boolean isolated) {
	    // BLZG-1658 This needs a readLock() on the allocation lock to protect against a concurrent close.
        final Lock lock = m_allocationLock.readLock();
    	lock.lock();
        try {
            assertOpen(); // BLZG-1658 MemoryManager should know when it has been closed

            AllocationContext ret = new AllocationContext(this, isolated);
	
			if (isolated) {
		        m_contexts.put(ret, ret);
		    	if (m_activeTxCount == 0 && m_contexts.size() == 1)
					acquireSessions();
			}
			
	 		return ret;
        } finally {
        	lock.unlock();
        }
	}
	
}
