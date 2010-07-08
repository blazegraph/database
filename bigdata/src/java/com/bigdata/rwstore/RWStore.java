/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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

package com.bigdata.rwstore;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.IReopenChannel;
import com.bigdata.io.writecache.WriteCache;
import com.bigdata.journal.ForceEnum;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RWStrategy.FileMetadataView;
import com.bigdata.quorum.Quorum;
import com.bigdata.util.ChecksumUtility;

/**
 * Storage class
 * 
 * Author: Martyn Cutcher
 * 
 * Provides an interface to allocating storage within a disk file.
 * 
 * Essentially provides a DiskMalloc interface.
 * 
 * In addition to the DiskMalloc/ReAlloc mechanism, a single root address can be
 * associated. This can be used when opening an existing storage file to
 * retrieve some management object - such as an object manager!
 * 
 * The allocator also support atomic update via a simple transaction mechanism.
 * 
 * Updates are normally committed immediately, but by using startTransaction and
 * commitTransaction, the previous state of the store is retained until the
 * moment of commitment.
 * 
 * It would also be possible to add some journalling/version mechanism, where
 * snapshots of the allocation maps are retained for sometime. For a store which
 * was only added to this would not be an unreasonable overhead and would
 * support the rolling back of the database weekly or monthly if required.
 * 
 * The input/output mechanism uses ByteArray Input and Output Streams.
 * 
 * One difference between the disk realloc and in memory realloc is that the
 * disk realloc will always return a new address and mark the old address as
 * ready to be freed.
 * 
 * The method of storing the allocation headers has been changed from always
 * allocting at the end of the file (and moving them on fle extend) to
 * allocation of fixed areas.  The meta-allocation data, containing the bitmap
 * that controls these allocations, is itself stored in the heap, and is now
 * structured to include both the bit data and the list of meta-storage
 * addresses.
 * 
 * Sizing:
 * 256 allocators would reference approximately 2M objects/allocations.  At 1K
 * per allocator this would require 250K of store.  The meta-allocation data
 * would therefore need a start address plus 32 bytes (or 8 ints) to represent
 * the meta-allocation bits.  An array of such data referencing sequentialy
 * allocated storage areas completes the meta-allocation requirements.
 * 
 * A meta-allocation address can therefore be represented as a single bit offset
 * from which the block, providing start address, and bit offset can be
 * directly determined.
 * 
 * The m_metaBits int array used to be fully used as allocaiton bits, but
 * now stores both the start address plus the 8 ints used to manage that data
 * block.
 * 
 * Allocation is reduced to sets of allocator objects which have a start address
 * and a bitmap of allocated storage maps.
 * 
 * Searching thousands of allocation blocks to find storage is not efficient,
 * but by utilising roving pointers and sorting blocks with free space available
 * this can be made most efficient.
 * 
 * In order to provide optimum use of bitmaps, this implementation will NOT use
 * the BitSet class.
 * 
 * Using the meta-allocation bits, it is straightforward to load ALL the
 * allocation headers. A total of (say) 100 allocation headers might provide
 * upto 4000 allocations each -> 400 000 objects, while 1000 headers -> 4m
 * objects and 2000 -> 8m objects.
 * 
 * The allocators are split into a set of FixedAllocators and then
 * BlobAllocation. The FixedAllocators will allocate from 128 to 32K objects,
 * with a minimum block allocation of 64K, and a minimum bit number per block of
 * 32.
 * 
 * Where possible lists and roving pointers will be used to minimise searching
 * of the potentially large structures.
 * 
 * Since the memory is allocated on (at least) a 128 byte boundary, there is
 * some leeway on storing the address. Added to the address is the shift
 * required to make to the "standard" 128 byte block, e.g. blocksize = 128 <<
 * (addr % 8)
 * 
 * NB Useful method on RandomAccessFile.setLength(newLength)
 * 
 * When session data is preserved two things must happen - the allocators must
 * not reallocate data that has been freed in this session, or more clearly can
 * only free data that has been allocated in this session. That should be it.
 * 
 * The ALLOC_SIZES table is the fibonacci sequence. We multiply by 64 bytes to
 * get actual allocation block sizes. We then allocate bits based on 8K
 * allocation rounding and 32 bits at a time allocation. Note that 4181 * 64 =
 * 267,584 and 256K is 262,144
 * 
 * All data is checksummed, both allocated/saved data and the allocation blocks.
 * 
 * BLOB allocation is not handled using chained data buffers but with a blob
 * header record.  This is indicated with a BlobAllocator that provides indexed
 * offsets to the header record (the address encodes the BlobAllocator and the
 * offset to the address). The header record stores the number of component
 * allocations and the address of each.
 * 
 * This approach makes for much more efficient freeing/re-allocation of Blob
 * storage, in particular avoiding the need to read in the component blocks
 * to determine chained blocks for freeing.  This is particularly important
 * for larger stores where a disk cache could be flushed through simply freeing
 * BLOB allocations.
 */

public class RWStore implements IStore {
	protected static final Logger log = Logger.getLogger(RWStore.class);

	static final int[] ALLOC_SIZES = { 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181 };
	// static final int[] ALLOC_SIZES = { 1, 2, 4, 8, 16, 32, 64, 128 };

	final int[] ALLOC_BUCKETS = new int[ALLOC_SIZES.length];
	
	static final int MAX_FIXED_ALLOC = 64 * 4181;
	static final int MIN_FIXED_ALLOC = 64;
	static final int ALLOC_BLOCK_SIZE = 1024;
	
	// from 32 bits, need 13 to hold max offset of 8 * 1024, leaving 19 for number of blocks: 256K
	static final int BLOCK_INDEX_BITS = 19;
	static final int OFFSET_BITS = 13;
	static final int OFFSET_BITS_MASK = 0x1FFF; // was 0xFFFF
	
	static final int ALLOCATION_SCALEUP = 16; // multiplier to convert allocations based on minimum allocation of 32k
	static final int META_ALLOCATION = 8; // 8 * 32K is size of meta Allocation

//	private ICommitCallback m_commitCallback;
//
//	public void setCommitCallback(final ICommitCallback callback) {
//		m_commitCallback = callback;
//	}

	// ///////////////////////////////////////////////////////////////////////////////////////
	// RWStore Data
	// ///////////////////////////////////////////////////////////////////////////////////////

	private File m_fd;
	private RandomAccessFile m_raf;
//	protected FileMetadata m_metadata;
//	protected int m_transactionCount;
//	private boolean m_committing;

	private boolean m_preserveSession = false;
	private boolean m_readOnly;

    /**
     * lists of total alloc blocks.
     * 
     * @todo examine concurrency and lock usage for {@link #m_alloc}, which is
     *       used by {@link #getStats(boolean)}, and the rest of these lists as
     *       well.
     */
	private ArrayList<Allocator> m_allocs;

	// lists of free alloc blocks
	private ArrayList m_freeFixed[];
	
	// lists of free blob allocators
	private ArrayList m_freeBlobs;

	// lists of blocks requiring commitment
	private ArrayList m_commitList;

	private WriteBlock m_writes;
	private final Quorum<?,?> m_quorum;
	private RWWriteCacheService m_writeCache;

    /**
     * This lock is used to exclude readers when the extent of the backing file
     * is about to be changed.
     * <p>
     * At present we use synchronized (this) for alloc/commitChanges and
     * getData, since only alloc and commitChanges can cause a file extend, and
     * only getData can read.
     * <p>
     * By using an explicit extensionLock we can unsure that that the taking of
     * the lock is directly related to the functionality, plus we can support
     * concurrent reads.
     * <p>
     * You MUST hold the {@link #m_allocationLock} before acquiring the
     * {@link ReentrantReadWriteLock#writeLock()} of the
     * {@link #m_extensionLock}.
     */
    final private ReentrantReadWriteLock m_extensionLock = new ReentrantReadWriteLock();
    
    /**
     * An explicit allocation lock allows for reads concurrent with allocation requests.
     * It is only when an allocation triggers a file extension that the write
     * extensionLock needs to be taken.
     * 
     * TODO: There is scope to take advantage of the different allocator sizes
     * and provide allocation locks on the fixed allocators.  We will still need
     * a store-wide allocation lock when creating new allocation areas, but 
     * significant contention may be avoided.
     */
    final private ReentrantLock m_allocationLock = new ReentrantLock();

    private ReopenFileChannel m_reopener = null;

    private void baseInit() {

		m_metaBitsSize = cDefaultMetaBitsSize;

		m_metaBits = new int[m_metaBitsSize];
		m_metaTransientBits = new int[m_metaBitsSize];

		m_maxFileSize = 2 * 1024 * 1024; // 1gb max (mult by 128)!!

		m_commitList = new ArrayList();
		m_allocs = new ArrayList<Allocator>();

		final int numFixed = ALLOC_SIZES.length;

		m_freeFixed = new ArrayList[numFixed];

		for (int i = 0; i < numFixed; i++) {
			m_freeFixed[i] = new ArrayList();
		}
		
		m_freeBlobs = new ArrayList();

		try {
			m_reopener = new ReopenFileChannel(m_fd, m_raf, "rw");
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}

		try {
			int buffers = m_fmv.getFileMetadata().writeCacheBufferCount;
			// buffers = 6;
            m_writeCache = new RWWriteCacheService(
            		buffers, m_raf
                            .length(), m_reopener, m_quorum) {
            	
		                public WriteCache newWriteCache(final ByteBuffer buf,
		                        final boolean useChecksum,
		                        final boolean bufferHasData,
		                        final IReopenChannel<? extends Channel> opener)
		                        throws InterruptedException {
		                    return new WriteCacheImpl(buf,
		                            useChecksum, bufferHasData,
		                            (IReopenChannel<FileChannel>) opener);
		                }
            	};
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
		} 
	}
		
	class WriteCacheImpl extends WriteCache.FileChannelScatteredWriteCache {
        public WriteCacheImpl(final ByteBuffer buf,
                final boolean useChecksum,
                final boolean bufferHasData,
                final IReopenChannel<FileChannel> opener)
                throws InterruptedException {

            super(buf, useChecksum, m_quorum!=null&&m_quorum
                    .isHighlyAvailable(), bufferHasData, opener);

        }

        @Override
        protected boolean writeOnChannel(final ByteBuffer data,
                final long firstOffsetignored,
                final Map<Long, RecordMetadata> recordMap,
                final long nanos) throws InterruptedException, IOException {
        	Lock readLock = m_extensionLock.readLock();
        	readLock.lock();
        	try {
        		return super.writeOnChannel(data, firstOffsetignored, recordMap, nanos);
        	} finally {
        		readLock.unlock();
        	}
        	
        }
		
	};
	
	private String m_filename;
	private LockFile m_lockFile;

	private FileMetadataView m_fmv;

	private IRootBlockView m_rb;

	volatile private long m_commitCounter;

	volatile private int m_metaBitsAddr;

	// Constructor

	public RWStore(final FileMetadataView fileMetadataView, final boolean readOnly,
	        final Quorum<?,?> quorum) {

	    if (false && Config.isLockFileNeeded() && !readOnly) {
			m_lockFile = LockFile.create(m_filename + ".lock");

			if (m_lockFile == null) {
				throw new OverlappingFileLockException();
			}
		}

        m_quorum = quorum;

        /*
         * FIXME elevate as much as possible into the constructor and make those
         * fields final.
         */
		reopen(fileMetadataView);
	}

	/** @deprecated by {@link #reset()}
	 */
	public void reopen() {
		reopen(m_fmv);
	}
    /** @deprecated by {@link #reset()}
     */
	public void reopen(final FileMetadataView fileMetadataView) {
		m_fmv = fileMetadataView;
		m_fd = fileMetadataView.getFile();
		m_raf = fileMetadataView.getRandomAccessFile();

		m_rb = m_fmv.getRootBlock();

		m_filename = m_fd.getAbsolutePath();

		baseInit();

		try {
			// m_writes = new WriteBlock(m_raf);

			if (m_rb.getNextOffset() == 0) { // if zero then new file

				m_fileSize = convertFromAddr(m_fd.length());
				
				// make space for meta-allocators
				m_metaBits[0] = -1;
				m_metaTransientBits[0] = -1;
				m_nextAllocation = -(1 + META_ALLOCATION); // keep on a minimum 8K boundary
				
				if (m_fileSize > m_nextAllocation) {
					m_fileSize = m_nextAllocation;
				}
				
				m_raf.setLength(convertAddr(m_fileSize));

				commitChanges();

			} else {
				initfromRootBlock();
			}
		} catch (IOException e) {
			throw new StorageTerminalError("Unable to initialize store", e);
		}
	}
	
	public void close() {
		try {
			m_writeCache.close();
			m_raf.close();
			if (m_lockFile != null) {
				m_lockFile.clear();
			}
		} catch (IOException e) {
			// ..oooh err... only trying to help
		} catch (InterruptedException e) {
			// thrown from writeCache?
			e.printStackTrace();
		}
	}

	/**
	 * Basic check on key root block validity
	 * 
	 * @param rbv
	 */
	public void checkRootBlock(final IRootBlockView rbv) {
		final long nxtOffset = rbv.getNextOffset();
		final int nxtalloc = -(int) (nxtOffset >> 32);

		final int metaBitsAddr = -(int) nxtOffset;

		final long metaAddr = rbv.getMetaStartAddr();
		final long rawMetaBitsAddr = rbv.getMetaBitsAddr();
		if (metaAddr == 0 || rawMetaBitsAddr == 0) {
			log.warn("No meta allocation data included in root block for RWStore"); // possible
																					// when
																					// rolling
																					// back
																					// to
																					// empty
																					// file
		}
		
		if (log.isInfoEnabled()) {
			int commitRecordAddr = (int) (rbv.getCommitRecordAddr() >> 32);
			log.info("CommitRecord " + rbv.getCommitRecordAddr() + " at physical address: " + physicalAddress(commitRecordAddr));
		}
		
		final long commitCounter = rbv.getCommitCounter();

		final int metaStartAddr = (int) -(metaAddr >> 32); // void
		final int fileSize = (int) -(metaAddr & 0xFFFFFFFF);

		if (log.isDebugEnabled())
			log.debug("m_allocation: " + nxtalloc + ", m_metaBitsAddr: "
					+ metaBitsAddr + ", m_commitCounter: " + commitCounter);
		
		/**
		 * Ensure rootblock is in sync with external request
		 */
		m_rb = rbv;
	}

	/**
	 * Should be called where previously initFileSpec was used.
	 * 
	 * Rather than reading from file, instead reads from the current root block.
	 * 
	 * We use the rootBlock fields, nextOffset, metaStartAddr, metaBitsAddr
	 * 
	 * metaBitsAddr indicates where the meta allocation bits are.
	 * 
	 * metaStartAddr is the offset in the file where the allocation blocks are
	 * allocated the long value also indicates the size of the allocation, such
	 * that the address plus the size is the "filesize".
	 * 
	 * Note that metaBitsAddr must be an absolute address, with the low order 16
	 * bits used to indicate the size.
	 * 
	 * @throws IOException
	 */
	private void initfromRootBlock() throws IOException {
		m_rb = m_fmv.getRootBlock();

		m_commitCounter = m_rb.getCommitCounter();

		final long nxtOffset = m_rb.getNextOffset();
		m_nextAllocation = -(int) (nxtOffset >> 32);

		m_metaBitsAddr = -(int) nxtOffset;
		
		if (log.isInfoEnabled()) {
			log.info("MetaBitsAddr: " + m_metaBitsAddr);
		}

		final long metaAddr = m_rb.getMetaStartAddr();
		// m_metaStartAddr = (int) -(metaAddr >> 32);
		m_fileSize = (int) -(metaAddr & 0xFFFFFFFF);

		long rawmbaddr = m_rb.getMetaBitsAddr();
		m_metaBitsSize = (int) (rawmbaddr & 0xFFFF); // take bottom 16 bits (
														// even 1K of metabits
														// is more than
														// sufficient)
		if (m_metaBitsSize > 0) {
			rawmbaddr >>= 16;
			m_metaBits = new int[m_metaBitsSize];
			if (log.isInfoEnabled()) {
				log.info("Raw MetaBitsAddr: " + rawmbaddr);
			}
	
			// RWStore now restore metabits
			final byte[] buf = new byte[m_metaBitsSize * 4];
			//m_raf.seek(rawmbaddr);
			//m_raf.read(buf);
			// m_raf.getChannel().read(ByteBuffer.wrap(buf), rawmbaddr);
			FileChannelUtility.readAll(m_reopener, ByteBuffer.wrap(buf), rawmbaddr);
	
			final DataInputStream strBuf = new DataInputStream(new ByteArrayInputStream(buf));
			for (int i = 0; i < m_metaBitsSize; i++) {
				m_metaBits[i] = strBuf.readInt();
			}
			m_metaTransientBits = (int[]) m_metaBits.clone();
	
			checkCoreAllocations();
	
			readAllocationBlocks();
			
			if (log.isTraceEnabled()) {
				final StringBuffer str = new StringBuffer();
				this.showAllocators(str);
				log.trace(str);
			}
			
			if (physicalAddress(m_metaBitsAddr) == 0) {
				throw new IllegalStateException("Free/Invalid metaBitsAddr on load");
			}

		}
		
		if (log.isInfoEnabled())
			log.info("restored from RootBlock: " + m_nextAllocation 
					+ ", " + m_metaBitsAddr);
	}

	/*********************************************************************
	 * make sure resource is closed!
	 **/
	protected void finalize() {
		close();
	}

	protected void readAllocationBlocks() throws IOException {
		
		assert m_allocs.size() == 0;

		/**
		 * Allocators are sorted in StartAddress order (which MUST be the order
		 * they were created and therefore will correspond to their index) The
		 * comparator also checks for equality, which would indicate an error in
		 * the metaAllocation if two allocation blocks were loaded for the same
		 * address (must be two version of same Allocator).
		 * 
		 * Meta-Allocations stored as {int address; int[8] bits}
		 */
		for (int b = 0; b < m_metaBits.length; b += 9) {
			long blockStart = convertAddr(m_metaBits[b]);
			int startBit = (b * 9 * 32) + 32;
			int endBit = startBit + (8*32);
			for (int i = startBit; i < endBit; i++) {
				if (tstBit(m_metaBits, i)) {
					long addr = blockStart + ((i-startBit) * ALLOC_BLOCK_SIZE);

					final byte buf[] = new byte[ALLOC_BLOCK_SIZE];

					FileChannelUtility.readAll(m_reopener, ByteBuffer.wrap(buf), addr);

					final DataInputStream strBuf = new DataInputStream(new ByteArrayInputStream(buf));

					final int allocSize = strBuf.readInt(); // if Blob < 0
					Allocator allocator = null;
					ArrayList freeList = null;
					if (allocSize > 0) {
						int index = 0;
						int fixedSize = MIN_FIXED_ALLOC;
						while (fixedSize < allocSize)
							fixedSize = 64 * ALLOC_SIZES[++index];

						allocator = new FixedAllocator(allocSize, m_preserveSession, m_writeCache);

						freeList = m_freeFixed[index];
					} else {
						allocator = new BlobAllocator(this, allocSize);
						freeList = m_freeBlobs;
					}

					allocator.read(strBuf);
					allocator.setDiskAddr(i); // store bit, not physical
												// address!
					allocator.setFreeList(freeList);

					m_allocs.add(allocator);

				}
			}
		}

		// add sorted blocks into index array and set index number for address
		// encoding
		// m_allocs.addAll(blocks);
		Collections.sort(m_allocs);
		for (int index = 0; index < m_allocs.size(); index++) {
			((Allocator) m_allocs.get(index)).setIndex(index);
		}
	}

//	// Root interface
//	public long getRootAddr() {
//		return m_rootAddr;
//	}
//
//	// Root interface
//	public PSInputStream getRoot() {
//		try {
//			return getData(m_rootAddr);
//		} catch (Exception e) {
//			throw new StorageTerminalError("Unable to read root data", e);
//		}
//	}
//
//	public void setRootAddr(long rootAddr) {
//		m_rootAddr = (int) rootAddr;
//	}

//	// Limits
//	public void setMaxFileSize(final int maxFileSize) {
//		m_maxFileSize = maxFileSize;
//	}

	public long getMaxFileSize() {
		long maxSize = m_maxFileSize;
		return maxSize << 8;
	}

//	// Allocators
//	public PSInputStream getData(final long addr) {
//		return getData((int) addr, addr2Size((int) addr));
//	}
//
//	// Allocators
//	public PSInputStream getData(final int addr, final int size) {
//        final Lock readLock = m_extensionLock.readLock();
//
//        readLock.lock();
//        
//		try {
//			try {
//				m_writeCache.flush(false);
//			} catch (InterruptedException e1) {
//			    throw new RuntimeException(e1);
//			}
//
//			if (addr == 0) {
//				return null;
//			}
//
//			final PSInputStream instr = PSInputStream.getNew(this, size);
//
//			try {
////				m_raf.seek(physicalAddress(addr));
////				m_raf.readFully(instr.getBuffer(), 0, size);
////				m_raf.getChannel().read(ByteBuffer.wrap(instr.getBuffer(), 0, size), physicalAddress(addr));
//				FileChannelUtility.readAll(m_reopener, ByteBuffer.wrap(instr.getBuffer(), 0, size),
//						physicalAddress(addr));
//			} catch (IOException e) {
//				throw new StorageTerminalError("Unable to read data", e);
//			}
//
//			return instr;
//		} finally {
//			readLock.unlock();
//		}
//	}

	volatile private long m_cacheReads = 0;
	volatile private long m_diskReads = 0;
	volatile private int m_allocations = 0;
	volatile private int m_frees = 0;
	volatile private long m_nativeAllocBytes = 0;

	/**
	 * If the buf[] size is greater than the maximum fixed allocation, then the direct read
	 * will be the blob header record.  In this case we should hand over the streaming to a PSInputStream.
	 * 
	 * FIXME: For now we do not use the PSInputStream but instead process directly
	 * 
	 * If it is a BlobAllocation, then the BlobAllocation address points to the address of the BlobHeader
	 * record.  
	 */
	public void getData(long addr, byte buf[]) {
		getData(addr, buf, 0, buf.length);
	}
	
	public void getData(long addr, byte buf[], int offset, int length) {
		if (addr == 0) {
			return;
		}

        final Lock readLock = m_extensionLock.readLock();

        readLock.lock();
        
		try {
			// length includes space for the checksum
			if (length > MAX_FIXED_ALLOC) {
				try {
					int nblocks = 1 + (length/(MAX_FIXED_ALLOC-4));
					byte[] hdrbuf = new byte[4 * (nblocks + 1) + 4]; // plus 4 bytes for checksum
					BlobAllocator ba = (BlobAllocator) getBlock((int) addr);
					getData(ba.getBlobHdrAddress(getOffset((int) addr)), hdrbuf); // read in header 
					DataInputStream hdrstr = new DataInputStream(new ByteArrayInputStream(hdrbuf));
					int rhdrs = hdrstr.readInt();
					if (rhdrs != nblocks) {
						throw new IllegalStateException("Incompatible BLOB header record, expected: " + nblocks + ", got: " + rhdrs);
					}
					int[] blobHdr = new int[nblocks];
					for (int i = 0; i < nblocks; i++) {
						blobHdr[i] = hdrstr.readInt();
					}
					// Now we have the header addresses, we can read MAX_FIXED_ALLOCS until final buffer
					int cursor = 0;
					int rdlen = MAX_FIXED_ALLOC;
					for (int i = 0; i < nblocks; i++) {
						if (i == (nblocks - 1)) {
							rdlen = length - cursor;
							// System.out.println("Calculated last read as " + rdlen);
						}
						getData(blobHdr[i], buf, cursor, rdlen); // include space for checksum
						cursor += rdlen-4; // but only increase cursor by data
					}
					
					return;
					
				} catch (IOException e) {
					e.printStackTrace();
					
					throw new IllegalStateException("Unable to restore Blob allocation", e);
				}
			}

			try {
				long paddr = physicalAddress((int) addr);
				if (paddr == 0) {
					log.warn("Address " + addr + " did not resolve to physical address");
					throw new IllegalArgumentException();
				}

				/**
				 * Check WriteCache first
				 * 
				 * Note that the buffer passed in should include the checksum 
				 * value, so the cached data is 4 bytes less than the
				 * buffer size.
				 */
				ByteBuffer bbuf = m_writeCache.read(paddr);
				if (bbuf != null) {
					byte[] in = bbuf.array(); // reads in with checksum - no need to check if in cache
					if (in.length != length-4) {
						throw new IllegalStateException("Incompatible buffer size for addr: " + addr + ", " + in.length
								+ " != " + length);
					}
					for (int i = 0; i < length-4; i++) {
						buf[offset+i] = in[i];
					}
					m_cacheReads++;
				} else {
					// If checksum is required then the buffer should be sized to include checksum in final 4 bytes
					ByteBuffer bb = ByteBuffer.wrap(buf, offset, length);
					FileChannelUtility.readAll(m_reopener, bb, paddr);
					int chk = ChecksumUtility.getCHK().checksum(buf, offset, length-4); // read checksum
					int tstchk = bb.getInt(offset + length-4);
					if (m_diskReads == 22000) {
						System.out.println("Problem Read");
					}
					if (chk != tstchk) {
						log.warn("Invalid data checksum for addr: " + paddr 
								+ ", chk: " + chk + ", tstchk: " + tstchk + ", length: " + length
								+ ", first byte: " + buf[0] + ", successful reads: " + m_diskReads
								+ ", at last extend: " + m_readsAtExtend + ", cacheReads: " + m_cacheReads);
						throw new IllegalStateException("Invalid data checksum");
					}
					
					m_diskReads++;
				}
			} catch (Exception e) {
				e.printStackTrace();
				
				throw new IllegalArgumentException("Unable to read data", e);
			}
		} finally {
			readLock.unlock();
		}
	}

	/**
	 * FIXME: This method is not currently used with BigData, if needed then
	 * the address mangling needs re-working
	 */
	public int getDataSize(long addr, byte buf[]) {
		throw new UnsupportedOperationException();
		
//		synchronized (this) {
//			m_writes.flush();
//
//			if (addr == 0) {
//				return 0;
//			}
//
//			try {
//				int size = addr2Size((int) addr);
//				synchronized (m_raf) {
////					m_raf.seek(physicalAddress((int) addr));
////					m_raf.readFully(buf, 0, size);
//					m_raf.getChannel().read(ByteBuffer.wrap(buf, 0, size), physicalAddress((int) addr));
//					}
//
//				return size;
//			} catch (IOException e) {
//				throw new StorageTerminalError("Unable to read data", e);
//			}
//		}
	}

	/***************************************************************************************
	 * this supports the core functionality of a WormStore, other stores should
	 * return zero, indicating no previous versions available
	 **/
	public long getPreviousAddress(long laddr) {
		return 0;
	}

	/**
	 * free
	 * 
	 * If the address is greater than zero than it is interpreted as a physical address and
	 * the allocators are searched to find the allocations.  Otherwise the address directly encodes
	 * the allocator index and bit offset, allowing direct access to clear the allocation.
	 * <p>
	 * A blob allocator contains the allocator index and offset, so an allocator contains up to
	 * 245 blob references.
	 * 
	 * @param sze 
	 */
	public void free(final long laddr, final int sze) {
		int addr = (int) laddr;

		switch (addr) {
		case 0:
		case -1:
		case -2:
			return;
		}
		
		m_allocationLock.lock();
		try {
			while (addr != 0) {
				final Allocator alloc = getBlockByAddress(addr);
				final long pa = alloc.getPhysicalAddress(getOffset(addr));
				m_writeCache.clearWrite(pa);
				alloc.free(addr, sze);
				m_frees++;
	
				if (!m_commitList.contains(alloc)) {
					m_commitList.add(alloc);
				}
	
				addr = 0;
			}
		} finally {
			m_allocationLock.unlock();
		}
	}

	/**
	 * alloc
	 * 
	 * Alloc always allocates from a FixedAllocation. Blob allocations are
	 * implemented using largest Fixed blocks as specified in MAX_FIXED_ALLOC.
	 * 
	 * The previous Stream method chained blocks together, but the new approach
	 * uses a master block and a list of allocations. Since we now have a
	 * MAX-FIXED_ALLOC of 256K this means that we would represent a 1MB
	 * allocation as a 64byte masters and four 256K blocks. For BigData 1MB
	 * bloom filters we would probably handle all in a single FixedAllocator of
	 * 256K allocations since we would hold 4096 of these in a single allocator,
	 * which with (say) 12 1MB bloom filters with 2-phase commit would only
	 * require 2 * (4 * 12) = 48 bits plus 12 64 byte headers. The maximum BLOB
	 * would be determined by a 256K header record with 64K * 256K allocations
	 * or 16GB, which is larger than MAXINT (we use an int to store allocation
	 * size in the address).
	 * 
	 * TODO: For BigData we need an additional set of "special" blocks to
	 * support storage of objects such as Bloom filters. These could be upto
	 * 1Mb, so we'd need to allocate a set (not necessarily 32), of these
	 * objects and provide an enhanced interface.
	 * 
	 * TODO: Alternatively, and more generally, a BLOB mechanism would allocate
	 * an array of blocks and store/write into a master block.
	 * 
	 * TODO: Remove use of synchronized and replace with lock.  The synchronized
	 * guarded reads from file extensions caused by allocations, but the new
	 * extensionFile lock fulfills this requirement, while a separate lock
	 * could be used to protect multi-threaded allocations (which is separate
	 * from file extension).
	 */

	private volatile long m_maxAllocation = 0;
	private volatile long m_spareAllocation = 0;
	
	public int alloc(final int size) {
		if (size > MAX_FIXED_ALLOC) {
			throw new IllegalArgumentException("Alloation size to big: " + size);
		}
		
		m_allocationLock.lock();
		try {
			try {
				if (size > m_maxAllocation) {
					m_maxAllocation = size;
				}

				ArrayList list;
				Allocator allocator = null;
				int i = 0;
				int addr = 0;

				int cmp = MIN_FIXED_ALLOC;
				while (size > cmp) {
					i++;
					cmp = 64 * ALLOC_SIZES[i];
				}
				ALLOC_BUCKETS[i]++;
				m_spareAllocation += (cmp - size); // Isn't adjusted by frees!
				
				list = m_freeFixed[i];
				if (list.size() == 0) {

					allocator = new FixedAllocator(cmp, m_preserveSession, m_writeCache);
					allocator.setFreeList(list);
					allocator.setIndex(m_allocs.size());

					addr = allocator.alloc(this, size);

					if (log.isDebugEnabled())
						log.debug("New FixedAllocator for " + cmp + " byte allocations at " + addr);

					m_allocs.add(allocator);
				} else {
					// Verify free list only has allocators with free bits
					{
						int tsti = 0;
						Iterator<Allocator> allocs = list.iterator();
						while (allocs.hasNext()) {
							Allocator tstAlloc = allocs.next();
							if (!tstAlloc.hasFree()) {
								throw new IllegalStateException("Free list contains full allocator, " + tsti + " of " + list.size());
							}
							tsti++;
						}
					}
					allocator = (Allocator) list.get(0);
					addr = allocator.alloc(this, size);
					if (addr == 0) {
						throw new IllegalStateException("Allocator " + allocator + " from FreeList allocating null address");
					}
				}

				if (!m_commitList.contains(allocator)) {
					m_commitList.add(allocator);
				}

				if (!allocator.hasFree()) {
					list.remove(allocator);
				}

				m_recentAlloc = true;

				long pa = physicalAddress(addr);
				if (pa == 0L) {
					throw new IllegalStateException("No physical address found for " + addr);
				}

				m_allocations++;
				m_nativeAllocBytes += size;
				
				return addr;
			} catch (Throwable t) {
				log.error(t,t);

				throw new RuntimeException(t);
			}
		} finally {
			m_allocationLock.unlock();
		}
	}

	/****************************************************************************
	 * The base realloc method that returns a stream for writing to rather than
	 * handle the reallocation immediately.
	 **/
	public PSOutputStream realloc(final long oldAddr, final int size) {
		free(oldAddr, size);

		return PSOutputStream.getNew(this);
	}
	
	/****************************************************************************
	 * Called by PSOutputStream to make to actual allocation or directly by lower
	 * level API clients.
	 * <p>
	 * If the allocation is for greater than MAX_FIXED_ALLOC, then a PSOutputStream
	 * is used to manage the chained buffers.
	 * 
	 * TODO: Instead of using PSOutputStream instead manage allocations written
	 * to the WriteCacheService, building BlobHeader as you go.
	 **/
	public long alloc(final byte buf[], final int size) {
		if (size >= MAX_FIXED_ALLOC) {
			if (log.isDebugEnabled())
				log.debug("BLOB ALLOC: " + size);

			final PSOutputStream psout = PSOutputStream.getNew(this);
			try {
				int i = 0;
				final int lsize = size - 512;
				while (i < lsize) {
					psout.write(buf, i, 512); // add 512 bytes at a time
					i += 512;
				}
				psout.write(buf, i, size - i);

				return psout.save();
			} catch (IOException e) {
				throw new RuntimeException("Closed Store?", e);
			}

		}

		final int newAddr = alloc(size + 4); // allow size for checksum

		final int chk = ChecksumUtility.getCHK().checksum(buf, size);

		try {
			m_writeCache.write(physicalAddress(newAddr), ByteBuffer.wrap(buf,  0, size), chk);
		} catch (InterruptedException e) {
            throw new RuntimeException("Closed Store?", e);
		}

		return newAddr;
	}

//	/****************************************************************************
//	 * Fixed buffer size reallocation
//	 **/
//	public long realloc(final long oldAddr, final int oldSize, final byte buf[]) {
//		
//	    free(oldAddr, oldSize);
//
//		return alloc(buf, buf.length);
//	}

//	/**
//	 * Must handle valid possibility that a request to start/commit transaction
//	 * could be made within a commitCallback request
//	 */
//	synchronized public void startTransaction() {
//		if (m_committing) {
//			return;
//		}
//
//		m_transactionCount++;
//	}
//
//	synchronized public void commitTransaction() {
//		if (m_committing) {
//			return;
//		}
//
//		if (log.isDebugEnabled())
//			log.debug("Commit Transaction");
//		
//		if (--m_transactionCount <= 0) {
//			commitChanges();
//
//			m_transactionCount = 0;
//		}
//	}
//
//	public int getTransactionCount() {
//		return m_transactionCount;
//	}
//
//	// --------------------------------------------------------------------------------------------
//	// rollbackTransaction
//	//
//	// clear write cache
//	// read in last committed header
//	synchronized public void rollbackTransaction() {
//		if (m_transactionCount > 0 || m_readOnly) { // hack for resync
//			baseInit();
//
//			try {
//				m_writeCache.reset(); // dirty writes are discarded
//
//				readAllocationBlocks();
//			} catch (Exception e) {
//				throw new StorageTerminalError("Unable to rollback transaction", e);
//			}
//		}
//	}

	// --------------------------------------------------------------------------------------------
	// reset
	//
	// Similar to rollbackTransaction but will force a re-initialization if transactions are not being
	//	used - update w/o commit protocol.
	public void reset() {
	    m_allocationLock.lock();
		try {
	        baseInit();
	        
			m_writeCache.reset(); // dirty writes are discarded
			m_writeCache.setExtent(convertAddr(m_fileSize)); // notify of current file length.
			
			initfromRootBlock();

			// readAllocationBlocks();
		} catch (Exception e) {
			throw new IllegalStateException("Unable reset the store", e);
		} finally {
		    m_allocationLock.lock();
		}
	}

//	synchronized public boolean isActiveTransaction() {
//		return m_transactionCount > 0;
//	}

	/**
	 * writeMetaBits must be called after all allocations have been made, the
	 * last one being the allocation for the metabits themselves (allowing for
	 * an extension!).
	 * 
	 * @throws IOException
	 */
	private void writeMetaBits() throws IOException {
	    final int len = 4 * m_metaBitsSize;
		final byte buf[] = new byte[len];

		final FixedOutputStream str = new FixedOutputStream(buf);
		for (int i = 0; i < m_metaBitsSize; i++) {
			str.writeInt(m_metaBits[i]);
		}

		str.flush();

		final long addr = physicalAddress(m_metaBitsAddr);
		if (addr == 0) {
			throw new IllegalStateException("Invalid metabits address: " + m_metaBitsAddr);
		}
		try {
			m_writeCache.write(addr, ByteBuffer.wrap(buf), 0, false);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	static final float s_version = 3.0f;

	/**
	 * This must now update the root block which is managed by FileMetadata in
	 * almost guaranteed secure manner
	 * 
	 * It is not the responsibility of the store to write this out, this is
	 * handled by whatever is managing the FileMetadata that this RWStore was
	 * initialised from and should be forced by newRootBlockView
	 * 
	 * It should now only be called by extend file to ensure that the metaBits
	 * are set correctly
	 * 
	 * In order to ensure that the new block is the one that would be chosen, we need to
	 * duplicate the rootBlock. This does mean that we lose the ability to roll
	 * back the commit.  It also means that until that point there is an invalid store state.
	 * Both rootBlocks would be valid but with different extents.  This is fine at
	 * that moment, but subsequent writes would effectively cause the initial rootBlock
	 * to reference invalid allocation blocks.
	 * 
	 * In any event we need to duplicate the rootblocks since any rootblock that references
	 * the old allocation area will be invalid.
	 * 
	 * TODO: Should be replaced with specific updateExtendedMetaData that will
	 * simply reset the metaBitsAddr
	 * @throws IOException 
	 */
	protected void writeFileSpec() throws IOException {
		m_rb = m_fmv.newRootBlockView(!m_rb.isRootBlock0(), m_rb.getOffsetBits(), getNextOffset(), m_rb
				.getFirstCommitTime(), m_rb.getLastCommitTime(), m_rb.getCommitCounter(), m_rb.getCommitRecordAddr(),
				m_rb.getCommitRecordIndexAddr(), getMetaStartAddr(), getMetaBitsAddr(), m_rb.getLastCommitTime());
		
		m_fmv.getFileMetadata().writeRootBlock(m_rb, ForceEnum.Force);
	}

//	float m_vers = 0.0f;
//
//	protected void readFileSpec() {
//		if (true) {
//			throw new Error("Unexpected old format initialisation called");
//		}
//
//		try {
//			m_raf.seek(0);
//			m_curHdrAddr = m_raf.readLong();
//
//			m_fileSize = m_raf.readInt();
//			m_metaStartAddr = m_raf.readInt();
//
//			m_vers = m_raf.readFloat();
//
//			if (m_vers != s_version) {
//				String msg = "Incorrect store version : " + m_vers + " expects : " + s_version;
//
//				throw new IOException(msg);
//			} else {
//				m_headerSize = m_raf.readInt();
//			}
//
//		} catch (IOException e) {
//			throw new StorageTerminalError("Unable to read file spec", e);
//		}
//	}

	public String getVersionString() {
		return "RWStore " + s_version;
	}

	public void commitChanges() {
		if (log.isDebugEnabled())
			log.debug("checking meta data save");

		checkCoreAllocations();

		// take allocation lock to prevent other threads allocating during commit
		m_allocationLock.lock();
		
		try {
//				m_committing = true;

//				if (m_commitCallback != null) {
//					m_commitCallback.commitCallback();
//				}

				// Allocate storage for metaBits
				long oldMetaBits = m_metaBitsAddr;
				int oldMetaBitsSize = m_metaBitsSize * 4;
				m_metaBitsAddr = alloc(getRequiredMetaBitsStorage());
				// System.out.println("Allocated metaBits: " + m_metaBitsAddr);
				// DEBUG SANITY CHECK!
				if (physicalAddress(m_metaBitsAddr) == 0) {
					throw new IllegalStateException("Returned MetaBits Address not valid!");
				}

				// System.out.println("Freeing metaBits: " + oldMetaBits);
				free(oldMetaBits, oldMetaBitsSize);

				// save allocation headers
				Iterator iter = m_commitList.iterator();
				while (iter.hasNext()) {
					final Allocator allocator = (Allocator) iter.next();
					final int old = allocator.getDiskAddr();
					metaFree(old);
					
					final int naddr = metaAlloc();
					allocator.setDiskAddr(naddr);
					
					if (log.isDebugEnabled())
						log.debug("Update allocator " + allocator.getIndex() + ", old addr: " + old + ", new addr: "
								+ naddr);

					try {
						m_writeCache.write(metaBit2Addr(naddr), ByteBuffer.wrap(allocator.write()), 0, false); // do not use checksum
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				}
				m_commitList.clear();

				writeMetaBits();

				try {
					m_writeCache.flush(true);
				} catch (InterruptedException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}

				// Should not write rootBlock, this is responsibility of client
				// to provide control
				// writeFileSpec();

				m_metaTransientBits = (int[]) m_metaBits.clone();

//				if (m_commitCallback != null) {
//					m_commitCallback.commitComplete();
//				}

				m_raf.getChannel().force(false); // TODO, check if required!
		} catch (IOException e) {
			throw new StorageTerminalError("Unable to commit transaction", e);
		} finally {
//			m_committing = false;
			m_recentAlloc = false;
			m_allocationLock.unlock();
		}

		checkCoreAllocations();

		if (log.isInfoEnabled())
			log.info("commitChanges for: " + m_nextAllocation + ", " + m_metaBitsAddr);
	}

	/**
	 * 
	 * @return conservative requirement for metabits storage, mindful that the
	 *         request to allocate the metabits may require an increase in the
	 *         number of allocation blocks and therefore an extension to the
	 *         number of metabits.
	 */
	private int getRequiredMetaBitsStorage() {
		return ((8 + m_commitList.size()) / 8) + (4 * (1 + m_metaBits.length));
	}

	// Header Data
	volatile private long m_curHdrAddr = 0;
	volatile private int m_rootAddr;

	volatile private int m_fileSize;
	volatile private int m_nextAllocation;
	volatile private int m_maxFileSize;

	private int m_headerSize = 2048;

	// Meta Allocator
	private static int cDefaultMetaBitsSize = 9; // DEBUG FIX ME
	private int m_metaBits[];
	volatile private int m_metaBitsSize = cDefaultMetaBitsSize;
	private int m_metaTransientBits[];
	// volatile private int m_metaStartAddr;

	volatile private boolean m_recentAlloc = false;

	protected int allocBlock(int size) {
		// minimum 1
		if (size <= 0) {
			throw new Error("allocBlock called with zero size request");
		}

		int allocAddr = m_nextAllocation;
		m_nextAllocation -= size;

		while (convertAddr(m_nextAllocation) >= convertAddr(m_fileSize)) {
			extendFile();
		}

		checkCoreAllocations();

		if (log.isDebugEnabled())
			log.debug("allocation created at " + convertAddr(allocAddr) + " for " + convertAddr(-size));

		return allocAddr;
	}

	void checkCoreAllocations() {
		long lfileSize = convertAddr(m_fileSize);
		long lnextAlloc = convertAddr(m_nextAllocation);

		if (lnextAlloc >= lfileSize) {
			throw new IllegalStateException("Core Allocation Error - file size: " 
					+ lfileSize + ", nextAlloc: " + lnextAlloc);
		}
	}

	/**
	 * meta allocation/free
	 * 
	 * Allocates persistant store for allocation blocks.
	 * 
	 * grows data from the top to the file, e.g. bit 0 is 1024 from end-of-file.
	 * 
	 * If metaStart <= nextAllocation, then the file must be extended. All the
	 * allocation blocks are moved to the new end of file area, and the
	 * metaStartAddress is incremented by the same delta value.
	 * 
	 * NB the metaStart calculation uses an address rounded to 8k, so on
	 * extension the new metaStart may be up to 8K less than the true start
	 * address.
	 * 
	 * The updated approach to metaAllocation uses native allocation from
	 * the heap (by simply incrementing from m_nextAllocation) to provide
	 * space for the allocation blocks.
	 * 
	 * This approach means that the file only needs to be extended when
	 * m_nextAllocation passes the m_fileSize, since we no longer store
	 * the allocation blocks at the end of the file.
	 */
	int metaAlloc() {
		long lnextAlloc = convertAddr(m_nextAllocation);

		int bit = fndMetabit();

		if (bit < 0) {
			// reallocate metaBits and recalculate m_headerSize
			// extend m_metaBits by 8 ints of bits plus start address!
			int nsize = m_metaBitsSize + cDefaultMetaBitsSize;

			// arrays initialized to zero by default
			int[] nbits = new int[nsize];
			int[] ntransients = new int[nsize];

			// copy existing values
			for (int i = 0; i < m_metaBitsSize; i++) {
				nbits[i] = m_metaBits[i];
				ntransients[i] = m_metaTransientBits[i];
			}
			m_metaBits = nbits;
			m_metaTransientBits = ntransients;
			
			m_metaBits[m_metaBitsSize] = m_nextAllocation;
			m_nextAllocation -= META_ALLOCATION; // 256K

			m_metaBitsSize = nsize;

			// now get new allocation!
			bit = fndMetabit();
		}

		setBit(m_metaTransientBits, bit);
		setBit(m_metaBits, bit);

		if (m_nextAllocation <= m_fileSize) {
			if (log.isInfoEnabled())
				log.info("ExtendFile called from metaAlloc");
			
			extendFile();
		}

		// cat.info("meta allocation at " + addr);

		checkCoreAllocations();

		return bit;
	}

	private int fndMetabit() {
		int blocks = m_metaBitsSize/9;
		for (int b = 0; b < blocks; b++) {
			int ret = fndBit(m_metaTransientBits, (b*9)+1, 8);
			if (ret != -1) {
				return ret;
			}
		}
		
		return -1; // none found
	}
	
	private int calcHdrAllocSize() {
		return 4 * (1 + m_metaBits.length);
	}

	void metaFree(int bit) {
		if (bit <= 0) {
			return;
		}
		
		if (tstBit(m_metaBits, bit)) {
			clrBit(m_metaBits, bit);
		} else {
			clrBit(m_metaTransientBits, bit);
		}
		
		m_writeCache.clearWrite(metaBit2Addr(bit));
	}

	long metaBit2Addr(final int bit) {
		final int bitsPerBlock = 9 * 32;
		
		int intIndex = bit / 32; // divide 32;
		int addrIndex = (intIndex/9)*9;
		long addr = convertAddr(m_metaBits[addrIndex]);

		int intOffset = bit - ((addrIndex+1) * 32);

		long ret =  addr + (ALLOC_BLOCK_SIZE * intOffset);
		
		return ret;
	}

//	/*
//	 * clear
//	 * 
//	 * reset the file size commit the root blocks
//	 */
//	public void clear() {
//		try {
//			baseInit();
//
//			m_fileSize = -4;
//			m_metaStartAddr = m_fileSize;
//			m_nextAllocation = -1; // keep on a 8K boundary (8K minimum
//			// allocation)
//			m_raf.setLength(convertAddr(m_fileSize));
//
//			m_curHdrAddr = 0;
//			m_rootAddr = 0;
//
//			startTransaction();
//			commitTransaction();
//		} catch (Exception e) {
//			throw new StorageTerminalError("Unable to clear store", e);
//		}
//	}

	public static long convertAddr(int addr) {
		long laddr = addr;

		if (laddr < 0) {
			long ret = (-laddr) << ALLOCATION_SCALEUP; 
			
			return ret;
		} else {
			return laddr & 0xFFFFFFF0;
		}
	}

	public int convertFromAddr(final long addr) {
		return (int) -(addr >> ALLOCATION_SCALEUP); 
	}

	private volatile boolean m_extendingFile = false;
	
	/**
	 * extendFile will extend by 10% and round up to be a multiple of 16k
	 * 
	 * The allocation blocks must also be moved. Note that it would be a bad
	 * idea if these were moved with an overlapping copy!
	 * 
	 * After moving the physical memory the in-memory allocation blocks must
	 * then be updated with their new position.
	 * 
	 * Note that since version 3.0 the size of the metaBits is variable. This
	 * must be taken into consideration when moving data. - Has the location
	 * changed as a result of the "reallocation". If this is incorrect then the
	 * wrong commit blocks will be copied, resulting in a corrupt data file.
	 * 
	 * There are two approaches to this problem. The first is only to copy the
	 * known committed (written) allocation blocks - but this cannot be implied
	 * by "zero'd" bits since this can indicate that memory has been cleared.
	 * 
	 * Synchronization
	 * 
	 * The writecache may contain allocation block writes that must be flushed 
	 * before the file can be extended.  The extend file explicitly moves the 
	 * written allocation blocks to there new location at the new end of the 
	 * file and then updates the rootblocks to ensure they point to the new 
	 * allocation areas.
	 * 
	 * Extend file is only triggered by either alloc or metaAlloc which are s
	 * synchronized by the allocation lock. So extend file ends up being 
	 * synchronized by the same lock.
	 *
	 * If we knew that the write cache had no writes to the allocation areas, 
	 * we would not need to flush, but calling flush prior to the extend is 
	 * sufficient to guarantee, in conjunction with holding the allocation lock,
	 * that no new writes to the allocation areas will be made.
	 * 
	 * Once the flush is complete we take the extension writeLock to prevent 
	 * further reads or writes, extend the file, moving the allocation areas on
	 * the disk, then force the new rootblocks to disk.
	 */
	private void extendFile() {
	    
		final int adjust = -1200 + (m_fileSize / 10);
		
		extendFile(adjust);
	}
	
	private volatile long m_readsAtExtend = 0;
	
	private void extendFile(final int adjust) {
		if (m_extendingFile) {
			throw new IllegalStateException("File concurrently extended");
		}
		try {
			/*
			 * The call to flush the cache cannot be made while holding the
			 * extension writeLock, since the writeOnChannel takes the
			 * extension readLock.
			 * TODO: Confirm that this cannot be a problem... that writes could
			 * not be added to the writeCache by another thread to the
			 * allocation block area.
			 */
			m_writeCache.flush(true);
		} catch (InterruptedException e) {
			throw new RuntimeException("Flush interrupted in extend file");
		}

		final Lock writeLock = this.m_extensionLock.writeLock();
		
		writeLock.lock();
		try {
			m_extendingFile = true;

			final long curSize = convertAddr(m_fileSize);

			m_fileSize += adjust;

			if (getMaxFileSize() < m_fileSize) {
				// whoops!! How to exit more gracefully?
				throw new Error("System greater than maximum size");
			}

			final long toAddr = convertAddr(m_fileSize);
			
			if (log.isInfoEnabled()) log.info("Extending file to: " + toAddr);

			m_raf.setLength(toAddr);
			
			if (log.isInfoEnabled()) log.warn("Extend file done");
		} catch (Throwable t) {
			throw new RuntimeException("Force Reopen", t);
		} finally {
			m_extendingFile = false;
			m_readsAtExtend = this.m_diskReads;
			writeLock.unlock();
		}
	}

	static void setBit(int[] bits, int bitnum) {
		int index = bitnum / 32;
		int bit = bitnum % 32;

		bits[(int) index] |= 1 << bit;
	}

	static boolean tstBit(int[] bits, int bitnum) {
		int index = bitnum / 32;
		int bit = bitnum % 32;

		return (bits[(int) index] & 1 << bit) != 0;
	}

	static void clrBit(int[] bits, int bitnum) {
		int index = bitnum / 32;
		int bit = bitnum % 32;

		int val = bits[index];

		val &= ~(1 << bit);

		bits[index] = val;
	}

	static int fndBit(final int[] bits, final int size) {
		return fndBit(bits, 0, size);
	}
	static int fndBit(final int[] bits, final int offset, final int size) {
		final int eob = size + offset;
		
		for (int i = offset; i < eob; i++) {
			if (bits[i] != 0xFFFFFFFF) {
				for (int k = 0; k < 32; k++) {
					if ((bits[i] & (1 << k)) == 0) {
						return (i * 32) + k;
					}
				}
			}
		}

		return -1;
	}

	// --------------------------------------------------------------------------------------
	private String allocListStats(ArrayList list,final AtomicLong counter) {
		final StringBuffer stats = new StringBuffer();
		final Iterator iter = list.iterator();
		while (iter.hasNext()) {
			stats.append(((Allocator) iter.next()).getStats(counter));
		}

		return stats.toString();
	}

	public String getStats(final boolean full) {
		
        final AtomicLong counter = new AtomicLong();

        final StringBuilder sb = new StringBuilder("FileSize : " + m_fileSize
                + " allocated : " + m_nextAllocation + "\r\n");

		if (full) {

            sb.append(allocListStats(m_allocs, counter));

            sb.append("Allocated : " + counter);

		}

		return sb.toString();

	}
	
	/**
	 * Utility debug outputing the allocator array, showing index, start
	 * address and alloc type/size
	 */
	public void showAllocators(StringBuffer str) {
		Iterator allocs = m_allocs.iterator();
		while (allocs.hasNext()) {
			Allocator alloc = (Allocator) allocs.next();
			alloc.appendShortStats(str);
		}
	}

	public ArrayList getStorageBlockAddresses() {
		ArrayList addrs = new ArrayList();

		Iterator allocs = m_allocs.iterator();
		while (allocs.hasNext()) {
			Allocator alloc = (Allocator) allocs.next();
			alloc.addAddresses(addrs);
		}

		return addrs;
	}

	// --------------------------------------------------------------------------------------

	public boolean verify(long laddr) {
		int addr = (int) laddr;

		if (addr == 0) {
			return false;
		}

		return getBlockByAddress(addr) != null;
	}

	/*****************************************************************************
	 * Address transformation
	 * 
	 * latched2Physical
	 **/
	public long physicalAddress(int addr) {
		if (addr > 0) {
			return addr & 0xFFFFFFE0;
		} else {
			Allocator allocator = getBlock(addr);
			int offset = getOffset(addr);
			long laddr = allocator.getPhysicalAddress(offset);

			return laddr;
		}
	}

	/********************************************************************************
	 * handle dual address format, if addr is positive then it is the physical
	 * address, so the Allocators must be searched.
	 **/
	private Allocator getBlockByAddress(int addr) {
		if (addr < 0) {
			return getBlock(addr);
		}

		Iterator allocs = m_allocs.iterator();

		Allocator alloc = null;
		while (allocs.hasNext()) {
			alloc = (Allocator) allocs.next();

			if (alloc.addressInRange(addr)) {
				break;
			}
			alloc = null;
		}

		return alloc;
	}

	private int blockIndex(int addr) {
		return (-addr) >>> OFFSET_BITS;
	}

	private Allocator getBlock(int addr) {
		int index = (-addr) >>> OFFSET_BITS;

		return (Allocator) m_allocs.get(index);
	}

	private int getOffset(int addr) {
		return (-addr) & OFFSET_BITS_MASK; // OFFSET_BITS
	}

	public int addr2Size(int addr) {
		if (addr > 0) {
			int size = 0;

			int index = ((int) addr) % 16;

			if (index == 15) { // blob
				throw new Error("FIX ME : legacy BLOB code being accessed somehow");
			} else {
				size = MIN_FIXED_ALLOC * ALLOC_SIZES[index];
			}

			return size;
		} else {
			return getBlock(addr).getPhysicalSize(getOffset(addr));
		}
	}

	public boolean isNativeAddress(long addr) {
		return addr <= 0;
	}

	/*******************************************************************************
	 * called when used as a server, returns whether facility is enabled, this
	 * is the whole point of the wormStore - so the answer is true
	 **/
	public boolean preserveSessionData() {
		m_preserveSession = true;

		Iterator allocs = m_allocs.iterator();
		while (allocs.hasNext()) {
			((Allocator) allocs.next()).preserveSessionData();
		}
		return true;
	}

	/*******************************************************************************
	 * called by allocation blocks to determine whether they can re-allocate
	 * data within this session.
	 **/
	protected boolean isSessionPreserved() {
		return m_preserveSession;
	}

//	/*********************************************************************
//	 * create backup file, copy data to it, and close it.
//	 **/
//	synchronized public void backup(String filename) throws FileNotFoundException, IOException {
//		File destFile = new File(filename);
//		destFile.createNewFile();
//
//		RandomAccessFile dest = new RandomAccessFile(destFile, "rw");
//
//		int bufSize = 64 * 1024;
//		byte[] buf = new byte[bufSize];
//
//		m_raf.seek(0);
//
//		int rdSize = bufSize;
//		while (rdSize == bufSize) {
//			rdSize = m_raf.read(buf);
//			if (rdSize > 0) {
//				dest.write(buf, 0, rdSize);
//			}
//		}
//
//		dest.close();
//	}
//
//	/*********************************************************************
//	 * copy storefile to output stream.
//	 **/
//	synchronized public void backup(OutputStream outstr) throws IOException {
//		int bufSize = 64 * 1024;
//		byte[] buf = new byte[bufSize];
//
//		m_raf.seek(0);
//
//		int rdSize = bufSize;
//		while (rdSize == bufSize) {
//			rdSize = m_raf.read(buf);
//			if (rdSize > 0) {
//				outstr.write(buf, 0, rdSize);
//			}
//		}
//	}
//
//	synchronized public void restore(InputStream instr) throws IOException {
//		int bufSize = 64 * 1024;
//		byte[] buf = new byte[bufSize];
//
//		m_raf.seek(0);
//
//		int rdSize = bufSize;
//		while (rdSize == bufSize) {
//			rdSize = instr.read(buf);
//			if (rdSize > 0) {
//				m_raf.write(buf, 0, rdSize);
//			}
//		}
//	}

	/***************************************************************************************
	 * Needed by PSOutputStream for BLOB buffer chaining.
	 **/
	public void absoluteWriteInt(final int addr, final int offset, final int value) {
		try {
			// must check write cache!!, or the write may be overwritten - just
			// flush for now
			m_writes.flush();

			m_raf.seek(physicalAddress(addr) + offset);
			m_raf.writeInt(value);
		} catch (IOException e) {
			throw new StorageTerminalError("Unable to write integer", e);
		}
	}

	/***************************************************************************************
	 * Needed to free Blob chains.
	 **/
	public int absoluteReadInt(int addr, int offset) {
		try {
			m_raf.seek(physicalAddress(addr) + offset);
			return m_raf.readInt();
		} catch (IOException e) {
			throw new StorageTerminalError("Unable to write integer", e);
		}
	}

	/***************************************************************************************
	 * Needed by PSOutputStream for BLOB buffer chaining.
	 **/
	public int bufferChainOffset() {
		return MAX_FIXED_ALLOC - 4;
	}

	public File getStoreFile() {
		return m_fd;
	}

	public boolean isLongAddress() {
		// always ints
		return false;
	}

	public int absoluteReadLong(long addr, int offset) {
		throw new UnsupportedOperationException();
	}

	public void absoluteWriteLong(long addr, int threshold, long value) {
		throw new UnsupportedOperationException();
	}

	public void absoluteWriteAddress(long addr, int threshold, long addr2) {
		absoluteWriteInt((int) addr, threshold, (int) addr2);
	}

	public int getAddressSize() {
		return 4;
	}

	// DiskStrategy Support
	public RandomAccessFile getRandomAccessFile() {
		return m_raf;
	}

	public FileChannel getChannel() {
		return m_raf.getChannel();
	}

	public boolean requiresCommit() {
		return m_recentAlloc;
	}

	/**
	 * Note that the representation of the
	 * 
	 * @return long representation of metaBitsAddr PLUS the size
	 */
	public long getMetaBitsAddr() {
		long ret = physicalAddress((int) m_metaBitsAddr);
		ret <<= 16;
		
		if (log.isDebugEnabled())
			log.debug("Returning metabitsAddr: " + ret + ", for " + m_metaBitsAddr);

		ret += m_metaBitsSize;
		
		return ret;
	}

	/**
	 * @return long representation of metaStartAddr PLUS the size where addr +
	 *         size is fileSize (not necessarily physical size)
	 */
	public long getMetaStartAddr() {
		return -m_fileSize;
	}

	/**
	 * 
	 * @return the nextAllocation from the file Heap to be provided to an
	 *         Allocation Block
	 */
	public long getNextOffset() {
		long ret = -m_nextAllocation;
		ret <<= 32;
		ret += -m_metaBitsAddr;

		if (log.isDebugEnabled())
			log.debug("Returning nextOffset: " + ret + ", for " + m_metaBitsAddr);

		return ret;
	}

	public void flushWrites(boolean metadata) throws IOException {
		try {
			m_writeCache.flush(metadata);
		} catch (InterruptedException e) {
			throw new ClosedByInterruptException();
		}
	}

	public long getTotalAllocations() {
		return m_allocations;
	}
	public long getTotalFrees() {
		return m_frees;
	}
	public long getTotalAllocationsSize() {
		return m_nativeAllocBytes;
	}

	/**
	 * A Blob Allocator maintains a list of Blob headers.  The allocator stores upto 255 blob headers plus
	 * a checksum.
	 * When a request is made to read the blob data, the blob allocator retrieves the blob header and reads the data
	 * from that into the passed byte array.
	 */
	public int registerBlob(int addr) {
		if (m_freeBlobs == null) {
			m_freeBlobs = new ArrayList();
		}
		BlobAllocator ba = null;
		if (m_freeBlobs.size() > 0) {
			ba = (BlobAllocator) m_freeBlobs.get(0);
		}
		if (ba == null) {
			Allocator lalloc = (Allocator) m_allocs.get(m_allocs.size()-1);
			int psa = lalloc.getRawStartAddr(); // previous block start address
			assert (psa-1) > m_nextAllocation;
			ba = new BlobAllocator(this, psa-1);
			ba.setFreeList(m_freeBlobs); // will add itself to the free list
			ba.setIndex(m_allocs.size());
			m_allocs.add(ba);
		}
		
		if (!m_commitList.contains(ba)) {
			m_commitList.add(ba);
		}
		
		return ba.register(addr);
	}

	public void addToCommit(Allocator allocator) {
		if (!m_commitList.contains(allocator)) {
			m_commitList.add(allocator);
		}
	}

	public Allocator getAllocator(int i) {
		return (Allocator) m_allocs.get(i);
	}

    /**
     * Simple implementation for a {@link RandomAccessFile} to handle the direct backing store.
     */
    private static class ReopenFileChannel implements
            IReopenChannel<FileChannel> {

        final private File file;

        private final String mode;

        private volatile RandomAccessFile raf;

        public ReopenFileChannel(final File file, final RandomAccessFile raf, final String mode)
                throws IOException {

            this.file = file;

            this.mode = mode;
            
            this.raf = raf;

            reopenChannel();

        }

        public String toString() {

            return file.toString();

        }

//        /**
//         * Hook used by the unit tests to destroy their test files.
//         */
//        public void destroy() {
//            try {
//                raf.close();
//            } catch (IOException e) {
//                if (!file.delete())
//                    log.warn("Could not delete file: " + file);
//            }
//        }

        synchronized public FileChannel reopenChannel() throws IOException {

            if (raf != null && raf.getChannel().isOpen()) {

                /*
                 * The channel is still open. If you are allowing concurrent
                 * reads on the channel, then this could indicate that two
                 * readers each found the channel closed and that one was able
                 * to re-open the channel before the other such that the channel
                 * was open again by the time the 2nd reader got here.
                 */

                return raf.getChannel();

            }

            // open the file.
            this.raf = new RandomAccessFile(file, mode);

            if (log.isInfoEnabled())
                log.info("(Re-)opened file: " + file);

            return raf.getChannel();

        }

    }

    /**
     * Delegated to from setExtentForLocalStore after expected call from HAGlue.replicateAndReceive.
     * 
     * If the current file extent is different from the required extent then the call is made to move
     * the allocation blocks.
     * 
     * @param extent
     */
	public void establishHAExtent(long extent) {
		long currentExtent = convertAddr(m_fileSize);
		
		if (extent != currentExtent) {
			extendFile(convertFromAddr(extent - currentExtent));
		}
		
	}

}
