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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.IReopenChannel;
import com.bigdata.io.writecache.BufferedWrite;
import com.bigdata.io.writecache.WriteCache;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.CommitRecordIndex;
import com.bigdata.journal.CommitRecordSerializer;
import com.bigdata.journal.ForceEnum;
import com.bigdata.journal.ICommitRecord;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.Journal;
import com.bigdata.journal.JournalTransactionService;
import com.bigdata.journal.Options;
import com.bigdata.journal.RWStrategy.FileMetadataView;
import com.bigdata.quorum.Quorum;
import com.bigdata.util.ChecksumUtility;

/**
 * Storage class
 * <p>
 * Provides an interface to allocating storage within a disk file.
 * <p>
 * Essentially provides a DiskMalloc interface.
 * <p>
 * In addition to the DiskMalloc/ReAlloc mechanism, a single root address can be
 * associated. This can be used when opening an existing storage file to
 * retrieve some management object - such as an object manager!
 * <p>
 * The allocator also support atomic update via a simple transaction mechanism.
 * <p>
 * Updates are normally committed immediately, but by using startTransaction and
 * commitTransaction, the previous state of the store is retained until the
 * moment of commitment.
 * <p>
 * It would also be possible to add some journaling/version mechanism, where
 * snapshots of the allocation maps are retained for sometime. For a store which
 * was only added to this would not be an unreasonable overhead and would
 * support the rolling back of the database weekly or monthly if required.
 * <p>
 * The input/output mechanism uses ByteArray Input and Output Streams.
 * <p>
 * One difference between the disk realloc and in memory realloc is that the
 * disk realloc will always return a new address and mark the old address as
 * ready to be freed.
 * <p>
 * The method of storing the allocation headers has been changed from always
 * allocating at the end of the file (and moving them on fle extend) to
 * allocation of fixed areas.  The meta-allocation data, containing the bitmap
 * that controls these allocations, is itself stored in the heap, and is now
 * structured to include both the bit data and the list of meta-storage
 * addresses.
 * <p>
 * Sizing:
 * 256 allocators would reference approximately 2M objects/allocations.  At 1K
 * per allocator this would require 250K of store.  The meta-allocation data
 * would therefore need a start address plus 32 bytes (or 8 ints) to represent
 * the meta-allocation bits.  An array of such data referencing sequentially
 * allocated storage areas completes the meta-allocation requirements.
 * <p>
 * A meta-allocation address can therefore be represented as a single bit offset
 * from which the block, providing start address, and bit offset can be
 * directly determined.
 * <p>
 * The m_metaBits int array used to be fully used as allocation bits, but
 * now stores both the start address plus the 8 ints used to manage that data
 * block.
 * <p>
 * Allocation is reduced to sets of allocator objects which have a start address
 * and a bitmap of allocated storage maps.
 * <p>
 * Searching thousands of allocation blocks to find storage is not efficient,
 * but by utilizing roving pointers and sorting blocks with free space available
 * this can be made most efficient.
 * <p>
 * In order to provide optimum use of bitmaps, this implementation will NOT use
 * the BitSet class.
 * <p>
 * Using the meta-allocation bits, it is straightforward to load ALL the
 * allocation headers. A total of (say) 100 allocation headers might provide
 * up to 4000 allocations each -> 400 000 objects, while 1000 headers -> 4m
 * objects and 2000 -> 8m objects.
 * <p>
 * The allocators are split into a set of FixedAllocators and then
 * BlobAllocation. The FixedAllocators will allocate from 128 to 32K objects,
 * with a minimum block allocation of 64K, and a minimum bit number per block of
 * 32.
 * <p>
 * Where possible lists and roving pointers will be used to minimise searching
 * of the potentially large structures.
 * <p>
 * Since the memory is allocated on (at least) a 128 byte boundary, there is
 * some leeway on storing the address. Added to the address is the shift
 * required to make to the "standard" 128 byte block, e.g. blocksize = 128 <<
 * (addr % 8)
 * <p>
 * NB Useful method on RandomAccessFile.setLength(newLength)
 * <p>
 * When session data is preserved two things must happen - the allocators must
 * not reallocate data that has been freed in this session, or more clearly can
 * only free data that has been allocated in this session. That should be it.
 * <p>
 * The ALLOC_SIZES table is the fibonacci sequence. We multiply by 64 bytes to
 * get actual allocation block sizes. We then allocate bits based on 8K
 * allocation rounding and 32 bits at a time allocation. Note that 4181 * 64 =
 * 267,584 and 256K is 262,144
 * <p>
 * All data is checksummed, both allocated/saved data and the allocation blocks.
 * <p>
 * BLOB allocation is not handled using chained data buffers but with a blob
 * header record.  This is indicated with a BlobAllocator that provides indexed
 * offsets to the header record (the address encodes the BlobAllocator and the
 * offset to the address). The header record stores the number of component
 * allocations and the address of each.
 * <p>
 * This approach makes for much more efficient freeing/re-allocation of Blob
 * storage, in particular avoiding the need to read in the component blocks
 * to determine chained blocks for freeing.  This is particularly important
 * for larger stores where a disk cache could be flushed through simply freeing
 * BLOB allocations.
 * <h2>
 * Deferred Free List
 * </h2>
 * <p>
 * The previous implementation has been amended to associate a single set of
 * deferredFree blocks with each CommitRecord.  The CommitRecordIndex will
 * then provide access to the CommitRecords to support the deferred freeing
 * of allocations based on age/earliestTxReleaseTime.
 * <p>
 * The last release time processed is held with the MetaAllocation data
 * 
 * @author Martyn Cutcher
 */

public class RWStore implements IStore {

    private static final transient Logger log = Logger.getLogger(RWStore.class);

	/**
	 * The sizes of the slots managed by a {@link FixedAllocator} are 64 times
	 * the values in this array.
	 * 
	 * @todo good to have 4k and 8k boundaries for better efficiency on SSD.
	 * 		 NB A 1K boundary is % 16, so 4K % 64
	 *       - can still use fibonacci base, but from 4K start
	 * 
	 * @todo This array should be configurable and must be written into the
	 *       store so changing values does not break older stores.
	 *       com.bigdata.rwstore.RWStore.allocSizes=1,2,3,5... 
	 *       
	 */
	private static final int[] DEFAULT_ALLOC_SIZES = { 1, 2, 3, 5, 8, 12, 16, 32, 48, 64, 128, 192, 320, 512, 832, 1344, 2176, 3520 };
	// private static final int[] DEFAULT_ALLOC_SIZES = { 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181 };
	// private static final int[] ALLOC_SIZES = { 1, 2, 4, 8, 16, 32, 64, 128 };

	final int m_maxFixedAlloc;
	final int m_minFixedAlloc;
	/**
	 * The fixed size of any allocator on the disk in bytes. The #of allocations
	 * managed by an allocator is this value times 8 because each slot uses one
	 * bit in the allocator. When an allocator is allocated, the space on the
	 * persistent heap is reserved for all slots managed by that allocator.
	 * However, the {@link FixedAllocator} only incrementally allocates the
	 * {@link AllocBlock}s.
	 */
	static final int ALLOC_BLOCK_SIZE = 1024;
	
	// from 32 bits, need 13 to hold max offset of 8 * 1024, leaving 19 for number of blocks: 256K
	static final int BLOCK_INDEX_BITS = 19;
	static final int OFFSET_BITS = 13;
	static final int OFFSET_BITS_MASK = 0x1FFF; // was 0xFFFF
	
	static final int ALLOCATION_SCALEUP = 16; // multiplier to convert allocations based on minimum allocation of 32k
	static final int META_ALLOCATION = 8; // 8 * 32K is size of meta Allocation

	static final int BLOB_FIXED_ALLOCS = 1024;
//	private ICommitCallback m_commitCallback;
//
//	public void setCommitCallback(final ICommitCallback callback) {
//		m_commitCallback = callback;
//	}

	// ///////////////////////////////////////////////////////////////////////////////////////
	// RWStore Data
	// ///////////////////////////////////////////////////////////////////////////////////////

	private final File m_fd;
	private RandomAccessFile m_raf;
//	protected FileMetadata m_metadata;
//	protected int m_transactionCount;
//	private boolean m_committing;

	private boolean m_preserveSession = true;
//	private boolean m_readOnly;

    /**
     * lists of total alloc blocks.
     * 
     * @todo examine concurrency and lock usage for {@link #m_alloc}, which is
     *       used by {@link #getStats(boolean)}, and the rest of these lists as
     *       well.
     */
	private final ArrayList<Allocator> m_allocs;

	// lists of free alloc blocks
	private ArrayList<FixedAllocator> m_freeFixed[];
	
	// lists of free blob allocators
	private final ArrayList<BlobAllocator> m_freeBlobs;

	// lists of blocks requiring commitment
	private final ArrayList<Allocator> m_commitList;

	private WriteBlock m_writes;
	private final Quorum<?,?> m_quorum;
	private final RWWriteCacheService m_writeCache;

	private int[] m_allocSizes;
	
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
	 * An explicit allocation lock allows for reads concurrent with allocation
	 * requests. You must hold the allocation lock while allocating or clearing
	 * allocations. It is only when an allocation triggers a file extension that
	 * the write extensionLock needs to be taken.
	 * 
	 * TODO: There is scope to take advantage of the different allocator sizes
	 * and provide allocation locks on the fixed allocators. We will still need
	 * a store-wide allocation lock when creating new allocation areas, but
	 * significant contention may be avoided.
	 */
    final private ReentrantLock m_allocationLock = new ReentrantLock();

	/**
	 * The deferredFreeList is simply an array of releaseTime,freeListAddrs
	 * stored at commit.
	 * <p> 
	 * Note that when the deferredFreeList is saved, ONLY thefreeListAddrs
	 * are stored, NOT the releaseTime.  This is because on any open of
	 * the store, all deferredFrees can be released immediately. This
	 * mechanism may be changed in the future to enable explicit history
	 * retention, but if so a different header structure would be used since
	 * it would not be appropriate to retain a simple header linked to
	 * thousands if not millions of commit points.
	 */
//    * 
//    * If the current txn list exceeds the MAX_DEFERRED_FREE then it is
//    * incrementally saved and a new list begun.  The master list itself
//    * serves as a BLOB header when there is more than a single entry with
//    * the same txReleaseTime.
//	private static final int MAX_DEFERRED_FREE = 4094; // fits in 16k block
	private volatile long m_lastDeferredReleaseTime = 0L;
//	private final ArrayList<Integer> m_currentTxnFreeList = new ArrayList<Integer>();
	private final PSOutputStream m_deferredFreeOut;

	private ReopenFileChannel m_reopener = null;

	private BufferedWrite m_bufferedWrite;
	
	class WriteCacheImpl extends WriteCache.FileChannelScatteredWriteCache {
        public WriteCacheImpl(final ByteBuffer buf,
                final boolean useChecksum,
                final boolean bufferHasData,
                final IReopenChannel<FileChannel> opener)
                throws InterruptedException {

            super(buf, useChecksum, m_quorum!=null&&m_quorum
                    .isHighlyAvailable(), bufferHasData, opener, m_bufferedWrite);

        }

        @Override
        protected boolean writeOnChannel(final ByteBuffer data,
                final long firstOffsetignored,
                final Map<Long, RecordMetadata> recordMap,
                final long nanos) throws InterruptedException, IOException {
            
            final Lock readLock = m_extensionLock.readLock();
            readLock.lock();
            try {
                return super.writeOnChannel(data, firstOffsetignored,
                        recordMap, nanos);
            } finally {
                readLock.unlock();
            }

        }
        
        // Added to enable debug of rare problem
        // FIXME: disable by removal once solved
        protected void registerWriteStatus(long offset, int length, char action) {
    		m_writeCache.debugAddrs(offset, length, action);
        }
		
	};
	
//	private String m_filename;

	private final FileMetadataView m_fmv;

	private IRootBlockView m_rb;

//	volatile private long m_commitCounter;

	volatile private int m_metaBitsAddr;

    /**
     * The ALLOC_SIZES must be initialized from either the file or the
     * properties associated with the fileMetadataView
     * 
     * @param fileMetadataView
     * @param readOnly
     * @param quorum
     * @throws InterruptedException
     */

    public RWStore(final FileMetadataView fileMetadataView,
            final boolean readOnly, final Quorum<?, ?> quorum) {

		m_metaBitsSize = cDefaultMetaBitsSize;

		m_metaBits = new int[m_metaBitsSize];
		m_metaTransientBits = new int[m_metaBitsSize];

		m_maxFileSize = 2 * 1024 * 1024; // 1gb max (mult by 128)!!
		
        m_quorum = quorum;

		m_fmv = fileMetadataView;
		
		m_fd = m_fmv.getFile();
		m_raf = m_fmv.getRandomAccessFile();

		m_rb = m_fmv.getRootBlock();

//		m_filename = m_fd.getAbsolutePath();

		m_commitList = new ArrayList<Allocator>();

		m_allocs = new ArrayList<Allocator>();
		
		m_freeBlobs = new ArrayList<BlobAllocator>();

		try {
			m_reopener = new ReopenFileChannel(m_fd, m_raf, "rw");
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}
		
		try {
			m_bufferedWrite = new BufferedWrite(this);
		} catch (InterruptedException e1) {
			m_bufferedWrite = null;
		}

		final int buffers = m_fmv.getFileMetadata().writeCacheBufferCount;
		
		if(log.isInfoEnabled())
		    log.info("RWStore using writeCacheService with buffers: " + buffers);

        try {
            m_writeCache = new RWWriteCacheService(buffers, m_raf.length(),
                    m_reopener, m_quorum) {
				
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
			throw new IllegalStateException("Unable to create write cache service", e);
		} catch (IOException e) {
			throw new IllegalStateException("Unable to create write cache service", e);
		}
		

		try {
			if (m_rb.getNextOffset() == 0) { // if zero then new file
				String buckets = m_fmv.getProperty(Options.RW_ALLOCATIONS, null);
				if (buckets == null) {				
					m_allocSizes = DEFAULT_ALLOC_SIZES;
				} else {
					String[] specs = buckets.split(",");
					m_allocSizes = new int[specs.length];
					int prevSize = 0;
					for (int i = 0; i < specs.length; i++) {
						int nxtSize = Integer.parseInt(specs[i]);
						if (nxtSize <= prevSize)
							throw new IllegalArgumentException("Invalid AllocSizes property");
						m_allocSizes[i] = nxtSize;
						prevSize = nxtSize;
					}
				}

				final int numFixed = m_allocSizes.length;

				m_freeFixed = new ArrayList[numFixed];

				for (int i = 0; i < numFixed; i++) {
					m_freeFixed[i] = new ArrayList<FixedAllocator>();
				}

				m_fileSize = convertFromAddr(m_fd.length());
				
				// make space for meta-allocators
				m_metaBits[0] = -1;
				m_metaTransientBits[0] = -1;
				m_nextAllocation = -(1 + META_ALLOCATION); // keep on a minimum 8K boundary
				
				if (m_fileSize > m_nextAllocation) {
					m_fileSize = m_nextAllocation;
				}
				
				m_raf.setLength(convertAddr(m_fileSize));

				m_maxFixedAlloc = m_allocSizes[m_allocSizes.length-1]*64;
				m_minFixedAlloc = m_allocSizes[0]*64;

				commitChanges(null);
				
			} else {
				
				initfromRootBlock();
				
				m_maxFixedAlloc = m_allocSizes[m_allocSizes.length-1]*64;
				m_minFixedAlloc = m_allocSizes[0]*64;
			}
			
			assert m_maxFixedAlloc > 0;
			
			m_deferredFreeOut = PSOutputStream.getNew(this, m_maxFixedAlloc, null);
		} catch (IOException e) {
			throw new StorageTerminalError("Unable to initialize store", e);
		}
	}

    synchronized public void close() {
        try {
            if (m_bufferedWrite != null) {
                m_bufferedWrite.release();
                m_bufferedWrite = null;
            }
            m_writeCache.close();
            m_raf.close();
        } catch (Throwable t) {
            throw new RuntimeException(t);
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
            /*
             * possible when rolling back to empty file.
             */
			log.warn("No meta allocation data included in root block for RWStore");
		}
		
		if (log.isTraceEnabled()) {
            final int commitRecordAddr = (int) (rbv.getCommitRecordAddr() >> 32);
            log.trace("CommitRecord " + rbv.getCommitRecordAddr()
                    + " at physical address: "
                    + physicalAddress(commitRecordAddr));
        }
		
		final long commitCounter = rbv.getCommitCounter();

//		final int metaStartAddr = (int) -(metaAddr >> 32); // void
//		final int fileSize = (int) -(metaAddr & 0xFFFFFFFF);

		if (log.isTraceEnabled())
			log.trace("m_allocation: " + nxtalloc + ", m_metaBitsAddr: "
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
		// m_rb = m_fmv.getRootBlock();
		assert(m_rb != null);

//		m_commitCounter = m_rb.getCommitCounter();

		final long nxtOffset = m_rb.getNextOffset();
		m_nextAllocation = -(int) (nxtOffset >> 32);

		m_metaBitsAddr = -(int) nxtOffset;
		
		if (log.isInfoEnabled()) {
			log.info("MetaBitsAddr: " + m_metaBitsAddr);
		}

		final long metaAddr = m_rb.getMetaStartAddr();
		m_fileSize = (int) -(metaAddr & 0xFFFFFFFF);

		long rawmbaddr = m_rb.getMetaBitsAddr();
		
        /*
         * Take bottom 16 bits (even 1K of metabits is more than sufficient)
         */
		final int metaBitsStore = (int) (rawmbaddr & 0xFFFF);
		
		if (metaBitsStore > 0) {
			rawmbaddr >>= 16;
	
			// RWStore now restore metabits
			final byte[] buf = new byte[metaBitsStore * 4];

			FileChannelUtility.readAll(m_reopener, ByteBuffer.wrap(buf), rawmbaddr);
	
			final DataInputStream strBuf = new DataInputStream(new ByteArrayInputStream(buf));
			
			m_lastDeferredReleaseTime = strBuf.readLong();
			
			final int allocBlocks = strBuf.readInt();
			m_allocSizes = new int[allocBlocks];
			for (int i = 0; i < allocBlocks; i++) {
				m_allocSizes[i] = strBuf.readInt();
			}
			m_metaBitsSize = metaBitsStore - allocBlocks - 3; // allow for deferred free
			m_metaBits = new int[m_metaBitsSize];
			if (log.isInfoEnabled()) {
				log.info("Raw MetaBitsAddr: " + rawmbaddr);
			}
			for (int i = 0; i < m_metaBitsSize; i++) {
				m_metaBits[i] = strBuf.readInt();
			}
			m_metaTransientBits = (int[]) m_metaBits.clone();
	
			final int numFixed = m_allocSizes.length;

			m_freeFixed = new ArrayList[numFixed];

			for (int i = 0; i < numFixed; i++) {
				m_freeFixed[i] = new ArrayList<FixedAllocator>();
			}

			checkCoreAllocations();
	
			readAllocationBlocks();
			
			// clearOutstandingDeferrels(deferredFreeListAddr, deferredFreeListEntries);

			if (log.isTraceEnabled()) {
				final StringBuilder str = new StringBuilder();
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

//	/*
//	 * Called when store is opened to make sure any deferred frees are
//	 * cleared.
//	 * 
//	 * Stored persistently is only the list of addresses of blocks to be freed,
//	 * the knowledge of the txn release time does not need to be held persistently,
//	 * this is only relevant for transient state while the RWStore is open.
//	 * 
//	 * The deferredCount is the number of entries - integer address and integer
//	 * count at each address
//	 */
//	private void clearOutstandingDeferrels(final int deferredAddr, final int deferredCount) {
//		if (deferredAddr != 0) {
//			assert deferredCount != 0;
//			final int sze = deferredCount * 8 + 4; // include space for checksum
//			
//			if (log.isDebugEnabled())
//				log.debug("Clearing Outstanding Deferrals: " + deferredCount);
//			
//			byte[] buf = new byte[sze];
//			getData(deferredAddr, buf);
//			
//			final byte[] blockBuf = new byte[8 * 1024]; // maximum size required 
//			
//			ByteBuffer in = ByteBuffer.wrap(buf);
//			for (int i = 0; i < deferredCount; i++) {
//				int blockAddr = in.getInt();
//				int addrCount = in.getInt();
//				
//				// now read in this block and free all addresses referenced
//				getData(blockAddr, blockBuf, 0, addrCount*4 + 4);
//				ByteBuffer inblock = ByteBuffer.wrap(blockBuf);
//				for (int b = 0; b < addrCount; b++) {
//					final int defAddr = inblock.getInt();
//					Allocator alloc = getBlock(defAddr);
//					if (alloc instanceof BlobAllocator) {
//						b++;
//						assert b < addrCount;
//						alloc.free(defAddr, inblock.getInt());
//					} else {
//						alloc.free(defAddr, 0); // size ignored for FreeAllocators
//					}
//				}
//				// once read then free the block allocation
//				free(blockAddr, 0);
//			}
//			
//			// lastly free the deferredAddr
//			free(deferredAddr, 0);			
//		}
//		
//	}

	/*********************************************************************
	 * make sure resource is closed!
	 **/
	protected void finalize() {
		close();
	}

	protected void readAllocationBlocks() throws IOException {
		
		assert m_allocs.size() == 0;
		
        if (log.isInfoEnabled())
            log.info("readAllocationBlocks, m_metaBits.length: "
                    + m_metaBits.length);

		/**
		 * Allocators are sorted in StartAddress order (which MUST be the order
		 * they were created and therefore will correspond to their index) The
		 * comparator also checks for equality, which would indicate an error in
		 * the metaAllocation if two allocation blocks were loaded for the same
		 * address (must be two version of same Allocator).
		 * 
		 * Meta-Allocations stored as {int address; int[8] bits}, so each block
		 * holds 8*32=256 allocation slots of 1K totaling 256K.
		 */
		for (int b = 0; b < m_metaBits.length; b += 9) {
			final long blockStart = convertAddr(m_metaBits[b]);
			final int startBit = (b * 32) + 32;
			final int endBit = startBit + (8*32);
			for (int i = startBit; i < endBit; i++) {
				if (tstBit(m_metaBits, i)) {
					final long addr = blockStart + ((i-startBit) * ALLOC_BLOCK_SIZE);

					final byte buf[] = new byte[ALLOC_BLOCK_SIZE];

					FileChannelUtility.readAll(m_reopener, ByteBuffer.wrap(buf), addr);

					final DataInputStream strBuf = new DataInputStream(new ByteArrayInputStream(buf));

					final int allocSize = strBuf.readInt(); // if Blob < 0
					final Allocator allocator;
					final ArrayList<? extends Allocator> freeList;
					if (allocSize > 0) {
						int index = 0;
						int fixedSize = m_minFixedAlloc;
						while (fixedSize < allocSize)
							fixedSize = 64 * m_allocSizes[++index];

						allocator = new FixedAllocator(this, allocSize, m_writeCache);

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

		if (false) {
			StringBuilder tmp = new StringBuilder();
			showAllocators(tmp);
			
			System.out.println("Allocators: " + tmp.toString());
		}
	}
	
	/**
	 * Called from ContextAllocation when no free FixedAllocator is immediately
	 * available. First the free list will be checked to see if one is
	 * available, otherwise it will be created.  When the calling 
	 * ContextAllocation is released, its allocators will be added to the 
	 * global free lists.
	 * 
	 * @param block - the index of the Fixed size allocation
	 * @return the FixedAllocator
	 */
	private FixedAllocator establishFreeFixedAllocator(final int block) {
		
	    final ArrayList<FixedAllocator> list = m_freeFixed[block];

		if (list.size() == 0) {
			
		    final int allocSize = 64 * m_allocSizes[block];
	
            final FixedAllocator allocator = new FixedAllocator(this,
                    allocSize, m_writeCache);

			allocator.setIndex(m_allocs.size());
			
			m_allocs.add(allocator);
			
			return allocator;
		} else {
			return list.remove(0);
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
		final long maxSize = m_maxFileSize;
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
	public void getData(final long addr, final byte buf[]) {
		getData(addr, buf, 0, buf.length);
	}
	
    public void getData(final long addr, final byte buf[], final int offset,
            final int length) {
        
        if (addr == 0) {
			return;
		}

        final Lock readLock = m_extensionLock.readLock();

        readLock.lock();
        
		try {
			// length includes space for the checksum
			if (length > m_maxFixedAlloc) {
				try {
				    final int alloc = m_maxFixedAlloc-4;
					final int nblocks = (alloc - 1 + (length-4))/alloc;
                    if (nblocks < 0)
                        throw new IllegalStateException(
                                "Allocation error, m_maxFixedAlloc: "
                                        + m_maxFixedAlloc);

                    final byte[] hdrbuf = new byte[4 * (nblocks + 1) + 4]; // plus 4 bytes for checksum
					final BlobAllocator ba = (BlobAllocator) getBlock((int) addr);
					getData(ba.getBlobHdrAddress(getOffset((int) addr)), hdrbuf); // read in header 
					final DataInputStream hdrstr = new DataInputStream(new ByteArrayInputStream(hdrbuf));
					final int rhdrs = hdrstr.readInt();
                    if (rhdrs != nblocks) {
                        throw new IllegalStateException(
                                "Incompatible BLOB header record, expected: "
                                        + nblocks + ", got: " + rhdrs);
                    }
					final int[] blobHdr = new int[nblocks];
					for (int i = 0; i < nblocks; i++) {
						blobHdr[i] = hdrstr.readInt();
					}
					// Now we have the header addresses, we can read MAX_FIXED_ALLOCS until final buffer
					int cursor = 0;
					int rdlen = m_maxFixedAlloc;
					for (int i = 0; i < nblocks; i++) {
						if (i == (nblocks - 1)) {
							rdlen = length - cursor;
						}
						getData(blobHdr[i], buf, cursor, rdlen); // include space for checksum
						cursor += rdlen-4; // but only increase cursor by data
					}
					
					return;
					
				} catch (IOException e) {
					log.error(e,e);
					
					throw new IllegalStateException("Unable to restore Blob allocation", e);
				}
			}

			try {
				final long paddr = physicalAddress((int) addr);
				if (paddr == 0) {
					assertAllocators();
					
					log.warn("Address " + addr + " did not resolve to physical address");
					
					throw new IllegalArgumentException("Address " + addr + " did not resolve to physical address");
				}

				/**
				 * Check WriteCache first
				 * 
				 * Note that the buffer passed in should include the checksum 
				 * value, so the cached data is 4 bytes less than the
				 * buffer size.
				 */
				final ByteBuffer bbuf;
				try {
					bbuf = m_writeCache.read(paddr);
				} catch (Throwable t) {
                    throw new IllegalStateException(
                            "Error reading from WriteCache addr: " + paddr
                                    + " length: " + (length - 4)
                                    + ", writeCacheDebug: "
                                    + m_writeCache.addrDebugInfo(paddr), t);
				}
				if (bbuf != null) {
					final byte[] in = bbuf.array(); // reads in with checksum - no need to check if in cache
					if (in.length != length-4) {
						assertAllocators();
                        throw new IllegalStateException(
                                "Incompatible buffer size for addr: " + paddr
                                        + ", " + in.length + " != "
                                        + (length - 4) + " writeCacheDebug: "
                                        + m_writeCache.addrDebugInfo(paddr));
					}
					for (int i = 0; i < length-4; i++) {
						buf[offset+i] = in[i];
					}
					m_cacheReads++;
				} else {
					// If checksum is required then the buffer should be sized to include checksum in final 4 bytes
				    final ByteBuffer bb = ByteBuffer.wrap(buf, offset, length);
					FileChannelUtility.readAll(m_reopener, bb, paddr);
					final int chk = ChecksumUtility.getCHK().checksum(buf, offset, length-4); // read checksum
					final int tstchk = bb.getInt(offset + length-4);
					if (chk != tstchk) {
						assertAllocators();
						
						final String cacheDebugInfo = m_writeCache.addrDebugInfo(paddr);
						log.warn("Invalid data checksum for addr: " + paddr 
								+ ", chk: " + chk + ", tstchk: " + tstchk + ", length: " + length
								+ ", first bytes: " + toHexString(buf, 32) + ", successful reads: " + m_diskReads
								+ ", at last extend: " + m_readsAtExtend + ", cacheReads: " + m_cacheReads
								+ ", writeCacheDebug: " + cacheDebugInfo);
						
                        throw new IllegalStateException(
                                "Invalid data checksum from address: " + paddr
                                        + ", size: " + (length - 4));
					}
					
					m_diskReads++;
				}
			} catch (Throwable e) {
				log.error(e,e);
				
				throw new IllegalArgumentException("Unable to read data", e);
			}
		} finally {
			readLock.unlock();
		}
	}

	private void assertAllocators() {
		for (int i = 0; i < m_allocs.size(); i++) {
			if (m_allocs.get(i).getIndex() != i) {
				throw new IllegalStateException("Allocator at invalid index: " + i + ", index  stored as: "
						+ m_allocs.get(i).getIndex());
			}
		}
	}

	static private final char[] HEX_CHAR_TABLE = {
		   '0', '1','2','3',
		   '4','5','6','7',
		   '8','9','a','b',
		   'c','d','e','f'
		  };    

	// utility to display byte array of maximum i bytes as hexString
	static private String toHexString(final byte[] buf, int n) {
		n = n < buf.length ? n : buf.length;
		final StringBuffer out = new StringBuffer();
		for (int i = 0; i < n; i++) {
			final int v = buf[i] & 0xFF;
			out.append(HEX_CHAR_TABLE[v >>> 4]);
			out.append(HEX_CHAR_TABLE[v &0xF]);
		}
		return out.toString();
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
	public long getPreviousAddress(final long laddr) {
		return 0;
	}

	public void free(final long laddr, final int sze) {
		free(laddr, sze, null); // call with null AlocationContext
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
	public void free(final long laddr, final int sze, final IAllocationContext context) {
//	    if (true) return;
		final int addr = (int) laddr;

		switch (addr) {
		case 0:
		case -1:
		case -2:
			return;
		}
		m_allocationLock.lock();
		try {
			final Allocator alloc = getBlockByAddress(addr);
			/*
			 * There are a few conditions here.  If the context owns the
			 * allocator and the allocation was made by this context then
			 * it can be freed immediately.
			 * The problem comes when the context is null and the allocator
			 * is NOT owned, BUT there are active AllocationContexts, in this
			 * situation, the free must ALWAYS be deferred.
			 */
			final boolean alwaysDefer = context == null && m_contexts.size() > 0;
			if (alwaysDefer)
				if (log.isDebugEnabled())
					log.debug("Should defer " + physicalAddress(addr));
			if (/*alwaysDefer ||*/ !alloc.canImmediatelyFree(addr, sze, context)) {
				deferFree(addr, sze);
			} else {
				immediateFree(addr, sze);
			}
		} finally {
			m_allocationLock.unlock();
		}
		
	}

//	private long immediateFreeCount = 0;
	private void immediateFree(final int addr, final int sze) {
		
		switch (addr) {
		case 0:
		case -1:
		case -2:
			return;
		}

		m_allocationLock.lock();
		try {
			final Allocator alloc = getBlockByAddress(addr);
			final int addrOffset = getOffset(addr);
			final long pa = alloc.getPhysicalAddress(addrOffset);
			alloc.free(addr, sze);
			// must clear after free in case is a blobHdr that requires reading!
			// the allocation lock protects against a concurrent re-allocation
			// of the address before the cache has been cleared
			assert pa != 0;
			m_writeCache.clearWrite(pa);
			m_frees++;
			if (alloc.isAllocated(addrOffset))
				throw new IllegalStateException("Reallocation problem with WriteCache");

			if (!m_commitList.contains(alloc)) {
				m_commitList.add(alloc);
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
	 * The use of a IAllocationContext adds some complexity to the previous
	 * simple freelist management.  The problem is two-fold.
	 * 
	 * Firstly it is okay for an Allocator on the free list to return a null
	 * address, since it may be managing  storage for a specific context.
	 * 
	 * Secondly we must try and ensure that Allocators used by a specific
	 * context can be found again.  For example, if allocator#1 is assigned to
	 * context#1 and allocator#2 to context#2, when context#1 is detached we
	 * want context#2 to first find allocator#2.  This is further complicated
	 * by the finer granularity of the AllocBlocks within a FixedAllocator.
	 */

//	private volatile long m_maxAllocation = 0;
	private volatile long m_spareAllocation = 0;
	
	public int alloc(final int size, final IAllocationContext context) {
		if (size > m_maxFixedAlloc) {
			throw new IllegalArgumentException("Allocation size to big: " + size);
		}
		
		m_allocationLock.lock();
		try {
			try {
				final Allocator allocator;
				final int i = fixedAllocatorIndex(size);
				if (context != null) {
					allocator = establishContextAllocation(context).getFreeFixed(i);
				} else {
					final int block = 64 * m_allocSizes[i];
					m_spareAllocation += (block - size); // Isn't adjusted by frees!
					
					final ArrayList<FixedAllocator> list = m_freeFixed[i];
					if (list.size() == 0) {

						allocator = new FixedAllocator(this, block, m_writeCache);
						allocator.setFreeList(list);
						allocator.setIndex(m_allocs.size());

						if (log.isTraceEnabled())
							log.trace("New FixedAllocator for " + block);

						m_allocs.add(allocator);
					} else {
						// Verify free list only has allocators with free bits
						if (log.isDebugEnabled()){
							int tsti = 0;
							final Iterator<FixedAllocator> allocs = list.iterator();
							while (allocs.hasNext()) {
								final Allocator tstAlloc = allocs.next();
								if (!tstAlloc.hasFree()) {
									throw new IllegalStateException("Free list contains full allocator, " + tsti + " of " + list.size());
								}
								tsti++;
							}
						}
						allocator = (Allocator) list.get(0);
					}
					
				}
				
				final int addr = allocator.alloc(this, size, context);

				if (!m_commitList.contains(allocator)) {
					m_commitList.add(allocator);
				}

				m_recentAlloc = true;

				final long pa = physicalAddress(addr);
                if (pa == 0L) {
                    throw new IllegalStateException(
                            "No physical address found for " + addr);
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
	
	int fixedAllocatorIndex(final int size) {
		int i = 0;

		int cmp = m_minFixedAlloc;
		while (size > cmp) {
			i++;
			cmp = 64 * m_allocSizes[i];
		}
		
		return i;
	}

	/****************************************************************************
	 * The base realloc method that returns a stream for writing to rather than
	 * handle the reallocation immediately.
	 **/
	public PSOutputStream realloc(final long oldAddr, final int size) {
		free(oldAddr, size);

		return PSOutputStream.getNew(this, m_maxFixedAlloc, null);
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
	public long alloc(final byte buf[], final int size, final IAllocationContext context) {
		if (size > (m_maxFixedAlloc-4)) {
			if (size > (BLOB_FIXED_ALLOCS * (m_maxFixedAlloc-4)))
				throw new IllegalArgumentException("Allocation request beyond maximum BLOB");
			
			if (log.isTraceEnabled())
				log.trace("BLOB ALLOC: " + size);

			final PSOutputStream psout = PSOutputStream.getNew(this, m_maxFixedAlloc, context);
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

		final int newAddr = alloc(size + 4, context); // allow size for checksum

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

    /**
     * Toss away all buffered writes and then reload from the current root
     * block.
     */
	public void reset() {
		if (log.isInfoEnabled()) {
			log.info("RWStore Reset");
		}
	    m_allocationLock.lock();
		try {
			m_commitList.clear();
			m_allocs.clear();
			m_freeBlobs.clear();
			
			final int numFixed = m_allocSizes.length;
			for (int i = 0; i < numFixed; i++) {
				m_freeFixed[i].clear();
			}


			try {
				m_writeCache.reset();
			} catch (InterruptedException e) {
			    throw new RuntimeException(e);
			}
	        			
			initfromRootBlock();

			// notify of current file length.
			m_writeCache.setExtent(convertAddr(m_fileSize));
		} catch (Exception e) {
			throw new IllegalStateException("Unable reset the store", e);
		} finally {
		    m_allocationLock.unlock();
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
		// the metabits is now prefixed by a long specifying the lastTxReleaseTime
		// used to free the deferedFree allocations.  This is used to determine
		//	which commitRecord to access to process the nextbatch of deferred
		//	frees.
	    final int len = 4 * (2 + 1 + m_allocSizes.length + m_metaBits.length);
		final byte buf[] = new byte[len];

        final FixedOutputStream str = new FixedOutputStream(buf);
        try {
            str.writeLong(m_lastDeferredReleaseTime);

            str.writeInt(m_allocSizes.length);
            for (int i = 0; i < m_allocSizes.length; i++) {
                str.writeInt(m_allocSizes[i]);
            }
            for (int i = 0; i < m_metaBits.length; i++) {
                str.writeInt(m_metaBits[i]);
            }

            str.flush();
        } finally {
            str.close();
        }

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

        m_rb = m_fmv.newRootBlockView(//
                !m_rb.isRootBlock0(), //
                m_rb.getOffsetBits(), //
                getNextOffset(), //
                m_rb.getFirstCommitTime(),//
                m_rb.getLastCommitTime(), //
                m_rb.getCommitCounter(), //
                m_rb.getCommitRecordAddr(),//
                m_rb.getCommitRecordIndexAddr(), //
                getMetaStartAddr(),//
                getMetaBitsAddr(), //
                m_rb.getLastCommitTime()//
                );

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

	public void commitChanges(final Journal journal) {
		checkCoreAllocations();

		// take allocation lock to prevent other threads allocating during commit
		m_allocationLock.lock();
		
		try {
		
			checkDeferredFrees(true, journal); // free now if possible

			// Allocate storage for metaBits
			final long oldMetaBits = m_metaBitsAddr;
			final int oldMetaBitsSize = (m_metaBits.length + m_allocSizes.length + 1) * 4;
			m_metaBitsAddr = alloc(getRequiredMetaBitsStorage(), null);

			// DEBUG SANITY CHECK!
			if (physicalAddress(m_metaBitsAddr) == 0) {
				throw new IllegalStateException("Returned MetaBits Address not valid!");
			}

			// Call immediateFree - no need to defer freeof metaBits, this
			//	has to stop somewhere!
			immediateFree((int) oldMetaBits, oldMetaBitsSize);

			// save allocation headers
			final Iterator<Allocator> iter = m_commitList.iterator();
			while (iter.hasNext()) {
				final Allocator allocator = iter.next();
				final int old = allocator.getDiskAddr();
				metaFree(old);
				
				final int naddr = metaAlloc();
				allocator.setDiskAddr(naddr);
				
                if (log.isTraceEnabled())
                    log.trace("Update allocator " + allocator.getIndex()
                            + ", old addr: " + old + ", new addr: " + naddr);

				try {
				    // do not use checksum
				    m_writeCache.write(metaBit2Addr(naddr), ByteBuffer
                        .wrap(allocator.write()), 0, false);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
			m_commitList.clear();

			writeMetaBits();

			try {
				m_writeCache.flush(true);
			} catch (InterruptedException e) {
			    log.error(e, e);
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
            try {
                // m_committing = false;
                m_recentAlloc = false;
            } finally {
                m_allocationLock.unlock();
            }
		}

		checkCoreAllocations();

        if (log.isTraceEnabled())
            log.trace("commitChanges for: " + m_nextAllocation + ", "
                    + m_metaBitsAddr + ", active contexts: "
                    + m_contexts.size());
    }

    /**
     * Called prior to commit, so check whether storage can be freed and then
     * whether the deferredheader needs to be saved.
     * <p>
     * Note: The caller MUST be holding the {@link #m_allocationLock}.
     * <p>
     * Note: This method is package private in order to expose it to the unit
     * tests.
     */
    /* public */void checkDeferredFrees(final boolean freeNow,
            final Journal journal) {

        // Note: Invoked from unit test w/o the lock...
//        if (!m_allocationLock.isHeldByCurrentThread())
//            throw new IllegalMonitorStateException();
		
        if (journal != null) {
		
            final JournalTransactionService transactionService = (JournalTransactionService) journal
                    .getLocalTransactionManager().getTransactionService();

            // the previous commit point.
            long latestReleasableTime = journal.getLastCommitTime(); 

            if (latestReleasableTime == 0L) {
                // Nothing committed.
                return;
            }
            
            // subtract out the retention period.
            latestReleasableTime -= transactionService.getMinReleaseAge();

            // add one to give this inclusive upper bound semantics.
            latestReleasableTime++;

            /*
             * Free deferrals.
             * 
             * Note: This adds one to the lastDeferredReleaseTime to give
             * exclusive lower bound semantics.
             */
            freeDeferrals(journal, m_lastDeferredReleaseTime+1,
                    latestReleasableTime);
            
        }
        
    }

    /**
     * 
     * @return conservative requirement for metabits storage, mindful that the
     *         request to allocate the metabits may require an increase in the
     *         number of allocation blocks and therefore an extension to the
     *         number of metabits.
     */
	private int getRequiredMetaBitsStorage() {
		int ints = 1 + m_allocSizes.length; // length prefixed alloc sizes
		ints += m_metaBits.length;
		
		// need to handle number of modified blocks
		final int commitInts = ((32 + m_commitList.size()) / 32);
		final int allocBlocks = (8 + commitInts)/8;
		ints += 9 * allocBlocks;
		
		ints += 2; // for deferredFreeListAddr and size
		
		return ints*4; // return as bytes
	}

	// Header Data
//	volatile private long m_curHdrAddr = 0;
//	volatile private int m_rootAddr;

	volatile private int m_fileSize;
	volatile private int m_nextAllocation;
	volatile private int m_maxFileSize;

//	private int m_headerSize = 2048;

	// Meta Allocator
	private static int cDefaultMetaBitsSize = 9; // DEBUG FIX ME
	private int m_metaBits[];
	volatile private int m_metaBitsSize = cDefaultMetaBitsSize;
	private int m_metaTransientBits[];
	// volatile private int m_metaStartAddr;

	volatile private boolean m_recentAlloc = false;

	/**
	 * Return the address of a contiguous region on the persistent heap.
	 * 
	 * @param size
	 *            The size of that region (this is not bytes, but something a
	 *            bit more complicated).
	 */
	protected int allocBlock(final int size) {
		// minimum 1
		if (size <= 0) {
			throw new Error("allocBlock called with zero size request");
		}

		final int allocAddr = m_nextAllocation;
		m_nextAllocation -= size;

		while (convertAddr(m_nextAllocation) >= convertAddr(m_fileSize)) {
			extendFile();
		}

		checkCoreAllocations();

		if (log.isTraceEnabled())
			log.trace("allocation created at " + convertAddr(allocAddr) + " for " + convertAddr(-size));

		return allocAddr;
	}

	private void checkCoreAllocations() {
		final long lfileSize = convertAddr(m_fileSize);
		final long lnextAlloc = convertAddr(m_nextAllocation);

		if (lnextAlloc >= lfileSize) {
			throw new IllegalStateException("Core Allocation Error - file size: " 
					+ lfileSize + ", nextAlloc: " + lnextAlloc);
		}
	}

	/**
	 * meta allocation/free
	 * 
	 * Allocates persistent store for allocation blocks.
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
//		long lnextAlloc = convertAddr(m_nextAllocation);

		int bit = fndMetabit();

		if (bit < 0) {
			// reallocate metaBits and recalculate m_headerSize
			// extend m_metaBits by 8 ints of bits plus start address!
		    final int nsize = m_metaBits.length + cDefaultMetaBitsSize;

			// arrays initialized to zero by default
			final int[] nbits = new int[nsize];
			final int[] ntransients = new int[nsize];

			// copy existing values
			for (int i = 0; i < m_metaBits.length; i++) {
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
		final int blocks = m_metaBits.length/9;
		for (int b = 0; b < blocks; b++) {
			final int ret = fndBit(m_metaTransientBits, (b*9)+1, 8);
			if (ret != -1) {
				return ret;
			}
		}
		
		return -1; // none found
	}
	
	void metaFree(final int bit) {
		
		if (!m_allocationLock.isHeldByCurrentThread()) {
			/*
			 * Must hold the allocation lock while allocating or clearing
			 * allocations.
			 */
			throw new IllegalMonitorStateException();
		}
		
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
//		final int bitsPerBlock = 9 * 32;
		
		final int intIndex = bit / 32; // divide 32;
		final int addrIndex = (intIndex/9)*9;
		final long addr = convertAddr(m_metaBits[addrIndex]);

		final int intOffset = bit - ((addrIndex+1) * 32);

		final long ret =  addr + (ALLOC_BLOCK_SIZE * intOffset);
		
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

	public static long convertAddr(final int addr) {
	    final long laddr = addr;
		if (laddr < 0) {
		    final long ret = (-laddr) << ALLOCATION_SCALEUP; 
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

//			final long curSize = convertAddr(m_fileSize);

			m_fileSize += adjust;

			if (getMaxFileSize() < m_fileSize) {
				// whoops!! How to exit more gracefully?
				throw new Error("System greater than maximum size");
			}

			final long toAddr = convertAddr(m_fileSize);
			
			if (log.isInfoEnabled()) log.info("Extending file to: " + toAddr);

			m_raf.setLength(toAddr);
			
			if (log.isInfoEnabled()) log.info("Extend file done");
		} catch (Throwable t) {
			throw new RuntimeException("Force Reopen", t);
		} finally {
            try {
                m_extendingFile = false;
                m_readsAtExtend = this.m_diskReads;
            } finally {
                writeLock.unlock();
            }
		}
	}

	static void setBit(final int[] bits, final int bitnum) {
		final int index = bitnum / 32;
		final int bit = bitnum % 32;

		bits[(int) index] |= 1 << bit;
	}

	static boolean tstBit(final int[] bits, final int bitnum) {
		final int index = bitnum / 32;
		final int bit = bitnum % 32;

		if (index >= bits.length)
			throw new IllegalArgumentException("Accessing bit index: " + index 
					+ " of array length: " + bits.length);

		return (bits[(int) index] & 1 << bit) != 0;
	}

	static void clrBit(final int[] bits, final int bitnum) {
		final int index = bitnum / 32;
		final int bit = bitnum % 32;

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
    private String allocListStats(final List<Allocator> list, final AtomicLong counter) {
		final StringBuffer stats = new StringBuffer();
		final Iterator<Allocator> iter = list.iterator();
		while (iter.hasNext()) {
            stats.append(iter.next().getStats(counter));
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
	
	public static class AllocationStats {
		public AllocationStats(final int i) {
			m_blockSize = i;
		}
		long m_blockSize;
		long m_reservedSlots;
		long m_filledSlots;
	}
	/**
	 * Utility debug outputing the allocator array, showing index, start
	 * address and alloc type/size
	 * 
	 * Collected statistics are against each Allocation Block size:
	 * total number of slots | store size
	 * number of filled slots | store used
	 */
	public void showAllocators(final StringBuilder str) {
		final AllocationStats[] stats = new AllocationStats[m_allocSizes.length];
		for (int i = 0; i < stats.length; i++) {
			stats[i] = new AllocationStats(m_allocSizes[i]*64);
		}
		final Iterator<Allocator> allocs = m_allocs.iterator();
		while (allocs.hasNext()) {
			Allocator alloc = (Allocator) allocs.next();
			alloc.appendShortStats(str, stats);
		}
		
		// Append Summary
		str.append("\n-------------------------\n");
		str.append("RWStore Allocation Summary\n");
		str.append("-------------------------\n");
		long treserved = 0;
		long treservedSlots = 0;
		long tfilled = 0;
		long tfilledSlots = 0;
		for (int i = 0; i < stats.length; i++) {
		    final long reserved = stats[i].m_reservedSlots * stats[i].m_blockSize;
			treserved += reserved;
			treservedSlots += stats[i].m_reservedSlots;
			final long filled = stats[i].m_filledSlots * stats[i].m_blockSize;
			tfilled += filled;
			tfilledSlots += stats[i].m_filledSlots;
			
			str.append("Allocation: " + stats[i].m_blockSize);
			str.append(", slots: "  + stats[i].m_filledSlots + "/" + stats[i].m_reservedSlots);
			str.append(", storage: "  + filled + "/" + reserved);
			str.append(", usage: "  + (reserved==0?0:(filled * 100 / reserved)) + "%");
			str.append("\n");
		}
        str.append("Total - file: " + convertAddr(m_fileSize) + //
                ", slots: " + tfilledSlots + "/" + treservedSlots + //
                ", storage: " + tfilled + "/" + treserved + //
                "\n");
    }

//	public ArrayList<Allocator> getStorageBlockAddresses() {
//		final ArrayList<Allocator> addrs = new ArrayList<Allocator>(m_allocs.size());
//
//		final Iterator<Allocator> allocs = m_allocs.iterator();
//		while (allocs.hasNext()) {
//			final Allocator alloc = allocs.next();
//			alloc.addAddresses(addrs);
//		}
//
//		return addrs;
//	}

	// --------------------------------------------------------------------------------------

	public boolean verify(final long laddr) {
		
	    final int addr = (int) laddr;

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
	public long physicalAddress(final int addr) {
		if (addr > 0) {
			return addr & 0xFFFFFFE0;
		} else {
			final Allocator allocator = getBlock(addr);
			final int offset = getOffset(addr);
			final long laddr = allocator.getPhysicalAddress(offset);

			return laddr;
		}
	}

	/********************************************************************************
	 * handle dual address format, if addr is positive then it is the physical
	 * address, so the Allocators must be searched.
	 **/
	Allocator getBlockByAddress(final int addr) {
		if (addr < 0) {
			return getBlock(addr);
		}

		final Iterator<Allocator> allocs = m_allocs.iterator();

		Allocator alloc = null;
		while (allocs.hasNext()) {
			alloc = allocs.next();

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

	private Allocator getBlock(final int addr) {
		int index = (-addr) >>> OFFSET_BITS;

		return (Allocator) m_allocs.get(index);
	}

	private int getOffset(final int addr) {
		return (-addr) & OFFSET_BITS_MASK; // OFFSET_BITS
	}

	public int addr2Size(final int addr) {
		if (addr > 0) {
			int size = 0;

			final int index = ((int) addr) % 16;

			if (index == 15) { // blob
				throw new Error("FIX ME : legacy BLOB code being accessed somehow");
			} else {
				size = m_minFixedAlloc * m_allocSizes[index];
			}

			return size;
		} else {
			return getBlock(addr).getPhysicalSize(getOffset(addr));
		}
	}

	public boolean isNativeAddress(final long addr) {
		return addr <= 0;
	}

	/*******************************************************************************
	 * called when used as a server, returns whether facility is enabled, this
	 * is the whole point of the wormStore - so the answer is true
	 **/
	public boolean preserveSessionData() {
		m_preserveSession = true;

		return true;
	}

	/*******************************************************************************
	 * called by allocation blocks to determine whether they can re-allocate
	 * data within this session.
	 **/
	protected boolean isSessionPreserved() {
		return m_preserveSession || m_contexts.size() > 0;
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

//	/***************************************************************************************
//	 * Needed by PSOutputStream for BLOB buffer chaining.
//	 **/
//	public void absoluteWriteInt(final int addr, final int offset, final int value) {
//		try {
//			// must check write cache!!, or the write may be overwritten - just
//			// flush for now
//			m_writes.flush();
//
//			m_raf.seek(physicalAddress(addr) + offset);
//			m_raf.writeInt(value);
//		} catch (IOException e) {
//			throw new StorageTerminalError("Unable to write integer", e);
//		}
//	}

//	/***************************************************************************************
//	 * Needed to free Blob chains.
//	 **/
//	public int absoluteReadInt(final int addr, final int offset) {
//		try {
//			m_raf.seek(physicalAddress(addr) + offset);
//			return m_raf.readInt();
//		} catch (IOException e) {
//			throw new StorageTerminalError("Unable to write integer", e);
//		}
//	}

	/***************************************************************************************
	 * Needed by PSOutputStream for BLOB buffer chaining.
	 **/
	public int bufferChainOffset() {
		return m_maxFixedAlloc - 4;
	}

	public File getStoreFile() {
		return m_fd;
	}

	public boolean isLongAddress() {
		// always ints
		return false;
	}

//	public int absoluteReadLong(long addr, int offset) {
//		throw new UnsupportedOperationException();
//	}
//
//	public void absoluteWriteLong(long addr, int threshold, long value) {
//		throw new UnsupportedOperationException();
//	}

//	public void absoluteWriteAddress(long addr, int threshold, long addr2) {
//		absoluteWriteInt((int) addr, threshold, (int) addr2);
//	}

	public int getAddressSize() {
		return 4;
	}

//	public RandomAccessFile getRandomAccessFile() {
//		return m_raf;
//	}

//	public FileChannel getChannel() {
//		return m_raf.getChannel();
//	}

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
		
		// include space for allocSizes and deferred free info
		final int metaBitsSize = 2 + m_metaBits.length + m_allocSizes.length + 1;
		ret += metaBitsSize;
		
        if (log.isTraceEnabled())
            log.trace("Returning metabitsAddr: " + ret + ", for "
                    + m_metaBitsAddr + " - " + m_metaBits.length + ", "
                    + metaBitsSize);

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

		if (log.isTraceEnabled())
			log.trace("Returning nextOffset: " + ret + ", for " + m_metaBitsAddr);

		return ret;
	}

	public void flushWrites(final boolean metadata) throws IOException {
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
     * A Blob Allocator maintains a list of Blob headers. The allocator stores
     * upto 255 blob headers plus a checksum. When a request is made to read the
     * blob data, the blob allocator retrieves the blob header and reads the
     * data from that into the passed byte array.
     */
    public int registerBlob(final int addr) {
		BlobAllocator ba = null;
		if (m_freeBlobs.size() > 0) {
			ba = (BlobAllocator) m_freeBlobs.get(0);
		}
		if (ba == null) {
		    final Allocator lalloc = (Allocator) m_allocs.get(m_allocs.size()-1);
			final int psa = lalloc.getRawStartAddr(); // previous block start address
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

	public void addToCommit(final Allocator allocator) {
		if (!m_commitList.contains(allocator)) {
			m_commitList.add(allocator);
		}
	}

	public Allocator getAllocator(final int i) {
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

        public ReopenFileChannel(final File file, final RandomAccessFile raf,
                final String mode) throws IOException {

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
	public void establishHAExtent(final long extent) {
		
	    final long currentExtent = convertAddr(m_fileSize);
		
		if (extent != currentExtent) {

		    extendFile(convertFromAddr(extent - currentExtent));
		    
		}
		
	}
	
	/**
	 * @return number of FixedAllocators
	 */
	public int getFixedAllocatorCount() {
		int fixed = 0;
		final Iterator<Allocator> allocs = m_allocs.iterator();
		while (allocs.hasNext()) {
			if (allocs.next() instanceof FixedAllocator) {
				fixed++;
			}
		}
		
		return fixed;
	}
	
	/**
	 * @return  the number of heap allocations made to the FixedAllocators.
	 */
	public int getAllocatedBlocks() {
		int allocated = 0;
		final Iterator<Allocator> allocs = m_allocs.iterator();
		while (allocs.hasNext()) {
			final Allocator alloc = allocs.next();
			if (alloc instanceof FixedAllocator) {
				allocated += ((FixedAllocator) alloc).getAllocatedBlocks();
			}
		}

		return allocated;
	}
	
	/**
	 * @return  the amount of heap storage assigned to the FixedAllocators.
	 */
	public long getFileStorage() {
		long allocated = 0;
		final Iterator<Allocator> allocs = m_allocs.iterator();
		while (allocs.hasNext()) {
			final Allocator alloc = allocs.next();
			if (alloc instanceof FixedAllocator) {
				allocated += ((FixedAllocator) alloc).getFileStorage();
			}
		}

		return allocated;
	}
	
	/**
	 * Computes the amount of utilised storage
	 * 
	 * @return the amount of storage to alloted slots in the allocation blocks
	 */
	public long getAllocatedSlots() {
		long allocated = 0;
		final Iterator<Allocator> allocs = m_allocs.iterator();
		while (allocs.hasNext()) {
			final Allocator alloc = allocs.next();
			if (alloc instanceof FixedAllocator) {
				allocated += ((FixedAllocator) alloc).getAllocatedSlots();
			}
		}

		return allocated;
	}

	/**
	 * Adds the address for later freeing to the deferred free list.
	 * <p>
	 * If the allocation is for a BLOB then the sze is also stored
	 * <p>
	 * The deferred list is checked on AllocBlock and prior to commit.
	 * <p>
	 * DeferredFrees are written to the deferred PSOutputStream
	 */
	public void deferFree(final int rwaddr, final int sze) {
	    m_allocationLock.lock();
		try {
			m_deferredFreeOut.writeInt(rwaddr);

			final Allocator alloc = getBlockByAddress(rwaddr);
			if (alloc instanceof BlobAllocator) {
				m_deferredFreeOut.writeInt(sze);
			}
		} catch (IOException e) {
            throw new RuntimeException("Could not free: rwaddr=" + rwaddr
                    + ", size=" + sze, e);
		} finally {
			m_allocationLock.unlock();
		}
	}
	
//	private void checkFreeable(final JournalTransactionService transactionService) {
//		if (transactionService == null) {
//			return;
//		}
//		
//		try {
//			final Long freeTime = transactionService.tryCallWithLock(new Callable<Long>() {
//	
//				public Long call() throws Exception {
//					final long now = transactionService.nextTimestamp();
//					final long earliest =  transactionService.getEarliestTxStartTime();
//					final long aged = now - transactionService.getMinReleaseAge();
//					
//					if (transactionService.getActiveCount() == 0) {
//						return aged;
//					} else {
//						return aged < earliest ? aged : earliest;
//					}
//				}
//				
//			}, 5L, TimeUnit.MILLISECONDS);
//		} catch (RuntimeException e) {
//			// fine, will try again later
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
//	}

	/**
	 * Writes the content of currentTxnFreeList to the store.
	 * 
	 * These are the current buffered frees that have yet been saved into
	 * a block referenced from the deferredFreeList
	 * 
	 * @return the address of the deferred addresses saved on the store
	 */
	public long saveDeferrals() {
	    m_allocationLock.lock();
		try {
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
		    m_allocationLock.unlock();
		}
	}

	/**
	 * Provided with the address of a block of addresses to be freed
	 * @param blockAddr
	 */
	private void freeDeferrals(final long blockAddr, final long lastReleaseTime) {
		final int addr = (int) (blockAddr >> 32);
		final int sze = (int) blockAddr & 0xFFFFFF;
		
		if (log.isTraceEnabled())
			log.trace("freeDeferrals at " + physicalAddress(addr) + ", size: " + sze + " releaseTime: " + lastReleaseTime);
		
		final byte[] buf = new byte[sze+4]; // allow for checksum
		getData(addr, buf);
		final DataInputStream strBuf = new DataInputStream(new ByteArrayInputStream(buf));
		m_allocationLock.lock();
		try {
			int nxtAddr = strBuf.readInt();
			
			while (nxtAddr != 0) { // while (false && addrs-- > 0) {
				
				final Allocator alloc = getBlock(nxtAddr);
				if (alloc instanceof BlobAllocator) {
					final int bloblen = strBuf.readInt();
					assert bloblen > 0; // a Blob address MUST have a size

					immediateFree(nxtAddr, bloblen);
				} else {
					immediateFree(nxtAddr, 0); // size ignored for FreeAllocators
				}
				
				nxtAddr = strBuf.readInt();
			}
            m_lastDeferredReleaseTime = lastReleaseTime;
            if (log.isTraceEnabled())
                log.trace("Updated m_lastDeferredReleaseTime="
                        + m_lastDeferredReleaseTime);
		} catch (IOException e) {
			throw new RuntimeException("Problem freeing deferrals", e);
		} finally {
			m_allocationLock.unlock();
		}
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
    private void freeDeferrals(final AbstractJournal journal,
            final long fromTime,
            final long toTime) {

        final ITupleIterator<CommitRecordIndex.Entry> commitRecords;
	    {
            /*
             * Commit can be called prior to Journal initialisation, in which
             * case the commitRecordIndex will not be set.
             */
            final IIndex commitRecordIndex = journal.getReadOnlyCommitRecordIndex();
    
            final IndexMetadata metadata = commitRecordIndex
                    .getIndexMetadata();

            final byte[] fromKey = metadata.getTupleSerializer()
                    .serializeKey(fromTime);

            final byte[] toKey = metadata.getTupleSerializer()
                    .serializeKey(toTime);

            commitRecords = commitRecordIndex
                    .rangeIterator(fromKey, toKey);
            
        }

        if(log.isTraceEnabled())
            log.trace("fromTime=" + fromTime + ", toTime=" + toTime);

        while (commitRecords.hasNext()) {
            
            final ITuple<CommitRecordIndex.Entry> tuple = commitRecords.next();

            final CommitRecordIndex.Entry entry = tuple.getObject();

            final ICommitRecord record = CommitRecordSerializer.INSTANCE
                    .deserialize(journal.read(entry.addr));

            final long blockAddr = record
                    .getRootAddr(AbstractJournal.DELETEBLOCK);
			
            if (blockAddr != 0) {
			
                freeDeferrals(blockAddr, record.getTimestamp());
                
			}

        }
        
	}

	/**
	 * The ContextAllocation object manages a freeList of associated allocators
	 * and an overall list of allocators.  When the context is detached, all
	 * allocators must be released and any that has available capacity will
	 * be assigned to the global free lists.
	 * 
	 * @param context
	 * 		The context to be released from all FixedAllocators.
	 */
	public void detachContext(final IAllocationContext context) {
		m_allocationLock.lock();
		try {
			final ContextAllocation alloc = m_contexts.remove(context);
			
			if (alloc != null) {
				alloc.release();			
			}
		} finally {
			m_allocationLock.unlock();
		}
	}
	
	/**
	 * The ContextAllocation class manages a set of Allocators.
	 * 
	 * A ContextAllocation can have a parent ContextAllocation such that when
	 * it is released it will transfer its Allocators to its parent.
	 * 
	 * 
	 * 
	 * @author Martyn Cutcher
	 *
	 */
	class ContextAllocation {
		private final ArrayList<FixedAllocator> m_freeFixed[];
		
		private final ArrayList<FixedAllocator> m_allFixed;
		
		// lists of free blob allocators
//		private final ArrayList<BlobAllocator> m_freeBlobs;
		
		private final ContextAllocation m_parent;
		private final IAllocationContext m_context;
		
        ContextAllocation(final int fixedBlocks,
                final ContextAllocation parent,
                final IAllocationContext acontext) {
			m_parent = parent;
			m_context = acontext;
			
			m_freeFixed = new ArrayList[fixedBlocks];
			for (int i = 0; i < m_freeFixed.length; i++) {
				m_freeFixed[i] = new ArrayList<FixedAllocator>();
			}
			
			m_allFixed = new ArrayList<FixedAllocator>();
			
//			m_freeBlobs = new ArrayList<BlobAllocator>();
			
		}
		
        void release() {
            final ArrayList<FixedAllocator> freeFixed[] = m_parent != null ? m_parent.m_freeFixed
                    : RWStore.this.m_freeFixed;

            final IAllocationContext pcontext = m_parent == null ? null
                    : m_parent.m_context;

			for (FixedAllocator f : m_allFixed) {
				f.setAllocationContext(pcontext);
			}
			
			for (int i = 0; i < m_freeFixed.length; i++) {
				freeFixed[i].addAll(m_freeFixed[i]);
			}
			
//			freeBlobs.addAll(m_freeBlobs);
		}
		
		FixedAllocator getFreeFixed(final int i) {
			final ArrayList<FixedAllocator> free = m_freeFixed[i];
			if (free.size() == 0) {
				final FixedAllocator falloc = establishFixedAllocator(i);
				falloc.setAllocationContext(m_context);
				free.add(falloc);
				m_allFixed.add(falloc);
			}
			
			return free.get(0); // take first in list
		}
		
		/**
		 * 
		 * @param i - the block-index for the allocator required
		 * @return
		 */
		FixedAllocator establishFixedAllocator(final int i) {
			if (m_parent == null) {
				 return RWStore.this.establishFreeFixedAllocator(i);
			} else {
				return m_parent.establishFixedAllocator(i);
			}
		}
	}

	/**
	 * A map of the {@link IAllocationContext}s.
	 * <p>
	 * Note: This map must be thread-safe since it is referenced from various
	 * methods outside of the governing {@link #m_allocationLock}.
	 */
	private final Map<IAllocationContext, ContextAllocation> m_contexts = 
		new ConcurrentHashMap<IAllocationContext, ContextAllocation>();
	
    private ContextAllocation establishContextAllocation(
            final IAllocationContext context) {
        
        ContextAllocation ret = m_contexts.get(context);
        
        if (ret == null) {
        
            ret = new ContextAllocation(m_freeFixed.length, null, context);

            m_contexts.put(context, ret);
        
        }

        return ret;
    
    }
	
	public int getSlotSize(final int data_len) {
		int i = 0;

		int ret = m_minFixedAlloc;
		while (data_len > ret) {
			i++;
			ret = 64 * m_allocSizes[i];
		}
		
		return ret;
	}

}
