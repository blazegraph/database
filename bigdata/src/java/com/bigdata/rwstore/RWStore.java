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
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.security.DigestException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree.Counter;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.striped.StripedCounters;
import com.bigdata.ha.HAPipelineGlue;
import com.bigdata.ha.QuorumPipeline;
import com.bigdata.ha.msg.HAWriteMessage;
import com.bigdata.ha.msg.IHALogRequest;
import com.bigdata.ha.msg.IHARebuildRequest;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.IBufferAccess;
import com.bigdata.io.IReopenChannel;
import com.bigdata.io.writecache.BufferedWrite;
import com.bigdata.io.writecache.IBufferedWriter;
import com.bigdata.io.writecache.WriteCache;
import com.bigdata.io.writecache.WriteCacheService;
import com.bigdata.journal.AbstractBufferStrategy;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.CommitRecordIndex;
import com.bigdata.journal.CommitRecordSerializer;
import com.bigdata.journal.FileMetadata;
import com.bigdata.journal.ForceEnum;
import com.bigdata.journal.ICommitRecord;
import com.bigdata.journal.ICommitter;
import com.bigdata.journal.IHABufferStrategy;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockView;
import com.bigdata.journal.StoreTypeEnum;
import com.bigdata.quorum.Quorum;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.service.AbstractTransactionService;
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
 * allocating at the end of the file (and moving them on file extend) to
 * allocation of fixed areas. The meta-allocation data, containing the bitmap
 * that controls these allocations, is itself stored in the heap, and is now
 * structured to include both the bit data and the list of meta-storage
 * addresses.
 * <p>
 * Sizing: 256 allocators would reference approximately 2M objects/allocations.
 * At 1K per allocator this would require 250K of store. The meta-allocation
 * data would therefore need a start address plus 32 bytes (or 8 ints) to
 * represent the meta-allocation bits. An array of such data referencing
 * sequentially allocated storage areas completes the meta-allocation
 * requirements.
 * <p>
 * A meta-allocation address can therefore be represented as a single bit offset
 * from which the block, providing start address, and bit offset can be directly
 * determined.
 * <p>
 * The m_metaBits int array used to be fully used as allocation bits, but now
 * stores both the start address plus the 8 ints used to manage that data block.
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
 * allocation headers. A total of (say) 100 allocation headers might provide up
 * to 4000 allocations each -> 400 000 objects, while 1000 headers -> 4m objects
 * and 2000 -> 8m objects.
 * <p>
 * The allocators are split into a set of FixedAllocators and then
 * BlobAllocation. The FixedAllocators will allocate from 128 to 32K objects,
 * with a minimum block allocation of 64K, and a minimum bit number per block of
 * 32.
 * <p>
 * Where possible lists and roving pointers will be used to minimize searching
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
 * header record. This is indicated with a BlobAllocator that provides indexed
 * offsets to the header record (the address encodes the BlobAllocator and the
 * offset to the address). The header record stores the number of component
 * allocations and the address of each.
 * <p>
 * This approach makes for much more efficient freeing/re-allocation of Blob
 * storage, in particular avoiding the need to read in the component blocks to
 * determine chained blocks for freeing. This is particularly important for
 * larger stores where a disk cache could be flushed through simply freeing BLOB
 * allocations.
 * <h2>
 * Deferred Free List</h2>
 * <p>
 * The previous implementation has been amended to associate a single set of
 * deferredFree blocks with each CommitRecord. The CommitRecordIndex will then
 * provide access to the CommitRecords to support the deferred freeing of
 * allocations based on age/earliestTxReleaseTime.
 * <p>
 * The last release time processed is held with the MetaAllocation data
 * 
 * @author Martyn Cutcher
 * 
 *         FIXME Release checklist:
 *         <p>
 *         Add metabits header record checksum field and verify on read back.
 *         <p>
 *         Done. Checksum fixed allocators (needs to be tested on read back).
 *         <p>
 *         Done. Add version field to the fixed allocator.
 *         <p>
 *         Done. Checksum delete blocks / blob records.
 *         <p>
 *         PSOutputStream - remove caching logic. It is unused and makes this
 *         class much more complex. A separate per-RWStore caching class for
 *         recycling PSOutputStreams can be added later.
 *         <p>
 *         Modify FixedAllocator to use arrayCopy() rather than clone and
 *         declare more fields to be final. See notes on {@link AllocBlock}.
 *         <p>
 *         Done. Implement logic to "abort" a shadow allocation context.
 *         <p>
 *         Unit test to verify that we do not recycle allocations from the last
 *         commit point even when the retention time is zero such that it is
 *         always possible to re-open the store from the alternative root block
 *         even after you have allocated things against the current root block
 *         (but not yet committed).
 *         <p>
 *         Read-only mode.
 *         <p>
 *         Unit tests looking for persistent memory leaks (e.g., all allocated
 *         space can be reclaimed).
 */

public class RWStore implements IStore, IBufferedWriter {

    private static final transient Logger log = Logger.getLogger(RWStore.class);

    /**
     * @see http://sourceforge.net/apps/trac/bigdata/ticket/443 (Logger for
     *      RWStore transaction service and recycler)
     */
    private static final Logger txLog = Logger.getLogger("com.bigdata.txLog");

    /**
     * Options understood by the {@link RWStore}.
     */
    public interface Options {

		/**
		 * Option defines the Allocation block sizes for the RWStore. The values
		 * defined are multiplied by 64 to provide the actual allocations. The
		 * list of allocations should be ',' delimited and in increasing order.
		 * This array is written into the store so changing the values does not
		 * break older stores. For example,
		 * 
		 * <pre>
		 * &quot;1,2,4,8,116,32,64&quot;
		 * </pre>
		 * 
		 * defines allocations from 64 to 4K in size. It is a good to define
		 * block sizes on 4K boundaries as soon as possible to optimize IO. This
		 * is particularly relevant for SSDs. A 1K boundary is expressed as
		 * <code>16</code> in the allocation sizes, so a 4K boundary is
		 * expressed as <code>64</code> and an 8k boundary as <code>128</code>.
		 * <p>
		 * The default allocations are {@value #DEFAULT_ALLOCATION_SIZES}.
		 * 
		 * @see #DEFAULT_ALLOCATION_SIZES
		 */
        String ALLOCATION_SIZES = RWStore.class.getName() + ".allocationSizes";

        /**
         * Note: The default allocation sizes SHOULD NOT provide for allocation
         * slots larger than an 8k page. This can lead to large allocation slots
         * when a B+Tree index is sparsely populated (less efficient prefix
         * compression) followed by a gradual reduction in the average page size
         * with the net effect that large allocators become unused and turn into
         * wasted and unrecoverable space on the backing file. Keeping to an 8k
         * maximum allocation slot size means that we have to do a few more IOs
         * if the page exceeds the 8k boundary, but we never wind up with those
         * large and (mostly) unused allocators. The B+Tree branching factors
         * should be tuned to target perhaps 80% of an 8k page in order to have
         * only a small number of pages that spill over into blobs.
         * 
         * @see #ALLOCATION_SIZES
         */
        String DEFAULT_ALLOCATION_SIZES = "1, 2, 3, 5, 8, 12, 16, 32, 48, 64, 128";
        // String DEFAULT_ALLOCATION_SIZES = "1, 2, 3, 5, 8, 12, 16, 32, 48, 64, 128, 192, 320, 512, 832, 1344, 2176, 3520";
//        String DEFAULT_ALLOCATION_SIZES = "1, 2, 3, 5, 8, 12, 16, 32, 48, 64, 128, 192, 320, 512";
        // private static final int[] DEFAULT_ALLOC_SIZES = { 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181 };
        // private static final int[] ALLOC_SIZES = { 1, 2, 4, 8, 16, 32, 64, 128 };

        /**
         * Option defines the initial size of the meta bits region and effects
         * how rapidly this region will grow (default
         * {@value #DEFAULT_META_BITS_SIZE}).
         * <p>
         * Note: A value of <code>9</code> may be used to stress the logic which
         * is responsible for the growth in the meta bits region.
         */
        String META_BITS_SIZE = RWStore.class.getName() + ".metaBitsSize";

        String DEFAULT_META_BITS_SIZE = "9";

        /**
         * Defines the number of bits that must be free in a FixedAllocator for
         * it to be added to the free list.  This is used to ensure a level
         * of locality when making large numbers of allocations within a single
         * commit.
         * <p>
         * The value should be >= 1 and <= 5000
         */
        String FREE_BITS_THRESHOLD = RWStore.class.getName() + ".freeBitsThreshold";

        String DEFAULT_FREE_BITS_THRESHOLD = "300";

		/**
		 * When <code>true</code>, scattered writes which are strictly ascending
		 * will be coalesced within a buffer and written out as a single IO
		 * (default {@value #DEFAULT_DOUBLE_BUFFER_WRITES}). This improves write
		 * performance for SATA, SAS, and even SSD.
		 */
        String DOUBLE_BUFFER_WRITES = RWStore.class.getName() + ".doubleBuffer";
        
        String DEFAULT_DOUBLE_BUFFER_WRITES = "true";
        
//        /**
//         * When <code>true</code> fills recycled storage with a recognizable
//         * byte pattern.
//         */
//        String OVERWRITE_DELETE = RWStore.class.getName() + ".overwriteDelete";
//        
//        String DEFAULT_OVERWRITE_DELETE = "false";
//        
//        /**
//         * When <code>true</code> the RWStore will protect any address from
//         * recycling, and generate an exception if the address is subsequently
//         * accessed
//         */
//        String MAINTAIN_BLACKLIST = RWStore.class.getName() + ".maintainBlacklist";
//        
//        String DEFAULT_MAINTAIN_BLACKLIST = "false";
        
    }

    /*
     * Error messages.
     */
    
    private static final String ERR_WRITE_CACHE_CREATE = "Unable to create write cache service";

	/**
	 * The fixed size of any allocator on the disk in bytes. The #of allocations
	 * managed by an allocator is this value times 8 because each slot uses one
	 * bit in the allocator. When an allocator is allocated, the space on the
	 * persistent heap is reserved for all slots managed by that allocator.
	 * However, the {@link FixedAllocator} only incrementally allocates the
	 * {@link AllocBlock}s.
	 */
	static private final int ALLOC_BLOCK_SIZE = 1024;
	
//	// from 32 bits, need 13 to hold max offset of 8 * 1024, leaving 19 for number of blocks: 256K
//	static final int BLOCK_INDEX_BITS = 19;
    /**
     * The #of low bits in a latched address that encode the offset of the bit
     * in a {@link FixedAllocator}. The {@link FixedAllocator} will map the bit
     * onto an allocation slot.
     * <p>
     * The high bits of the latched address is index of the
     * {@link FixedAllocator}. The index of the {@link FixedAllocator} is the
     * order in which it was created. This is used to index into
     * {@link #m_allocs}, which are the {@link FixedAllocator}s.
     */
	static final int OFFSET_BITS = 13;
	static final int OFFSET_BITS_MASK = 0x1FFF; // was 0xFFFF
	
	static final int ALLOCATION_SCALEUP = 16; // multiplier to convert allocations based on minimum allocation of 32k
	static private final int META_ALLOCATION = 8; // 8 * 32K is size of meta Allocation

	// If required, then allocate 1M direct buffers
	private static final int cDirectBufferCapacity = 1024 * 1024;

	private int cMaxDirectBuffers = 20; // 20M of direct buffers
	static final int cDirectAllocationOffset = 64 * 1024;

	// ///////////////////////////////////////////////////////////////////////////////////////
	// RWStore Data
	// ///////////////////////////////////////////////////////////////////////////////////////

	private final File m_fd;
//	private RandomAccessFile m_raf;
//	protected FileMetadata m_metadata;
//	protected int m_transactionCount;
//	private boolean m_committing;

//    /**
//     * When <code>true</code> the allocations will not actually be recycled
//     * until after a store restart. When <code>false</code>, the allocations are
//     * recycled once they satisfy the history retention requirement.
//     */
//	private boolean m_preserveSession = false;
//	private boolean m_readOnly;

	/**
	 * lists of total alloc blocks.
	 * 
	 * @todo examine concurrency and lock usage for {@link #m_alloc} and the
	 *       rest of these lists.
	 */
	private final ArrayList<FixedAllocator> m_allocs;

	/**
	 * A fixed length array of lists of free {@link FixedAllocator}s with one
	 * entry in the array for each configured allocator size. An allocator is
	 * put onto this free list when it is initially created. When the store is
	 * opened, it will be added to this list if {@link Allocator#hasFree()}
	 * returns true. It will be removed when it has no free space remaining. It
	 * will be added back to the free list when its free slots exceeds a
	 * configured threshold.
	 */
	private ArrayList<FixedAllocator> m_freeFixed[];
	
//	/** lists of free blob allocators. */
	// private final ArrayList<BlobAllocator> m_freeBlobs;

	/** lists of blocks requiring commitment. */
	private final ArrayList<Allocator> m_commitList;

//	private WriteBlock m_writes;
	
	private final Quorum<?,?> m_quorum;
	
	/**
	 * The #of buffers that will be used by the {@link WriteCacheService}.
	 */
	private final int m_writeCacheBufferCount;
	
    /**
     * Note: This is not final because we replace the {@link WriteCacheService}
     * during {@link #reset(long)} in order to propagate the then current quorum
     * token to the {@link WriteCacheService}.
     */
    RWWriteCacheService m_writeCache;

	/**
	 * The actual allocation sizes as read from the store.
	 * 
	 * @see #DEFAULT_ALLOCATION_SIZES
	 */
	private int[] m_allocSizes;

	/**
	 * The maximum allocation size (bytes).
	 */
    final int m_maxFixedAlloc;

    /**
     * The minimum allocation size (bytes).
     */
    final int m_minFixedAlloc;
	
    /**
     * Currently we do not support a Blob header to be a Blob, so the
     * maximum possible Blob is ((maxFixed-4) * maxFixed) - 4.
     */
    final int m_maxBlobAllocSize;
	
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
    private final long m_minReleaseAge;

    /**
     * The #of open transactions (read-only or read-write).
     * 
     * This is guarded by the {@link #m_allocationLock}.
     */
    private int m_activeTxCount = 0;
    
	private volatile long m_lastDeferredReleaseTime = 0L;
//	private final ArrayList<Integer> m_currentTxnFreeList = new ArrayList<Integer>();
	private final PSOutputStream m_deferredFreeOut;

    /**
     * Used to transparently re-open the backing channel if it has been closed
     * by an interrupt during an IO.
     */
	private final ReopenFileChannel m_reopener;

	private volatile BufferedWrite m_bufferedWrite;
	
	/**
	 * Our StoreageStats objects
	 */
	private StorageStats m_storageStats;
	private long m_storageStatsAddr = 0;
	
    /**
     * <code>true</code> iff the backing store is open.
     */
    private volatile boolean m_open = true;
    
    
//    /**
//     * If m_blacklist is non-null then a request to blacklist as address will
//     * add the address to the blacklist.
//     * 
//     * When a blacklisted address is freed and is re-allocated, the re-allocation
//     * is intercepted (see alloc()), the address is locked and a new allocation is made.
//     * 
//     * The purpose of the blacklist is to trap erroneus references to an
//     * address that is retained (and used) after it should be.
//     */
//    private ConcurrentHashMap<Integer, String> m_blacklist = null;
    private ConcurrentHashMap<Integer, Long> m_lockAddresses = null;

	class WriteCacheImpl extends WriteCache.FileChannelScatteredWriteCache {
        public WriteCacheImpl(final IBufferAccess buf,
                final boolean useChecksum,
                final boolean bufferHasData,
                final IReopenChannel<FileChannel> opener,
                final long fileExtent)
                throws InterruptedException {

            super(buf, useChecksum, m_quorum != null
                    && m_quorum.isHighlyAvailable(), bufferHasData, opener,
                    fileExtent,
                    m_bufferedWrite);

        }

        /**
         * {@inheritDoc}
         * <p>
         * Note: The performance counters for writes to the disk are reported by
         * the {@link WriteCacheService}. The {@link RWStore} never writes
         * directly onto the disk (other than the root blocks).
         */
        @Override
        protected boolean writeOnChannel(final ByteBuffer data,
                final long firstOffsetignored,
                final Map<Long, RecordMetadata> recordMap,
                final long nanos) throws InterruptedException, IOException {
            final Lock readLock = m_extensionLock.readLock();
            readLock.lock();
            try {
                boolean ret = super.writeOnChannel(data, firstOffsetignored,
                        recordMap, nanos);
                return ret;
            } finally {
                readLock.unlock();
            }

        }
        
        // Added to enable debug of rare problem
        // FIXME: disable by removal once solved
        protected void registerWriteStatus(long offset, int length, char action) {
    		m_writeCache.debugAddrs(offset, length, action);
        }
		
        @Override
        protected void addAddress(int latchedAddr, int size) {
        	RWStore.this.addAddress(latchedAddr, size);
        }

        @Override
        protected void removeAddress(int latchedAddr) {
        	RWStore.this.removeAddress(latchedAddr);
        }

	};
	
    /**
     * The ALLOC_SIZES must be initialized from either the file or the
     * properties associated with the fileMetadataView
     * 
     * @param fileMetadataView
     * @param readOnly
     * @param quorum
     * @throws InterruptedException
     * 
     * @todo support read-only open.
     */
    public RWStore(final FileMetadata fileMetadata, final Quorum<?, ?> quorum) {

        if (fileMetadata == null)
            throw new IllegalArgumentException();

        this.m_minReleaseAge = Long.valueOf(fileMetadata.getProperty(
                AbstractTransactionService.Options.MIN_RELEASE_AGE,
                AbstractTransactionService.Options.DEFAULT_MIN_RELEASE_AGE));

        if (log.isInfoEnabled())
            log.info(AbstractTransactionService.Options.MIN_RELEASE_AGE + "="
                    + m_minReleaseAge);

        cDefaultMetaBitsSize = Integer.valueOf(fileMetadata.getProperty(
                Options.META_BITS_SIZE,
                Options.DEFAULT_META_BITS_SIZE));

        if (cDefaultMetaBitsSize < 9)
            throw new IllegalArgumentException(Options.META_BITS_SIZE
                    + " : Must be GTE 9");
        
        m_metaBitsSize = cDefaultMetaBitsSize;

        cDefaultFreeBitsThreshold = Integer.valueOf(fileMetadata.getProperty(
                Options.FREE_BITS_THRESHOLD,
                Options.DEFAULT_FREE_BITS_THRESHOLD));
        
        if (cDefaultFreeBitsThreshold < 1 || cDefaultFreeBitsThreshold > 5000) {
            throw new IllegalArgumentException(Options.FREE_BITS_THRESHOLD
                    + " : Must be between 1 and 5000");
        }

        m_metaBits = new int[m_metaBitsSize];
		
		m_metaTransientBits = new int[m_metaBitsSize];
		
		// @todo Review maximum file size constraints - is this old stuff?
		m_maxFileSize = 2 * 1024 * 1024; // 1gb max (mult by 128)!!
		
        m_quorum = quorum;
		
		m_fd = fileMetadata.file;
		
		// initialize striped performance counters for this store.
        this.storeCounters.set(new StoreCounters(10/* batchSize */));
        
		final IRootBlockView m_rb = fileMetadata.rootBlock;

		m_commitList = new ArrayList<Allocator>();

		m_allocs = new ArrayList<FixedAllocator>();
		
		// m_freeBlobs = new ArrayList<BlobAllocator>();

		try {
	        final RandomAccessFile m_raf = fileMetadata.getRandomAccessFile();
			m_reopener = new ReopenFileChannel(m_fd, m_raf, "rw");
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}

		if (Boolean.valueOf(fileMetadata.getProperty(
				Options.DOUBLE_BUFFER_WRITES,
				Options.DEFAULT_DOUBLE_BUFFER_WRITES))) {
			try {
				m_bufferedWrite = new BufferedWrite(this);
			} catch (InterruptedException e1) {
				m_bufferedWrite = null;
			}
		} else {
			m_bufferedWrite = null;
		}

		m_writeCacheBufferCount = fileMetadata.writeCacheBufferCount;
		
		if(log.isInfoEnabled())
		    log.info("RWStore using writeCacheService with buffers: " + m_writeCacheBufferCount);

		// m_writeCache = newWriteCache();

		try {
            if (m_rb.getNextOffset() == 0) { // if zero then new file
            	setAllocations(fileMetadata);
            	
            	defaultInit();
            	
        		m_maxFixedAlloc = m_allocSizes[m_allocSizes.length-1]*64;
        		m_minFixedAlloc = m_allocSizes[0]*64;
        		
        		m_storageStats = new StorageStats(m_allocSizes);

//        		// Check for overwrite option and set overwrite buffer if
//        		// required
//        		if (Boolean.valueOf(fileMetadata.getProperty(
//		                Options.OVERWRITE_DELETE,
//		                Options.DEFAULT_OVERWRITE_DELETE))) {
//        			m_writeCache.setOverwriteBuffer(m_maxFixedAlloc);
//        		}
			} else {
				
				initfromRootBlock(m_rb);
				
				m_maxFixedAlloc = m_allocSizes[m_allocSizes.length-1]*64;
				m_minFixedAlloc = m_allocSizes[0]*64;

				if (m_storageStatsAddr != 0) {
					final long statsAddr = m_storageStatsAddr >> 16;
					final int statsLen = ((int) m_storageStatsAddr) & 0xFFFF;
					final byte[] stats = new byte[statsLen + 4]; // allow for checksum
					getData(statsAddr, stats);
					final DataInputStream instr = new DataInputStream(new ByteArrayInputStream(stats));
					m_storageStats = new StorageStats(instr);
					
					for (FixedAllocator fa: m_allocs) {
						m_storageStats.register(fa);
					}
				} else {
	        		m_storageStats = new StorageStats(m_allocSizes);
				}
				
                if (log.isTraceEnabled()) {
                    final StringBuilder str = new StringBuilder();
                    this.showAllocators(str);
                    log.trace(str);
                }
                
			}
			
            // setup write cache AFTER init to ensure filesize is correct!
            
    		m_writeCache = newWriteCache();

    		final int maxBlockLessChk = m_maxFixedAlloc-4;
             
            m_maxBlobAllocSize = Integer.MAX_VALUE;
            
			assert m_maxFixedAlloc > 0;
			
			m_deferredFreeOut = PSOutputStream.getNew(this, m_maxFixedAlloc, null);

//    		if (Boolean.valueOf(fileMetadata.getProperty(
//	                Options.MAINTAIN_BLACKLIST,
//	                Options.DEFAULT_MAINTAIN_BLACKLIST))) {
//    			m_blacklist = new ConcurrentHashMap<Integer, String>();
//    			m_lockAddresses = new ConcurrentHashMap<Integer, Long>();
//    		}

		} catch (IOException e) {
			throw new StorageTerminalError("Unable to initialize store", e);
		}
	}
    
    /**
     * Called from WriteCache.resetRecordMapFromBuffer
     * 
     * If a FixedAllocator already exists for the address then just set the address as active,
     * otherwise, create a new allocator and try again, which should work second time around
     * if we are correctly in sync.
     * 
     * @param latchedAddr
     * @param size
     */
    void addAddress(final int latchedAddr, final int size) {
    	// ignore zero address
    	if (latchedAddr == 0)
    		return;
    	
		m_allocationLock.lock();
		try {		
	    	FixedAllocator alloc = null;
	    	try {
	    		alloc = getBlock(latchedAddr);			
	    	} catch (final PhysicalAddressResolutionException par) {
	    		// Must create new allocator    		
	    	}
	    	if (alloc == null) {
				final int i = fixedAllocatorIndex(size);
				final int block = 64 * m_allocSizes[i];
				final ArrayList<FixedAllocator> list = m_freeFixed[i];
				final FixedAllocator allocator = new FixedAllocator(this, block);
						
				allocator.setFreeList(list);
				allocator.setIndex(m_allocs.size());
	
				m_allocs.add(allocator);
				
				// Check correctly synchronized creation
				assert allocator == getBlock(latchedAddr);
	    		
				alloc = allocator;    		
	    	}
	    	
	   		assert size <= alloc.getSlotSize();
			
			alloc.setAddressExternal(latchedAddr);
		} finally {
			m_allocationLock.unlock();
		}
	}
    
    /**
     * Called from WriteCache.resetRecordMapFromBuffer
     * 
     * Must clear the bit in the allocator.
     * 
     * @param latchedAddr
     */
	void removeAddress(final int latchedAddr) {
    	// ignore zero address
    	if (latchedAddr == 0)
    		return;

    	m_allocationLock.lock();
		try {		
			// assert m_commitList.size() == 0;
			
			final FixedAllocator alloc = getBlockByAddress(latchedAddr);
			
			assert alloc != null;
			
			final int addrOffset = getOffset(latchedAddr);
			if (alloc == null) {
				throw new IllegalArgumentException("Invalid address provided to immediateFree: " + latchedAddr);
			}
	        final long pa = alloc.getPhysicalAddress(addrOffset);
	        
	        if (log.isTraceEnabled())
	            log.trace("Freeing allocation at " + latchedAddr + ", physical address: " + pa);
	        
	        alloc.free(latchedAddr, 0, false);
	        
			// assert m_commitList.size() == 0;
		} finally {
			m_allocationLock.unlock();
		}
	}

	/**
     * Create and return a new {@link RWWriteCacheService} instance. The caller
     * is responsible for closing out the old one and must be holding the
     * appropriate locks when it switches in the new instance.
     */
    private RWWriteCacheService newWriteCache() {
        try {
            return new RWWriteCacheService(m_writeCacheBufferCount,
            		convertAddr(m_fileSize), m_reopener, m_quorum) {
                
                        @SuppressWarnings("unchecked")
                        public WriteCache newWriteCache(final IBufferAccess buf,
                                final boolean useChecksum,
                                final boolean bufferHasData,
                                final IReopenChannel<? extends Channel> opener,
                                final long fileExtent)
                                throws InterruptedException {
                            return new WriteCacheImpl(buf,
                                    useChecksum, bufferHasData,
                                    (IReopenChannel<FileChannel>) opener,
                                    fileExtent);
                        }
                };
        } catch (InterruptedException e) {
            throw new IllegalStateException(ERR_WRITE_CACHE_CREATE, e);
        } catch (IOException e) {
            throw new IllegalStateException(ERR_WRITE_CACHE_CREATE, e);
        }       
    }
    
	private void setAllocations(final FileMetadata fileMetadata)
            throws IOException {
        
        final String buckets = fileMetadata.getProperty(
                Options.ALLOCATION_SIZES, Options.DEFAULT_ALLOCATION_SIZES);
        final String[] specs = buckets.split("\\s*,\\s*");
        m_allocSizes = new int[specs.length];
        int prevSize = 0;
        for (int i = 0; i < specs.length; i++) {
            final int nxtSize = Integer.parseInt(specs[i]);
            if (nxtSize <= prevSize)
                throw new IllegalArgumentException(
                        "Invalid AllocSizes property");
            m_allocSizes[i] = nxtSize;
            prevSize = nxtSize;
        }
    }
    
    private void defaultInit() throws IOException {
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
		m_committedNextAllocation = m_nextAllocation;
		
		if (m_fileSize > m_nextAllocation) {
			m_fileSize = m_nextAllocation;
		}
		
		if (log.isInfoEnabled())
			log.info("Set default file extent " + convertAddr(m_fileSize));
		
		m_reopener.raf.setLength(convertAddr(m_fileSize));

    }

    public boolean isOpen() {
        return m_open;
    }
    
    private void assertOpen() {
    
        if (!m_open)
            throw new IllegalStateException(AbstractBufferStrategy.ERR_NOT_OPEN);
        
    }
    
    synchronized public void close() {
        m_open = false;
        try {
            if (m_bufferedWrite != null) {
                m_bufferedWrite.release();
                m_bufferedWrite = null;
            }
            m_writeCache.close();
            m_reopener.raf.close();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

	/**
	 * Basic check on key root block validity
	 * 
	 * @param rbv
	 */
	private void checkRootBlock(final IRootBlockView rbv) {
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
		
	}
	
	/**
	 * Should be called where previously initFileSpec was used.
	 * 
	 * Rather than reading from file, instead reads from the current root block.
	 * 
	 * We use the rootBlock fields, nextOffset, metaStartAddr, metaBitsAddr.
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
	private void initfromRootBlock(final IRootBlockView m_rb) throws IOException {
		// m_rb = m_fmv.getRootBlock();
		assert(m_rb != null);

		if (m_rb.getNextOffset() == 0) {

		    defaultInit();
		    
		} else {		

            /*
             * The RWStore stores in IRootBlock.getNextOffset() two distinct
             * int32 words.
             * 
             * The high int32 word is the next allocation that will handed out
             * and is represented in units of -32K. This is used for things like
             * getting a new metabits region or a new region from which fixed
             * allocators will be recruited (through the metabits).
             * 
             * The low int32 word is the latched address of the current metabits
             * region. It must be interpreted using the metaBits and the
             * FixedAllocators in order to turn it into a byte offset on the
             * file.
             */
		    final long nxtOffset = m_rb.getNextOffset();

		    // next allocation to be made (in -32K units).
		    m_nextAllocation = -(int) (nxtOffset >> 32);
			
			if (m_nextAllocation == 0) {

                /*
                 * Skip the first 32K in the file. The root blocks live here but
                 * nothing else.
                 */
	
			    m_nextAllocation = -(1 + META_ALLOCATION);
			    
			}
			
			m_committedNextAllocation = m_nextAllocation;
	
			// latched offset of the metabits region.
			m_metaBitsAddr = -(int) nxtOffset;
			
			if (log.isInfoEnabled()) {
				log.info("MetaBitsAddr: " + m_metaBitsAddr);
			}

			/*
			 * Get the fileSize in -32K units from the root block.
			 */
			{
                final long metaAddr = m_rb.getMetaStartAddr();

                // in units of -32K.
                m_fileSize = (int) -(metaAddr & 0xFFFFFFFF);
                
        		if (log.isInfoEnabled())
        			log.info("InitFromRootBlock m_fileSize: " + convertAddr(m_fileSize));
        		
			}
	
            /*
             * This stores the byte offset and length of the metabits region in
             * the file. The bottom 16-bits are the length (see below). The top
             * 48-bits are the byte offset.
             */
			long rawmbaddr = m_rb.getMetaBitsAddr();
			
            /*
             * The #of int32 values in the metabits region.
             * 
             * We get this by taking bottom 16 bits of the metaBitsAddr. This
             * gives the #of int32 values in the metabits regions (up to 64k
             * int32 values). Each int32 value in the metaBits[] gives us 32
             * allocators. So, 16-bits gives us up 64k * 32 = 2M allocators.
             * Except, that the total #of allocators is reduced by the presence
             * of a startAddr every N positions in the metaBits[].
             */
			final int metaBitsStore = (int) (rawmbaddr & 0xFFFF);
			
			if (metaBitsStore > 0) {
				
			    // The byte offset of the metabits region in the file.
			    rawmbaddr >>= 16;
				
                /*
                 * Read the metabits block, including a header and the int32[]
                 * that encodes both startAddrs and bit vectors.
                 */
                final byte[] buf = new byte[metaBitsStore * 4];
	
				FileChannelUtility.readAll(m_reopener, ByteBuffer.wrap(buf), rawmbaddr);
		
				final DataInputStream strBuf = new DataInputStream(new ByteArrayInputStream(buf));
				
				// Can handle minor store version incompatibility
				final int storeVersion = strBuf.readInt();
				if ((storeVersion & 0xFF00) != (cVersion & 0xFF00)) {
                    throw new IllegalStateException(
                            "Incompatible RWStore header version: storeVersion="
                                    + storeVersion + ", cVersion=" + cVersion);
				}
				m_lastDeferredReleaseTime = strBuf.readLong();
				cDefaultMetaBitsSize = strBuf.readInt();
				
				final int allocBlocks = strBuf.readInt();
				m_storageStatsAddr = strBuf.readLong();

				// and let's read in those reserved ints
	            for (int i = 0; i < cReservedMetaBits; i++) {
	                strBuf.readInt();
	            }

				m_allocSizes = new int[allocBlocks];
				for (int i = 0; i < allocBlocks; i++) {
					m_allocSizes[i] = strBuf.readInt();
				}
				m_metaBitsSize = metaBitsStore - allocBlocks - cMetaHdrFields; // allow for header fields
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
	
				if (physicalAddress(m_metaBitsAddr) == 0) {
					throw new IllegalStateException("Free/Invalid metaBitsAddr on load");
				}
	
			}
			
			if (log.isInfoEnabled())
				log.info("restored from RootBlock: " + m_nextAllocation 
						+ ", " + m_metaBitsAddr);
		}
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
		for (int b = 0; b < m_metaBits.length; b += cDefaultMetaBitsSize) {
			final long blockStart = convertAddr(m_metaBits[b]);
			final int startBit = (b * 32) + 32;
			final int endBit = startBit + ((cDefaultMetaBitsSize-1)*32);
			for (int i = startBit; i < endBit; i++) {
				if (tstBit(m_metaBits, i)) {
					final long addr = blockStart + ((i-startBit) * ALLOC_BLOCK_SIZE);

					final byte buf[] = new byte[ALLOC_BLOCK_SIZE];

					FileChannelUtility.readAll(m_reopener, ByteBuffer.wrap(buf), addr);

					final ByteArrayInputStream baBuf = new ByteArrayInputStream(buf);
					final DataInputStream strBuf = new DataInputStream(baBuf);

					final int allocSize = strBuf.readInt(); // if Blob < 0
					final FixedAllocator allocator;
					final ArrayList<? extends Allocator> freeList;
					assert allocSize > 0;

					// m_minFixedAlloc and m_maxFixedAlloc may not be set since
					// as finals they must be set in the constructor.  Therefore
					// recalculate for local load
					final int minFixedAlloc = 64 * m_allocSizes[0];
					final int maxFixedAlloc = 64 * m_allocSizes[m_allocSizes.length-1];
					int index = 0;
					int fixedSize = minFixedAlloc;
					while (fixedSize < allocSize && fixedSize < maxFixedAlloc)
						fixedSize = 64 * m_allocSizes[++index];

					if (allocSize != fixedSize) {
						throw new IllegalStateException("Unexpected allocator size: " 
								+ allocSize + " != " + fixedSize);
					}
					allocator = new FixedAllocator(this, allocSize);//, m_writeCache);

					freeList = m_freeFixed[index];

					allocator.read(strBuf);
	                final int chk = ChecksumUtility.getCHK().checksum(buf,
	                        buf.length - baBuf.available());
	                
	                int tstChk = strBuf.readInt();
	                if (tstChk != chk) {
	                	throw new IllegalStateException("FixedAllocator checksum error");
	                }

					allocator.setDiskAddr(i); // store bit, not physical
												// address!
					allocator.setFreeList(freeList);

					m_allocs.add(allocator);
					
					if (m_storageStats != null) {
						m_storageStats.register(allocator);
					}

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
	    for (int i = 0; i < list.size(); i++) {
	    	FixedAllocator f = list.get(i);
	    	if (!m_commitList.contains(f)) {
	    		list.remove(i);
	    		return f;
	    	}
	    }

	    // no valid free allocators, so create a new one
	    final int allocSize = 64 * m_allocSizes[block];

        final FixedAllocator allocator = new FixedAllocator(this,
                allocSize);//, m_writeCache);

		allocator.setIndex(m_allocs.size());
		
		m_allocs.add(allocator);
		
		if (m_storageStats != null) {
			m_storageStats.register(allocator, true);
		}

		return allocator;
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
     * If the buf[] size is greater than the maximum fixed allocation, then the
     * direct read will be the blob header record. In this case we should hand
     * over the streaming to a PSInputStream.
     * 
     * FIXME: Javadoc update (was: For now we do not use the PSInputStream but instead process
     * directly...)
     * 
     * If it is a BlobAllocation, then the BlobAllocation address points to the
     * address of the BlobHeader record.
     */
	public void getData(final long addr, final byte buf[]) {
		
	    getData(addr, buf, 0, buf.length);
	    
	}
	
    public void getData(final long addr, final byte buf[], final int offset,
            final int length) {

        assertOpen();

        if (addr == 0) {
			return;
		}

        final long begin = System.nanoTime();

        final Lock readLock = m_extensionLock.readLock();

        readLock.lock();
        
		try {
	        assertOpen(); // check again after taking lock

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
                    if (hdrbuf.length > m_maxFixedAlloc) {
                    	if (log.isInfoEnabled()) {
                    		log.info("LARGE BLOB - header is BLOB");
                    	}
                    }
                    
                    getData(addr, hdrbuf); // will work even if header is also a blob
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

	        {
	            final StoreCounters<?> storeCounters = (StoreCounters<?>) this.storeCounters
	                    .get().acquire();
	            try {
	                final int nbytes = length;
	                if (nbytes > storeCounters.maxReadSize) {
	                    storeCounters.maxReadSize = nbytes;
	                }
	            } finally {
	                storeCounters.release();
	            }
	        }

	        try {
	        	
	        	final int slotSize = getBlock((int) addr).getBlockSize();
	        	if (slotSize < length) {
	        		throw new IllegalStateException("Bad Address: length requested greater than allocated slot");
	        	}

	            final long paddr = physicalAddress((int) addr);
                
	            if (paddr == 0) {
                
	                assertAllocators();

                    throw new PhysicalAddressResolutionException(addr);
                    
				}
	            
				assert paddr > 0;

                /**
                 * Check WriteCache first
                 * 
                 * Note that the buffer passed in should include the checksum
                 * value, so the cached data is 4 bytes less than the buffer
                 * size.
                 */
				final ByteBuffer bbuf;
				try {
					bbuf = m_writeCache != null ? m_writeCache.read(paddr) : null;
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
	                /*
	                 * Hit on the write cache.
	                 * 
	                 * Update the store counters.
	                 */
	                final StoreCounters<?> c = (StoreCounters<?>) storeCounters
	                        .get().acquire();
	                try {
	                    final int nbytes = length;
	                    c.nreads++;
	                    c.bytesRead += nbytes;
	                    c.elapsedReadNanos += (System.nanoTime() - begin);
	                } finally {
	                    c.release();
	                }
				} else {
		            // Read through to the disk.
		            final long beginDisk = System.nanoTime();
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
		            // Update counters.
		            final StoreCounters<?> c = (StoreCounters<?>) storeCounters.get()
		                    .acquire();
		            try {
		                final int nbytes = length;
		                c.nreads++;
		                c.bytesRead += nbytes;
		                c.bytesReadFromDisk += nbytes;
		                c.elapsedReadNanos += (System.nanoTime() - begin);
		                c.elapsedDiskReadNanos += (System.nanoTime() - beginDisk);
		            } finally {
		                c.release();
		            }
				}
			} catch (PhysicalAddressResolutionException e) {
				throw new IllegalArgumentException("Unable to read data: "+e, e);
			} catch (Throwable e) {
				/*
				 * Note: ClosedByInterruptException can be thrown out of
				 * FileChannelUtility.readAll(), typically because the LIMIT on
				 * a query was satisfied, but we do not want to log that as an
				 * error.
				 */
//				log.error(e,e);
				throw new RuntimeException("addr=" + addr + " : cause=" + e, e);

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

//	static private final char[] HEX_CHAR_TABLE = {
//		   '0', '1','2','3',
//		   '4','5','6','7',
//		   '8','9','a','b',
//		   'c','d','e','f'
//		  };    

	// utility to display byte array of maximum i bytes as hexString
	static private String toHexString(final byte[] buf, int n) {
//		n = n < buf.length ? n : buf.length;
//		final StringBuffer out = new StringBuffer();
//		for (int i = 0; i < n; i++) {
//			final int v = buf[i] & 0xFF;
//			out.append(HEX_CHAR_TABLE[v >>> 4]);
//			out.append(HEX_CHAR_TABLE[v &0xF]);
//		}
//		return out.toString();
	    return BytesUtil.toHexString(buf, n);
	}

	public void free(final long laddr, final int sze) {
        
	    free(laddr, sze, null/* AlocationContext */);
	    
	}
//	private long m_unsafeFrees = 0;
    /**
     * free
     * <p>
     * If the address is greater than zero than it is interpreted as a physical
     * address and the allocators are searched to find the allocations.
     * Otherwise the address directly encodes the allocator index and bit
     * offset, allowing direct access to clear the allocation.
     * <p>
     * A blob allocator contains the allocator index and offset, so an allocator
     * contains up to 245 blob references.
     * 
     * @param laddr
     * @param sze
     * @param context
     */
	public void free(final long laddr, final int sze, final IAllocationContext context) {
	    assertOpen();
		final int addr = (int) laddr;
		
		switch (addr) {
		case 0:
		case -1:
		case -2:
			return;
		}
		m_allocationLock.lock();
		try {
			if (m_lockAddresses != null && m_lockAddresses.containsKey((int)laddr))
				throw new IllegalStateException("address locked: " + laddr);
			
			if (sze > m_maxFixedAlloc-4) {
				freeBlob(addr, sze, context);
			} else {
				final FixedAllocator alloc = getBlockByAddress(addr);
				
				/*
                 * There are a few conditions here. If the context owns the
                 * allocator and the allocation was made by this context then it
                 * can be freed immediately. The problem comes when the context
                 * is null and the allocator is NOT owned, BUT there are active
                 * AllocationContexts, in this situation, the free must ALWAYS
                 * be deferred.
                 * 
                 * If the MIN_RELEASE_AGE is ZERO then we can protect allocations
                 * and read-only transactions with Session protection, avoiding
                 * the need to manage deferred frees.
                 * 
                 * FIXME We need unit tests when MIN_RELEASE_AGE is GT ZERO.
                 * 
                 * FIXME We need unit test when MIN_RELEASE_AGE is ZERO AND
                 * there are open read-only transactions.
                 */
				if (m_minReleaseAge == 0) {
					/*
					 * The session protection is complicated by the mix of
					 * transaction protection and isolated AllocationContexts.
					 * 
					 * If this is the first use of an IAllocationContext then
					 * then isSessionProtected may return false, so check the
					 * context first.
					 */
					if (context != null) {
						if (alloc.canImmediatelyFree(addr, sze, context)) {
							immediateFree(addr, sze, true);
						} else {
							establishContextAllocation(context).deferFree(encodeAddr(addr, sze));
						}
					} else if (this.isSessionProtected()) {
						immediateFree(addr, sze, false);
					} else {
						immediateFree(addr, sze);
					}
				} else {
					// if a free request is made within a context not managed by
					// the allocator then it is not safe to free
	                boolean alwaysDefer = m_activeTxCount > 0;

					if (!alwaysDefer)
	                    alwaysDefer = context == null && !m_contexts.isEmpty();
					
	                if (alwaysDefer)
						if (log.isDebugEnabled())
						    log.debug("Should defer " + addr + " real: " + physicalAddress(addr));
	                if (alwaysDefer || !alloc.canImmediatelyFree(addr, sze, context)) {
						deferFree(addr, sze);
					} else {
						immediateFree(addr, sze);
					}
				}
			}
		} finally {
			m_allocationLock.unlock();
		}
		
	}
	
	private long encodeAddr(long alloc, final int nbytes) {
		alloc <<= 32;
		alloc += nbytes;

		return alloc;
	}

	long getHistoryRetention() {
		return m_minReleaseAge;
	}

	/**
	 * Session protection can only be used in preference to deferred frees when 
	 * the minReleaseAge is zero.  If so then two protection states are checked:
	 * either a positive activeTxCount incremented by the TransactionManager
	 * or if there are active AllocationContexts.
	 * 
	 * The activeTxCount essentially protects read-only transactions while the
	 * AllocationContexts enable concurrent store allocations, whilst also
	 * supporting immediate re-cycling of localized allocations (those made
	 * and released within the same AllocationContext).
	 * 
	 * Also check to see if there is an uncomplete quorum being established, in
	 * which case provide default session protection to avoid recycling.
	 * 
	 * @return whether there is a logical active session
	 */
	boolean isSessionProtected() {
	    
	    if (!m_allocationLock.isHeldByCurrentThread()) {
            /*
             * In order for changes to m_activeTxCOunt to be visible the caller
             * MUST be holding the lock.
             */
            throw new IllegalMonitorStateException();
        }
	    
	    // backoff until synchronization is implemented
//	    // protect recyling with unmet quorum
//	    if (m_quorum != null && !m_quorum.isQuorumMet()) {
//	    	return true;
//	    }
	    
	    return m_minReleaseAge == 0 && (m_activeTxCount > 0 || !m_contexts.isEmpty());
	}

	/**
	 * Sessions will only be used to protect transactions and read-only views
	 * when the m_minReleaseAge is no zero, otherwise the deferredFree
	 * approach will be used.
	 * 
	 * When called, will call through to the Allocators to re-sync the
	 * transient bits with the committed and live.
	 * 
	 * The writeCache is passed into the allocator to enable any "now free"
	 * allocations to be cleared from the cache.  Until the session is released
	 * the writeCache must be maintained to support readers of uncommitted and
	 * unwritten allocations.
	 */
	void releaseSessions() {
		assert(m_activeTxCount == 0 && m_contexts.isEmpty());
		
		if (m_minReleaseAge == 0) {
			if (log.isDebugEnabled())
				log.debug("RELEASE SESSIONS");
			for (FixedAllocator fa : m_allocs) {
				fa.releaseSession(m_writeCache);
			}
		}
	}
	
	private boolean freeBlob(final int hdr_addr, final int sze, final IAllocationContext context) {
		if (sze <= (m_maxFixedAlloc-4))
			throw new IllegalArgumentException("Unexpected address size");
		
        if (m_storageStats != null) {
        	m_storageStats.deleteBlob(sze);
        }

        final int alloc = m_maxFixedAlloc-4;
		final int blcks = (alloc - 1 + sze)/alloc;		
		
		// read in header block, then free each reference
		final byte[] hdr = new byte[(blcks+1) * 4 + 4]; // add space for checksum
		getData(hdr_addr, hdr);
		
		final DataInputStream instr = new DataInputStream(
				new ByteArrayInputStream(hdr, 0, hdr.length-4) );
		try {
			final int allocs = instr.readInt();
			int rem = sze;
			for (int i = 0; i < allocs; i++) {
				final int nxt = instr.readInt();
				free(nxt, rem < alloc ? rem : alloc);
				rem -= alloc;
			}
			free(hdr_addr, hdr.length);
			
			return true;
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}	
	
	private boolean freeImmediateBlob(final int hdr_addr, final int sze) {
		if (sze <= (m_maxFixedAlloc-4))
			throw new IllegalArgumentException("Unexpected address size");
		
        if (m_storageStats != null) {
        	m_storageStats.deleteBlob(sze);
        }

        final int alloc = m_maxFixedAlloc-4;
		final int blcks = (alloc - 1 + sze)/alloc;		
		
		// read in header block, then free each reference
		final byte[] hdr = new byte[(blcks+1) * 4 + 4]; // add space for checksum
		getData(hdr_addr, hdr);
		
		final DataInputStream instr = new DataInputStream(
				new ByteArrayInputStream(hdr, 0, hdr.length-4) );
		
		// retain lock for all frees
		m_allocationLock.lock();
		try {
			final int allocs = instr.readInt();
			int rem = sze;
			for (int i = 0; i < allocs; i++) {
				final int nxt = instr.readInt();
				immediateFree(nxt, rem <= alloc ? rem : alloc);
				rem -= alloc;
			}
			immediateFree(hdr_addr, hdr.length);
			
			return true;
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		} finally {
			m_allocationLock.unlock();
		}
	}

	//	private long immediateFreeCount = 0;
	private void immediateFree(final int addr, final int sze) {
		immediateFree(addr, sze, false);
	}
	
	private void immediateFree(final int addr, final int sze, final boolean overrideSession) {
		
		switch (addr) {
		case 0:
		case -1:
		case -2:
			return;
		}
		
		if (sze > (this.m_maxFixedAlloc-4)) {
			freeImmediateBlob(addr, sze);
			
			return;
		}
		
		m_allocationLock.lock();
		try {		
			final FixedAllocator alloc = getBlockByAddress(addr);
			final int addrOffset = getOffset(addr);
			if (alloc == null) {
				throw new IllegalArgumentException("Invalid address provided to immediateFree: " + addr + ", size: " + sze);
			}
            final long pa = alloc.getPhysicalAddress(addrOffset);
            
            if (log.isTraceEnabled())
                log.trace("Freeing allocation at " + addr + ", physical address: " + pa);
            alloc.free(addr, sze, overrideSession);
			// must clear after free in case is a blobHdr that requires reading!
			// the allocation lock protects against a concurrent re-allocation
			// of the address before the cache has been cleared
			assert pa != 0;
			// only clear any existing write to cache if no active session
			if (overrideSession || !this.isSessionProtected()) {
                // Only overwrite if NOT committed
                if (!alloc.isCommitted(addrOffset)) {
    				    m_writeCache.clearWrite(pa,addr);
//                    m_writeCache.overwrite(pa, sze);
                    /*
                     * Pass the size of the allocator, NOT the size of the
                     * allocation.
                     * 
                     * @see <a
                     * href="https://sourceforge.net/apps/trac/bigdata/ticket/586"
                     * > RWStore immedateFree() not removing Checkpoint
                     * addresses from the historical index cache. </a>
                     */
//                    removeFromExternalCache(pa, sze);
                    removeFromExternalCache(pa, alloc.m_size);
                }
			}
			m_frees++;
			
			if (alloc.isAllocated(addrOffset))
				throw new IllegalStateException("Reallocation problem with WriteCache");

			if (alloc.isUnlocked() && !m_commitList.contains(alloc)) {
				m_commitList.add(alloc);
			}
			
			m_recentAlloc = true;
		} finally {
			m_allocationLock.unlock();
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
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/586">
     *      RWStore immedateFree() not removing Checkpoint addresses from the
     *      historical index cache. </a>
     */
	void removeFromExternalCache(final long clr, final int slotSize) {

	    assert m_allocationLock.isLocked();

        if (m_externalCache == null)
            return;

        if (slotSize == 0 || slotSize == m_cachedDatasize) {

            /*
             * Either known to be the same slot size as a checkpoint record -or-
             * the slot size is not known.
             */

            m_externalCache.remove(clr);
            
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
	/** Core allocation method. */
	public int alloc(final int size, final IAllocationContext context) {
		if (size > m_maxFixedAlloc) {
			throw new IllegalArgumentException("Allocation size to big: " + size + " > " + m_maxFixedAlloc);
		}
		
		m_allocationLock.lock();
		try {
			try {
				final FixedAllocator allocator;
				final int i = fixedAllocatorIndex(size);
				if (context != null) {
					allocator = establishContextAllocation(context).getFreeFixed(i);
				} else {
					final int block = 64 * m_allocSizes[i];
					m_spareAllocation += (block - size); // Isn't adjusted by frees!
					
					final ArrayList<FixedAllocator> list = m_freeFixed[i];
					if (list.size() == 0) {

						allocator = new FixedAllocator(this, block);
						
						allocator.setFreeList(list);
						allocator.setIndex(m_allocs.size());

						if (log.isTraceEnabled())
							log.trace("New FixedAllocator for " + block);

						m_allocs.add(allocator);
						
						if (m_storageStats != null) {
							m_storageStats.register(allocator, true);
						}
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
						allocator = list.get(0);
					}
					
				}
				
				final int addr = allocator.alloc(this, size, context);

				if (allocator.isUnlocked() && !m_commitList.contains(allocator)) {
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
	
	private int fixedAllocatorIndex(final int size) {
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
     * Called by PSOutputStream to make to actual allocation or directly by
     * lower level API clients.
     * <p>
     * If the allocation is for greater than MAX_FIXED_ALLOC, then a
     * PSOutputStream is used to manage the chained buffers.
     * 
     * TODO: Instead of using PSOutputStream, manage allocations written to the
     * WriteCacheService, building BlobHeader as you go.
     **/
    public long alloc(final byte buf[], final int size,
            final IAllocationContext context) {

        final long begin = System.nanoTime();
        
        if (size > (m_maxFixedAlloc - 4)) {
        
            if (size > getMaxBlobSize())
                throw new IllegalArgumentException(
                        "Allocation request beyond maximum BLOB of " + getMaxBlobSize());

            if (log.isTraceEnabled())
                log.trace("BLOB ALLOC: " + size);
            
            if (m_storageStats != null) {
            	m_storageStats.allocateBlob(size);
            }

            final PSOutputStream psout = PSOutputStream.getNew(this,
                    m_maxFixedAlloc, context);
            try {
                
                int i = 0;
                final int blocks = size/512;
                for (int b = 0; b < blocks; b++) {
                    psout.write(buf, i, 512); // add 512 bytes at a time
                    i += 512;
                }
                psout.write(buf, i, size - i);

                return psout.save();
            
            } catch (IOException e) {
                
                throw new RuntimeException("Closed Store?", e);
                
            } finally {
            	try {
            		psout.close(); // return stream
            	} catch (IOException ioe) {
            		// should not happen, since this should only be
            		// recycling
            		log.warn("Unexpected error closing PSOutputStream", ioe);
            	}
            }

        }

		final int newAddr = alloc(size + 4, context); // allow size for checksum
		
		if (newAddr == 0)
			throw new IllegalStateException("NULL address allocated");

		final int chk = ChecksumUtility.getCHK().checksum(buf, size);
		
		final long pa = physicalAddress(newAddr);

		try {
			m_writeCache.write(pa, ByteBuffer.wrap(buf, 0, size), chk, true/*writeChecksum*/, newAddr/*latchedAddr*/);
		} catch (InterruptedException e) {
            throw new RuntimeException("Closed Store?", e);
		}

        // Update counters.
        final StoreCounters<?> c = (StoreCounters<?>) storeCounters.get()
                .acquire();
        try {
            final int nwrite = size + 4;// size plus checksum.
            c.nwrites++;
            c.bytesWritten += nwrite;
            c.elapsedWriteNanos += (System.nanoTime() - begin);
            if (nwrite > c.maxWriteSize) {
                c.maxWriteSize = nwrite;
            }
        } finally {
            c.release();
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
     * The semantics of reset are to revert unisolated writes to committed state.
     * 
     * Unisolated writes must also be removed from the write cache.
     * 
     * The AllocBlocks of the FixedAllocators maintain the state to determine
     * the correct reset behavior.
     * 
     * If the store is using DirectFixedAllocators then an IllegalStateException
     * is thrown
     */
    public void reset() {

        if (log.isInfoEnabled()) {
            log.info("RWStore Reset");
        }
	    m_allocationLock.lock();
		try {
	        assertOpen();
	        
	        boolean isolatedWrites = false;
            /**
             * Clear all allocators, not just dirty allocators, since we also
             * need to reset the transient bits associated with session
             * protection.
             * 
             * Need to know if there are any isolated modifications, in which case
             * we must remember so that we avoid clearing down the store.
             */
            for (FixedAllocator fa : m_allocs) {
            	isolatedWrites = isolatedWrites || fa.reset(m_writeCache, m_committedNextAllocation);
			}
            
            if (!isolatedWrites) {
	            /**
	             * Now we should be able to unwind any unused allocators and unused
	             * alloc blocks.  An unused allocator is one with no diskAddr (never
	             * committed).  But it may be more difficult to determine if
	             * an alloc block has never been used, for that we really need to
	             * know what the nextAllocationOffset was at the previous commit.
	             * This could be cached as lastCommittedOffset, in which case we can unwind any
	             * allocBlocks with addresses >= to that.
	             */
	            int origAllocs = m_allocs.size();
	            while (m_allocs.size() > 0) {
	            	final int last = m_allocs.size()-1;
	            	final FixedAllocator fa = m_allocs.get(last);
	            	if (fa.getDiskAddr() == 0) {
	            		// must remove from free list!
	            		m_freeFixed[fixedAllocatorIndex(fa.m_size)].remove(fa);
	            		// ..and then from main allocation list
	            		m_allocs.remove(last);
	            	} else {
	            		break;
	            	}
	            }
	            m_nextAllocation = m_committedNextAllocation;
	            if (log.isDebugEnabled())
	            	log.debug("Reset allocators, old: " + origAllocs + ", now: " + m_allocs.size());
	            
				// Clear the dirty list.
	            // FIXME: we should be able to clear the dirty list, but this currently causes
	            //	problems in HA.
	            // If the allocators are torn down correctly, we should be good to clear the commitList
				m_commitList.clear();
            }
            if (m_quorum != null) {
                /**
                 * When the RWStore is part of an HA quorum, we need to close
                 * out and then reopen the WriteCacheService every time the
                 * quorum token is changed. For convienence, this is handled by
                 * extending the semantics of abort() on the Journal and reset()
                 * on the RWStore.
                 * 
                 * @see <a
                 *      href="https://sourceforge.net/apps/trac/bigdata/ticket/530">
                 *      HA Journal </a>
                 */
                m_writeCache.close();
                m_writeCache = newWriteCache();
            }
            /*
             * Discard any writes on the delete blocks. Those deletes MUST NOT
             * be applied after a reset() on the RWStore.
             * 
             * @see https://sourceforge.net/apps/trac/bigdata/ticket/602
             * (RWStore does not discard deferred deletes on reset)
             */
            m_deferredFreeOut.reset();
		} catch (Exception e) {
			throw new IllegalStateException("Unable to reset the store", e);
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
		// the cDefaultMetaBitsSize is also written since this can now be
		//	parameterized.
	    final int len = 4 * (cMetaHdrFields + m_allocSizes.length + m_metaBits.length);
		final byte buf[] = new byte[len];

        final FixedOutputStream str = new FixedOutputStream(buf);
        try {
            str.writeInt(cVersion);
            str.writeLong(m_lastDeferredReleaseTime);
            str.writeInt(cDefaultMetaBitsSize);            
            str.writeInt(m_allocSizes.length);           
            str.writeLong(m_storageStatsAddr);

            // Let's reserve ourselves some space 
            for (int i = 0; i < cReservedMetaBits; i++) {
            	str.writeInt(0);
            }
            
            /*
             * Write out the size of the allocation slots as defined by
             * Options.ALLOCATION_SIZES (this is where we store that
             * information).
             */
            for (int i = 0; i < m_allocSizes.length; i++) {
                str.writeInt(m_allocSizes[i]);
            }
            
            /*
             * Write out XXX
             */
            for (int i = 0; i < m_metaBits.length; i++) {
                str.writeInt(m_metaBits[i]);
            }

            str.flush();
        } finally {
            str.close();
        }

        /*
         * Note: this address is set by commit() prior to calling
         * writeMetaBits().
         */
		final long addr = physicalAddress(m_metaBitsAddr);
		if (addr == 0) {
			throw new IllegalStateException("Invalid metabits address: " + m_metaBitsAddr);
		}
		
		assert addr > 0;
		
		try {
			m_writeCache.write(addr, ByteBuffer.wrap(buf), 0/*chk*/, false/*useChecksum*/, m_metaBitsAddr/*latchedAddr*/);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

//	static final float s_version = 3.0f;
//
//	public String getVersionString() {
//		return "RWStore " + s_version;
//	}

	public void commit() {
	    assertOpen();
		checkCoreAllocations();

		// take allocation lock to prevent other threads allocating during commit
		m_allocationLock.lock();
		
		try {
		
//			final int totalFreed = checkDeferredFrees(true, journal); // free now if possible
//			
//			if (totalFreed > 0 && log.isInfoEnabled()) {
//				log.info("Freed " + totalFreed + " deferralls on commit");
//			}
			// free old storageStatsAddr
			if (m_storageStatsAddr != 0) {
				final int len = (int) (m_storageStatsAddr & 0xFFFF);				
				final int addr = (int) (m_storageStatsAddr >> 16);
            	    immediateFree(addr, len);
			}
			if (m_storageStats != null) {
				final byte[] buf = m_storageStats.getData();
				final long addr = alloc(buf, buf.length, null);
				m_storageStatsAddr = (addr << 16) + buf.length;
			}

            /*
             * Pre-allocate storage for metaBits from FixedAllocators (ensure
             * that we do not need to reallocate the metabits region when we are
             * writing out the updated versions of the FixedAllocators).
             */
			final long oldMetaBits = m_metaBitsAddr;
			final int oldMetaBitsSize = (m_metaBits.length + m_allocSizes.length + 1) * 4;
			m_metaBitsAddr = alloc(getRequiredMetaBitsStorage(), null);

			// DEBUG SANITY CHECK!
			if (physicalAddress(m_metaBitsAddr) == 0) {
				throw new IllegalStateException("Returned MetaBits Address not valid!");
			}
			
			// Call immediateFree - no need to defer freeof metaBits, this
			//	has to stop somewhere!
			// No more allocations must be made
			immediateFree((int) oldMetaBits, oldMetaBitsSize);

			// There must be no buffered deferred frees
			// assert m_deferredFreeOut.getBytesWritten() == 0;

			// save allocation headers
			final Iterator<Allocator> iter = m_commitList.iterator();
			
			while (iter.hasNext()) {
			    
				final Allocator allocator = iter.next();
				
				// the bit in metabits for the old allocator version.
				final int old = allocator.getDiskAddr();

				// mark old version - reclaimed after commit.
				metaFree(old);
				
				// the bit in metabits for the new allocator version.
				final int naddr = metaAlloc();

				// set that bit on the allocator.
				allocator.setDiskAddr(naddr);
				
                if (log.isTraceEnabled())
                    log.trace("Update allocator " + allocator.getIndex()
                            + ", old addr: " + old + ", new addr: " + naddr);

				try {

				    // do not use checksum
				    m_writeCache.write(metaBit2Addr(naddr), ByteBuffer
                        .wrap(allocator.write()), 0/*chk*/, false/*useChecksum*/,0/*latchedAddr*/);
				    
				} catch (InterruptedException e) {
					
				    throw new RuntimeException(e);
				    
				}
			}
			m_commitList.clear();

			writeMetaBits();

			try {
				m_writeCache.flush(true);
				lastBlockSequence = m_writeCache.resetSequence();
			} catch (InterruptedException e) {
			    log.error(e, e);
				throw new RuntimeException(e);
			}
			
			// Now remember the committed next allocation that will be checked in reset()
			m_committedNextAllocation = m_nextAllocation;

			// Should not write rootBlock, this is responsibility of client
			// to provide control
			// writeFileSpec();

			m_metaTransientBits = (int[]) m_metaBits.clone();

//				if (m_commitCallback != null) {
//					m_commitCallback.commitComplete();
//				}

			// The Journal handles the force in doubleSync
			// m_reopener.reopenChannel().force(false); // TODO, check if required!
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
     * 
     * returns number of addresses freed
     */
    public /* public */int checkDeferredFrees(//final boolean freeNowIsIgnored,
            final AbstractJournal journal) {
//        return 0;
//        // Note: Invoked from unit test w/o the lock...
//        if (!m_allocationLock.isHeldByCurrentThread())
//            throw new IllegalMonitorStateException();
        
        if (journal == null)
            return 0;

        /**
         * Since this is now called direct from the AbstractJournal commit
         * method we must take the allocation lock.
         * 
         * This may have adverse effects wrt concurrency deadlock issues, but
         * none have been noticed so far.
         */
        m_allocationLock.lock();

        try {
            /**
             * if session protected then do not free any deferrals!
             */
            if (isSessionProtected()) {
                return 0;
            }

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

            if (lastCommitTime <= latestReleasableTime) {
                throw new AssertionError("lastCommitTime=" + lastCommitTime
                        + ", latestReleasableTime=" + latestReleasableTime
                        + ", lastDeferredReleaseTime="
                        + m_lastDeferredReleaseTime + ", activeTxCount="
                        + m_activeTxCount);
            }
            
// Note: This is longer true. Delete blocks are attached to the
// commit point in which the deletes were made. 
//              /*
//               * add one because we want to read the delete blocks for all
//               * commit points up to and including the first commit point that
//               * we may not release.
//               */
//              latestReleasableTime++;

//              /*
//               * add one to give this inclusive upper bound semantics to the
//               * range scan.
//               */
//              latestReleasableTime++;

            if (txLog.isInfoEnabled())
                txLog.info("RECYCLER: lastCommitTime=" + lastCommitTime
                        + ", latestReleasableTime=" + latestReleasableTime
                        + ", lastDeferredReleaseTime="
                        + m_lastDeferredReleaseTime + ", activeTxCount="
                        + m_activeTxCount);

            /*
             * Free deferrals.
             * 
             * Note: Per ticket#480, we can not begin recycling from the first
             * commit point in the commit record index as there are some bigdata
             * versions (1.0.4) where we did not prune the commit record index.
             * Therefore, this relies on the (lastDeferredReleaseTime+1) for the
             * exclusive lower bound. This is avoids triggering an exception
             * from an attempt to process deferred free blocks which have
             * already been released.
             * 
             * @see https://sourceforge.net/apps/trac/bigdata/ticket/480
             */
            return freeDeferrals(journal, m_lastDeferredReleaseTime + 1,
                    latestReleasableTime);

        } finally {
        
            m_allocationLock.unlock();
            
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
		int ints = cMetaHdrFields;
		ints += m_allocSizes.length + m_metaBits.length;
		
		// add the maximum number of new metaBits storage that may be
		//	needed to save the current committed objects
		final int commitInts = ((32 + m_commitList.size()) / 32);
		final int allocBlocks = (cDefaultMetaBitsSize - 1 + commitInts)/(cDefaultMetaBitsSize-1);
		ints += cDefaultMetaBitsSize * allocBlocks;
		
		return ints*4; // return as bytes
	}

	// Header Data
//	volatile private long m_curHdrAddr = 0;
//	volatile private int m_rootAddr;

    /**
     * {@link #m_fileSize} is in units of -32K.
     */
	volatile private int m_fileSize;
	volatile private int m_nextAllocation;
	/**
	 * The value of nextAllocation at commit is cached and used
	 * in reset() to unwind new FixedAllocators and/or AllocBlocks
	 */
	volatile private int m_committedNextAllocation;
	final private int m_maxFileSize;

//	private int m_headerSize = 2048;

    /*
     * Meta Allocator
     */
	
	/**
	 * MetaBits HEADER version must be changed when the header or allocator
	 * serialization changes
	 * 
	 * Use BCD-style numbering so
	 * 0x0200 == 2.00
	 * 0x0320 == 3.20
	 * 
	 * The minor byte values should maintain binary compatibility, with
	 * major bytes
	 * Versions
	 * 0x0300 - extended header to include reserved ints
	 * 0x0400 - removed explicit BlobAllocators
	 */
	final private int cVersion = 0x0400;
	
    /**
     * cReservedMetaBits is the reserved space in the metaBits header
     * to alloc for binary compatibility moving forward.
     * 
     * If we need to add int values to the header we can do so and reduce the
     * reservation by 1 each time
     */
    final int cReservedMetaBits = 20;
    
    /**
     * MetaBits Header
     * 0 int version
     * 1-2 int[2] long deferredFree
     * 3 int defaultMetaBitsSize
     * 4 int length of allocation sizes
     * 5-6 int[2] storage stats addr
     * + 20 reserved
     */
	final private int cMetaHdrFields = 7 + cReservedMetaBits;  
	/**
	 * @see Options#META_BITS_SIZE
	 */
	private int cDefaultMetaBitsSize;
	/**
	 * @see Options#META_BITS_SIZE
	 */
    volatile private int m_metaBitsSize;
	/**
	 * Package private since is uded by FixedAllocators
	 * 
	 * @see Options#META_BITS_SIZE
	 */
	final int cDefaultFreeBitsThreshold;
	
	/**
	 * Each "metaBit" is a file region
	 */
	private int m_metaBits[];
	private int m_metaTransientBits[];
	// volatile private int m_metaStartAddr;
    private volatile int m_metaBitsAddr;
    // @todo javadoc please.
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

            assert bit >= 0;
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

    /**
     * Search the metabits for a bit that is free for allocation of space that
     * an allocator could write on.
     * 
     * @return The bit -or- <code>-1</code> if the meta bits region is currently
     *         ful.
     */
    private int fndMetabit() {
        final int blocks = m_metaBits.length / cDefaultMetaBitsSize;
        for (int b = 0; b < blocks; b++) {
            final int ret = fndBit(m_metaTransientBits,
                    (b * cDefaultMetaBitsSize) + 1, cDefaultMetaBitsSize-1);
            
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
		
		m_writeCache.clearWrite(metaBit2Addr(bit),0/*latchedAddr*/);
	}

    /**
     * The metabits are encoded in {@link #cDefaultMetaBitsSize} int runs as
     * follows
     * 
     * <pre>
     * [startAddr1][bits0][bits1]...[bitsN]
     * [startAddr2]...
     * ...
     * </pre>
     * 
     * where <code>N</code> is {@link #cDefaultMetaBitsSize} MINUS TWO and
     * <code>[bits0]...[bitsN]</code> are interpreted as a bit map.
     * <p>
     * The bit parameter is processed to determine which run it is part of.
     * <p>
     * Note that the bit offsets are not contiguous since there are "holes"
     * where the meta allocation [startAddr] are stored.
     * <p>
     * When the metabits region is first created, and each time it is grown, a
     * region is reserved at the then current nextOffset on the file that is
     * used for {@link FixedAllocator}s associated with the bit vector in the
     * next run of the metabits block. Those {@link FixedAllocator}s will be
     * recruited and used as needed. Note that {@link FixedAllocator}s are
     * always written onto an unused "bit" at each commit, and the old "bit" is
     * then freed. Thus dirty {@link FixedAllocator}s move at each commit and
     * can move between runs in the metabits.
     */
	long metaBit2Addr(final int bit) {
//		final int bitsPerBlock = 9 * 32;
		
	    /*
	     * The integer index into the m_metaBits[].
	     */
		final int intIndex = bit / 32; // divide 32;

        /*
         * Make sure that the [bit] is a bit that falls into one of the bit
         * regions (versus one of the startAddr int32 values).
         */

		assert intIndex % cDefaultMetaBitsSize != 0; // used by the start addrs!
		
        /*
         * The index into the metabits region corresponding to the int32 value
         * before the start of the bit vector in which this bit falls. This
         * offset is relative to the start of the m_metaBits[].
         */
		
        final int addrIndex = (intIndex / cDefaultMetaBitsSize)
                * cDefaultMetaBitsSize;

        /*
         * Pull out convert the startAddr for the bit vector addressed by that
         * bit. This gives us the int64 byte offset of some region on the
         * backing file.
         */
        final long addr = convertAddr(m_metaBits[addrIndex]);

        /*
         * The bit index of this bit in the bit vector for this region in the
         * metaBits[].
         */
        final int intOffset = bit - ((addrIndex + 1) * 32);

        /*
         * The byte offset into the backing file of the FixedAllocator for that
         * bit. All FixedAllocators are the same size [ALLOC_BLOCK_SIZE]. The
         * FixedAllocator knows what size allocations it makes and manages the
         * regions on the backing store in which those allocation are made.
         */
        final long ret = addr + (ALLOC_BLOCK_SIZE * intOffset);

        return ret;

	}

    /**
     * Convert an implicitly scaled int32 offset into the backing file into an
     * int64 address into the backing file.
     * 
     * @param addr
     *            An int32 offset into the backing file formed by
     *            {@link #convertFromAddr(long)}. The representation is a
     *            negative integer that has been left shifted by
     *            {@link #ALLOCATION_SCALEUP} to reduce its bit size.
     * 
     * @return A byte offset in the backing file.
     * 
     * @see #convertFromAddr(long)
     * @see #ALLOCATION_SCALEUP
     */
    public static long convertAddr(final int addr) {
        final long laddr = addr;
        if (laddr < 0) {
            final long ret = (-laddr) << ALLOCATION_SCALEUP;
            return ret;
        } else {
            return laddr & 0xFFFFFFF0;
        }
    }

    /**
     * Convert an int64 address into the backing file into an int32 offset that
     * is implicitly scaled by {@link #ALLOCATION_SCALEUP}.
     * 
     * @param addr
     *            An int64 offset into the backing file.
     * 
     * @return The implicitly scaled int32 offset.
     * @see #convertAddr(int)
     * @see #ALLOCATION_SCALEUP
     */
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

			m_reopener.reopenChannel();
			m_reopener.raf.setLength(toAddr);
            storeCounters.get().ntruncate++;
			
            // must ensure writeCache is in sync for HA
			m_writeCache.setExtent(toAddr);

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
	 * <dl>
	 * <dt>AllocatorSize</dt><dd>The #of bytes in the allocated slots issued by this allocator.</dd>
	 * <dt>AllocatorCount</dt><dd>The #of fixed allocators for that slot size.</dd>
	 * <dt>SlotsInUse</dt><dd>The difference between the two previous columns (net slots in use for this slot size).</dd>
	 * <dt>SlotsReserved</dt><dd>The #of slots in this slot size which have had storage reserved for them.</dd>
	 * <dt>SlotsAllocated</dt><dd>Cumulative allocation of slots to date in this slot size (regardless of the transaction outcome).</dd>
	 * <dt>SlotsRecycled</dt><dd>Cumulative recycled slots to date in this slot size (regardless of the transaction outcome).</dd>
	 * <dt>SlotsChurn</dt><dd>How frequently slots of this size are re-allocated (SlotsInUse/SlotsAllocated).</dd>
	 * <dt>%SlotsUnused</dt><dd>The percentage of slots of this size which are not in use (1-(SlotsInUse/SlotsReserved)).</dd>
	 * <dt>BytesReserved</dt><dd>The space reserved on the backing file for those allocation slots</dd>
	 * <dt>BytesAppData</dt><dd>The #of bytes in the allocated slots which are used by application data (including the record checksum).</dd>
	 * <dt>%SlotWaste</dt><dd>How well the application data fits in the slots (BytesAppData/(SlotsInUse*AllocatorSize)).</dd>
	 * <dt>%AppData</dt><dd>How much of your data is stored by each allocator (BytesAppData/Sum(BytesAppData)).</dd>
	 * <dt>%StoreFile</dt><dd>How much of the backing file is reserved for each allocator (BytesReserved/Sum(BytesReserved)).</dd>
	 * <dt>%StoreWaste</dt><dd>How much of the total waste on the store is waste for this allocator size ((BytesReserved-BytesAppData)/(Sum(BytesReserved)-Sum(BytesAppData))).</dd>
	 * </dl>
	 * @see StorageStats#showStats(StringBuilder)
	 */
	public void showAllocators(final StringBuilder str) {
		m_storageStats.showStats(str);
//		final AllocationStats[] stats = new AllocationStats[m_allocSizes.length];
//		for (int i = 0; i < stats.length; i++) {
//			stats[i] = new AllocationStats(m_allocSizes[i]*64);
//		}
//		
//		final Iterator<FixedAllocator> allocs = m_allocs.iterator();
//		while (allocs.hasNext()) {
//			Allocator alloc = (Allocator) allocs.next();
//			alloc.appendShortStats(str, stats);
//		}
//		
//		// Append Summary
//		str.append("\n-------------------------\n");
//		str.append("RWStore Allocation Summary\n");
//		str.append("-------------------------\n");
//		str.append(padRight("Allocator", 10));
//		str.append(padLeft("SlotsUsed", 12));
//		str.append(padLeft("reserved", 12));
//		str.append(padLeft("StoreUsed", 14));
//		str.append(padLeft("reserved", 14));
//		str.append(padLeft("Usage", 8));
//		str.append(padLeft("Store", 8));
//		str.append("\n");
//		long treserved = 0;
//		long treservedSlots = 0;
//		long tfilled = 0;
//		long tfilledSlots = 0;
//		for (int i = 0; i < stats.length; i++) {
//		    final long reserved = stats[i].m_reservedSlots * stats[i].m_blockSize;
//			treserved += reserved;
//			treservedSlots += stats[i].m_reservedSlots;
//			final long filled = stats[i].m_filledSlots * stats[i].m_blockSize;
//			tfilled += filled;
//			tfilledSlots += stats[i].m_filledSlots;
//		}
//		for (int i = 0; i < stats.length; i++) {
//		    final long reserved = stats[i].m_reservedSlots * stats[i].m_blockSize;
//			final long filled = stats[i].m_filledSlots * stats[i].m_blockSize;
//			str.append(padRight("" + stats[i].m_blockSize, 10));
//			str.append(padLeft("" + stats[i].m_filledSlots, 12) + padLeft("" + stats[i].m_reservedSlots, 12));
//			str.append(padLeft("" + filled, 14) + padLeft("" + reserved, 14));
//			str.append(padLeft("" + (reserved==0?0:(filled * 100 / reserved)) + "%", 8));
//			str.append(padLeft("" + (treserved==0?0:(reserved * 100 / treserved)) + "%", 8));
//			str.append("\n");
//		}
//		str.append("\n");
//
//		str.append(padRight("Totals", 10));
//        str.append(padLeft("" + tfilledSlots, 12));
//        str.append(padLeft("" + treservedSlots, 12));
//        str.append(padLeft("" + tfilled, 14));
//        str.append(padLeft("" + treserved, 14));
//        str.append(padLeft("" + (treserved==0?0:(tfilled * 100 / treserved)) + "%", 8));
//        str.append("\nFile size: " + convertAddr(m_fileSize) + "bytes\n");
    }
	
//	private String padLeft(String str, int minlen) {
//		if (str.length() >= minlen)
//			return str;
//		
//		StringBuffer out = new StringBuffer();
//		int pad = minlen - str.length();
//		while (pad-- > 0) {
//			out.append(' ');
//		}
//		out.append(str);
//		
//		return out.toString();
//	}
//	private String padRight(String str, int minlen) {
//		if (str.length() >= minlen)
//			return str;
//		
//		StringBuffer out = new StringBuffer();
//		out.append(str);
//		int pad = minlen - str.length();
//		while (pad-- > 0) {
//			out.append(' ');
//		}
//		
//		return out.toString();
//	}

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

	/**
	 * Given a physical address (byte offset on the store), return true if that
	 * address could be managed by an allocated block.
	 * 
	 * @param a
	 *            the storage address to be tested.
	 */
	public boolean verify(final long laddr) {
		
	    final int addr = (int) laddr;

		if (addr == 0) {
			return false;
		}

		return getBlockByAddress(addr) != null;
	}

    /*****************************************************************************
     * Address transformation: latched2Physical
     */
	
    /**
     * Return the byte offset in the file.
     * 
     * @param addr
     *            The latched address.
     * 
     * @return The byte offset in the file.
     */
    final private long physicalAddress(final int addr, final boolean nocheck) {

        if (addr >= 0) {

            return addr & 0xFFFFFFE0;

        } else {

            // Find the allocator.
            final FixedAllocator allocator = getBlock(addr);

            // Get the bit index into the allocator.
            final int offset = getOffset(addr);

            // Translate the bit index into a byte offset on the file.
            final long laddr = allocator.getPhysicalAddress(offset, nocheck);

            return laddr;
        }

	}
	
    /**
     * Return the byte offset in the file.
     * 
     * @param addr
     *            A latched address.
     *            
     * @return The byte offset.
     */
	final public long physicalAddress(final int addr) {

        return physicalAddress(addr, false/* nocheck */);

	}

	/********************************************************************************
	 * handle dual address format, if addr is positive then it is the physical
	 * address, so the Allocators must be searched.
	 **/
	FixedAllocator getBlockByAddress(final int addr) {
		if (addr < 0) {
			return getBlock(addr);
		}

		final Iterator<FixedAllocator> allocs = m_allocs.iterator();

		FixedAllocator alloc = null;
		while (allocs.hasNext()) {
			alloc = allocs.next();

			if (alloc.addressInRange(addr)) {
				break;
			}
			alloc = null;
		}

		return alloc;
	}

    /**
     * Get the {@link FixedAllocator} for a latched address.
     * 
     * @param addr
     *            The latched address.
     * 
     * @return The {@link FixedAllocator} for that latched address.
     */
	private FixedAllocator getBlock(final int addr) {
	    
	    // index of the FixedAllocator for that latched address.
		final int index = (-addr) >>> OFFSET_BITS;
		
		if (index >= m_allocs.size()) {
			throw new PhysicalAddressResolutionException(addr);
		}

		// Return the FixedAllocator for that index.
		return m_allocs.get(index);
	}

    /**
     * Return the bit index into a {@link FixedAllocator}.
     * <p>
     * Note: This is directly encoded by the latched address. You do not need to
     * know which {@link FixedAllocator} is being addressed in order to figure
     * this out.
     * 
     * @param addr
     *            A latched address.
     *            
     * @return The bit index into the {@link FixedAllocator}.
     */
	private int getOffset(final int addr) {

	    return (-addr) & OFFSET_BITS_MASK; // OFFSET_BITS
	    
	}

	/**
	 * The {@link RWStore} always generates negative address values.
	 * 
	 * @return whether the address given is a native IStore address
	 */
	public boolean isNativeAddress(final long addr) {
		return addr <= 0;
	}

	public File getStoreFile() {
		return m_fd;
	}

	public boolean requiresCommit() {
		return m_recentAlloc;
	}

	/**
	 * The 
	 * @return long representation of metaBitsAddr PLUS the size
	 */
	public long getMetaBitsAddr() {
		long ret = physicalAddress((int) m_metaBitsAddr);
		ret <<= 16;
		
		// include space for version, allocSizes and deferred free info AND cDefaultMetaBitsSize
		final int metaBitsSize = cMetaHdrFields + m_metaBits.length + m_allocSizes.length;
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

        assertOpen();
        
        try {
        
            m_writeCache.flush(metadata);

            // sync the disk.
            m_reopener.reopenChannel().force(metadata);

            final StoreCounters<?> c = (StoreCounters<?>) storeCounters.get()
                    .acquire();
            try {
                c.nforce++;
            } finally {
                c.release();
            }
            
        } catch (InterruptedException e) {
            
            throw new ClosedByInterruptException();
            
        }

    }

    /** The # of allocation requests made. */
    public long getTotalAllocations() {
		return m_allocations;
	}

    /**
     * The # of free requests made
     */
	public long getTotalFrees() {
		return m_frees;
	}
	
    /**
     * The # of bytes requested - as opposed to the size of the slots allocated.
     */
	public long getTotalAllocationsSize() {
		return m_nativeAllocBytes;
	}

    /**
     * A Blob Allocator maintains a list of Blob headers. The allocator stores
     * up to 255 blob headers plus a checksum. When a request is made to read the
     * blob data, the blob allocator retrieves the blob header and reads the
     * data from that into the passed byte array.
     */
//    public int registerBlob(final int addr) {
//		m_allocationLock.lock();
//		try {
//			BlobAllocator ba = null;
//			if (m_freeBlobs.size() > 0) {
//				ba = (BlobAllocator) m_freeBlobs.get(0);
//			}
//			if (ba == null) {
//				final Allocator lalloc = (Allocator) m_allocs.get(m_allocs.size() - 1);
//				// previous block start address
//				final int psa = lalloc.getRawStartAddr();
//				assert (psa - 1) > m_nextAllocation;
//				ba = new BlobAllocator(this, psa - 1);
//				ba.setFreeList(m_freeBlobs); // will add itself to the free list
//				ba.setIndex(m_allocs.size());
//				m_allocs.add(ba);
//			}
//
//			if (!m_commitList.contains(ba)) {
//				m_commitList.add(ba);
//			}
//
//			return ba.register(addr);
//		} finally {
//			m_allocationLock.unlock();
//		}
//	}

	void addToCommit(final Allocator allocator) {
		if (!m_commitList.contains(allocator)) {
			m_commitList.add(allocator);
		}
	}


	void removeFromCommit(final Allocator allocator) {
		m_commitList.remove(allocator);
	}

	public Allocator getAllocator(final int i) {
		return (Allocator) m_allocs.get(i);
	}

    /**
     * Simple implementation for a {@link RandomAccessFile} to handle the direct
     * backing store.
     */
    private class ReopenFileChannel implements
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

        public FileChannel reopenChannel() throws IOException {

			/*
			 * Note: This is basically a double-checked locking pattern. It is
			 * used to avoid synchronizing when the backing channel is already
			 * open.
			 */
			{
				final RandomAccessFile tmp = raf;
				if (tmp != null) {
					final FileChannel channel = tmp.getChannel();
					if (channel.isOpen()) {
						// The channel is still open.
						return channel;
					}
				}
			}
        	
        	synchronized(this) {

				if (raf != null) {
					final FileChannel channel = raf.getChannel();
					if (channel.isOpen()) {
						/*
						 * The channel is still open. If you are allowing
						 * concurrent reads on the channel, then this could
						 * indicate that two readers each found the channel
						 * closed and that one was able to re-open the channel
						 * before the other such that the channel was open again
						 * by the time the 2nd reader got here.
						 */
						return channel;
					}
				}

				// open the file.
				this.raf = new RandomAccessFile(file, mode);

				// Update counters.
				final StoreCounters<?> c = (StoreCounters<?>) storeCounters
						.get().acquire();
				try {
					c.nreopen++;
				} finally {
					c.release();
				}

				return raf.getChannel();

			}

        }

    }
    
    /**
     * If the current file extent is different from the required extent then the
     * call is made to {@link #extendFile(int)}.
     * 
     * @param extent
     *            The new file extent.
     */
	public void establishExtent(final long extent) {
		
	    assertOpen();

	    final long currentExtent = convertAddr(m_fileSize);
		
		if (extent > currentExtent) {
			
		    extendFile(convertFromAddr(extent - currentExtent));
		    
		} else if (extent < currentExtent) {
            throw new IllegalArgumentException(
                    "Cannot shrink RWStore extent: currentExtent="
                            + currentExtent + ", fileSize=" + m_fileSize
                            + ", newValue=" + extent);
        }
		
	}
	
	/**
	 * @return number of FixedAllocators
	 */
	public int getFixedAllocatorCount() {
		int fixed = 0;
		final Iterator<FixedAllocator> allocs = m_allocs.iterator();
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
		final Iterator<FixedAllocator> allocs = m_allocs.iterator();
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
		final Iterator<FixedAllocator> allocs = m_allocs.iterator();
		while (allocs.hasNext()) {
			final FixedAllocator alloc = allocs.next();
			allocated += ((FixedAllocator) alloc).getFileStorage();
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
		final Iterator<FixedAllocator> allocs = m_allocs.iterator();
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
			if (sze > (this.m_maxFixedAlloc-4)) {
				m_deferredFreeOut.writeInt(-rwaddr);
				m_deferredFreeOut.writeInt(sze);
			} else {
				m_deferredFreeOut.writeInt(rwaddr);				
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
	 * @return the total number of addresses freed
	 */
	private int freeDeferrals(final long blockAddr, final long lastReleaseTime) {
		final int addr = (int) (blockAddr >> 32);
		final int sze = (int) blockAddr & 0xFFFFFF;
		
		if (log.isTraceEnabled())
			log.trace("freeDeferrals at " + physicalAddress(addr) + ", size: " + sze + " releaseTime: " + lastReleaseTime);
		
		final byte[] buf = new byte[sze+4]; // allow for checksum
		getData(addr, buf);
		final DataInputStream strBuf = new DataInputStream(new ByteArrayInputStream(buf));
		m_allocationLock.lock();
		int totalFreed = 0;
		try {
			int nxtAddr = strBuf.readInt();
			
			int cnt = 0;
			
			while (nxtAddr != 0) { // while (false && addrs-- > 0) {
				
				if (nxtAddr > 0) { // Blob
					final int bloblen = strBuf.readInt();
					assert bloblen > 0; // a Blob address MUST have a size

					immediateFree(-nxtAddr, bloblen);
				} else {
					// The lack of size messes with the stats
					immediateFree(nxtAddr, 1); // size ignored for FixedAllocators
				}
				
				totalFreed++;
				
				nxtAddr = strBuf.readInt();
			}
			// now free delete block
			immediateFree(addr, sze);
            m_lastDeferredReleaseTime = lastReleaseTime;
            if (log.isTraceEnabled())
                log.trace("Updated m_lastDeferredReleaseTime="
                        + m_lastDeferredReleaseTime);
		} catch (IOException e) {
			throw new RuntimeException("Problem freeing deferrals", e);
		} finally {
			m_allocationLock.unlock();
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
        
        final ITupleIterator<CommitRecordIndex.Entry> commitRecords;
            /*
             * Commit can be called prior to Journal initialisation, in which
             * case the commitRecordIndex will not be set.
             */
            final IIndex commitRecordIndex = journal.getReadOnlyCommitRecordIndex();
            if (commitRecordIndex == null) { // TODO Why is this here?
                return 0;
            }
    
            final IndexMetadata metadata = commitRecordIndex
                    .getIndexMetadata();

            final byte[] fromKey = metadata.getTupleSerializer()
                    .serializeKey(fromTime);

            final byte[] toKey = metadata.getTupleSerializer()
                    .serializeKey(toTime);

            commitRecords = commitRecordIndex
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
                
                    totalFreed += freeDeferrals(blockAddr,
                            record.getTimestamp());
                    
                }
                
// Note: This is releasing the ICommitRecord itself.  I've moved the responsibilty
// for that into AbstractJournal#removeCommitRecordEntries() (invoked below).
//              
//                immediateFree((int) (entry.addr >> 32), (int) entry.addr);

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
            txLog.info("RECYCLED: fromTime=" + fromTime + ", toTime=" + toTime
                    + ", totalFreed=" + totalFreed 
                    + ", commitPointsRecycled=" + commitPointsRecycled
                    + ", commitPointsRemoved=" + commitPointsRemoved
                    );

        if (commitPointsRecycled != commitPointsRemoved)
            throw new AssertionError("commitPointsRecycled="
                    + commitPointsRecycled + " != commitPointsRemoved="
                    + commitPointsRemoved);

        return totalFreed;
    }

    /**
     * When a new context is started it must be registered to ensure it is
     * protected.
     * 
     * @param context
     */
	public void registerContext(IAllocationContext context) {
		m_allocationLock.lock();
		try {
			establishContextAllocation(context);
		} finally {
			m_allocationLock.unlock();
		}
	}
	
    /**
     * {@inheritDoc}
     * <p>
     * The {@link ContextAllocation} object manages a freeList of associated
     * allocators and an overall list of allocators. When the context is
     * detached, all allocators must be released and any that has available
     * capacity will be assigned to the global free lists. See
     * {@link AllocBlock #releaseSession}
     * 
     * @param context
     *            The context to be released from all {@link FixedAllocator}s.
     */
	public void detachContext(final IAllocationContext context) {
	    assertOpen();
		m_allocationLock.lock();
		try {
			final ContextAllocation alloc = m_contexts.remove(context);
			
			if (alloc != null) {
				m_contextRemovals++;
				alloc.release();			
			} else {
				throw new IllegalStateException("Multiple call to detachContext");
			}
			
			if (m_contexts.isEmpty() && this.m_activeTxCount == 0) {
				releaseSessions();
			}
		} finally {
			m_allocationLock.unlock();
		}
	}
	
    /**
     * The ContextAllocation object manages a freeList of associated allocators
     * and an overall list of allocators.  When the context is aborted then
     * allocations made by that context should be released.
	 * 	See {@link AllocBlock #abortShadow}
     * 
     * @param context
     *            The context to be released from all FixedAllocators.
     */
	public void abortContext(final IAllocationContext context) {
	    assertOpen();
		m_allocationLock.lock();
		try {
			final ContextAllocation alloc = m_contexts.remove(context);
			
			if (alloc != null) {
				m_contextRemovals++;
				alloc.abort();			
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
	static class ContextAllocation {
	    private final RWStore m_store;
		private final ArrayList<FixedAllocator> m_freeFixed[];
		
		private final ArrayList<FixedAllocator> m_allFixed;
		
		private final ArrayList<Long> m_deferredFrees;
		
		// lists of free blob allocators
//		private final ArrayList<BlobAllocator> m_freeBlobs;
		
		private final ContextAllocation m_parent;
		private final IAllocationContext m_context;
		
        @SuppressWarnings("unchecked")
        ContextAllocation(final RWStore store,
                final int fixedBlocks,
                final ContextAllocation parent,
                final IAllocationContext acontext) {
            
            m_store = store;
			m_parent = parent;
			m_context = acontext;
			
			m_freeFixed = new ArrayList[fixedBlocks];
			
			for (int i = 0; i < m_freeFixed.length; i++) {
			
			    m_freeFixed[i] = new ArrayList<FixedAllocator>();
			    
			}
			
			m_allFixed = new ArrayList<FixedAllocator>();
			
			m_deferredFrees = new ArrayList<Long>();
			
//			m_freeBlobs = new ArrayList<BlobAllocator>();
			
		}
		
        /**
         * For frees made against a shadowed FixedAlocator that is NOT owned
         * by the context, the physical free must be deferred until the
         * context is deshadowed or aborted.
         * 
         * @param encodeAddr
         */
        public void deferFree(final long encodeAddr) {
			m_deferredFrees.add(encodeAddr);
		}

		/**
         * Must return the shadowed allocators to the parent/global
         * environment, resetting the freeList association.
         */
        void release() {
            final ArrayList<FixedAllocator> freeFixed[] = m_parent != null ? m_parent.m_freeFixed
                    : m_store.m_freeFixed;

            final IAllocationContext pcontext = m_parent == null ? null
                    : m_parent.m_context;

			for (FixedAllocator f : m_allFixed) {
				f.setAllocationContext(pcontext);
				// will add to free list if required
				f.setFreeList(freeFixed[m_store.fixedAllocatorIndex(f.m_size)]);
			}
			
//			for (int i = 0; i < m_freeFixed.length; i++) {
//				freeFixed[i].addAll(m_freeFixed[i]);
//			}
			
//			freeBlobs.addAll(m_freeBlobs);
			
			// now free all deferred frees made within this context for other
			// allocators
			if (log.isDebugEnabled())
				log.debug("Releasing " + m_deferredFrees.size() + " deferred frees");
			
			for (Long l : m_deferredFrees) {
				m_store.immediateFree((int) (l >> 32), l.intValue());
			}
			m_deferredFrees.clear();
		}
		
        void abort() {
            final ArrayList<FixedAllocator> freeFixed[] = m_parent != null ? m_parent.m_freeFixed
                    : m_store.m_freeFixed;

            final IAllocationContext pcontext = m_parent == null ? null
                    : m_parent.m_context;

			for (FixedAllocator f : m_allFixed) {
				f.abortAllocationContext(pcontext, m_store.m_writeCache);
				f.setFreeList(freeFixed[m_store.fixedAllocatorIndex(f.m_size)]);
			}
			
			if (log.isDebugEnabled())
				log.debug("Aborting " + m_deferredFrees.size() + " deferred frees");
			
			m_deferredFrees.clear();
		}
		
		FixedAllocator getFreeFixed(final int i) {
			final ArrayList<FixedAllocator> free = m_freeFixed[i];
			if (free.size() == 0) {
				final FixedAllocator falloc = establishFixedAllocator(i);
				falloc.setAllocationContext(m_context);
				falloc.setFreeList(free); // will add to free list
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
				 return m_store.establishFreeFixedAllocator(i);
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
	
	private int m_contextRequests = 0;
	private int m_contextRemovals = 0;
    private ContextAllocation establishContextAllocation(
            final IAllocationContext context) {

        /*
         * The allocation lock MUST be held to make changes in the membership of
         * m_contexts atomic with respect to free().
         */
        assert m_allocationLock.isHeldByCurrentThread();
        
        ContextAllocation ret = m_contexts.get(context);
        
        if (ret == null) {
        	
            ret = new ContextAllocation(this, m_freeFixed.length, null, context);

            if (m_contexts.put(context, ret) != null) {
                
                throw new AssertionError();
                
            }
        
            if (log.isTraceEnabled())
				log.trace("Establish ContextAllocation: " + ret 
						+ ", total: " + m_contexts.size() 
						+ ", requests: " + ++m_contextRequests 
						+ ", removals: " + m_contextRemovals 
						+ ", allocators: " + m_allocs.size() );
      
            
            if (log.isInfoEnabled())
                log.info("Context: ncontexts=" + m_contexts.size()
                        + ", context=" + context);
            
        }

        return ret;
    
    }
	
	public int getSlotSize(final int data_len) {
		int i = 0;

		int ret = m_minFixedAlloc;
		while (data_len > ret) {
			i++;
			// If we write directly to the writeCache then the data_len
			//	may be larger than largest slot
			if (i == m_allocSizes.length)
				return data_len;
			
			ret = 64 * m_allocSizes[i];
		}
		
		return ret;
	}

    /**
     * The maximum allocation size (bytes).
     */
    public int getMaxAllocSize() {

        return m_maxFixedAlloc;

    }

    /**
     * This can be called as part of the HA downstream replication.
     * 
     * FIXME: If part of downstream replication then the current metabits
     * held by the RWStore will not be in sync with that stored on disk.
     * 
     * This will only be a problem if the RWStore needs to take over as
     * leader and be able to allocate and write to the store.
     * 
     * Note that the metabits are not needed in order to determine the
     * physical address mapping of an rw-native address.
     * 
     * @param rootBlock
     * @param forceOnCommit
     */
    public void writeRootBlock(final IRootBlockView rootBlock,
            final ForceEnum forceOnCommit) {

        if (rootBlock == null)
            throw new IllegalArgumentException();
        
        checkRootBlock(rootBlock);
        
        assertOpen();

        if (log.isTraceEnabled()) {
            log.trace("Writing new rootblock with commitCounter: "
                    + rootBlock.getCommitCounter() + ", commitRecordAddr: "
                    + rootBlock.getCommitRecordAddr()
                    + ", commitRecordIndexAddr: "
                    + rootBlock.getCommitRecordIndexAddr());
        }
        
        try {
            
            final ByteBuffer data = rootBlock.asReadOnlyBuffer();

            final long pos = rootBlock.isRootBlock0()
                    ? FileMetadata.OFFSET_ROOT_BLOCK0
                    : FileMetadata.OFFSET_ROOT_BLOCK1;

            /*
             * Note: This uses the [opener] to automatically retry the operation
             * in case concurrent readers are interrupting, causing an
             * asynchronous close of the backing channel.
             */
            final Lock lock = m_extensionLock.readLock();
            lock.lock();
            try {

                // Update the root block.
                FileChannelUtility.writeAll(m_reopener, data, pos);

                /*
                 * Generally, you want to force the file data to the disk here.
                 * The file metadata MIGHT not matter since we always force it
                 * to the disk when we change the file size (unless the file
                 * system updates other aspects of file metadata during normal
                 * writes).
                 */

                // sync the disk.
                m_reopener.reopenChannel().force(forceOnCommit == ForceEnum.ForceMetadata);

                // Update counters.
                final StoreCounters<?> c = (StoreCounters<?>) storeCounters.get()
                        .acquire();
                try {
                    c.nwriteRootBlock++;
                } finally {
                    c.release();
                }
                
                // ensure cached commitNextAllocation
                if (m_committedNextAllocation != m_nextAllocation ) {
                	if (log.isTraceEnabled())
                		log.trace("Updating committedNextAllocation from writeRootBlock");
                	
                	m_committedNextAllocation = m_nextAllocation;
                }
                
            } finally {

                lock.unlock();
                
            }

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        if (log.isDebugEnabled())
            log.debug("wrote root block: "+rootBlock);
        
    }

    public ByteBuffer readRootBlock(final boolean rootBlock0) {
        
        assertOpen();
        
        final ByteBuffer tmp = ByteBuffer
                .allocate(RootBlockView.SIZEOF_ROOT_BLOCK);

        try {

            FileChannelUtility.readAll(m_reopener, tmp,
                rootBlock0 ? FileMetadata.OFFSET_ROOT_BLOCK0
                        : FileMetadata.OFFSET_ROOT_BLOCK1);
            
            tmp.position(0); // resets the position.

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        return tmp;

    }


    /**
     * Striped performance counters for {@link IRawStore} access, including
     * operations that read or write through to the underlying media.
     * <p>
     * Note: The performance counters for writes to the disk are reported by the
     * {@link WriteCacheService}. The {@link RWStore} never writes directly onto
     * the disk (other than the root blocks).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @param <T>
     * 
     * @todo report elapsed time and average latency for force, reopen, and
     *       writeRootBlock.
     * 
     *       FIXME CAT may be much faster than striped locks (2-3x faster).
     */
    static public class StoreCounters<T extends StoreCounters<T>> extends
            StripedCounters<T> {

        /**
         * #of read requests.
         */
        public volatile long nreads;

        /**
         * #of read requests that read through to the backing file.
         */
        public volatile long ndiskRead;
        
        /**
         * #of bytes read.
         */
        public volatile long bytesRead;

        /**
         * #of bytes that have been read from the disk.
         */
        public volatile long bytesReadFromDisk;
        
        /**
         * Total elapsed time for reads.
         */
        public volatile long elapsedReadNanos;

        /**
         * Total elapsed time for reading on the disk.
         */
        public volatile long elapsedDiskReadNanos;

        /**
         * The #of checksum errors while reading on the local disk.
         */
        public volatile long checksumErrorCount;
        
        /**
         * #of write requests.
         */
        public volatile long nwrites;
        
        // This is reported by the WriteCacheService.
//        /**
//         * #of write requests that write through to the backing file.
//         */
//        public volatile long ndiskWrite;

        /**
         * The size of the largest record read.
         */
        public volatile long maxReadSize;
        
        /**
         * The size of the largest record written.
         */
        public volatile long maxWriteSize;
        
        /**
         * #of bytes written.
         */
        public volatile long bytesWritten;

        // This is reported by the WriteCacheService.
//        /**
//         * #of bytes that have been written on the disk.
//         */
//        public volatile long bytesWrittenOnDisk;
        
        /**
         * Total elapsed time for writes.
         */
        public volatile long elapsedWriteNanos;
        
        // This is reported by the WriteCacheService.
//        /**
//         * Total elapsed time for writing on the disk.
//         */
//        public volatile long elapsedDiskWriteNanos;
        
        /**
         * #of times the data were forced to the disk.
         */
        public volatile long nforce;
        
        /**
         * #of times the length of the file was changed (typically, extended).
         */
        public volatile long ntruncate;
        
        /**
         * #of times the file has been reopened after it was closed by an
         * interrupt.
         */
        public volatile long nreopen;
        
        /**
         * #of times one of the root blocks has been written.
         */
        public volatile long nwriteRootBlock;

        /**
         * {@inheritDoc}
         */
        public StoreCounters() {
            super();
        }

        /**
         * {@inheritDoc}
         */
        public StoreCounters(final int batchSize) {
            super(batchSize);
        }

        /**
         * {@inheritDoc}
         */
        public StoreCounters(final int nstripes, final int batchSize) {
            super(nstripes, batchSize);
        }

        @Override
        public void add(final T o) {

            super.add(o);
            
            nreads += o.nreads;
            ndiskRead += o.ndiskRead;
            bytesRead += o.bytesRead;
            bytesReadFromDisk += o.bytesReadFromDisk;
            maxReadSize = Math.max(maxReadSize, o.maxReadSize);
            elapsedReadNanos += o.elapsedReadNanos;
            elapsedDiskReadNanos += o.elapsedDiskReadNanos;
            checksumErrorCount += o.checksumErrorCount;

            nwrites += o.nwrites;
//            ndiskWrite += o.ndiskWrite;
            maxWriteSize = Math.max(maxWriteSize, o.maxWriteSize);
            bytesWritten += o.bytesWritten;
//            bytesWrittenOnDisk += o.bytesWrittenOnDisk;
            elapsedWriteNanos += o.elapsedWriteNanos;
//            elapsedDiskWriteNanos += o.elapsedDiskWriteNanos;

            nforce += o.nforce;
            ntruncate += o.ntruncate;
            nreopen += o.nreopen;
            nwriteRootBlock += o.nwriteRootBlock;
            
        }

        @Override
        public T subtract(final T o) {

            // make a copy of the current counters.
            final T t = super.subtract(o);
            
            // subtract out the given counters.
            t.nreads -= o.nreads;
            t.ndiskRead -= o.ndiskRead;
            t.bytesRead -= o.bytesRead;
            t.bytesReadFromDisk -= o.bytesReadFromDisk;
            t.maxReadSize -= o.maxReadSize; // @todo report max? min?
            t.elapsedReadNanos -= o.elapsedReadNanos;
            t.elapsedDiskReadNanos -= o.elapsedDiskReadNanos;
            t.checksumErrorCount -= o.checksumErrorCount;

            t.nwrites -= o.nwrites;
//            t.ndiskWrite -= o.ndiskWrite;
            t.maxWriteSize -= o.maxWriteSize; // @todo report max? min?
            t.bytesWritten -= o.bytesWritten;
//            t.bytesWrittenOnDisk -= o.bytesWrittenOnDisk;
            t.elapsedWriteNanos -= o.elapsedWriteNanos;
//            t.elapsedDiskWriteNanos -= o.elapsedDiskWriteNanos;

            t.nforce -= o.nforce;
            t.ntruncate -= o.ntruncate;
            t.nreopen -= o.nreopen;
            t.nwriteRootBlock -= o.nwriteRootBlock;

            return t;
            
        }
        
        @Override
        public void clear() {

            // subtract out the given counters.
            nreads = 0;
            ndiskRead = 0;
            bytesRead = 0;
            bytesReadFromDisk = 0;
            maxReadSize = 0;
            elapsedReadNanos = 0;
            elapsedDiskReadNanos = 0;
            checksumErrorCount = 0;

            nwrites = 0;
//            ndiskWrite = 0;
            maxWriteSize = 0;
            bytesWritten = 0;
//            bytesWrittenOnDisk = 0;
            elapsedWriteNanos = 0;
//            elapsedDiskWriteNanos = 0;

            nforce = 0;
            ntruncate = 0;
            nreopen = 0;
            nwriteRootBlock = 0;

        }
        
        @Override
        public CounterSet getCounters() {

            final CounterSet root = super.getCounters();

            // IRawStore API
            {

                /*
                 * reads
                 */

                root.addCounter("nreads", new Instrument<Long>() {
                    public void sample() {
                        setValue(nreads);
                    }
                });

                root.addCounter("bytesRead", new Instrument<Long>() {
                    public void sample() {
                        setValue(bytesRead);
                    }
                });

                root.addCounter("readSecs", new Instrument<Double>() {
                    public void sample() {
                        final double elapsedReadSecs = (elapsedReadNanos / 1000000000.);
                        setValue(elapsedReadSecs);
                    }
                });

                root.addCounter("bytesReadPerSec", new Instrument<Double>() {
                    public void sample() {
                        final double readSecs = (elapsedReadNanos / 1000000000.);
                        final double bytesReadPerSec = (readSecs == 0L ? 0d
                                : (bytesRead / readSecs));
                        setValue(bytesReadPerSec);
                    }
                });

                root.addCounter("maxReadSize", new Instrument<Long>() {
                    public void sample() {
                        setValue(maxReadSize);
                    }
                });

                root.addCounter("checksumErrorCount", new Instrument<Long>() {
                    public void sample() {
                        setValue(checksumErrorCount);
                    }
                });

                /*
                 * writes
                 */

                root.addCounter("nwrites", new Instrument<Long>() {
                    public void sample() {
                        setValue(nwrites);
                    }
                });

                root.addCounter("bytesWritten", new Instrument<Long>() {
                    public void sample() {
                        setValue(bytesWritten);
                    }
                });

                root.addCounter("writeSecs", new Instrument<Double>() {
                    public void sample() {
                        final double writeSecs = (elapsedWriteNanos / 1000000000.);
                        setValue(writeSecs);
                    }
                });

                root.addCounter("bytesWrittenPerSec", new Instrument<Double>() {
                    public void sample() {
                        final double writeSecs = (elapsedWriteNanos / 1000000000.);
                        final double bytesWrittenPerSec = (writeSecs == 0L ? 0d
                                : (bytesWritten / writeSecs));
                        setValue(bytesWrittenPerSec);
                    }
                });

                root.addCounter("maxWriteSize", new Instrument<Long>() {
                    public void sample() {
                        setValue(maxWriteSize);
                    }
                });

            } // IRawStore

            // disk statistics
            {
                final CounterSet disk = root.makePath("disk");

                /*
                 * read
                 */

                disk.addCounter("nreads", new Instrument<Long>() {
                    public void sample() {
                        setValue(ndiskRead);
                    }
                });

                disk.addCounter("bytesRead", new Instrument<Long>() {
                    public void sample() {
                        setValue(bytesReadFromDisk);
                    }
                });

                disk.addCounter("bytesPerRead", new Instrument<Double>() {
                    public void sample() {
                        final double bytesPerDiskRead = (ndiskRead == 0 ? 0d
                                : (bytesReadFromDisk / (double) ndiskRead));
                        setValue(bytesPerDiskRead);
                    }
                });

                disk.addCounter("readSecs", new Instrument<Double>() {
                    public void sample() {
                        final double diskReadSecs = (elapsedDiskReadNanos / 1000000000.);
                        setValue(diskReadSecs);
                    }
                });

                disk.addCounter("bytesReadPerSec", new Instrument<Double>() {
                    public void sample() {
                        final double diskReadSecs = (elapsedDiskReadNanos / 1000000000.);
                        final double bytesReadPerSec = (diskReadSecs == 0L ? 0d
                                : bytesReadFromDisk / diskReadSecs);
                        setValue(bytesReadPerSec);
                    }
                });

                disk.addCounter("secsPerRead", new Instrument<Double>() {
                    public void sample() {
                        final double diskReadSecs = (elapsedDiskReadNanos / 1000000000.);
                        final double readLatency = (diskReadSecs == 0 ? 0d
                                : diskReadSecs / ndiskRead);
                        setValue(readLatency);
                    }
                });

                /*
                 * write
                 */

//                disk.addCounter("nwrites", new Instrument<Long>() {
//                    public void sample() {
//                        setValue(ndiskWrite);
//                    }
//                });
//
//                disk.addCounter("bytesWritten", new Instrument<Long>() {
//                    public void sample() {
//                        setValue(bytesWrittenOnDisk);
//                    }
//                });
//
//                disk.addCounter("bytesPerWrite", new Instrument<Double>() {
//                    public void sample() {
//                        final double bytesPerDiskWrite = (ndiskWrite == 0 ? 0d
//                                : (bytesWrittenOnDisk / (double) ndiskWrite));
//                        setValue(bytesPerDiskWrite);
//                    }
//                });
//
//                disk.addCounter("writeSecs", new Instrument<Double>() {
//                    public void sample() {
//                        final double diskWriteSecs = (elapsedDiskWriteNanos / 1000000000.);
//                        setValue(diskWriteSecs);
//                    }
//                });
//
//                disk.addCounter("bytesWrittenPerSec", new Instrument<Double>() {
//                    public void sample() {
//                        final double diskWriteSecs = (elapsedDiskWriteNanos / 1000000000.);
//                        final double bytesWrittenPerSec = (diskWriteSecs == 0L ? 0d
//                                : bytesWrittenOnDisk / diskWriteSecs);
//                        setValue(bytesWrittenPerSec);
//                    }
//                });
//
//                disk.addCounter("secsPerWrite", new Instrument<Double>() {
//                    public void sample() {
//                        final double diskWriteSecs = (elapsedDiskWriteNanos / 1000000000.);
//                        final double writeLatency = (diskWriteSecs == 0 ? 0d
//                                : diskWriteSecs / ndiskWrite);
//                        setValue(writeLatency);
//                    }
//                });

                /*
                 * other
                 */

                disk.addCounter("nforce", new Instrument<Long>() {
                    public void sample() {
                        setValue(nforce);
                    }
                });

                disk.addCounter("nextend", new Instrument<Long>() {
                    public void sample() {
                        setValue(ntruncate);
                    }
                });

                disk.addCounter("nreopen", new Instrument<Long>() {
                    public void sample() {
                        setValue(nreopen);
                    }
                });

                disk.addCounter("rootBlockWrites", new Instrument<Long>() {
                    public void sample() {
                        setValue(nwriteRootBlock);
                    }
                });

            } // disk
            
            return root;

        } // getCounters()
        
    } // class StoreCounters
    
    /**
     * Striped performance counters for this class.
     */
    @SuppressWarnings("unchecked")
    private final AtomicReference<StoreCounters> storeCounters = new AtomicReference<StoreCounters>();

    /**
     * Returns the striped performance counters for the store.
     */
    public StoreCounters<?> getStoreCounters() {

        return storeCounters.get();

    }

    /**
     * Replaces the {@link StoreCounters} object.
     * 
     * @param storeCounters
     *            The new {@link Counter}s.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     */
    public void setStoreCounters(final StoreCounters<?> storeCounters) {

        if (storeCounters == null)
            throw new IllegalArgumentException();

        this.storeCounters.set(storeCounters);
        
    }

    /**
     * Return interesting information about the write cache and file operations.
     * 
     * @todo allocations data? user extent allocated? user extent used? etc.
     */
    public CounterSet getCounters() {

        final CounterSet root = new CounterSet();

//        root.addCounter("nextOffset", new Instrument<Long>() {
//            public void sample() {
//                setValue(nextOffset.get());
//            }
//        });

        root.addCounter("extent", new Instrument<Long>() {
            public void sample() {
                setValue(getStoreFile().length());
            }
        });

        // attach the most recently updated values from the striped counters.
        root.attach(storeCounters.get().getCounters());

        if (m_writeCache != null) {

            final CounterSet tmp = root.makePath("writeCache");

            tmp.attach(m_writeCache.getCounters());

        }
        
        return root;

    }

    public void writeRawBuffer(final IHAWriteMessage msg, final IBufferAccess b)
            throws IOException, InterruptedException {

        /*
         * Wrap up the data from the message as a WriteCache object. This will
         * build up a RecordMap containing the allocations to be made, and
         * including a ZERO (0) data length if any offset winds up being deleted
         * (released).
         */
        final WriteCache writeCache = m_writeCache.newWriteCache(b,
                true/* useChecksums */, true/* bufferHasData */, m_reopener,
                msg.getFileExtent());

		/*
		 * Setup buffer for writing. We receive the buffer with pos=0, Ê
		 * limit=#ofbyteswritten. However, flush() expects pos=limit, will
		 * clear pos to zero and then write bytes up to the limit. So,
		 * we set the position to the limit before calling flush. Ê Ê Ê
		 */
		final ByteBuffer bb = b.buffer();
		final int limit = bb.limit();
		bb.position(limit);
        
        /*
         * Flush the scattered writes in the write cache to the backing
         * store.
         */
        m_allocationLock.lock();
        try {
            // Flush writes.
            writeCache.flush(false/* force */);
        } finally {
            m_allocationLock.unlock();
        }

    }

    public Future<Void> sendHALogBuffer(final IHALogRequest req,
            final IHAWriteMessage msg, final IBufferAccess buf)
            throws IOException, InterruptedException {

        final ByteBuffer b = buf.buffer();

        assert b.remaining() > 0 : "Empty buffer: " + b;

        @SuppressWarnings("unchecked")
        final QuorumPipeline<HAPipelineGlue> quorumMember = (QuorumPipeline<HAPipelineGlue>) m_quorum
                .getMember();

        final Future<Void> remoteWriteFuture = quorumMember.replicate(req, msg, b);

        return remoteWriteFuture;

    }

    /**
     * @see IHABufferStrategy#sendRawBuffer(IHARebuildRequest, long,
     *      long, long, long, int, ByteBuffer)
     */
    public Future<Void> sendRawBuffer(final IHARebuildRequest req,
//            final long commitCounter, final long commitTime,
            final long sequence, final long quorumToken, final long fileExtent,
            final long offset, final int nbytes, final ByteBuffer b)
            throws IOException, InterruptedException {

        // read direct from store
        final ByteBuffer clientBuffer = b;
        clientBuffer.position(0);
        clientBuffer.limit(nbytes);

        readRaw(/*nbytes,*/ offset, clientBuffer);
        
        assert clientBuffer.remaining() > 0 : "Empty buffer: " + clientBuffer;

        @SuppressWarnings("unchecked")
        final QuorumPipeline<HAPipelineGlue> quorumMember = (QuorumPipeline<HAPipelineGlue>) m_quorum
                .getMember();

        final int chk = ChecksumUtility.threadChk.get().checksum(b);
        
        final IHAWriteMessage msg = new HAWriteMessage(-1L/* commitCounter */,
                -1L/* commitTime */, sequence, nbytes, chk, StoreTypeEnum.RW,
                quorumToken, fileExtent, offset/* firstOffset */);

        final Future<Void> remoteWriteFuture = quorumMember.replicate(req, msg,
                clientBuffer);

        return remoteWriteFuture;

    }
    
    /**
     * Read on the backing file. {@link ByteBuffer#remaining()} bytes will be
     * read into the caller's buffer, starting at the specified offset in the
     * backing file.
     * 
     * @param offset
     *            The offset of the first byte (relative to the start of the
     *            data region).
     * @param dst
     *            Where to put the data. Bytes will be written at position until
     *            limit.
     * 
     * @return The caller's buffer, prepared for reading.
     */
    private ByteBuffer readRaw(final long offset, final ByteBuffer dst) {

        final Lock readLock = m_extensionLock.readLock();
        readLock.lock();
        try {

            try {

                // the offset into the disk file.
                final long pos = FileMetadata.headerSize0 + offset;

                // read on the disk.
                final int ndiskRead = FileChannelUtility.readAll(m_reopener,
                        dst, pos);

                // update performance counters.
                final StoreCounters<?> c = (StoreCounters<?>) storeCounters
                        .get().acquire();
                try {
                    c.ndiskRead += ndiskRead;
                } finally {
                    c.release();
                }

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }

            // flip for reading.
            dst.flip();

            return dst;
        } finally {

            readLock.unlock();
        }

    }

    public int getMaxBlobSize() {
		return m_maxBlobAllocSize-4; // allow for checksum
	}
	
	public StorageStats getStorageStats() {
		return m_storageStats;
	}

	private final class RawTx implements IRawTx {

	    private final AtomicBoolean m_open = new AtomicBoolean(true);
		
	    RawTx() {
			activateTx();
		}
		
		public void close() {
			if (m_open.compareAndSet(true/*expect*/, false/*update*/)) {
				deactivateTx();
			}
		}
	}
	
	public IRawTx newTx() {
		return new RawTx();
	}
	
    private void activateTx() {
        m_allocationLock.lock();
        try {
            m_activeTxCount++;
            if(log.isInfoEnabled())
                log.info("#activeTx="+m_activeTxCount);
        } finally {
            m_allocationLock.unlock();
        }
    }
    
    private void deactivateTx() {
        m_allocationLock.lock();
        try {
        	if (m_activeTxCount == 0) {
        		throw new IllegalStateException("Tx count must be positive!");
        	}
            m_activeTxCount--;
            if(log.isInfoEnabled())
                log.info("#activeTx="+m_activeTxCount);
            
            if (m_activeTxCount == 0 && m_contexts.isEmpty()) {
            	releaseSessions();
            }
        } finally {
            m_allocationLock.unlock();
        }
    }

	/**
	 * Returns the slot size associated with this address
	 */
	public int getAssociatedSlotSize(int addr) {
		return getBlock(addr).getBlockSize();
	}

	/**
	 * lockAddress adds the address passed to a lock list.  This is for
	 * debug only and is not intended to be used generally for the live system.
	 * 
	 * @param addr - address to be locked
	 */
	public void lockAddress(int addr) {
		if (m_lockAddresses.putIfAbsent(addr, System.currentTimeMillis()) != null) {
			throw new IllegalStateException("address already locked, logical: " + addr + ", physical: " + physicalAddress(addr, true));
		}
	}

	public void showWriteCacheDebug(long paddr) {
		log.warn("WriteCacheDebug: " + paddr + " - " + m_writeCache.addrDebugInfo(paddr));
	}

	public CounterSet getWriteCacheCounters() {
		return m_writeCache.getCounters();
	}

//	/**
//	 * If historical data is maintained then this will return the earliest time for which
//	 * data can be safely retrieved.
//	 * 
//	 * @return time of last release
//	 */
	public long getLastReleaseTime() {
		return m_lastDeferredReleaseTime;
	}

	private ConcurrentWeakValueCache<Long, ICommitter> m_externalCache = null;
	private int m_cachedDatasize = 0;

    public void registerExternalCache(
            final ConcurrentWeakValueCache<Long, ICommitter> externalCache,
            final int dataSize) {
	
		m_allocationLock.lock();
		try {
			m_externalCache = externalCache;
			m_cachedDatasize = getSlotSize(dataSize);
		} finally {
			m_allocationLock.unlock();
		}
	}


    /**
     * Return <code>true</code> iff the allocation having that address is
     * flagged as committed. The caller must be holding the allocation lock in
     * order for the result to remain valid outside of the method call.
     * 
     * @param addr
     *            The address.
     * 
     * @return <code>true</code> iff the address is currently committed.
     */
    public boolean isCommitted(final int rwaddr) {
        m_allocationLock.lock();
        try {

            final FixedAllocator alloc = getBlockByAddress(rwaddr);
            
            final int offset = getOffset(rwaddr);

            return alloc.isCommitted(offset);
            
        } finally {
            m_allocationLock.unlock();
        }
	}

	public boolean inWriteCache(final int rwaddr) {
		
	    return m_writeCache.isPresent(physicalAddress(rwaddr, true));
	    
	}

	public InputStream getInputStream(long addr) {
		return new PSInputStream(this, addr);
	}

	public IPSOutputStream getOutputStream() {
		return getOutputStream(null);
	}  

	public IPSOutputStream getOutputStream(final IAllocationContext context) {
		return PSOutputStream.getNew(this, m_maxFixedAlloc, context);
	}

    /**
     * Called from {@link AbstractJournal} commit2Phase to ensure that a
     * downstream HA quorum member ensures it is able to read committed data
     * that has been streamed directly to the backing store.
     * <p>
     * The data stream will have included metabits and modified
     * {@link FixedAllocator}s so these must be reset using the metabitsAddr
     * data in the root block.
     */
	public void resetFromHARootBlock(final IRootBlockView rootBlock) {

	    try {

		    // should not be any dirty allocators
			// assert m_commitList.size() == 0;
			
			// Remove all current allocators
			m_allocs.clear();
			
			assert m_nextAllocation != 0;
			
			m_nextAllocation = 0;

			initfromRootBlock(rootBlock);
			
			assert m_nextAllocation != 0;
		
	    } catch (IOException e) {
		
	        throw new RuntimeException(e);
	        
		}

	}

	/**
	 * Simple class to collect up DeleteBlockStats and returned by
	 * checkDeleteBlocks, called from DumpJournal.
	 */
	public static class DeleteBlockStats {
		private int m_commitRecords = 0;;
		private int m_addresses = 0;
		private int m_blobs = 0;
		private int m_badAddresses = 0;
		private final HashMap<Integer, Integer> m_freed = new HashMap<Integer, Integer>();
        /**
         * The latched address of each address that appears more than once
         * across the delete blocks.
         */
        private final Set<Integer> m_duplicates = new LinkedHashSet<Integer>();
//        /**
//         * The hexstring version of the data associated with the addresses that
//         * are present more than once in the delete blocks.
//         */
//		private final ArrayList<String> m_dupData = new ArrayList<String>();
			
		/**
		 * The #of commit records that would be processed.
		 */
		public int getCommitRecords() {
			return m_commitRecords;
        }

        /**
         * Return the #of addresses in the delete blocks acrosss the commit
         * records.
         */
        public int getAddresses() {
            return m_addresses;
        }

        /**
         * Return the #of addresses that are not committed data across the
         * commit records.
         */
		public int getBadAddresses() {
			return m_badAddresses;
		}

        /**
         * Return the latched addresses that appear more than once in the delete
         * blocks across the commit records.
         */
		public Set<Integer> getDuplicateAddresses() {
		    return m_duplicates;
		}
		
		public String toString(final RWStore store) {
			final StringBuilder sb = new StringBuilder();
			sb.append("CommitRecords: " + m_commitRecords + ", Addresses: " + m_addresses 
					+ ", Blobs: " + m_blobs + ", bad: " + + m_badAddresses);
			if (!m_duplicates.isEmpty()) {
				for (int latchedAddr : m_duplicates) {
//				    final int latchedAddr = m_duplicates.get(i);
					sb.append("\nDuplicate: latchedAddr=" + latchedAddr + "\n");
					/*
					 * Note: Now dumped by DumpJournal.
					 */
//                    final byte[] data;
//                    try {
//                        data = store.readFromLatchedAddress(latchedAddr);
//                    } catch (IOException ex) {
//                        final String msg = "Could not read data: addr="
//                                + latchedAddr;
//                        log.error(msg, ex);
//                        sb.append(msg);
//                        continue;
//                    }
//
//                    final String hexStr = BytesUtil.toHexString(data,
//                            data.length);
//                    
//                    BytesUtil.printHexString(sb, hexStr);
                    
                }
            }
			
			return sb.toString();
		}
	}
	
	/**
	 * Utility to check the deleteBlocks associated with each active CommitRecord
	 */
	public DeleteBlockStats checkDeleteBlocks(final AbstractJournal journal) {

	    final DeleteBlockStats stats = new DeleteBlockStats();

		/*
		 * Commit can be called prior to Journal initialisation, in which case
		 * the commitRecordIndex will not be set.
		 */
		final IIndex commitRecordIndex = journal.getReadOnlyCommitRecordIndex();

		if (commitRecordIndex == null) {
		
		    return stats;
		    
		}

		@SuppressWarnings("unchecked")
        final ITupleIterator<CommitRecordIndex.Entry> commitRecords = commitRecordIndex
				.rangeIterator();

		while (commitRecords.hasNext()) {

			final ITuple<CommitRecordIndex.Entry> tuple = commitRecords.next();

			final CommitRecordIndex.Entry entry = tuple.getObject();

			try {

				final ICommitRecord record = CommitRecordSerializer.INSTANCE
						.deserialize(journal.read(entry.addr));

				final long blockAddr = record
						.getRootAddr(AbstractJournal.DELETEBLOCK);

				if (blockAddr != 0) {

					checkDeferrals(blockAddr, record.getTimestamp(), stats);

				}

				stats.m_commitRecords++;

			} catch (RuntimeException re) {

				throw new RuntimeException("Problem with entry at "
						+ entry.addr, re);

			}

		}

		return stats;
	}

    /**
     * Utility method to verify the deferred delete blocks.
     * 
     * @param blockAddr
     *            The address of a deferred delete block.
     * @param commitTime
     *            The commitTime associated with the {@link ICommitRecord}.
     * @param stats
     *            Where to collect statistics.
     */
	private void checkDeferrals(final long blockAddr,
			final long commitTime, final DeleteBlockStats stats) {

        /**
         * Debug flag. When true, writes all frees onto stderr so they can be
         * read into a worksheet for analysis.
         */
        final boolean writeAll = false;
	    
		final int addr = (int) (blockAddr >> 32);
		final int sze = (int) blockAddr & 0xFFFFFF;

		if (log.isTraceEnabled())
			log.trace("freeDeferrals at " + physicalAddress(addr) + ", size: "
					+ sze + " releaseTime: " + commitTime);

		final byte[] buf = new byte[sze + 4]; // allow for checksum
		getData(addr, buf);
		final DataInputStream strBuf = new DataInputStream(
				new ByteArrayInputStream(buf));
		m_allocationLock.lock();
//		int totalFreed = 0;
		try {
			int nxtAddr = strBuf.readInt();

//			int cnt = 0;

			while (nxtAddr != 0) { // while (false && addrs-- > 0) {

				stats.m_addresses++;

				if (nxtAddr > 0) { // Blob
					stats.m_blobs++;
					final int bloblen = strBuf.readInt();
					assert bloblen > 0; // a Blob address MUST have a size
					nxtAddr = -nxtAddr;
				}

				if (!isCommitted(nxtAddr)) {
					stats.m_badAddresses++;
				}
				
                if (stats.m_freed.containsKey(nxtAddr)) {
                    stats.m_duplicates.add(nxtAddr);
                    if (writeAll) {
                        System.err.println("" + commitTime + " " + nxtAddr
                                + " FREE DUP");
                    }
                } else {
                    stats.m_freed.put(nxtAddr, nxtAddr);
                    if (writeAll) {
                        System.err.println("" + commitTime + " " + nxtAddr
                                + " FREE");
                    }
				}

				nxtAddr = strBuf.readInt();
			}
			// now check delete block
			assert isCommitted(addr);
        } catch (IOException e) {
            throw new RuntimeException("Problem checking deferrals: " + e, e);
        } finally {
            m_allocationLock.unlock();
        }
    }

    /**
     * A low level utility method that reads directly from the backing
     * {@link FileChannel}.
     * <p>
     * Note: The latched address does not encode the actual length of the data.
     * Therefore, all data in the slot addressed by the latched address will be
     * returned.
     * 
     * @param nxtAddr
     *            The latched address.
     *            
     * @return The byte[] in the addressed slot.
     * 
     * @throws IOException
     */
    public final byte[] readFromLatchedAddress(final int nxtAddr)
            throws IOException {

        final FixedAllocator alloc = getBlockByAddress(nxtAddr);

        final byte[] data = new byte[alloc.m_size];

        final ByteBuffer bb = ByteBuffer.wrap(data);
        
        final int offset = getOffset(nxtAddr);
        
        final long paddr = alloc.getPhysicalAddress(offset);
        
        FileChannelUtility.readAll(m_reopener, bb, paddr);
        
        return data;

    }

    /**
     * @see IHABufferStrategy#getBlockSequence()
     */
    public long getBlockSequence() {

        return lastBlockSequence;

    }

    private long lastBlockSequence = 0;
	
	 //    /**
//     * Only blacklist the addr if not already available, in other words
//     * a blacklisted address only makes sense if it for previously 
//     * committed data and not instantly recyclable.
//     */
//    public void blacklistAddress(int addr, final String info) {
//    	if (m_blacklist == null) {
//    		// add delay/synchronization to emulate blacklist delay?
//    		return;
//    	}
//    	
//        if (physicalAddress(addr) == 0)
//            throw new IllegalStateException("Blacklist should only be called for a valid address");
//        
//        if (info == null) {
//            throw new IllegalStateException("Blacklist must have info String");
//        }
//        
//        if ( m_blacklist.putIfAbsent(addr, info) != null)
//            throw new IllegalStateException("Address already blacklisted: "
//                    + addr + ", info: " + info + ", prev: " + m_blacklist.get(addr));
//
//       ;
//    }

    /**
     * @see IHABufferStrategy#computeDigest(Object, MessageDigest)
     */
    public void computeDigest(final Object snapshot, final MessageDigest digest)
            throws DigestException, IOException {

        if (snapshot != null)
            throw new UnsupportedOperationException();

        IBufferAccess buf = null;
        try {

            try {
                // Acquire a buffer.
                buf = DirectBufferPool.INSTANCE.acquire();
            } catch (InterruptedException ex) {
                // Wrap and re-throw.
                throw new IOException(ex);
            }

            // The backing ByteBuffer.
            final ByteBuffer b = buf.buffer();

            // A byte[] with the same capacity as that ByteBuffer.
            final byte[] a = new byte[b.capacity()];
            
            // The capacity of that buffer (typically 1MB).
            final int bufferCapacity = b.capacity();

            // The size of the file at the moment we begin.
            final long fileExtent = getStoreFile().length();

            // The #of bytes whose digest will be computed.
            final long totalBytes = fileExtent;

            // The #of bytes remaining.
            long remaining = totalBytes;

            // The offset of the current block.
            long offset = 0L;

            // The block sequence.
            long sequence = 0L;
            
            if (log.isInfoEnabled())
                log.info("Computing digest: nbytes=" + totalBytes);

            while (remaining > 0) {

                final int nbytes = (int) Math.min((long) bufferCapacity,
                        remaining);

                if (log.isDebugEnabled())
                    log.debug("Computing digest: sequence=" + sequence
                            + ", offset=" + offset + ", nbytes=" + nbytes);

                // Setup for read.
                b.position(0);
                b.limit(nbytes);

                // read block
                readRaw(/*nbytes,*/ offset, b);

                // Copy data into our byte[].
                final byte[] c = BytesUtil.toArray(b, false/* forceCopy */, a);

                // update digest
                digest.digest(c, 0/* off */, nbytes/* len */);

                remaining -= nbytes;

                sequence++;

            }

            if (log.isInfoEnabled())
                log.info("Computed digest: #blocks=" + sequence + ", #bytes="
                        + totalBytes);

            // Done.
            return;

        } finally {

            if (buf != null) {
                try {
                    // Release the direct buffer.
                    buf.release();
                } catch (InterruptedException e) {
                    log.warn(e);
                }
            }

        }

    }

}
