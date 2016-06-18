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

package com.bigdata.rwstore;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.Channel;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.DigestException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.log4j.Logger;
import org.apache.system.SystemUtil;

import com.bigdata.btree.BTree.Counter;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.striped.StripedCounters;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAPipelineGlue;
import com.bigdata.ha.QuorumPipeline;
import com.bigdata.ha.QuorumService;
import com.bigdata.ha.msg.HAWriteMessage;
import com.bigdata.ha.msg.IHALogRequest;
import com.bigdata.ha.msg.IHARebuildRequest;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.io.ChecksumUtility;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.FileChannelUtility.AsyncTransfer;
import com.bigdata.io.IBufferAccess;
import com.bigdata.io.IReopenChannel;
import com.bigdata.io.MergeStreamWithSnapshotData;
import com.bigdata.io.compression.CompressorRegistry;
import com.bigdata.io.compression.IRecordCompressor;
import com.bigdata.io.writecache.BufferedWrite;
import com.bigdata.io.writecache.IBackingReader;
import com.bigdata.io.writecache.IBufferedWriter;
import com.bigdata.io.writecache.WriteCache;
import com.bigdata.io.writecache.WriteCacheService;
import com.bigdata.journal.AbstractBufferStrategy;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.AbstractJournal.ISnapshotData;
import com.bigdata.journal.CommitRecordIndex;
import com.bigdata.journal.CommitRecordSerializer;
import com.bigdata.journal.FileMetadata;
import com.bigdata.journal.ForceEnum;
import com.bigdata.journal.ICommitRecord;
import com.bigdata.journal.ICommitter;
import com.bigdata.journal.IHABufferStrategy;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockView;
import com.bigdata.journal.StoreState;
import com.bigdata.journal.StoreTypeEnum;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumException;
import com.bigdata.rawstore.IAllocationContext;
import com.bigdata.rawstore.IPSOutputStream;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rwstore.StorageStats.Bucket;
import com.bigdata.service.AbstractTransactionService;
import com.bigdata.util.BytesUtil;
import com.bigdata.util.ChecksumError;

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

public class RWStore implements IStore, IBufferedWriter, IBackingReader {

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
         * TODO: We should consider a more adaptable BLOB approach where we
         * specify the maximum "slop" in an allocation as the means to determine
         * a blob boundary.  So, for example, a 5.5K allocation, with maximum slop of
         * 1K, would be allocated as a blob of 4K + 2K and not an 8K slot.
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
         * <p>
         * This has now been deprecated since it adds complexity with no significant benefit
         */
        @Deprecated String META_BITS_SIZE = RWStore.class.getName() + ".metaBitsSize";

        @Deprecated String DEFAULT_META_BITS_SIZE = "9";

        /**
         * Defines whether the metabits should be allocated an explicit demispace (default)
         * or if not, then to use a standard Allocation (which limits the metabits size to
         * the maximum FixedAllocator slot size).
         * <p>
         * The value should be either "true" or "false"
         */
        String META_BITS_DEMI_SPACE = RWStore.class.getName() + ".metabitsDemispace";

        String DEFAULT_META_BITS_DEMI_SPACE = "false";
        
        /**
         * Defines whether blobs, which are stored in multiple slot locations,
         * are read concurrently using Async NIO. This was introduced
         * specifically to reduce commit latency in scenarios where large
         * transactions can lead to very large deferred free lists (>> 10
         * million addresses), stored as blobs.
         * <p>
         * BLZG-1884 indicated a possible problem with this approach. The root
         * causes of that problem (poor handling of exceptions) have been dealt
         * with.  This option was also introduced so the async IO support can
         * now be disabled if a problem does materialize.
         */
        String READ_BLOBS_ASYNC = RWStore.class.getName() + ".readBlobsAsync";

        /**
         * Note: Windows does not handle async IO channel reopens in the same
         * fashion as Linux, leading to "overlapping file exceptions" and other
         * weirdness. Therefore this option is explicitly disabled by default 
         * on Windows.
         * 
         * @see https://jira.blazegraph.com/browse/BLZG-1911 (Blazegraph 2.1
         *      version does not work on Windows (async IO causes file lock
         *      errors))
         */
        String DEFAULT_READ_BLOBS_ASYNC = SystemUtil.isWindows() ? "false" : "true";

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
         * Defines the size of a slot that defines it as a small slot.
         * <p>
         * Any slot equal to or less than this is considered a small slot and
         * its availability for allocation is restricted to ensure a high
         * chance that contiguous allocations can be made.
         * <p>
         * This is arranged by only returning small slot allocators to the free list
         * if they have greater than 50% available slots, and then only allocating
         * slots from sparse regions with >= 50% free/committed bits.
         * <p>
         * Small slot processing can be disabled by setting the smallSlotType to zero.
         */
        String SMALL_SLOT_TYPE = RWStore.class.getName() + ".smallSlotType";

        /**
         * Enable the small slot optimization by default.
         * 
         * @see BLZG-1596 (Enable small slot optimization by default)
         */
         String DEFAULT_SMALL_SLOT_TYPE = "1024"; // standard default
//        String DEFAULT_SMALL_SLOT_TYPE = "0"; // initial default to no special processing

        /**
         * The #of free bits required to be free in a "small slot" allocator before
         * it is automatically returned to the free list.  Once the small slot waste
         * threshold comes into play, the small slot allocator for a given slot size
         * having the maximum free bits will be automatically returned to the free 
         * list if the percentage of waste in that slot size exceeds a threshold.
         * 
         * @see BLZG-1278 (Implement maximum waste policy for small slot allocators)
         */
        String SMALL_SLOT_THRESHOLD = RWStore.class.getName() + ".smallSlotThreshold";

        String DEFAULT_SMALL_SLOT_THRESHOLD = "4096"; // 50% of available bits
        
        /**
         * We have introduced extra parameters to adjust allocator usage if we notice that
         * a significant amount of storage is wasted.
         * <p>
         * First we check how many allocators of a given slot size have been created.  If
         * above {@value #SMALL_SLOT_WASTE_CHECK_ALLOCATORS} then we look a little closer.
         * <p>
         * We retrieve the allocation statistics and determine if the waste threshold is
         * exceeded, as determined by {@link SMALL_SLOT_HIGH_WASTE}.
         * <p>
         * If so, then we attempt to find an available allocator with more free bits as
         * determined by {@link SMALL_SLOT_THRESHOLD_HIGH_WASTE}.
         * 
         * @see BLZG-1278 (Implement maximum waste policy for small slot allocators)
         */
        String SMALL_SLOT_WASTE_CHECK_ALLOCATORS = RWStore.class.getName() + ".smallSlotWasteCheckAllocators";

        String DEFAULT_SMALL_SLOT_WASTE_CHECK_ALLOCATORS = "100"; // Check waste when more than 100 allocators

        /**
         * Once there are at least {@link #SMALL_SLOT_WASTE_CHECK_ALLOCATORS}
         * for a given slot size, then the {@link #SMALL_SLOT_HIGH_WASTE}
         * specifies the maximum percentage of waste that will be allowed for
         * that slot size. This prevents the amount of waste for small slot
         * allocators from growing significantly as the size of the backing
         * store increases.
         * <p>
         * The dynamic policy for small slots can be thought of as follows.
         * <dl>
         * <li>A normal allocator will be dropped onto the free list once it has
         * {@link #FREE_BITS_THRESHOLD} bits free (default 300 bits out of 8192
         * = 3.6%).</li>
         * <li>For a new store, a small slot allocator will be dropped onto the
         * free list once it has {@link #SMALL_SLOT_THRESHOLD} bits free
         * (default 4096 bits out of 8192 = 50%).</li>
         * <li>Once the #of small slots allocators for a given sized allocator
         * exceeds the {@link #DEFAULT_SMALL_SLOT_WASTE_CHECK_ALLOCATORS}, a
         * small slot allocator will be dropped onto the free list once it is
         * {@link #SMALL_SLOT_HIGH_WASTE} percent free (this amounts to 1638
         * bits out of 8192).</li>
         * </dl>
         * Thus, the small slot allocators initially are created freely because
         * they need to be highly sparse before they can be on the free list.
         * Once we have "enough" small slot allocators, we create them less
         * freely - this is achieved by changing the sparsity threshold to a
         * value that still requires the small slot allocator to be
         * significantly more sparse than a general purpose allocator.
         * 
         * @see BLZG-1278 (Implement maximum waste policy for small slot
         *      allocators)
         */
        String SMALL_SLOT_HIGH_WASTE = RWStore.class.getName() + ".smallSlotHighWaste";

        String DEFAULT_SMALL_SLOT_HIGH_WASTE = "20.0f"; // 1638 bits: 20% waste, less than 80% usage

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
    
//  // from 32 bits, need 13 to hold max offset of 8 * 1024, leaving 19 for number of blocks: 256K
//  static final int BLOCK_INDEX_BITS = 19;
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
    
    static final int ALLOCATION_SCALEUP = 16; // multiplier to convert allocations based on minimum allocation of 64k
    static private final int META_ALLOCATION = 8; // 8 * 32K is size of meta Allocation

    // If required, then allocate 1M direct buffers
    private static final int cDirectBufferCapacity = 1024 * 1024;

    private int cMaxDirectBuffers = 20; // 20M of direct buffers
    static final int cDirectAllocationOffset = 64 * 1024;

    // ///////////////////////////////////////////////////////////////////////////////////////
    // RWStore Data
    // ///////////////////////////////////////////////////////////////////////////////////////

    private final File m_fd;
//  private RandomAccessFile m_raf;
//  protected FileMetadata m_metadata;
//  protected int m_transactionCount;
//  private boolean m_committing;

//    /**
//     * When <code>true</code> the allocations will not actually be recycled
//     * until after a store restart. When <code>false</code>, the allocations are
//     * recycled once they satisfy the history retention requirement.
//     */
//  private boolean m_preserveSession = false;
//  private boolean m_readOnly;

    /**
     * The UUID of the backing store.
     * 
     * @see #initfromRootBlock(IRootBlockView)
     * @see IRawStore#getUUID()
     */
    private UUID m_storeUUID;
    
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
    
//  /** lists of free blob allocators. */
    // private final ArrayList<BlobAllocator> m_freeBlobs;

    /** lists of blocks requiring commitment. */
    // private final ArrayList<FixedAllocator> m_commitList;
    FixedAllocator m_commitHead;
    FixedAllocator m_commitTail;

//  private WriteBlock m_writes;
    
    private final Quorum<?,?> m_quorum;
    
    /**
     * The #of buffers that will be used by the {@link WriteCacheService}.
     * 
     * @see com.bigdata.journal.Options#WRITE_CACHE_BUFFER_COUNT
     */
    private final int m_writeCacheBufferCount;

    /**
     * @see com.bigdata.journal.Options#WRITE_CACHE_MIN_CLEAN_LIST_SIZE
     */
    private final int m_minCleanListSize;

    /**
     * The #of read buffers that will be used by the {@link WriteCacheService}.
     * 
     * @see com.bigdata.journal.Options#READ_CACHE_BUFFER_COUNT
     */
    private final int m_readCacheBufferCount;

    /**
     * @see com.bigdata.journal.Options#WRITE_CACHE_COMPACTION_THRESHOLD
     */
    private final int m_compactionThreshold;
    
    /**
     * @see com.bigdata.journal.Options#HOT_CACHE_THRESHOLD
     */
    private final int m_hotCacheThreshold;
    
    /**
     * @see com.bigdata.journal.Options#HOT_CACHE_SIZE
     */
    private final int m_hotCacheSize;
    
    /**
     * The key for the {@link CompressorRegistry} which identifies the
     * {@link IRecordCompressor} to be applied (optional).
     * 
     * @see com.bigdata.journal.Options#HALOG_COMPRESSOR
     */
    private final String m_compressorKey;
    
    /**
     * Note: This is not final because we replace the {@link WriteCacheService}
     * during {@link #reset(long)} in order to propagate the then current quorum
     * token to the {@link WriteCacheService}.
     */
    private RWWriteCacheService m_writeCacheService;

    /**
     * Return the then current {@link WriteCacheService} object.
     * 
     * @see IHABufferStrategy#getWriteCacheService()
     */
    public RWWriteCacheService getWriteCacheService() {
        m_allocationReadLock.lock();
        try {
            return m_writeCacheService;
        } finally {
            m_allocationReadLock.unlock();
        }
        
    }
    
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
     * We allow blob headers so the maximum blob size is Integer.MAX_VALUE.
     */
    final int m_maxBlobAllocSize = Integer.MAX_VALUE;
    
    /**
     * This lock is used to exclude readers/writers performing IOs against the
     * backing file when the extent of the backing file is about to be changed.
     * Readers and writers take the {@link ReadLock}. The {@link WriteLock} is
     * taken when the file extent must be changed. This is a workaround for an
     * old (an unresolved as of February 2010) Sun bug.
     * <p>
     * Note: Any public method that ONLY takes the extensionLock MUST NOT make
     * calls that could take the {@link #m_allocationLock}. This would cause a
     * lock ordering problem. If both locks must be taken, then the
     * {@link #m_allocationLock} MUST be taken first.
     * 
     * @see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6371642
     * @see #m_allocationLock
     */
    final private ReentrantReadWriteLock m_extensionLock = new ReentrantReadWriteLock();

    /**
     * An explicit allocation lock supports exclusive access for allocator
     * mutation and shared access for readers.
     * <p>
     * Note: You must hold the {@link #m_allocationReadLock} to read the
     * allocators.
     * <p>
     * Note: You must hold the {@link #m_allocationWriteLock} while allocating
     * or clearing allocations.
     * <p>
     * Note: It is only when an allocation triggers a file extension that the
     * {@link WriteLock} of the {@link #m_extensionLock} needs to be taken.
     * 
     * TODO: There is scope to take advantage of the different allocator sizes
     * and provide allocation locks on the fixed allocators. We will still need
     * a store-wide allocation lock when creating new allocation areas, but
     * significant contention may be avoided.
     */
    final private ReentrantReadWriteLock m_allocationLock = new ReentrantReadWriteLock();
    /**
     * Lock used for exclusive access to the allocators.
     * <p>
     * Note: Historically, this lock was only required for mutation and readers
     * did not content for a lock.
     */
    final private WriteLock m_allocationWriteLock = m_allocationLock.writeLock();
    /**
     * Lock used for shared access to allocators.
     * <p>
     * Note: Historically the allocators were unprotected for shared acccess
     * (readers) and protected by a single lock for mutation (writes). Shared
     * access by readers was safe since (a) old allocators were never replaced;
     * and (b) readers had access only to committed data.
     * <p>
     * This situation was changed when the {@link #postHACommit(IRootBlockView)}
     * method was introduced since it could replace allocators in a manner that
     * was not safe for shared access by readers. Methods that were historically
     * using unprotected shared access now require protected shared access using
     * this lock.
     * 
     * @see #postHACommit(IRootBlockView)
     * @see #getData(long, int)
     * @see #getData(long, byte[])
     * @see #getData(long, byte[], int, int)
     */
    final private ReadLock m_allocationReadLock = m_allocationLock.readLock();

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
//  private static final int MAX_DEFERRED_FREE = 4094; // fits in 16k block
    private final long m_minReleaseAge;
    
    /**
     * The #of open transactions (read-only or read-write).
     * 
     * This is guarded by the {@link #m_allocationLock}.
     */
    private int m_activeTxCount = 0;
    
    private volatile long m_lastDeferredReleaseTime = 0L;
//  private final ArrayList<Integer> m_currentTxnFreeList = new ArrayList<Integer>();
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
        
        final private String compressorKey;
        
        public WriteCacheImpl(final IBufferAccess buf,
                final boolean useChecksum,
                final boolean bufferHasData,
                final IReopenChannel<FileChannel> opener,
                final long fileExtent, final String compressorKey)
                throws InterruptedException {

            super(buf, useChecksum, m_quorum != null
                    /*&& m_quorum.isHighlyAvailable()*/, bufferHasData, opener,
                    fileExtent,
                    m_bufferedWrite);

            this.compressorKey = compressorKey;
            
        }

        @Override
        public String getCompressorKey() {

            return compressorKey;
            
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
            m_writeCacheService.debugAddrs(offset, length, action);
        }
        
        @Override
        protected void addAddress(int latchedAddr, int size) {
            // No longer valid
            // RWStore.this.addAddress(latchedAddr, size);
        }

        @Override
        protected void removeAddress(int latchedAddr) {
            // No longer valid
            // RWStore.this.removeAddress(latchedAddr);
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

        // Remove parameterisation, we want to use fixed Allocator block sizing
        //	there is no significant advantage to parameterize this since file cache
        //	locality is handled by size of the allocation - 256K is a reasonable
        //	number as 32 * 8 * 1K size.
        //
        // Equally there is no benefit to increasing the size of the Allocators beyond
        //	1K.
//        cDefaultMetaBitsSize = Integer.valueOf(fileMetadata.getProperty(
//                Options.META_BITS_SIZE,
//                Options.DEFAULT_META_BITS_SIZE));
        
//        cDefaultMetaBitsSize = 9;

//        if (cDefaultMetaBitsSize < 9)
//            throw new IllegalArgumentException(Options.META_BITS_SIZE
//                    + " : Must be GTE 9");
                
        m_metaBitsSize = cDefaultMetaBitsSize;

        m_useMetabitsDemispace = Boolean.valueOf(fileMetadata.getProperty(
                Options.META_BITS_DEMI_SPACE,
                Options.DEFAULT_META_BITS_DEMI_SPACE));
        
        cDefaultFreeBitsThreshold = Integer.valueOf(fileMetadata.getProperty(
                Options.FREE_BITS_THRESHOLD,
                Options.DEFAULT_FREE_BITS_THRESHOLD));
        
        if (cDefaultFreeBitsThreshold < 1 || cDefaultFreeBitsThreshold > 5000) {
            throw new IllegalArgumentException(Options.FREE_BITS_THRESHOLD
                    + " : Must be between 1 and 5000");
        }
        
        m_readBlobsAsync = Boolean.valueOf(fileMetadata.getProperty(
                Options.READ_BLOBS_ASYNC,
                Options.DEFAULT_READ_BLOBS_ASYNC));

    	cSmallSlot = Integer.valueOf(fileMetadata.getProperty(
                Options.SMALL_SLOT_TYPE,
                Options.DEFAULT_SMALL_SLOT_TYPE));
        
    	cSmallSlotThreshold = Integer.valueOf(fileMetadata.getProperty(
                Options.SMALL_SLOT_THRESHOLD,
                Options.DEFAULT_SMALL_SLOT_THRESHOLD));
        
    	cSmallSlotWasteCheckAllocators = Integer.valueOf(fileMetadata.getProperty(
                Options.SMALL_SLOT_WASTE_CHECK_ALLOCATORS,
                Options.DEFAULT_SMALL_SLOT_WASTE_CHECK_ALLOCATORS));
    	
    	cSmallSlotHighWaste = Float.valueOf(fileMetadata.getProperty(
                Options.SMALL_SLOT_HIGH_WASTE,
                Options.DEFAULT_SMALL_SLOT_HIGH_WASTE));
    	
//    	cSmallSlotThresholdHighWaste = Integer.valueOf(fileMetadata.getProperty(
//                Options.SMALL_SLOT_THRESHOLD_HIGH_WASTE,
//                Options.DEFAULT_SMALL_SLOT_THRESHOLD_HIGH_WASTE));
    	/*
    	 * The highWasteThreshold is more sensibly calculated from
    	 * the high waste value.
    	 */
    	cSmallSlotThresholdHighWaste = (int) (cSmallSlotHighWaste * 8192 / 100);
    	
        if (cSmallSlot < 0 || cSmallSlot > 2048) {
            throw new IllegalArgumentException(Options.SMALL_SLOT_TYPE
                    + " : Must be between 0 and 2048");
        }
        
        m_metaBits = new int[m_metaBitsSize];
        
        m_metaTransientBits = new int[m_metaBitsSize];
                
        m_quorum = quorum;
        
        m_fd = fileMetadata.file;
        
        // initialize striped performance counters for this store.
        this.storeCounters.set(new StoreCounters(10/* batchSize */));
        
        final IRootBlockView m_rb = fileMetadata.rootBlock;

        m_allocs = new ArrayList<FixedAllocator>();
        
        // m_freeBlobs = new ArrayList<BlobAllocator>();

        try {
            final RandomAccessFile m_raf = fileMetadata.getRandomAccessFile();
            m_reopener = new ReopenFileChannel(m_fd, m_raf, fileMetadata.readOnly);
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
        
        m_readCacheBufferCount = Integer.valueOf(fileMetadata.getProperty(
                com.bigdata.journal.Options.READ_CACHE_BUFFER_COUNT,
                com.bigdata.journal.Options.DEFAULT_READ_CACHE_BUFFER_COUNT));
        
        if (log.isInfoEnabled())
            log.info(com.bigdata.journal.Options.WRITE_CACHE_BUFFER_COUNT
                    + "=" + m_writeCacheBufferCount);

        this.m_minCleanListSize = Integer.valueOf(fileMetadata.getProperty(
                com.bigdata.journal.Options.WRITE_CACHE_MIN_CLEAN_LIST_SIZE,
                com.bigdata.journal.Options.DEFAULT_WRITE_CACHE_MIN_CLEAN_LIST_SIZE));

        if (log.isInfoEnabled())
            log.info(com.bigdata.journal.Options.WRITE_CACHE_MIN_CLEAN_LIST_SIZE + "="
                    + m_minCleanListSize);

        this.m_compactionThreshold = Double.valueOf(fileMetadata.getProperty(
                com.bigdata.journal.Options.WRITE_CACHE_COMPACTION_THRESHOLD,
                com.bigdata.journal.Options.DEFAULT_WRITE_CACHE_COMPACTION_THRESHOLD)).intValue();

        if (log.isInfoEnabled())
            log.info(com.bigdata.journal.Options.WRITE_CACHE_COMPACTION_THRESHOLD + "="
                    + m_compactionThreshold);

        this.m_hotCacheThreshold = Double.valueOf(fileMetadata.getProperty(
                com.bigdata.journal.Options.HOT_CACHE_THRESHOLD,
                com.bigdata.journal.Options.DEFAULT_HOT_CACHE_THRESHOLD)).intValue();

        if (log.isInfoEnabled())
            log.info(com.bigdata.journal.Options.HOT_CACHE_THRESHOLD + "="
                    + m_hotCacheThreshold);

        this.m_hotCacheSize = Double.valueOf(fileMetadata.getProperty(
                com.bigdata.journal.Options.HOT_CACHE_SIZE,
                com.bigdata.journal.Options.DEFAULT_HOT_CACHE_SIZE)).intValue();

        if (log.isInfoEnabled())
            log.info(com.bigdata.journal.Options.HOT_CACHE_SIZE + "="
                    + m_hotCacheSize);

        this.m_compressorKey = fileMetadata.getProperty(
                com.bigdata.journal.Options.HALOG_COMPRESSOR,
                com.bigdata.journal.Options.DEFAULT_HALOG_COMPRESSOR);

        if (log.isInfoEnabled())
            log.info(com.bigdata.journal.Options.HALOG_COMPRESSOR + "="
                    + m_compressorKey);

        // m_writeCache = newWriteCache();

        try {
            if (m_rb.getNextOffset() == 0) { // if zero then new file
                setAllocations(fileMetadata);
    
            /*
             * FIXME Martyn, the code paths here are crazy complicated.
             * defaultInit() is also invoked from initFromRootBlock().
             * Simplify this. BBT
             */
                m_storeUUID = m_rb.getUUID();

                defaultInit();
                
                m_maxFixedAlloc = m_allocSizes[m_allocSizes.length-1]*64;
                m_minFixedAlloc = m_allocSizes[0]*64;
                
                m_storageStats = new StorageStats(m_allocSizes);

//              // Check for overwrite option and set overwrite buffer if
//              // required
//              if (Boolean.valueOf(fileMetadata.getProperty(
//                      Options.OVERWRITE_DELETE,
//                      Options.DEFAULT_OVERWRITE_DELETE))) {
//                  m_writeCache.setOverwriteBuffer(m_maxFixedAlloc);
//              }
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
            
            // Maximum theoretically addressable file size is determined by the
            //  maximum allocator slot size multiplied by Integer.MAX_VALUE
            // FIXME: do we want to constrain this as a system property?
            m_maxFileSize = ((long) Integer.MAX_VALUE) * m_maxFixedAlloc;

            // setup write cache AFTER init to ensure filesize is correct!
            
            m_writeCacheService = newWriteCacheService();

            final int maxBlockLessChk = m_maxFixedAlloc-4;
             
            assert m_maxFixedAlloc > 0;
            
            m_deferredFreeOut = PSOutputStream.getNew(this, m_maxFixedAlloc, null);

//          if (Boolean.valueOf(fileMetadata.getProperty(
//                  Options.MAINTAIN_BLACKLIST,
//                  Options.DEFAULT_MAINTAIN_BLACKLIST))) {
//              m_blacklist = new ConcurrentHashMap<Integer, String>();
//              m_lockAddresses = new ConcurrentHashMap<Integer, Long>();
//          }

        } catch (IOException e) {
            throw new StorageTerminalError("Unable to initialize store", e);
        }
    }
    
    /**
     * Called from WriteCache.resetRecordMapFromBuffer
     * 
     * If a FixedAllocator already exists for the address then just set the
     * address as active, otherwise, create a new allocator and try again, which
     * should work second time around if we are correctly in sync.
     * 
     * @param latchedAddr
     *            The latched address.
     * @param size
     *            The size of the application data -or- <code>-size</code> if
     *            this provides notice of the existence of an allocator for that
     *            <i>latchedAddr</i> but the address itself should not yet be
     *            allocated.
     */
    void addAddress(final int latchedAddr, final int size) {
        // ignore zero address
        if (latchedAddr == 0)
            return;

        m_allocationWriteLock.lock();
        try {
            FixedAllocator alloc = null;
            try {
                alloc = getBlock(latchedAddr);
            } catch (final PhysicalAddressResolutionException par) {
                // Must create new allocator
            }
            final int size2 = size < 0 ? -size : size;
            if (alloc == null) {
                final int i = fixedAllocatorIndex(size2);
                final int block = 64 * m_allocSizes[i];
                final ArrayList<FixedAllocator> list = m_freeFixed[i];
                if (log.isTraceEnabled())
                    log.trace("Creating new Allocator for address: "
                            + latchedAddr);

                final FixedAllocator allocator = new FixedAllocator(this, block);

                allocator.setFreeList(list);
                allocator.setIndex(m_allocs.size());

                m_allocs.add(allocator);

                // Check correctly synchronized creation
                assert allocator == getBlock(latchedAddr);

                alloc = allocator;
            }

            assert size2 <= alloc.getSlotSize();

            if (size > 0) {

                /*
                 * This is a real allocation.
                 */

                alloc.setAddressExternal(latchedAddr);

            }

        } finally {

            m_allocationWriteLock.unlock();

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

        m_allocationWriteLock.lock();
        try {
            // assert m_commitList.size() == 0;

            final FixedAllocator alloc = getBlockByAddress(latchedAddr);

            assert alloc != null;

            final int addrOffset = getOffset(latchedAddr);
            if (alloc == null) {
                throw new IllegalArgumentException(
                        "Invalid address provided to immediateFree: "
                                + latchedAddr);
            }
            final long pa = alloc.getPhysicalAddress(addrOffset);

            if (log.isTraceEnabled())
                log.trace("Freeing allocation at " + latchedAddr
                        + ", physical address: " + pa);

            alloc.free(latchedAddr, 0, false);

            // assert m_commitList.size() == 0;
        } finally {
            m_allocationWriteLock.unlock();
        }
    }
    
    /**
     * Create and return a new {@link RWWriteCacheService} instance. The caller
     * is responsible for closing out the old one and must be holding the
     * appropriate locks when it switches in the new instance.
     */
    private RWWriteCacheService newWriteCacheService() {
        try {

//            final boolean highlyAvailable = m_quorum != null
//                    && m_quorum.isHighlyAvailable();

            final boolean prefixWrites = m_quorum != null; // highlyAvailable

            return new RWWriteCacheService(m_writeCacheBufferCount,
                    m_minCleanListSize, m_readCacheBufferCount, prefixWrites, m_compactionThreshold, m_hotCacheSize, m_hotCacheThreshold,

                    convertAddr(m_fileSize), m_reopener, m_quorum, this) {

                        @Override
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
                                    fileExtent, m_compressorKey);
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
            m_writeCacheService.close();
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
        
        // CANNOT check physicalAddress if follower
        if (m_quorum == null && log.isTraceEnabled()) {
            final int commitRecordAddr = (int) (rbv.getCommitRecordAddr() >> 32);
            log.trace("CommitRecord " + rbv.getCommitRecordAddr()
                    + " at physical address: "
                    + physicalAddress(commitRecordAddr));
        }
        
        final long commitCounter = rbv.getCommitCounter();

//      final int metaStartAddr = (int) -(metaAddr >> 32); // void
//      final int fileSize = (int) -(metaAddr & 0xFFFFFFFF);

        if (log.isTraceEnabled())
            log.trace("m_allocation: " + nxtalloc + ", m_metaBitsAddr: "
                    + metaBitsAddr + ", m_commitCounter: " + commitCounter);
        
    }
    
    /**
     * Utility to encapsulate RootBlock interpretation.
     */
    static private class RootBlockInfo {
        
//        int nextAllocation(final IRootBlockView rb) {
//            final long nxtOffset = rb.getNextOffset();
//
//            // next allocation to be made (in -32K units).
//            final int ret = -(int) (nxtOffset >> 32);
//            
//            /*
//             * Skip the first 32K in the file. The root blocks live here but
//             * nothing else.
//             */
//            return ret == 0 ? -(1 + META_ALLOCATION) : ret;
//        }
        
        /**
         * Used to transparently re-open the backing channel if it has been closed
         * by an interrupt during an IO.
         */
        private final ReopenFileChannel m_reopener;
        /**
         * Meta-Allocations stored as {int address; int[8] bits}, so each block
         * holds 8*32=256 allocation slots of 1K totaling 256K.
         * <p>
         * The returned int array is a flattened list of these int[9] blocks
         */
        private final int[] m_metabits;
        private final long m_storageStatsAddr;
        private final long m_lastDeferredReleaseTime;
        
        RootBlockInfo(final IRootBlockView rb,
                final ReopenFileChannel reopener) throws IOException {
            
            this.m_reopener = reopener;
            
            final long rawmbaddr = rb.getMetaBitsAddr();
            
            /*
             * The #of int32 values in the metabits region.
             * 
             * We get this by taking bottom 16 bits of the metaBitsAddr. This
             * gives the #of int32 values in the metabits regions (up to 64k
             * int32 values).
             */
            final int metaBitsStore = (int) (rawmbaddr & 0xFFFF);
            
            
            // The byte offset of the metabits region in the file.
            final long pmaddr = rawmbaddr >> 16;
            
            /*
             * Read the metabits block, including a header and the int32[]
             * that encodes both startAddrs and bit vectors.
             */
            final byte[] buf = new byte[metaBitsStore * 4];

            FileChannelUtility.readAll(m_reopener, ByteBuffer.wrap(buf), pmaddr);
    
            final DataInputStream strBuf = new DataInputStream(new ByteArrayInputStream(buf));
            
            // Can handle minor store version incompatibility
            strBuf.readInt(); // STORE VERSION
            m_lastDeferredReleaseTime = strBuf.readLong(); // Last Deferred Release Time
            strBuf.readInt(); // cDefaultMetaBitsSize
            
            final int allocBlocks = strBuf.readInt();
            m_storageStatsAddr = strBuf.readLong(); // m_storageStatsAddr

            // step over those reserved ints
            for (int i = 0; i < cReservedMetaBits; i++) {
                strBuf.readInt();
            }

            // step over the allocSizes
            for (int i = 0; i < allocBlocks; i++) {
                strBuf.readInt();
            }
            final int metaBitsSize = metaBitsStore - allocBlocks - cMetaHdrFields; // allow for header fields
            
            // Must be multiple of 9
            assert metaBitsSize % 9 == 0;
            
            final int[] ret = new int[metaBitsSize];
            for (int i = 0; i < metaBitsSize; i++) {
                ret[i] = strBuf.readInt();
            }

            /*
             * Meta-Allocations stored as {int address; int[8] bits}, so each block
             * holds 8*32=256 allocation slots of 1K totaling 256K.
             */
            m_metabits = ret;
        }
        
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
    private void initfromRootBlock(final IRootBlockView rb) throws IOException {
        // m_rb = m_fmv.getRootBlock();
        assert(rb != null);

        m_storeUUID = rb.getUUID();
        
        if (rb.getNextOffset() == 0) {

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
            final long nxtOffset = rb.getNextOffset();

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
                final long metaAddr = rb.getMetaStartAddr();

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
            long rawmbaddr = rb.getMetaBitsAddr();
            
            /*
             * The #of int32 values in the metabits region.
             * 
             * We get this by taking bottom 16 bits of the metaBitsAddr. This
             * gives the #of int32 values in the metabits regions (up to 64k
             * int32 values). Each int32 value in the metaBits[] gives us 32
             * allocators. So, 16-bits gives us up 64k * 32 = 2M allocators.
             * Except, that the total #of allocators is reduced by the presence
             * of a startAddr every N positions in the metaBits[].
             * 
             * The theoretical maximum number is also reduced since the number
             * of "committed" bits could be half the total number of bits.
             * 
             * The theoretical restriction is also limited by the maximum indexable
             * allocator, since only 19 bits is available to the index, which, once
             * the sign is removed reduces the maximum number of addressable
             * allocators to 256K.
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
                
                switch ((storeVersion & 0xFF00)) {
            	case (cVersion & 0xFF00):
            	case (cVersionDemispace & 0xFF00):
                	break;
                default:
                    throw new IllegalStateException(
                            "Incompatible RWStore header version: storeVersion="
                                    + storeVersion + ", cVersion=" + cVersion + ", demispace: " + isUsingDemiSpace());
                }
                m_lastDeferredReleaseTime = strBuf.readLong();
                if (strBuf.readInt() != cDefaultMetaBitsSize) {
                	throw new IllegalStateException("Store opened with unsupported metabits size");
                }
                
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
                // m_metaTransientBits = (int[]) m_metaBits.clone();
                
                syncMetaTransients();
        
                final int numFixed = m_allocSizes.length;
    
                m_freeFixed = new ArrayList[numFixed];
    
                for (int i = 0; i < numFixed; i++) {
                    m_freeFixed[i] = new ArrayList<FixedAllocator>();
                }
    
                checkCoreAllocations();
        
                readAllocationBlocks();
                
            }
            
            if (log.isInfoEnabled())
                log.info("restored from RootBlock: " + m_nextAllocation 
                        + ", " + m_metaBitsAddr);
        }
    }
    
    /**
     * Uses System.arraycopy rather than clone() to duplicate the
     * metaBits to the metaTransientBits, which will be faster.
     */
    private void syncMetaTransients() {
    	if (m_metaTransientBits == null ||  m_metaTransientBits.length != m_metaBits.length) {
    		m_metaTransientBits = (int[]) m_metaBits.clone();
    	} else {
    		System.arraycopy(m_metaBits, 0, m_metaTransientBits, 0, m_metaTransientBits.length);
    	}
    }

//  /*
//   * Called when store is opened to make sure any deferred frees are
//   * cleared.
//   * 
//   * Stored persistently is only the list of addresses of blocks to be freed,
//   * the knowledge of the txn release time does not need to be held persistently,
//   * this is only relevant for transient state while the RWStore is open.
//   * 
//   * The deferredCount is the number of entries - integer address and integer
//   * count at each address
//   */
//  private void clearOutstandingDeferrels(final int deferredAddr, final int deferredCount) {
//      if (deferredAddr != 0) {
//          assert deferredCount != 0;
//          final int sze = deferredCount * 8 + 4; // include space for checksum
//          
//          if (log.isDebugEnabled())
//              log.debug("Clearing Outstanding Deferrals: " + deferredCount);
//          
//          byte[] buf = new byte[sze];
//          getData(deferredAddr, buf);
//          
//          final byte[] blockBuf = new byte[8 * 1024]; // maximum size required 
//          
//          ByteBuffer in = ByteBuffer.wrap(buf);
//          for (int i = 0; i < deferredCount; i++) {
//              int blockAddr = in.getInt();
//              int addrCount = in.getInt();
//              
//              // now read in this block and free all addresses referenced
//              getData(blockAddr, blockBuf, 0, addrCount*4 + 4);
//              ByteBuffer inblock = ByteBuffer.wrap(blockBuf);
//              for (int b = 0; b < addrCount; b++) {
//                  final int defAddr = inblock.getInt();
//                  Allocator alloc = getBlock(defAddr);
//                  if (alloc instanceof BlobAllocator) {
//                      b++;
//                      assert b < addrCount;
//                      alloc.free(defAddr, inblock.getInt());
//                  } else {
//                      alloc.free(defAddr, 0); // size ignored for FreeAllocators
//                  }
//              }
//              // once read then free the block allocation
//              free(blockAddr, 0);
//          }
//          
//          // lastly free the deferredAddr
//          free(deferredAddr, 0);          
//      }
//      
//  }

    /*********************************************************************
     * make sure resource is closed!
     **/
    protected void finalize() {
        close();
    }

    @SuppressWarnings("unchecked")
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

                    final FixedAllocator allocator = readAllocator(addr);

                    allocator.setDiskAddr(i); // store bit, not physical address!
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
    
    private FixedAllocator readAllocator(final long addr) throws IOException {
        final byte buf[] = new byte[ALLOC_BLOCK_SIZE];

        FileChannelUtility.readAll(m_reopener, ByteBuffer.wrap(buf), addr);

        final ByteArrayInputStream baBuf = new ByteArrayInputStream(buf);
        final DataInputStream strBuf = new DataInputStream(baBuf);

        final int allocSize = strBuf.readInt(); // if Blob < 0

        assert allocSize > 0;
        
        final int slotSizeIndex = slotSizeIndex(allocSize);
        
        if (slotSizeIndex == -1) {
            throw new IllegalStateException("Unexpected allocation size of: " + allocSize);
        }

        final FixedAllocator fa =  new FixedAllocator(this, allocSize);//, m_writeCache);
        fa.read(strBuf);
        
        final int chk = ChecksumUtility.getCHK().checksum(buf,
                buf.length - baBuf.available());
        
        int tstChk = strBuf.readInt();
        if (tstChk != chk) {
            throw new IllegalStateException("FixedAllocator checksum error");
        }

        if (slotSizeIndex == -1) {
            throw new IllegalStateException("Unexpected allocation size of: " + allocSize);
        }

        final ArrayList<? extends Allocator> freeList;
        
        freeList = m_freeFixed[slotSizeIndex];

        fa.setFreeList(freeList);

        return fa;
    }
    
    /**
     * Computes the slot size index given the absolute slot size.
     * 
     * If the slotSizes are [1,2,4] this corresponds to absolute sizes by 
     * multiplying by 64 of [64, 128, 256], so slotSizeIndex(64) would return 0,
     * and any parameter other than 64, 128 or 256 would return -1.
     * 
     * @param allocSize - absolute slot size
     * @return
     */
    private int slotSizeIndex(final int allocSize) {
        if (allocSize % 64 != 0)
            return -1;
        
        final int slotSize = allocSize / 64;
        int slotSizeIndex = -1;
        for (int index = 0; index < m_allocSizes.length; index++) {
            if (m_allocSizes[index] == slotSize) {
                slotSizeIndex = index;
                break;
            }
        }
        
        return slotSizeIndex;
    }
    
    /**
     * Required for HA to support post commit message to synchronize allocators
     * with new state.  By this time the new allocator state will have been flushed
     * to the disk, so should be 1) On disk, 2) Probably in OS cache and 3) Possibly
     * in the WriteCache.
     * 
     * For efficiency we do not want to default to reading from disk.
     * 
     * If there is an existing allocator, then we can compare the old with the new state
     * to determine which addresses have been freed and hence which addresses should be
     * removed from the external cache.
     * 
     * @param index of Alloctor to be updated
     * @param addr on disk to be read
     * @throws InterruptedException 
     * @throws ChecksumError 
     * @throws IOException 
     */
    private void updateFixedAllocator(final int index, final long addr) throws ChecksumError, InterruptedException, IOException {
        final ByteBuffer buf = m_writeCacheService.read(addr, ALLOC_BLOCK_SIZE);

        final ByteArrayInputStream baBuf = new ByteArrayInputStream(buf.array());
        final DataInputStream strBuf = new DataInputStream(baBuf);

        final int allocSize = strBuf.readInt(); // if Blob < 0
        assert allocSize > 0;
        
        final int slotIndex = slotSizeIndex(allocSize);
        if (slotIndex == -1)
            throw new IllegalStateException("Invalid allocation size: " + allocSize);
        
        final FixedAllocator allocator = new FixedAllocator(this, allocSize);
        final ArrayList<? extends Allocator> freeList = m_freeFixed[slotIndex];
        
        if (index < m_allocs.size()) {
            final FixedAllocator old = m_allocs.get(index);
            freeList.remove(old);
            
            m_allocs.set(index,  allocator);
            allocator.setFreeList(freeList);
            
            // Need to iterate over all allocated bits in "old" and see if they
            //  are clear in "new".  If so then clear from externalCache

        } else {
            assert index == m_allocs.size();
            m_allocs.add(allocator);
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
            if (!isOnCommitList(f)) {
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

//  // Root interface
//  public long getRootAddr() {
//      return m_rootAddr;
//  }
//
//  // Root interface
//  public PSInputStream getRoot() {
//      try {
//          return getData(m_rootAddr);
//      } catch (Exception e) {
//          throw new StorageTerminalError("Unable to read root data", e);
//      }
//  }
//
//  public void setRootAddr(long rootAddr) {
//      m_rootAddr = (int) rootAddr;
//  }

//  // Limits
//  public void setMaxFileSize(final int maxFileSize) {
//      m_maxFileSize = maxFileSize;
//  }

    public long getMaxFileSize() {
        return m_maxFileSize;
    }

//  // Allocators
//  public PSInputStream getData(final long addr) {
//      return getData((int) addr, addr2Size((int) addr));
//  }
//
//  // Allocators
//  public PSInputStream getData(final int addr, final int size) {
//        final Lock readLock = m_extensionLock.readLock();
//
//        readLock.lock();
//        
//      try {
//          try {
//              m_writeCache.flush(false);
//          } catch (InterruptedException e1) {
//              throw new RuntimeException(e1);
//          }
//
//          if (addr == 0) {
//              return null;
//          }
//
//          final PSInputStream instr = PSInputStream.getNew(this, size);
//
//          try {
////                m_raf.seek(physicalAddress(addr));
////                m_raf.readFully(instr.getBuffer(), 0, size);
////                m_raf.getChannel().read(ByteBuffer.wrap(instr.getBuffer(), 0, size), physicalAddress(addr));
//              FileChannelUtility.readAll(m_reopener, ByteBuffer.wrap(instr.getBuffer(), 0, size),
//                      physicalAddress(addr));
//          } catch (IOException e) {
//              throw new StorageTerminalError("Unable to read data", e);
//          }
//
//          return instr;
//      } finally {
//          readLock.unlock();
//      }
//  }

    volatile private long m_cacheReads = 0;
    volatile private long m_diskReads = 0;
    volatile private int m_allocations = 0;
    volatile private int m_frees = 0;
    volatile private long m_nativeAllocBytes = 0;
    
    /**
     * Alternative method signature returning a ByteBuffer rather than receiving a
     * byte array.
     * <p>
     * If a blob then an extra byte array is required in which to build the data,
     * but otherwise extra buffering could be avoided be reading directly from
     * the WriteCacheService.
     * 
     * @param rwaddr
     * @param sze
     * @return
     */
    public ByteBuffer getData(final long rwaddr, final int sze) {
        /*
         * Note: Contend with postHACommit().
         */
        final Lock lock = m_allocationReadLock;
        lock.lock();
        try {
            // must allow for checksum
            if (sze > (m_maxFixedAlloc-4) || m_writeCacheService == null) {
                final byte buf[] = new byte[sze + 4]; // 4 bytes for checksum
            
                getData(rwaddr, buf, 0, sze+4);
            
                return ByteBuffer.wrap(buf, 0, sze);
            } else {
                final long paddr = physicalAddress((int) rwaddr);
                
                if (paddr == 0) {
                    
                    assertAllocators();
    
                    throw new PhysicalAddressResolutionException(rwaddr);
                    
                }
                
                assert paddr > 0;
                try {
                    return m_writeCacheService.read(paddr, sze+4);
                } catch (Throwable e) {
                    /*
                     * Note: ClosedByInterruptException can be thrown out of
                     * FileChannelUtility.readAll(), typically because the LIMIT on
                     * a query was satisfied, but we do not want to log that as an
                     * error.
                     */
    //              log.error(e,e);
                    throw new RuntimeException("addr=" + rwaddr + " : cause=" + e, e);
    
                }
            }
        } finally {
            lock.unlock();
        }
    }


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
    
    /**
     * Set the option below to true to enable asynchronous reads of blob data.
     * The aim is to reduce latency when reading blobs from disk as it will
     * enable the disk controllers to re-order IO requests nd where possible
     * process in parallel. This should benefit all Blob reads but specifically
     * helps large deferredFree data to reduce commit latency as described in
     * BLZG-1663.
     * 
     * @see BLZG-1663
     * @see BLZG-1884 RWStore ASYNC IO fails to make progress (apparent deadlock)
     */
    final private boolean m_readBlobsAsync;
    
    public void getData(final long addr, final byte buf[], final int offset,
            final int length) {

        assertOpen();

        if (addr == 0) {
            return;
        }

        final long begin = System.nanoTime();

        /*
         * Note: Contend with postHACommit().
         */
        final Lock lock = m_allocationReadLock;

        lock.lock();
        
        try {
            assertOpen(); // check again after taking lock
            
//          assertNoRebuild();

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
                    if (!m_readBlobsAsync) { // synchronous read of blob data
	                    int cursor = 0;
	                    int rdlen = m_maxFixedAlloc;
	                    for (int i = 0; i < nblocks; i++) {
	                        if (i == (nblocks - 1)) {
	                            rdlen = length - cursor;
	                        }
	                        getData(blobHdr[i], buf, cursor, rdlen); // include space for checksum
	                        cursor += rdlen-4; // but only increase cursor by data
	                    }
//                    } else { // s_readBlobsAsync
//	                    final AsynchronousFileChannel channel = m_reopener.getAsyncChannel();
//						final ArrayList<Future<Integer>> reads = new ArrayList<Future<Integer>>();
//						try {
//							int cursor = 0;
//							int rdlen = m_maxFixedAlloc;
//							int cacheReads = 0;
//							for (int i = 0; i < nblocks; i++) {
//								if (i == (nblocks - 1)) {
//									rdlen = length - cursor;
//								}
//								final ByteBuffer bb = ByteBuffer.wrap(buf,
//										cursor, rdlen-4); // strip off checksum to avoid overlapping buffer reads!
//								final long paddr = physicalAddress(blobHdr[i]);
//								final ByteBuffer cache = m_writeCacheService._readFromCache(paddr, rdlen);
//								if (cache != null) {
//									bb.put(cache); // write cached data!
//									cacheReads++;
//								} else {
//									reads.add(channel.read(bb,
//											paddr));
//								}
//								cursor += rdlen - 4; // but only increase cursor by data
//							}
//							for (Future<Integer> r : reads) {
//								r.get();
//							}
//						} catch (Exception e) {
//	                         throw new IOException("Error from async IO", e);
//	    				} finally {
//							for (Future r : reads) {
//								r.cancel(true);
//							}
//	                    }
					} else { // read non-cached data with FileChannelUtility
						final ArrayList<AsyncTransfer> transfers = new ArrayList<AsyncTransfer>();
							int cursor = 0;
							int rdlen = m_maxFixedAlloc;
							for (int i = 0; i < nblocks; i++) {
								if (i == (nblocks - 1)) {
									rdlen = length - cursor;
								}
								final ByteBuffer bb = ByteBuffer.wrap(buf,
										cursor, rdlen - 4); // strip off
															// checksum to avoid
															// overlapping
															// buffer reads!
								final long paddr = physicalAddress(blobHdr[i]);
								final ByteBuffer cache;
								try {
									cache = m_writeCacheService._readFromCache(paddr, rdlen);
								} catch (Exception e) {
									throw new IOException("Error from async IO", e);
								}
								if (cache != null) {
									bb.put(cache); // write cached data!
								} else {
									transfers.add(new AsyncTransfer(paddr, bb));
								}
								cursor += rdlen - 4; // but only increase cursor
														// by data
							}
							FileChannelUtility.readAllAsync(m_reopener, transfers);
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
                    throw new IllegalStateException("Bad Address: length requested greater than allocated slot: " + slotSize + " < " + length);
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
                    bbuf = m_writeCacheService != null ? m_writeCacheService.read(paddr, length) : null;
                } catch (Throwable t) {
                    throw new IllegalStateException(
                            "Error reading from WriteCache addr: " + paddr
                                    + " length: " + (length - 4)
                                    + ", writeCacheDebug: "
                                    + m_writeCacheService.addrDebugInfo(paddr), t);
                }
                if (bbuf != null) {
                    if (bbuf.limit() != length-4) {
                        assertAllocators();
                        throw new IllegalStateException(
                                "Incompatible buffer size for addr: " + paddr
                                        + ", " + bbuf.limit() + " != "
                                        + (length - 4) + " writeCacheDebug: "
                                        + m_writeCacheService.addrDebugInfo(paddr));
                    }
                    final byte[] in = bbuf.array(); // reads in with checksum - no need to check if in cache
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
                    // With a non-null WCS, the actual read should be via a callback to readRaw, it should not get here
                    //  unless it is not possible to cache - but maybe even then the WCS should read into a temporary
                    //  buffer

                	// If checksum is required then the buffer should be sized to include checksum in final 4 bytes
                    final ByteBuffer bb = ByteBuffer.wrap(buf, offset, length);
                    
                    // Use ReadRaw - should be the same read all
                    readRaw(paddr, bb);
                    
                    final int chk = ChecksumUtility.getCHK().checksum(buf, offset, length-4); // read checksum
                    final int tstchk = bb.getInt(offset + length-4);
                    if (chk != tstchk) {
                        assertAllocators();
                        
                        if (m_writeCacheService != null) {
                            final String cacheDebugInfo = m_writeCacheService.addrDebugInfo(paddr);
                            log.warn("Invalid data checksum for addr: " + paddr 
                                    + ", chk: " + chk + ", tstchk: " + tstchk + ", length: " + length
                                    + ", first bytes: " + toHexString(buf, 32) + ", successful reads: " + m_diskReads
                                    + ", at last extend: " + m_readsAtExtend + ", cacheReads: " + m_cacheReads
                                    + ", writeCacheDebug: " + cacheDebugInfo);
                        }
                        
                        throw new IllegalStateException(
                                "Invalid data checksum from address: " + paddr
                                        + ", size: " + (length - 4));
                    }

                    // do not explicitly cache the read, it will be cached by the WCS!
//                  if (m_writeCache != null) { // cache the read!
//                      m_writeCache.cache(paddr, bb);
//                  }
                    
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
//              log.error(e,e);
                throw new RuntimeException("addr=" + addr + " : cause=" + e, e);

            }
        } finally {
            lock.unlock();
        }
    }

//    /**
//     * Convenience check for thoseA batch invoice public methods that must be restricted if a rebuild is in progress
//     */
//  private void assertNoRebuild() {
//      if (m_rebuildRequest != null)
//          throw new IllegalStateException("Invalid when rebuilding");
//  }

    private void assertAllocators() {
        final Lock lock = m_allocationReadLock;
        lock.lock();
        try {
        for (int i = 0; i < m_allocs.size(); i++) {
            if (m_allocs.get(i).getIndex() != i) {
                throw new IllegalStateException("Allocator at invalid index: " + i + ", index  stored as: "
                        + m_allocs.get(i).getIndex());
            }
        }
        } finally {
            lock.unlock();
        }
    }

//  static private final char[] HEX_CHAR_TABLE = {
//         '0', '1','2','3',
//         '4','5','6','7',
//         '8','9','a','b',
//         'c','d','e','f'
//        };    

    // utility to display byte array of maximum i bytes as hexString
    static private String toHexString(final byte[] buf, int n) {
//      n = n < buf.length ? n : buf.length;
//      final StringBuffer out = new StringBuffer();
//      for (int i = 0; i < n; i++) {
//          final int v = buf[i] & 0xFF;
//          out.append(HEX_CHAR_TABLE[v >>> 4]);
//          out.append(HEX_CHAR_TABLE[v &0xF]);
//      }
//      return out.toString();
        return BytesUtil.toHexString(buf, n);
    }

    public void free(final long laddr, final int sze) {
        
        free(laddr, sze, null/* AlocationContext */);
        
    }
//  private long m_unsafeFrees = 0;
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
//        assertNoRebuild();
        final int addr = (int) laddr;
        
        switch (addr) {
        case 0:
        case -1:
        case -2:
            return;
        }
        m_allocationWriteLock.lock();
        try {
        	checkContext(context);
        	
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
                    if (context != null && context.isIsolated()) {
                        if (alloc.canImmediatelyFree(addr, sze, context)) {
                            immediateFree(addr, sze, true);
                        } else {
                             getContextAllocation(context).deferFree(encodeAddr(addr, sze));
                        }
                    } else if (this.isSessionProtected()) {
                        immediateFree(addr, sze, false);
                    } else {
                        immediateFree(addr, sze);
                    }
                } else if (context != null && (context.isIsolated()) && alloc.canImmediatelyFree(addr, sze, context)){
                    immediateFree(addr, sze);
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
                    	// If the context is != null, then the deferral must be against that context!
                    	if (context != null && context.isIsolated()) {
                     		getContextAllocation(context).deferFree(encodeAddr(addr, sze));
                    	} else {
                    		deferFree(addr, sze);
                    	}
                    } else {
                        immediateFree(addr, sze);
                    }
                }
            }
        } finally {
            m_allocationWriteLock.unlock();
        }
        
    }
    
    private void checkContext(final IAllocationContext context) {
		if (context != null) {
			context.checkActive();
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
        
        if (!m_allocationWriteLock.isHeldByCurrentThread()) {
            /*
             * In order for changes to m_activeTxCount to be visible the caller
             * MUST be holding the lock.
             */
            throw new IllegalMonitorStateException();
        }
        
        // backoff until synchronization is implemented
//      // protect recyling with unmet quorum
//      if (m_quorum != null && !m_quorum.isQuorumMet()) {
//          return true;
//      }
        
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
    private void releaseSessions() {
        assert(m_activeTxCount == 0 && m_contexts.isEmpty());
        
        if (m_minReleaseAge == 0) {
            if (log.isDebugEnabled())
                log.debug("RELEASE SESSIONS");
            for (FixedAllocator fa : m_allocs) {
                fa.releaseSession(m_writeCacheService);
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
                free(nxt, rem < alloc ? rem : alloc, context);
                rem -= alloc;
            }
            free(hdr_addr, hdr.length, context);
            
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
        m_allocationWriteLock.lock();
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
            m_allocationWriteLock.unlock();
        }
    }

    //  private long immediateFreeCount = 0;
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
        
        m_allocationWriteLock.lock();
        try {       
            final FixedAllocator alloc = getBlockByAddress(addr);
            final int addrOffset = getOffset(addr);
            if (alloc == null) {
                throw new IllegalArgumentException("Invalid address provided to immediateFree: " + addr + ", size: " + sze);
            }
            final long pa = alloc.getPhysicalAddress(addrOffset);
            
            // In a tight loop, this log level test shows up as a hotspot
//            if (log.isTraceEnabled())
//                log.trace("Freeing allocation at " + addr + ", physical address: " + pa);
            alloc.free(addr, sze, overrideSession);
            // must clear after free in case is a blobHdr that requires reading!
            // the allocation lock protects against a concurrent re-allocation
            // of the address before the cache has been cleared
            assert pa != 0;
            // only clear any existing write to cache if no active session
            if (overrideSession || !this.isSessionProtected()) {
                // Only overwrite if NOT committed
                if (!alloc.isCommitted(addrOffset)) {
                        m_writeCacheService.clearWrite(pa,addr);
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

            if (alloc.isUnlocked()) {
                addToCommit(alloc);
            }
            
            m_recentAlloc = true;
        } finally {
            m_allocationWriteLock.unlock();
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

        assert m_allocationWriteLock.isHeldByCurrentThread();

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

//  private volatile long m_maxAllocation = 0;
    private volatile long m_spareAllocation = 0;
    /** Core allocation method. */
    public int alloc(final int size, final IAllocationContext context) {
        if (size > m_maxFixedAlloc) {
            throw new IllegalArgumentException("Allocation size to big: " + size + " > " + m_maxFixedAlloc);
        }
        
        m_allocationWriteLock.lock();
        try {
        	checkContext(context);
        	
            try {
                final FixedAllocator allocator;
                final int i = fixedAllocatorIndex(size);
                if (context != null && context.isIsolated()) {
                    allocator = getContextAllocation(context).getFreeFixed(i);
                    
                    if (allocator.checkBlock0()) {
                    	if (log.isInfoEnabled())
                    		log.info("Adding new shadowed allocator, index: " + allocator.getIndex() + ", diskAddr: " + allocator.getDiskAddr());
                    	addToCommit(allocator);
                    }

                } else {
                    final int block = 64 * m_allocSizes[i];
                    m_spareAllocation += (block - size); // Isn't adjusted by frees!
                    
                    final ArrayList<FixedAllocator> list = m_freeFixed[i];
                    if (list.size() == 0) {
                        /*
                         * No allocator on the free list for that slot size.
                         */
                    	final FixedAllocator candidate;
                    	if (size < this.cSmallSlot) {
                    		/*
                             * Check to see if can locate a good enough
                             * Allocator
                             * 
                             * @see BLZG-1278 (Small slot optimization to
                             * minimize waste).
                             */
                    		candidate = findAllocator(block);
                    	} else {
                    		candidate = null;
                    	}
                    	
                    	if (candidate != null) {
                    		candidate.addToFreeList();
                    		allocator = candidate;
                    	} else {
							/*
							 * We need a new allocator.
							 */
	                        allocator = new FixedAllocator(this, block);
	                        
	                        allocator.setFreeList(list);
	                        allocator.setIndex(m_allocs.size());
	                        
	                        if (log.isTraceEnabled())
	                            log.trace("New FixedAllocator for " + block);

	                        m_allocs.add(allocator);
	                        
	                        if (m_storageStats != null) {
	                            m_storageStats.register(allocator, true);
	                        }
	                        
                    	}

                        if (allocator.checkBlock0()) {
                        	addToCommit(allocator);
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
                
                if (addr == 0) {
                	throw new IllegalStateException("Free Allocator unable to allocate address: " + allocator.getSummaryStats());
                }

                if (allocator.isUnlocked()) {
                	addToCommit(allocator);
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
            m_allocationWriteLock.unlock();
        }
    }
    
    /**
     * For a small slot size only, look for an existing allocator that has a
     * sufficient percentage of free bits and add it to the free list. If this
     * test fails then the caller must allocate a new allocator.
     * 
     * @param block
     * 
     * @return
     * 
     * @see BLZG-1278 (Small slot optimization to minimize waste).
     */
    private FixedAllocator findAllocator(final int block) {
		// only look if small slot
    	if (block > cSmallSlot) {
    		return null;
    	}
    	
    	// Look up the statistics for that slot size.
    	final Bucket stats = m_storageStats.findBucket(block);
    	if (stats == null) {
    	    // Can't do anything.  This is not an expected code path.
    		return null;
    	}
    	
        /*
         * Only check waste if number of allocators is greater than some
         * configurable amount.
         * 
         * The thought here is that it is not necessary to focus on minimizing
         * waste for small stores and that by allowing that waste we permit
         * better locality (co-location on a page) for small slots. Once we
         * start to limit the small slot waste we essentially just change the
         * #of free bits before we are willing to allow a small slot allocator
         * onto the free list.
         */
    	if (stats.m_allocators < cSmallSlotWasteCheckAllocators) {
    		return null;
    	}
    	
    	// only check small slots if total waste is larger than some configurable amount
    	final float slotWaste = stats.slotsUnused();
    	if (slotWaste < cSmallSlotHighWaste) {
    		return null;
    	}
    	
    	// Now find candidate allocator with maximum free slots above a minimum threshold
    	FixedAllocator candidate = null;
    	int candidateFreeBits = cSmallSlotThresholdHighWaste; // minimum threshold
    	for (int i = 0; i < m_allocs.size(); i++) {
    		final FixedAllocator tst = m_allocs.get(i);
    		if (tst.getBlockSize() == block) { // right size
    			if (tst.m_freeBits > candidateFreeBits) {
    				candidate = tst;
    				candidateFreeBits = candidate.m_freeBits;
    			}
     		}
    	}
    	
    	if (candidate != null) {
    		candidate.m_smallSlotHighWaste = true;
    		if (log.isDebugEnabled()) {
    			log.debug("Found candidate small slot allocator");
    		}
    	}
    	
    	return candidate;   	
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

        m_allocationWriteLock.lock();
        try {
        	checkContext(context);
        	
            final long begin = System.nanoTime();

            if (size > (m_maxFixedAlloc - 4)) {

                if (size > getMaxBlobSize())
                    throw new IllegalArgumentException(
                            "Allocation request beyond maximum BLOB of "
                                    + getMaxBlobSize());

                if (log.isTraceEnabled())
                    log.trace("BLOB ALLOC: " + size);

                if (m_storageStats != null) {
                    m_storageStats.allocateBlob(size);
                }

                final PSOutputStream psout = PSOutputStream.getNew(this,
                        m_maxFixedAlloc, context);
                try {

                    int i = 0;
                    final int blocks = size / 512;
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

            final int newAddr = alloc(size + 4, context); // allow size for
                                                            // checksum

            if (newAddr == 0)
                throw new IllegalStateException("NULL address allocated");

            final int chk = ChecksumUtility.getCHK().checksum(buf, size);

            final long pa = physicalAddress(newAddr);

            try {
                m_writeCacheService.write(pa, ByteBuffer.wrap(buf, 0, size),
                        chk, true/* writeChecksum */, newAddr/* latchedAddr */);
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
        } finally {
            m_allocationWriteLock.unlock();
        }
    }

//  /****************************************************************************
//   * Fixed buffer size reallocation
//   **/
//  public long realloc(final long oldAddr, final int oldSize, final byte buf[]) {
//      
//      free(oldAddr, oldSize);
//
//      return alloc(buf, buf.length);
//  }

//  /**
//   * Must handle valid possibility that a request to start/commit transaction
//   * could be made within a commitCallback request
//   */
//  synchronized public void startTransaction() {
//      if (m_committing) {
//          return;
//      }
//
//      m_transactionCount++;
//  }
//
//  synchronized public void commitTransaction() {
//      if (m_committing) {
//          return;
//      }
//
//      if (log.isDebugEnabled())
//          log.debug("Commit Transaction");
//      
//      if (--m_transactionCount <= 0) {
//          commitChanges();
//
//          m_transactionCount = 0;
//      }
//  }
//
//  public int getTransactionCount() {
//      return m_transactionCount;
//  }
//
//  // --------------------------------------------------------------------------------------------
//  // rollbackTransaction
//  //
//  // clear write cache
//  // read in last committed header
//  synchronized public void rollbackTransaction() {
//      if (m_transactionCount > 0 || m_readOnly) { // hack for resync
//          baseInit();
//
//          try {
//              m_writeCache.reset(); // dirty writes are discarded
//
//              readAllocationBlocks();
//          } catch (Exception e) {
//              throw new StorageTerminalError("Unable to rollback transaction", e);
//          }
//      }
//  }

//    /*
//     * Slug
//     */
//    private int fibslug(int n) {
//    	if (n < 2) 
//    		return 1;
//    	else
//    		return fibslug(n-1) + fibslug(n-2);
//    }
    
    /**
     * The semantics of reset are to revert unisolated writes to committed
     * state.
     * <p>
     * Unisolated writes must also be removed from the write cache.
     * <p>
     * The AllocBlocks of the FixedAllocators maintain the state to determine
     * the correct reset behavior.
     * <p>
     * If the store is using DirectFixedAllocators then an IllegalStateException
     * is thrown.
     * <p>
     * If there is an active {@link #m_commitStateRef}, then this indicates a
     * failure after the {@link RWStore#commit()} had "succeeded".
     */
    public void reset() {

        if (log.isInfoEnabled()) {
            log.info("RWStore Reset");
        }
        m_allocationWriteLock.lock();
        try {
        	// DEBUG
        	// fibslug(40); // slug to improve odds of interruption of reset (if possible)
            assertOpen();
//          assertNoRebuild();

            final CommitState commitState = m_commitStateRef
                    .getAndSet(null/* newValue */);

            if (commitState != null) {
            
                commitState.reset(); // restore state values on RWStore.
                
            }
            
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
                isolatedWrites |= fa.reset(m_writeCacheService, m_committedNextAllocation);
            }
            
            /**
             * Now clone the transient metabits for protection if this service becomes leader
             */
            syncMetaTransients();
                       
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
                    	fa.setIndex(-1);
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
                //  problems in HA.
                // If the allocators are torn down correctly, we should be good to clear the commitList
                 clearCommitList();
                
                // Flag no allocations since last commit
                m_recentAlloc = false;
            } else {
            	// there are isolated writes, so we must not clear the commit list since otherwise
            	//	the Alloction index wil get out of sync as per Ticket #1136
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
                m_writeCacheService.close();
                m_writeCacheService = newWriteCacheService();
            } else if (m_writeCacheService != null) {
                /*
                 * Note: We DO NOT need to reset() the WriteCacheService. If a
                 * record was already flushed to the disk, then it is on the
                 * disk and clearing the record from the cache will not change
                 * that. If the record has not yet been flushed to the disk,
                 * then we already cleared it from the WCS when we reset the
                 * FixedAllocators (above).
                 */
//                m_writeCacheService.reset();
//                m_writeCacheService.setExtent(convertAddr(m_fileSize));
            }
            /*
             * Discard any writes on the delete blocks. Those deletes MUST NOT
             * be applied after a reset() on the RWStore.
             * 
             * @see https://sourceforge.net/apps/trac/bigdata/ticket/602
             * (RWStore does not discard deferred deletes on reset)
             */
            m_deferredFreeOut.reset();
            
            /*
             * Reset any storage stats
             */
            if (m_storageStatsAddr != 0) {
                m_storageStats.reset();             
            } else {
                m_storageStats = new StorageStats(m_allocSizes);
            }

        } catch (Exception e) {
            throw new IllegalStateException("Unable to reset the store", e);
        } finally {
            m_allocationWriteLock.unlock();
        }
    }

//  synchronized public boolean isActiveTransaction() {
//      return m_transactionCount > 0;
//  }

    /**
     * writeMetaBits must be called after all allocations have been made, the
     * last one being the allocation for the metabits themselves (allowing for
     * an extension!).
     * 
     * Ticket #936: The meta-bits allocation is currently made from the FixedAllocator
     * region. This works well providing the required allocation bits is less than
     * the maximum FixedAllocator slot size.  While this is neat, there are problems at scale
     * for maximum slot sizes less than 64K.
     * 
     * To address the 8K bits in a 1K alloctor, 13 bits are required, this leaves 19 bits
     * to index an Allocator, or 18 bits without the sign => 256K maximum index.
     * 
     * To be able to commit changes to all 256K allocators requires 512K metabits => 64K bytes.
     * We would like to associate the 64K allocations with the root block, so a single 128K
     * allocation would be split into 64K demi-spaces, one for each root block.
     * 
     * While a negative address indicates a standard RW allocation a ositive address can be used
     * to indicate an explicitly allocated region. The trick is to ensure that the region is
     * allocated on a 128K boundary, then the lower bits can indicate which demi-space is used with
     * a simple XOR.
     * 
     * Note that we must ensure that any previous demi-space write is removed from the WCS.
     * 
     * @throws IOException
     */
    private void writeMetaBits() throws IOException {
        final byte buf[] = genMetabitsData();

        /*
         * Note: this address is set by commit() prior to calling
         * writeMetaBits().
         */
        //final long addr = physicalAddress(m_metaBitsAddr);
        final long addr = m_metaBitsAddr < 0 ? physicalAddress(m_metaBitsAddr) : ((long) m_metaBitsAddr) << ALLOCATION_SCALEUP;
        if (addr == 0) {
            throw new IllegalStateException("Invalid metabits address: " + m_metaBitsAddr);
        }
        
        assert addr > 0;
        
        try {
        	if (log.isDebugEnabled())
        		log.debug("writing metabits at: " + addr);
        	
        	// Similar to writeMetaBits, we are no longer writing to a FixedAllocator managed region,
        	//	so no latched address is provided
            m_writeCacheService.write(addr, ByteBuffer.wrap(buf), 0/*chk*/, false/*useChecksum*/, m_metaBitsAddr < 0 ? m_metaBitsAddr : 0 /*latchedAddr*/);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    
    private byte[] genMetabitsData() throws IOException {
        // the metabits is now prefixed by a long specifying the lastTxReleaseTime
        // used to free the deferedFree allocations.  This is used to determine
        //  which commitRecord to access to process the nextbatch of deferred
        //  frees.
        // the cDefaultMetaBitsSize is also written since this can now be
        //  parameterized.
        final int len = 4 * (cMetaHdrFields + m_allocSizes.length + m_metaBits.length);
        final byte buf[] = new byte[len];

        final FixedOutputStream str = new FixedOutputStream(buf);
        try {
            str.writeInt(m_metaBitsAddr > 0 ? cVersionDemispace : cVersion);
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
        
        return buf;
    }

    /**
     * 
     * @return
     */
    public boolean isDirty() {
        return requiresCommit();
    }
    
    /**
     * Object recording the undo state for the {@link RWStore#commit()} ...
     * {@link RWStore#postCommit()} sequence. The {@link CommitState} must
     * either {@link CommitState#commit()} or {@link CommitState#reset()}. Those
     * {@link CommitState} methods are invoked out of the corresponding
     * {@link RWStore} methods.
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/973" >RWStore commit is not
     *      robust to internal failure.</a>
     */
    private class CommitState {
        /*
         * Critical pre-commit state that must be restored if a commit is
         * discarded.
         */
        private final int m_lastCommittedNextAllocation;
        private final long m_storageStatsAddr;
        private final int m_metaBitsAddr;

        CommitState() {
            // retain copy of critical pre-commit state
            if (!m_allocationWriteLock.isHeldByCurrentThread())
                throw new IllegalMonitorStateException();
            m_lastCommittedNextAllocation = RWStore.this.m_committedNextAllocation;
            m_storageStatsAddr = RWStore.this.m_storageStatsAddr;
            m_metaBitsAddr = RWStore.this.m_metaBitsAddr;
        }

        void postCommit() {

            // NOP
            
        }

        /** Reset pre-commit state to support reset/abort/rollback. */
        void reset() {
        	if (!m_allocationWriteLock.isHeldByCurrentThread())
                throw new IllegalMonitorStateException();
            RWStore.this.m_storageStatsAddr = m_storageStatsAddr;
            RWStore.this.m_committedNextAllocation = m_lastCommittedNextAllocation;
            RWStore.this.m_metaBitsAddr = m_metaBitsAddr;
         }

    }

    /**
     * @see <a href="http://trac.blazegraph.com/ticket/973" >RWStore commit is not
     *      robust to internal failure.</a>
     */
    private final AtomicReference<CommitState> m_commitStateRef = new AtomicReference<CommitState>();

    /**
     * Package private method used by the test suite.
     */
    void clearCommitStateRef() {

        m_commitStateRef.set(null/* newValue */);

    }
    
    @Override
    public void commit() {
        assertOpen();
//        assertNoRebuild();

        checkCoreAllocations();

    	// take allocation lock to prevent other threads allocating during commit
        m_allocationWriteLock.lock();
        
        try {
        
            /*
             * Create a transient object to retain values of previous
             * commitState to support abort/reset/rollback if requested after
             * this commit() is requested.
             */
            if (!m_commitStateRef.compareAndSet(null/* expect */,
                    new CommitState())) {
                throw new IllegalStateException(
                        "RWStore commitState found, incomplete previous commit must be rolled back/aborted");
            }

//          final int totalFreed = checkDeferredFrees(true, journal); // free now if possible
//          
//          if (totalFreed > 0 && log.isInfoEnabled()) {
//              log.info("Freed " + totalFreed + " deferralls on commit");
//          }
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
            if (m_metaBitsAddr > 0) {
            	// already using demi-space, remove from WCS
            	m_writeCacheService.removeWriteToAddr(convertAddr(-m_metaBitsAddr), 0);
            } else {
				final int reqmbc = getRequiredMetaBitsStorage();
				int nmbaddr = 0;
				// if > max alloc or explicitly use the demi-space, then drop through for demi-space
				if ((!m_useMetabitsDemispace) && reqmbc < m_maxFixedAlloc) { 
					nmbaddr = alloc(reqmbc, null);
				}            	
            
				// If existing allocation, then free it
            if (m_metaBitsAddr < 0) {
	            
    				final int oldMetaBitsSize = (m_metaBits.length
    						+ m_allocSizes.length + 1) * 4;
    				
	            // Call immediateFree - no need to defer freeof metaBits, this
	            //  has to stop somewhere!
	            // No more allocations must be made
	            immediateFree((int) m_metaBitsAddr, oldMetaBitsSize);
	            
            }
            
   				m_metaBitsAddr = nmbaddr;
           }

            if (m_metaBitsAddr == 0) {
            	// Allocate special region to be able to store maximum metabits (128k of 2 64K demi-space
            	// Must be aligned on 128K boundary and allocations are made in units of 64K.
            	//
            	// May need to extend the file for teh demi-space!
            	while (m_nextAllocation % 2 != 0) {
            		m_nextAllocation--;
            	}
            	m_metaBitsAddr = -m_nextAllocation; // must be positive to differentiate from FixedAllocator address
            	m_nextAllocation -= 2; // allocate 2 * 64K
            	
            	// Check for file extension
                while (m_nextAllocation <= m_fileSize) {
                    extendFile();
                }
            	
            	if (log.isInfoEnabled())
            		log.info("Using Demi-space metabits");
            }
            
            if (m_metaBitsAddr > 0) { // Demi-Space
            // Now "toggle" m_metaBitsAddr - 64K boundary
            m_metaBitsAddr ^= 0x01; // toggle zero or 64K offset
            }

            if (log.isDebugEnabled()) {
            	final long mbaddr;
        		if (m_metaBitsAddr < 0) {
        			mbaddr = physicalAddress((int) m_metaBitsAddr);
        		} else {
        			mbaddr = convertAddr(-m_metaBitsAddr); // maximum 48 bit address range
        		}
            
        		log.debug("Writing metabits at " + mbaddr);
            }
        	
            // There must be no buffered deferred frees
            // assert m_deferredFreeOut.getBytesWritten() == 0;

            // save allocation headers
            FixedAllocator fa = m_commitHead;
            
            while (fa != null) {
                
                final FixedAllocator allocator = fa;
                
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
                    m_writeCacheService.write(metaBit2Addr(naddr), ByteBuffer
                        .wrap(allocator.write()), 0/*chk*/, false/*useChecksum*/,0/*latchedAddr*/);
                    
                } catch (InterruptedException e) {
                    
                    throw new RuntimeException(e);
                    
                }
                
                fa = fa.m_nextCommit;
            }
            // DO NOT clear the commit list until the writes have been flushed
            // m_commitList.clear();

            writeMetaBits();

            try {
                m_writeCacheService.flush(true);
                lastBlockSequence = m_writeCacheService.resetSequence();
            } catch (InterruptedException e) {
                log.error(e, e);
                throw new RuntimeException(e);
            }
            
            // Should not write rootBlock, this is responsibility of client
            // to provide control
            // writeFileSpec();

            syncMetaTransients();
            
            // Must be called from AbstractJournal commitNow after writeRootBlock
            // postCommit();

//              if (m_commitCallback != null) {
//                  m_commitCallback.commitComplete();
//              }

            // The Journal handles the force in doubleSync
            // m_reopener.reopenChannel().force(false); // TODO, check if required!
        } catch (IOException e) {
            throw new StorageTerminalError("Unable to commit transaction", e);
        } finally {
            m_recentAlloc = false;
            m_allocationWriteLock.unlock();
        }

        checkCoreAllocations();

        if (log.isTraceEnabled())
            log.trace("commitChanges for: " + m_nextAllocation + ", "
                    + m_metaBitsAddr + ", active contexts: "
                    + m_contexts.size());

        if (log.isDebugEnabled() && m_quorum != null && m_quorum.isHighlyAvailable()) {
            
            log.debug(showAllocatorList());

        }
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Lock getCommitLock() {

        return m_allocationWriteLock;
        
    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Commits the FixedAllocator bits
     */
    @Override
    public void postCommit() {
       
        if (!m_allocationWriteLock.isHeldByCurrentThread())
            throw new IllegalMonitorStateException();

        final CommitState commitState = m_commitStateRef.getAndSet(null/* newValue */);
        
        if (commitState == null) {

            throw new IllegalStateException(
                    "No current CommitState found on postCommit");
            
        } else {
            
            commitState.postCommit();
            
        }
        
        {
        	FixedAllocator fa = m_commitHead;
	        while (fa != null) {
	
	            fa.postCommit();
	            
	            fa = fa.m_nextCommit;
	            
	        }
        }

        if (m_storageStats != null) {
        	m_storageStats.commit();
        }

        clearCommitList();

    }

    @Override
    public int checkDeferredFrees(final AbstractJournal journal) {
        
        if (journal == null)
            return 0;

        /*
         * Note: since this is now called directly from the AbstractJournal
         * commit method (and is part of a public API) we must take the
         * allocation lock.
         * 
         * This may have adverse effects wrt concurrency deadlock issues, but
         * none have been noticed so far.
         */
        m_allocationWriteLock.lock();

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
            if (m_lastDeferredReleaseTime >= latestReleasableTime) {
                /**
                 * Note: Added for HA. I have observed both values equal to
                 * ZERO. Since we add ONE (1) to the lastDeferredReleaseTime it
                 * MUST BE LT the latestReleasableTime or we will get a
                 * "toKey LT fromKey" exception.
                 * 
                 * @see <a href=
                 *      "https://sourceforge.net/apps/trac/bigdata/ticket/530#comment:116">
                 *      Journal HA </a>
                 */
                return 0;
            }
            return freeDeferrals(journal, m_lastDeferredReleaseTime + 1,
                    latestReleasableTime);

        } finally {
        
            m_allocationWriteLock.unlock();
            
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
        //  needed to save the current committed objects
        final int commitInts = ((32 + commitListSize()) / 32);
        final int allocBlocks = (cDefaultMetaBitsSize - 1 + commitInts)/(cDefaultMetaBitsSize-1);
        ints += cDefaultMetaBitsSize * allocBlocks;
        
        return ints*4; // return as bytes
    }

    // Header Data
//  volatile private long m_curHdrAddr = 0;
//  volatile private int m_rootAddr;

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
    final private long m_maxFileSize;

//  private int m_headerSize = 2048;

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
     * 0x0500 - using metaBits demi-space
     */
    final private int cVersion = 0x0400;
    /**
     * The {@link #cVersion} value corresponding to the use of the demi-space
     * for the metabits.
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/936"> Support larger metabit
     *      allocations</a>
     * @see <a href="http://wiki.blazegraph.com/wiki/index.php/DataMigration" >
     *      Data migration </a>
     */
    final private int cVersionDemispace = 0x0500;
    
    /**
     * cReservedMetaBits is the reserved space in the metaBits header
     * to alloc for binary compatibility moving forward.
     * 
     * If we need to add int values to the header we can do so and reduce the
     * reservation by 1 each time
     */
    final static int cReservedMetaBits = 20;
    
    /**
     * MetaBits Header
     * 0 int version
     * 1-2 int[2] long deferredFree
     * 3 int defaultMetaBitsSize
     * 4 int length of allocation sizes
     * 5-6 int[2] storage stats addr
     * + 20 reserved
     */
    final static private int cMetaHdrFields = 7 + cReservedMetaBits;  
    /**
     * @see Options#META_BITS_SIZE
     */
    final private int cDefaultMetaBitsSize = 9;
    /**
     * @see Options#META_BITS_SIZE
     */
    volatile private int m_metaBitsSize;
    
    volatile private boolean m_useMetabitsDemispace = true;
    /**
     * Package private since is uded by FixedAllocators
     * 
     * @see Options#META_BITS_SIZE
     */
    final int cDefaultFreeBitsThreshold;
    
    /**
     * The smallSlotThreshold, when activated, is intended to ensure improve the
     * opportunity for write elissions (to mechanical disks) whilst also reducing 
     * the read-backs on current generation (2014-15) SSDs that can impact 
     * write throughput.
     * Given that the objective is to statistically improve write elission,
     * the number of required free bits needs to be large - around 50%.
     * However, this can result in a large amount of store waste for certain
     * patterns of data - for example when small slots are used to store large
     * literals that will not be recycled.  In this scenario it is possible
     * that allocators are not recycled.
     * Some further thoughts:
     * 1) The more efficient elission of small slots for the allocation of large literals
     * is probably the major throughput benefit
     * 2) OTOH, at a lower level, small sparse but localised writes (eg 16 64 byte writes to a 4k
     * sector) may only incur a single read-back with good firmware.
     * To address the concern for high waste, when a statistically large number of allocators have
     * been created, and the waste is beyond some threshold, then a lower small slot threshold
     * is used.  The logic for this is implemented in {@link FixedAllocator#meetsSmallSlotThreshold()}
     */
	int cSmallSlot = 1024; // @see from Options#SMALL_SLOT_TYPE
    
	int cSmallSlotThreshold = 4096;  // @see from Options#SMALL_SLOT_THRESHOLD
	
	/**
	 * High Waste Criteria
	 */
	int cSmallSlotThresholdHighWaste = 2048;  // @see from Options#SMALL_SLOT_THRESHOLD_HIGH_WASTE
	int cSmallSlotWasteCheckAllocators = 100;  // @see from Options#SMALL_SLOT_WASTE_CHECK_ALLOCATORS
	float cSmallSlotHighWaste = 0.2f;  // @see from Options#SMALL_SLOT_HIGH_WASTE
	
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
            	// The assumption is that this bit is also NOT set in m_metaBits
            	assert !tstBit(m_metaBits, ret);
            	
                return ret;
            }
        }
        
        return -1; // none found
    }
    
    void metaFree(final int bit) {
        
        if (!m_allocationWriteLock.isHeldByCurrentThread()) {
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
        
        m_writeCacheService.clearWrite(metaBit2Addr(bit),0/*latchedAddr*/);
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
//      final int bitsPerBlock = 9 * 32;
        
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
        /**
         * Note: Synchronous flush of the WriteCacheService should not be
         * required. It has been commented out in support of
         * 
         * <a href="https://sourceforge.net/apps/trac/bigdata/ticket/621">
         * Coalesce records in write cache</a>
         */
//      try {
//          /*
//           * The call to flush the cache cannot be made while holding the
//           * extension writeLock, since the writeOnChannel takes the
//           * extension readLock.
//           */
//          m_writeCache.flush(true);
//      } catch (InterruptedException e) {
//          throw new RuntimeException("Flush interrupted in extend file");
//      }

        final Lock lock = this.m_extensionLock.writeLock();
        lock.lock();
        try {
            m_extendingFile = true;

//          final long curSize = convertAddr(m_fileSize);

            m_fileSize += adjust;

            final long toAddr = convertAddr(m_fileSize);
            
            if (getMaxFileSize() < toAddr) {
                // whoops!! How to exit more gracefully?
                throw new Error("System greater than maximum size");
            }

            if (log.isInfoEnabled()) log.info("Extending file to: " + toAddr);

            m_reopener.reopenChannel();
            m_reopener.raf.setLength(toAddr);
            storeCounters.get().ntruncate++;
            
            // must ensure writeCache is in sync for HA
            m_writeCacheService.setExtent(toAddr);

            if (log.isInfoEnabled()) log.info("Extend file done");
        } catch (Throwable t) {
            throw new RuntimeException("Force Reopen", t);
        } finally {
            m_extendingFile = false;
            m_readsAtExtend = this.m_diskReads;
            lock.unlock();
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
            final int b = fndBit(bits[i]);
            if (b != -1) {
            	return (i * 32) + b;
            }
        }

        return -1;
    }
    
    static int fndBit(final int bits) {
        if (bits != 0xFFFFFFFF) {
            for (int k = 0; k < 32; k++) {
                if ((bits & (1 << k)) == 0) {
                    return k;
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
        str.append("\nChecking regions.....");
        
        // Now check all allocators to confirm that each file region maps to only one allocator
        final Lock lock = m_allocationLock.readLock();
        lock.lock();
        try {
	        final HashMap<Integer, FixedAllocator> map = new HashMap<Integer, FixedAllocator>();
	        for (FixedAllocator fa : m_allocs) {
	        	fa.addToRegionMap(map);
	        }
	        str.append("okay\n");
        } catch (IllegalStateException is) {
        	str.append(is.getMessage() + "\n");
        } finally {
        	lock.unlock();
        }
        
    }
    
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

        /*
         * Guard against concurrent mutation.
         * 
         * Note: Taking the lock here is necessary since physicalAddress/1 is
         * public.
         */
        final Lock lock = m_allocationReadLock;
        
        lock.lock();

        try {

            if (addr >= 0) {
        		
                return addr & 0xFFFFFFE0;

            } else {

                // Find the allocator.
                final FixedAllocator allocator = getBlock(addr);

                // Get the bit index into the allocator.
                final int offset = getOffset(addr);

                // Translate the bit index into a byte offset on the file.
                final long laddr = allocator
                        .getPhysicalAddress(offset, nocheck);

                return laddr;
            }
            
        } finally {
            
            lock.unlock();
            
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
     * Since we need to store the absolute address and the size can be
     * a maximum of 64K, the absolute address is limited to 48 bits, setting
     * the maximum address as 140T, which is sufficient.
     *  
     * @return long representation of metaBitsAddr PLUS the size
     */
    public long getMetaBitsAddr() {
		long ret = 0;
    	
		if (m_metaBitsAddr < 0) {
			ret = physicalAddress((int) m_metaBitsAddr);
		} else {
        // long ret = physicalAddress((int) m_metaBitsAddr);
			ret = convertAddr(-m_metaBitsAddr); // maximum 48 bit address range
		}
        ret <<= 16;
        
		// include space for version, allocSizes and deferred free info AND
		// cDefaultMetaBitsSize
		final int metaBitsSize = cMetaHdrFields + m_metaBits.length
				+ m_allocSizes.length;
        ret += metaBitsSize;
        
        if (log.isTraceEnabled())
            log.trace("Returning metabitsAddr: " + ret + ", for "
                    + m_metaBitsAddr + " - " + m_metaBits.length + ", "
                    + metaBitsSize);

        return ret;
    }

    /**
     * 
     * @return the address of the metaBits
     */
    public long getMetaBitsStoreAddress() {
		if (m_metaBitsAddr < 0) {
			return physicalAddress((int) m_metaBitsAddr);
		} else {
			return convertAddr(-m_metaBitsAddr); // maximum 48 bit address range
		}
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
        if (m_metaBitsAddr > 0) {
        	// FIX for sign use in m_metaBitsAddr when packing into long
        	ret++;
        }
        ret <<= 32;
        ret += -m_metaBitsAddr;

        if (log.isTraceEnabled())
            log.trace("Returning nextOffset: " + ret + ", for " + m_metaBitsAddr);

        return ret;
    }

    public void flushWrites(final boolean metadata) throws IOException {

        assertOpen();
        
        try {
        
            m_writeCacheService.flush(metadata);

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
//      m_allocationLock.lock();
//      try {
//          BlobAllocator ba = null;
//          if (m_freeBlobs.size() > 0) {
//              ba = (BlobAllocator) m_freeBlobs.get(0);
//          }
//          if (ba == null) {
//              final Allocator lalloc = (Allocator) m_allocs.get(m_allocs.size() - 1);
//              // previous block start address
//              final int psa = lalloc.getRawStartAddr();
//              assert (psa - 1) > m_nextAllocation;
//              ba = new BlobAllocator(this, psa - 1);
//              ba.setFreeList(m_freeBlobs); // will add itself to the free list
//              ba.setIndex(m_allocs.size());
//              m_allocs.add(ba);
//          }
//
//          if (!m_commitList.contains(ba)) {
//              m_commitList.add(ba);
//          }
//
//          return ba.register(addr);
//      } finally {
//          m_allocationLock.unlock();
//      }
//  }

    void addToCommit(final FixedAllocator allocator) {
    	if (allocator.m_prevCommit == null && m_commitHead != allocator) { // not on list
    		allocator.m_prevCommit = m_commitTail;
    		if (allocator.m_prevCommit != null) {
    			allocator.m_prevCommit.m_nextCommit = allocator;
    			m_commitTail = allocator;
    		} else {
    			m_commitHead = m_commitTail = allocator;
    		}   		
        }
    }

    final boolean isOnCommitList(final FixedAllocator allocator) {
    	return allocator.m_prevCommit != null || allocator == m_commitHead;
    }
    
    final void clearCommitList() {
    	FixedAllocator cur = m_commitHead;
    	while (cur != null) {
    		final FixedAllocator t = cur;
    		cur = t.m_nextCommit;
    		
    		t.m_prevCommit = t.m_nextCommit = null;
    	}
    	
    	m_commitHead = m_commitTail = null;
    }
    
    final int commitListSize() {
    	int count = 0;
    	FixedAllocator cur = m_commitHead;
    	while (cur != null) {
    		count++;
    		cur = cur.m_nextCommit;
    	}
    	
    	return count;
    }
    
//    void removeFromCommit(final Allocator allocator) {
//        m_commitList.remove(allocator);
//    }

    public Allocator getAllocator(final int i) {
        return (Allocator) m_allocs.get(i);
    }

    /**
     * Simple implementation for a {@link RandomAccessFile} to handle the direct
     * backing store.
     */
    private class ReopenFileChannel implements
            IReopenChannel<FileChannel>, FileChannelUtility.IAsyncOpener {

        final private File file;

        private final boolean readOnly;
        
        private final String mode;

        private volatile RandomAccessFile raf;
        
        private final Path path;

        private volatile AsynchronousFileChannel asyncChannel;
        
        private int asyncChannelOpenCount = 0;;
        
        public ReopenFileChannel(final File file, final RandomAccessFile raf,
                final boolean readOnly) throws IOException {

            this.file = file;

            this.readOnly = readOnly;
            
            this.mode = readOnly == true ? "r" : "rw";
            
            this.raf = raf;
            
            this.path = Paths.get(file.getAbsolutePath());

            reopenChannel();

        }
        
        @Override
        public AsynchronousFileChannel getAsyncChannel() {
        	if (asyncChannel != null) {
        		if (asyncChannel.isOpen())
        			return asyncChannel;
        	}
        	
        	synchronized(this) {
            	if (asyncChannel != null) { // check again while synchronized
            		if (asyncChannel.isOpen())
            			return asyncChannel;
            	}

	        	try {
	        	    if(readOnly) {
	        	        asyncChannel = AsynchronousFileChannel.open(path, StandardOpenOption.READ);
	        	    } else {
	                    asyncChannel = AsynchronousFileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
	        	    }
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
	        	
	        	asyncChannelOpenCount++;
	        	
	        	return asyncChannel;
    		}
        }
        
        public int getAsyncChannelOpenCount() {
        	return asyncChannelOpenCount;
        }

        @Override
        public String toString() {

            return file.toString();

        }

        @Override
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
        	//See https://github.com/SYSTAP/db-enterprise/issues/12
        	//TODO:  Determine if there is a more graceful way to handle this.
            // throw new IllegalArgumentException(
        	log.warn("Cannot shrink RWStore extent: currentExtent="
                            + currentExtent + ", fileSize=" + m_fileSize
                            + ", newValue=" + extent);
        }
        
    }
    
    /**
     * @return number of FixedAllocators
     */
    public int getFixedAllocatorCount() {
        final Lock lock = m_allocationReadLock;
        lock.lock();
        try {
            int fixed = 0;
            final Iterator<FixedAllocator> allocs = m_allocs.iterator();
            while (allocs.hasNext()) {
                if (allocs.next() instanceof FixedAllocator) {
                    fixed++;
                }
            }
            return fixed;
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * @return  the number of heap allocations made to the FixedAllocators.
     */
    public int getAllocatedBlocks() {
        final Lock lock = m_allocationReadLock;
        lock.lock();
        try {
            int allocated = 0;
            final Iterator<FixedAllocator> allocs = m_allocs.iterator();
            while (allocs.hasNext()) {
                final Allocator alloc = allocs.next();
                if (alloc instanceof FixedAllocator) {
                    allocated += ((FixedAllocator) alloc).getAllocatedBlocks();
                }
            }
            return allocated;
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * @return the amount of heap storage assigned to the FixedAllocators.
     */
    public long getFileStorage() {
        final Lock lock = m_allocationReadLock;
        lock.lock();
        try {
            long allocated = 0;
            final Iterator<FixedAllocator> allocs = m_allocs.iterator();
            while (allocs.hasNext()) {
                final FixedAllocator alloc = allocs.next();
                allocated += ((FixedAllocator) alloc).getFileStorage();
            }
            return allocated;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Computes the amount of utilised storage
     * 
     * @return the amount of storage to alloted slots in the allocation blocks
     */
    public long getAllocatedSlots() {
        final Lock lock = m_allocationReadLock;
        lock.lock();
        try {
            long allocated = 0;
            final Iterator<FixedAllocator> allocs = m_allocs.iterator();
            while (allocs.hasNext()) {
                final Allocator alloc = allocs.next();
                if (alloc instanceof FixedAllocator) {
                    allocated += ((FixedAllocator) alloc).getAllocatedSlots();
                }
            }
            return allocated;
        } finally {
            lock.unlock();
        }
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
        m_allocationWriteLock.lock();
        try {
            if (sze > (this.m_maxFixedAlloc-4)) {          	
                m_deferredFreeOut.writeInt(-rwaddr);
                m_deferredFreeOut.writeInt(sze);
                
            	/*
            	 * rather than write out blob address, instead flatten the blob addresses and
            	 * write all to remove the latency on commit caused by reading potentially many blob headers.
            	 * 
            	 * This idea was propposed to support BLZG-641/BLZG-1663 to redcue commit latency.
            	 * 
            	 * However, it appears that deferFree is not called with the raw blob size and is already
            	 * reduced to the blob part addrs.
            	 */
                log.debug("Unexpected code path deferring free of direct blob address");

//                final int alloc = m_maxFixedAlloc-4;
//                final int nblocks = (alloc - 1 + (sze-4))/alloc;
//                if (nblocks < 0)
//                    throw new IllegalStateException(
//                            "Allocation error, m_maxFixedAlloc: "
//                                    + m_maxFixedAlloc);
//
//                final byte[] hdrbuf = new byte[4 * (nblocks + 1) + 4]; // plus 4 bytes for checksum
//                if (hdrbuf.length > m_maxFixedAlloc) {
//                    if (log.isInfoEnabled()) {
//                        log.info("LARGE BLOB - header is BLOB");
//                    }
//                }
//                
//                getData(rwaddr, hdrbuf); // will work even if header is also a blob
//                
//                // deferFree header
//                deferFree(rwaddr, hdrbuf.length);
//                
//                // Now read all blob part addresses
//                final DataInputStream hdrstr = new DataInputStream(new ByteArrayInputStream(hdrbuf));
//                final int rhdrs = hdrstr.readInt();
//                if (rhdrs != nblocks) {
//                    throw new IllegalStateException(
//                            "Incompatible BLOB header record, expected: "
//                                    + nblocks + ", got: " + rhdrs);
//                }
//                
//                int remaining = sze;
//                int partSize = alloc;
//                for (int i = 0; i < nblocks; i++) {
//                    final int blobpartAddr = hdrstr.readInt();
//                    // deferFree(blobpartAddr, partSize);
//                    m_deferredFreeOut.writeInt(blobpartAddr);             
//                    
//                    remaining -= partSize;
//                    
//                    if (remaining < partSize) {
//                    	partSize = remaining;
//                    }                  
//                }

            } else {
                m_deferredFreeOut.writeInt(rwaddr);             
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not free: rwaddr=" + rwaddr
                    + ", size=" + sze, e);
        } finally {
            m_allocationWriteLock.unlock();
        }
    }
    
//  private void checkFreeable(final JournalTransactionService transactionService) {
//      if (transactionService == null) {
//          return;
//      }
//      
//      try {
//          final Long freeTime = transactionService.tryCallWithLock(new Callable<Long>() {
//  
//              public Long call() throws Exception {
//                  final long now = transactionService.nextTimestamp();
//                  final long earliest =  transactionService.getEarliestTxStartTime();
//                  final long aged = now - transactionService.getMinReleaseAge();
//                  
//                  if (transactionService.getActiveCount() == 0) {
//                      return aged;
//                  } else {
//                      return aged < earliest ? aged : earliest;
//                  }
//              }
//              
//          }, 5L, TimeUnit.MILLISECONDS);
//      } catch (RuntimeException e) {
//          // fine, will try again later
//      } catch (Exception e) {
//          throw new RuntimeException(e);
//      }
//  }

    public long saveDeferrals() {
        m_allocationWriteLock.lock();
        try {
            if (m_deferredFreeOut.getBytesWritten() == 0) {
                return 0;
            }
            m_deferredFreeOut.writeInt(0); // terminate!
            final int outlen = m_deferredFreeOut.getBytesWritten();
            
            long addr = m_deferredFreeOut.save();
            
            addr <<= 32;
            addr += outlen;
            
            // Ensure added to blob allocation stats: BLZG-1646
            if (outlen > this.m_maxFixedAlloc && m_storageStats != null) {
            	m_storageStats.allocateBlob(outlen);
            }
            
            m_deferredFreeOut.reset();
            return addr;            
        } catch (IOException e) {
            throw new RuntimeException("Cannot write to deferred free", e);
        } finally {
            m_allocationWriteLock.unlock();
        }
    }

    /**
     * Provided with the address of a block of addresses to be freed
     * @param blockAddr
     * @return the total number of addresses freed
     */
    private int freeDeferrals(final long blockAddr, final long lastReleaseTime) {
        final int addr = (int) (blockAddr >> 32);
        final int sze = (int) blockAddr & 0xFFFFFFFF; // Resolution for BLZG-1236 (recycler error)
        
        if (log.isTraceEnabled())
            log.trace("freeDeferrals at " + physicalAddress(addr) + ", size: " + sze + " releaseTime: " + lastReleaseTime);
        
        final byte[] buf = new byte[sze+4]; // allow for checksum
        getData(addr, buf);
        final DataInputStream strBuf = new DataInputStream(new ByteArrayInputStream(buf));
        m_allocationWriteLock.lock();
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
            m_allocationWriteLock.unlock();
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
        m_allocationWriteLock.lock();
        try {
        	context.release();
        	
        	if (context.isIsolated()) {
            final ContextAllocation alloc = m_contexts.remove(context);
            
            if (alloc != null) {
                alloc.release();            
            } else {
                throw new IllegalStateException("Multiple call to detachContext");
            }
            
            if (m_contexts.isEmpty() && this.m_activeTxCount == 0) {
                releaseSessions();
            }
        	}
        } finally {
            m_allocationWriteLock.unlock();
        }
    }
    
    /**
     * The ContextAllocation object manages a freeList of associated allocators
     * and an overall list of allocators.  When the context is aborted then
     * allocations made by that context should be released.
     *  See {@link AllocBlock #abortShadow}
     * 
     * @param context
     *            The context to be released from all FixedAllocators.
     */
    public void abortContext(final IAllocationContext context) {
        assertOpen();
        m_allocationWriteLock.lock();
        try {
        	context.release();
        	
        	if (context.isIsolated()) {
            final ContextAllocation alloc = m_contexts.remove(context);
            
            if (alloc != null) {
                alloc.abort();          
            }
        	}
            
        } finally {
            m_allocationWriteLock.unlock();
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
//      private final ArrayList<BlobAllocator> m_freeBlobs;
        
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
            
//          m_freeBlobs = new ArrayList<BlobAllocator>();
            
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
            
//          for (int i = 0; i < m_freeFixed.length; i++) {
//              freeFixed[i].addAll(m_freeFixed[i]);
//          }
            
//          freeBlobs.addAll(m_freeBlobs);
            
            // now free all deferred frees made within this context for other
            // allocators
            if (log.isDebugEnabled())
                log.debug("Releasing " + m_deferredFrees.size() + " deferred frees");
            
            final boolean defer = m_store.m_minReleaseAge > 0 || m_store.m_activeTxCount > 0 || m_store.m_contexts.size() > 0;
            for (Long l : m_deferredFrees) {
            	final int addr = (int) (l >> 32);
            	final int sze = l.intValue();
            	if (defer) {
            		m_store.deferFree(addr, sze);
            	} else {
            		m_store.immediateFree(addr, sze);
            	}
            }
            m_deferredFrees.clear();
        }
        
        void abort() {
            final ArrayList<FixedAllocator> freeFixed[] = m_parent != null ? m_parent.m_freeFixed
                    : m_store.m_freeFixed;

            final IAllocationContext pcontext = m_parent == null ? null
                    : m_parent.m_context;

            for (FixedAllocator f : m_allFixed) {
                f.abortAllocationContext(pcontext, m_store.m_writeCacheService);
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
                if (falloc.m_pendingContextCommit) {
                	throw new IllegalStateException("Allocator on free list while pendingContextCommit");
                }
                
                falloc.setAllocationContext(m_context);
                // The normal check for adding to the free list is whether to return to the free list,
                //	but in this case, we are moving to another free list, so we should not need to
                //	check for the smallAllocation threshold.
                falloc.setFreeList(free, true/*force*/);
                
                if (free.size() == 0 ) {
                	throw new IllegalStateException("Free list should not be empty, pendingContextCommit: " + falloc.m_pendingContextCommit);
                }
                
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
    
    private ContextAllocation getContextAllocation(
            final IAllocationContext context) {

        /*
         * The allocation lock MUST be held to make changes in the membership of
         * m_contexts atomic with respect to free().
         */
        assert m_allocationWriteLock.isHeldByCurrentThread();
        
        ContextAllocation ret = m_contexts.get(context);
        
        if (ret == null) {
            
        	// This is no longer a valid state
                
        	throw new IllegalStateException("No associated ContextAllocation");
                
//            ret = new ContextAllocation(this, m_freeFixed.length, null, context);
//
//            if (m_contexts.put(context, ret) != null) {
//                
//                throw new AssertionError();
//                
//            }
//        
//            if (log.isTraceEnabled())
//                log.trace("Establish ContextAllocation: " + ret 
//                        + ", total: " + m_contexts.size() 
//                        + ", requests: " + ++m_contextRequests 
//                        + ", removals: " + m_contextRemovals 
//                        + ", allocators: " + m_allocs.size() );
//      
//            
//            if (log.isInfoEnabled())
//                log.info("Context: ncontexts=" + m_contexts.size()
//                        + ", context=" + context);
            
        }

        return ret;
    
    }
    
    public int getSlotSize(final int data_len) {
        int i = 0;

        int ret = m_minFixedAlloc;
        while (data_len > ret) {
            i++;
            // If we write directly to the writeCache then the data_len
            //  may be larger than largest slot
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
            // Note: extensionLock required for file IO.
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
//        assertNoRebuild();
        
        final ByteBuffer tmp = ByteBuffer
                .allocate(RootBlockView.SIZEOF_ROOT_BLOCK);

        // Guard IO against concurrent file extension.
        final Lock lock = m_extensionLock.readLock();
        lock.lock();
        try {

            FileChannelUtility.readAll(m_reopener, tmp,
                rootBlock0 ? FileMetadata.OFFSET_ROOT_BLOCK0
                        : FileMetadata.OFFSET_ROOT_BLOCK1);
            
            tmp.position(0); // resets the position.

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        } finally {
            lock.unlock();
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
         * buffer counters
         */
        public volatile long bufferDataBytes;
        public volatile long bufferDataWrites;
        public volatile long bufferFileWrites;

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

            // BufferedWriter
    		final CounterSet bc = root.makePath("buffer");
    		
    		bc.addCounter("ndataWrites", new Instrument<Long>() {
                public void sample() {
                    setValue(bufferDataWrites);
                }
            });

    		bc.addCounter("nfileWrites", new Instrument<Long>() {
                public void sample() {
                    setValue(bufferFileWrites);
                }
            });
            
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

        if (m_writeCacheService != null) {

            final CounterSet tmp = root.makePath("writeCache");

            tmp.attach(m_writeCacheService.getCounters());

        }
        
        return root;

    }

    public void writeRawBuffer(final IHAWriteMessage msg, final IBufferAccess b)
            throws IOException, InterruptedException {

        // expand buffer before writing on the store.
        final ByteBuffer xb = msg.expand(b.buffer());

        if (log.isTraceEnabled()) {
            log.trace("expanded buffer, position: " + xb.position()
                    + ", limit: " + xb.limit());
        }
    	
        final IBufferAccess ba = new IBufferAccess() {

			@Override
			public ByteBuffer buffer() {
				return xb;
			}

			@Override
			public void release() throws InterruptedException {
			}

			@Override
			public void release(long timeout, TimeUnit unit)
					throws InterruptedException {
			}
		};
         
        /*
         * Wrap up the data from the message as a WriteCache object. This will
         * build up a RecordMap containing the allocations to be made, and
         * including a ZERO (0) data length if any offset winds up being deleted
         * (released).
         * 
         * Note: We do not need to pass in the compressorKey here. It is ignored
         * by WriteCache.flush(). We have expanded the payload above. Now we are
         * just flushing the write cache onto the disk.
         */
        final WriteCache writeCache = m_writeCacheService.newWriteCache(ba,
                true/* useChecksums */, true/* bufferHasData */, m_reopener,
                msg.getFileExtent());
        
        // Ensure that replicated buffers are not compacted.
        writeCache.closeForWrites();
        
		/*
		 * Setup buffer for writing. We receive the buffer with pos=0,
		 * limit=#ofbyteswritten. However, flush() expects pos=limit, will
		 * clear pos to zero and then write bytes up to the limit. So,
		 * we set the position to the limit before calling flush.
		 */
		final ByteBuffer bb = ba.buffer();
		final int limit = bb.limit();
		bb.position(limit);
        
        /*
         * Flush the scattered writes in the write cache to the backing store.
         * 
         * Note: WriteCacheImpl.writeOnChannel() will take the extensionLock for
         * the IO against the channel.
         */
//        final Lock lock = m_allocationReadLock; // TODO Is the allocation lock required here?  I doubt it.
//        lock.lock();
//        try {
            // Flush writes.
            writeCache.flush(false/* force */);

//        } finally {
//            lock.unlock();
//        }

        // install reads into readCache (if any)
        m_writeCacheService.installReads(writeCache);
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
        
        final IHAWriteMessage msg = new HAWriteMessage(m_storeUUID,
                -1L/* commitCounter */, -1L/* commitTime */, sequence, nbytes,
                chk, StoreTypeEnum.RW, quorumToken, fileExtent, offset/* firstOffset */);

        final Future<Void> remoteWriteFuture = quorumMember.replicate(req, msg,
                clientBuffer);

        return remoteWriteFuture;

    }
    
    public void writeOnStream(final OutputStream os, final ISnapshotData snapshotData,
            final Quorum<HAGlue, QuorumService<HAGlue>> quorum, final long token)
            throws IOException, QuorumException, InterruptedException {
    	
    	// final FileInputStream filein = new FileInputStream(this.m_fd);
    	final FileChannelUtility.ReopenerInputStream filein = new FileChannelUtility.ReopenerInputStream(m_reopener);
    	try {
    		MergeStreamWithSnapshotData.process(filein, snapshotData, os);
    	} finally {
    		filein.close();
    	}
    	
        if (quorum!=null&&!quorum.getClient().isJoinedMember(token)) {
           // See #1172
            throw new QuorumException();
        }
    }
    
    public void writeOnStream2(final OutputStream os, final Set<java.util.Map.Entry<Long, byte[]>> snapshotData,
            final Quorum<HAGlue, QuorumService<HAGlue>> quorum, final long token)
            throws IOException, QuorumException {
    
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

            // The capacity of that buffer (typically 1MB).
            final int bufferCapacity = b.capacity();

            // A big enough byte[].
            final byte[] a = new byte[bufferCapacity];
            
            // The size of the root blocks (which we skip).
            final int headerSize = FileMetadata.headerSize0;

            /*
             * The size of the file at the moment we begin. We will not
             * replicate data on new extensions of the file. Those data will
             * be captured by HALog files that are replayed by the service
             * that is doing the rebuild.
             */
//            final long fileExtent = getExtent();
            final long fileExtent = getStoreFile().length();
            
            // The #of bytes to be transmitted.
            final long totalBytes = fileExtent - headerSize;
            
            // The #of bytes remaining.
            long remaining = totalBytes;
            
            // The offset from which data is retrieved.
            long offset = headerSize;
            
            long sequence = 0L;
            
            if (log.isInfoEnabled())
                log.info("Writing on stream: nbytes=" + totalBytes);

            while (remaining > 0) {

                int nbytes = (int) Math.min((long) bufferCapacity,
                        remaining);
                
                if (sequence == 0L && nbytes == bufferCapacity
                        && remaining > bufferCapacity) {
                    
                    /*
                     * Adjust the first block so the remainder will be
                     * aligned on the bufferCapacity boundaries (IO
                     * efficiency).
                     */
                    nbytes -= headerSize;

                }

                if (log.isDebugEnabled())
                    log.debug("Writing block: sequence=" + sequence
                            + ", offset=" + offset + ", nbytes=" + nbytes);

                // read direct from store
                final ByteBuffer clientBuffer = b;
                clientBuffer.position(0);
                clientBuffer.limit(nbytes);

                readRaw(/*nbytes,*/ offset, clientBuffer);

                assert clientBuffer.remaining() > 0 : "Empty buffer: " + clientBuffer;
                
                if (BytesUtil
                        .toArray(clientBuffer, false/* forceCopy */, a/* dst */) != a) {

                    // Should have copied into our array.
                    throw new AssertionError();
                    
                }

                // write onto the stream.
                os.write(a, 0/* off */, nbytes/* len */);
                
                remaining -= nbytes;
                
                offset += nbytes;

                sequence++;
                
                if (!quorum.getClient().isJoinedMember(token))
                    throw new QuorumException();

            }

            if (log.isInfoEnabled())
                log.info("Wrote on stream: #blocks=" + sequence + ", #bytes="
                        + (fileExtent - headerSize));

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
    public ByteBuffer readRaw(final long offset, final ByteBuffer dst) {

        // Guard against concurrent file extension.
        final Lock lock = m_extensionLock.readLock();
        lock.lock();
        try {
            final int position = dst.position();
            try {
 
                final long beginDisk = System.nanoTime();

                // the offset into the disk file.
                // final long pos = FileMetadata.headerSize0 + offset;
                final long pos = offset;
                final int length = dst.limit();

                // read on the disk.
                final int ndiskRead = FileChannelUtility.readAll(m_reopener,
                        dst, pos);

                m_diskReads += ndiskRead;
                
                final long now = System.nanoTime();
                
                // update performance counters.
                final StoreCounters<?> c = (StoreCounters<?>) storeCounters
                        .get().acquire();
                try {
                    c.ndiskRead += ndiskRead;
                    final int nbytes = length;
                    c.nreads++;
                    c.bytesRead += nbytes;
                    c.bytesReadFromDisk += nbytes;
                    c.elapsedReadNanos += now - beginDisk;
                    c.elapsedDiskReadNanos += now - beginDisk;
                } finally {
                    c.release();
                }

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }

            // reset for reading
            dst.position(position);

            return dst;
        } finally {

            lock.unlock();
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
        
        @Override
        public void close() {
            if (m_open.compareAndSet(true/*expect*/, false/*update*/)) {
                deactivateTx();
            }
        }
    }
    
    @Override
    public IRawTx newTx() {
        return new RawTx();
    }
    
    private void activateTx() {
        m_allocationWriteLock.lock();
        try {
            m_activeTxCount++;
            if(log.isInfoEnabled())
                log.info("#activeTx="+m_activeTxCount);
        } finally {
            m_allocationWriteLock.unlock();
        }
    }
    
    private void deactivateTx() {
        m_allocationWriteLock.lock();
        try {
            if (log.isInfoEnabled())
                log.info("Deactivating TX " + m_activeTxCount);
            
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
            m_allocationWriteLock.unlock();
        }
    }

	/**
	 * Debug ONLY method added to permit unit tests to be written that the
	 * native transaction counter is correctly decremented to zero. The returned
	 * value is ONLY valid while holding the {@link #m_allocationLock}.
	 * Therefore this method MAY NOT be used reliably outside of code that can
	 * guarantee that there are no concurrent committers on the {@link RWStore}.
	 * 
	 * @see <a href="http://trac.blazegraph.com/ticket/1036"> Journal file growth
	 *      reported with 1.3.3 </a>
	 */
    public int getActiveTxCount() {
		m_allocationWriteLock.lock();
		try {
			return m_activeTxCount;
        } finally {
            m_allocationWriteLock.unlock();
        }
    }
    
    /**
     * Returns the slot size associated with this address
     */
    @Override
    public int getAssociatedSlotSize(final int addr) {
        return getBlock(addr).getBlockSize();
    }

    /**
     * lockAddress adds the address passed to a lock list.  This is for
     * debug only and is not intended to be used generally for the live system.
     * 
     * @param addr - address to be locked
     */
    public void lockAddress(final int addr) {
        if (m_lockAddresses.putIfAbsent(addr, System.currentTimeMillis()) != null) {
            throw new IllegalStateException("address already locked, logical: " + addr + ", physical: " + physicalAddress(addr, true));
        }
    }

    public void showWriteCacheDebug(final long paddr) {
        log.warn("WriteCacheDebug: " + paddr + " - " + m_writeCacheService.addrDebugInfo(paddr));
    }

    public CounterSet getWriteCacheCounters() {
        return m_writeCacheService.getCounters();
    }

//  /**
//   * If historical data is maintained then this will return the earliest time for which
//   * data can be safely retrieved.
//   * 
//   * @return time of last release
//   */
    @Override
    public long getLastReleaseTime() {
        return m_lastDeferredReleaseTime;
    }

    private ConcurrentWeakValueCache<Long, ICommitter> m_externalCache = null;
    private int m_cachedDatasize = 0;

    @Override
    public void registerExternalCache(
            final ConcurrentWeakValueCache<Long, ICommitter> externalCache,
            final int dataSize) {
    
        m_allocationWriteLock.lock();
        try {
            m_externalCache = externalCache;
            m_cachedDatasize = getSlotSize(dataSize);
        } finally {
            m_allocationWriteLock.unlock();
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

        // FIXME ALLOCATION LOCK : Why not use the ReadLock here?
        final Lock lock = m_allocationWriteLock;
        
        lock.lock();
       
        try {

            final FixedAllocator alloc = getBlockByAddress(rwaddr);
            
            final int offset = getOffset(rwaddr);

            return alloc.isCommitted(offset);
            
        } finally {
            
            lock.unlock();
            
        }
    }

    public boolean inWriteCache(final int rwaddr) {
        
        return m_writeCacheService.isPresent(physicalAddress(rwaddr, true));
        
    }

    @Override
    public InputStream getInputStream(long addr) {
        return new PSInputStream(this, addr);
    }

    @Override
    public IPSOutputStream getOutputStream() {
        return getOutputStream(null);
    }  

    public IPSOutputStream getOutputStream(final IAllocationContext context) {
    	checkContext(context);
    	
        return PSOutputStream.getNew(this, m_maxFixedAlloc, context);
    }

    /**
     * Low level routine used when we replace the root blocks of an empty
     * journal in HA with those from the leader.
     * <p>
     * Note: This method is only invoked in contexts where there should not be
     * concurrent access to the journal. This we should not need to worry about
     * concurrent readers during {@link #resetFromHARootBlock(IRootBlockView)}.
     * 
     * @see #postHACommit(IRootBlockView)
     */
    public void resetFromHARootBlock(final IRootBlockView rootBlock) {

        /*
         * Acquire exclusive access to the allocators.
         * 
         * Note: Since the allocation lock must be held before you may take the
         * extensionLock, and we have exclusive access to the allocation lock,
         * we SHOULD NOT need to take the extension lock as well.
         */
        final Lock outerLock = m_allocationWriteLock;
        outerLock.lock();

        try {
        
            // Exclude IOs.
            final Lock innerLock = m_extensionLock.writeLock();
            innerLock.lock();

            try {
            
                // should not be any dirty allocators
                // assert m_commitList.size() == 0;
                
                // Remove all current allocators
                m_allocs.clear();
                
                assert m_nextAllocation != 0;
                
                m_nextAllocation = 0;
    
                initfromRootBlock(rootBlock);
                
                // KICK external cache into touch - FIXME: handle with improved Allocator synchronization
                m_externalCache.clear();
                
                assert m_nextAllocation != 0;

            } finally {
            
                innerLock.unlock();
                
            }

        } catch (IOException e) {

            throw new RuntimeException(e);

        } finally {

            outerLock.unlock();
            
        }

    }

    /**
     * Called from {@link AbstractJournal} commit2Phase to ensure that a
     * downstream HA quorum member ensures it is able to read committed data
     * that has been streamed directly to the backing store.
     * <p>
     * The data stream will have included metabits and modified
     * {@link FixedAllocator}s so these must be reset using the metabitsAddr
     * data in the root block.
     * <p>
     * Note: Reads on the {@link RWStore} MUST block during this method since
     * some allocators may be replaced as part of the post-commit protocol.
     * <p>
     * Ticket #778 was for a problem when a follower takes over as leader and
     * was not correctly synchronised.  This was traced, eventually, to a problem
     * in calculating the diskAddr metabit for the modified Allocator.  The problem
     * was demonstrated by a temporary method to reserve metaAllocations by extending and
     * setting the m_transient bits.  But that has to be done within the commit() method
     * before it attempts to save all the dirty allocators.  If we need to contrive a similar
     * scenario in the future a better approach would be a special debug property on the 
     * RWStore that indicates a "TRANSIENT_RESERVE" or something similar.
     * 
     * @param rbv
     *            The new {@link IRootBlockView}.
     */
    @SuppressWarnings("unchecked")
    public void postHACommit(final IRootBlockView rbv) {

        /*
         * Acquire exclusive access to the allocators.
         * 
         * Note: Since the allocation lock must be held before you may take the
         * extensionLock, and we have exclusive access to the allocation lock,
         * we SHOULD NOT need to take the extension lock as well.
         */
        final Lock outerLock = m_allocationWriteLock;
        outerLock.lock();
        try {
            final Lock innerLock = m_extensionLock.writeLock();
            innerLock.lock();
            try {
                // Current FixedAllocators for sanity
                if (log.isTraceEnabled()) 
                {
                    log.trace("POSTHACOMMIT START");
                    for (int index = 0; index < m_allocs.size(); index++) {
                        final FixedAllocator xfa = m_allocs.get(index);
                        log.trace("Allocator " + index + ", size: " + xfa.m_size + ", startAddress: " + xfa.getStartAddr() + ", allocated: " + (xfa.getAllocatedSlots()/xfa.m_size));
                    }
                }
                
                // Update m_metaBits addr and m_nextAllocation to ensure able to allocate as well as read!
                {
	                final long nxtOffset = rbv.getNextOffset();
	
	                // next allocation to be made (in -32K units).
	                m_nextAllocation = -(int) (nxtOffset >> 32);
	                
	                if (m_nextAllocation == 0) {
	                	throw new IllegalStateException("Invalid state for non-empty store");
	                }
	                
	                m_committedNextAllocation = m_nextAllocation;
	        
	                // latched offset of the metabits region.
	                m_metaBitsAddr = -(int) nxtOffset;
	                
                }
                
                final ArrayList<FixedAllocator> nallocs = new ArrayList<FixedAllocator>();
                
                // current metabits
                final int[] oldmetabits = m_metaBits;
                // new metabits
                final RootBlockInfo rbi = new RootBlockInfo(rbv, m_reopener);
                m_metaBits = rbi.m_metabits;
                
                // and grab the last deferred release and storageStats!
                m_lastDeferredReleaseTime = rbi.m_lastDeferredReleaseTime;
                m_storageStatsAddr = rbi.m_storageStatsAddr;
                
                if(log.isTraceEnabled())
                    log.trace("Metabits length: " + m_metaBits.length);
                
                // Valid metabits should be multiples of default sizes
                if (oldmetabits.length % cDefaultMetaBitsSize != 0)
                	throw new AssertionError();
                if (m_metaBits.length % cDefaultMetaBitsSize != 0)
                	throw new AssertionError("New metabits: " + m_metaBits.length + ", old: " + oldmetabits.length);

                // Is it always valid to assume that:
                //	metabits.length >= oldmetabits.length
                if (m_metaBits.length < oldmetabits.length)
                	throw new AssertionError();
                
                // need to compute modded metabits, those newly written slots by ANDing
                // new bits with compliment of current
                final int[] moddedBits = m_metaBits.clone();
                for (int b = 0; b < oldmetabits.length; b+=cDefaultMetaBitsSize) {
                    // int[0] is startAddr, int[1:cDefaultMetaBitsSize] bits
                    for (int i = 1; i < cDefaultMetaBitsSize; i++) {
                        moddedBits[b+i] &= ~oldmetabits[b+i];
                    }
                }
                
                if (log.isTraceEnabled()) {
                    StringBuffer sb = new StringBuffer();
                    Iterator<Entry<Long, WeakReference<ICommitter>>> entries = m_externalCache.entryIterator();
                    while (entries.hasNext()) {
                        sb.append(entries.next().getKey() + "|");
                    }
                    
                    log.trace("External Cache Start Size: " + m_externalCache.size() + ", entries: " + sb.toString());
                }
                // Stage One: Count moddedBits
                // Stage Two: Compute Address of modded bits
                // Stage Three: Read Allocator from modded address
                // Stage Four: Update Live Allocators
                
                int modCount = 0;
                int totalFreed = 0;
                for (int i = 0; i < moddedBits.length; i+=cDefaultMetaBitsSize) {
                    final long startAddr = convertAddr(m_metaBits[i]);
                    for (int j = 1; j < cDefaultMetaBitsSize; j++) {
                        final int chkbits = moddedBits[i+j];
                        for (int b = 0; b < 32; b++) {
                            if ((chkbits & (1 << b)) != 0) {
                                modCount++;
                                // Calculate address
                                final int bit = b + (32 * (j-1));
                                final long paddr = startAddr + (bit * ALLOC_BLOCK_SIZE);
                                if (log.isTraceEnabled())
                                    log.trace("Allocator at: " + paddr);
                                
                                // metaBit
//                                final int metaBit = (i * cDefaultMetaBitsSize * 32) + (j * 32) + b;
                                final int metaBit = ((i + j) * 32) + b;
                                
                                // Now try to read it in
                                final FixedAllocator nalloc = readAllocator(paddr);
                                if (log.isTraceEnabled())
                                    log.trace("Allocator read of size: " + nalloc.m_size + ", metaBit: " + metaBit);
                                
                                nalloc.setDiskAddr(metaBit);
                                
                                // Now can we find an existing one to replace, otherwise we need to add to the new list
                                boolean found = false;
                                if (log.isTraceEnabled())
                                    log.trace("Checking allocator at " + nalloc.getStartAddr());
                                
                                for (int index = 0; !found && index < m_allocs.size(); index++) {
                                    final FixedAllocator xfa = m_allocs.get(index);
                                    if (xfa.getStartAddr() == nalloc.getStartAddr()) {
                                        if (log.isTraceEnabled())
                                            log.trace("Found updated allocator at " + index 
                                                + ", size: " + xfa.m_size + " vs " + nalloc.m_size + ", allocated slots: " + (xfa.getAllocatedSlots()/xfa.m_size) + " vs " + (nalloc.getAllocatedSlots()/xfa.m_size));
                                        
                                        // Compare allocators to see if same
                                        found = true;
                                        
                                        // Replace old with new
                                        m_allocs.set(index,  nalloc);
                                        nalloc.setIndex(index);
                                        
                                        // remove old from free list (if set)
                                        xfa.removeFromFreeList();
                                        
                                        // now clear any cached writes now freed
                                        totalFreed +=nalloc.removeFreedWrites(xfa, m_externalCache);
                                    }
                                }
                                
                                if (!found) {
                                    nallocs.add(nalloc);
                                }
                            }
                        }
                    }
                }
                
                
                if (log.isInfoEnabled())
                    log.info("Released: " + totalFreed + " addresses from " + modCount + " modified Allocators");

                if (log.isTraceEnabled()) {
                    log.trace("OLD BITS: " + BytesUtil.toHexString(oldmetabits));
                    log.trace("NEW BITS: " + BytesUtil.toHexString(m_metaBits));
                    log.trace("MODDED BITS: " + BytesUtil.toHexString(moddedBits));
                    log.trace("MODDED COUNT: " + modCount + " from " + m_allocs.size() + " Allocators");
                }
                
                // Now add in any new allocators, first sorting and setting their index number
                if (nallocs.size() > 0) {
                    Collections.sort(nallocs);
                    
                    final int sindex = m_allocs.size();
                    for (int index = 0; index < nallocs.size(); index++) {
                        ((Allocator) nallocs.get(index)).setIndex(sindex + index);
                        if (log.isTraceEnabled())
                            log.trace("New Allocator, index: " + (sindex + index));
                    }

                    if (log.isTraceEnabled())
                        log.trace("Adding new allocators: " + sindex);
                    
                    m_allocs.addAll(nallocs);
                }
                
                {
                    final long nxtOffset = rbv.getNextOffset();
    
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
                }
                
                if (log.isTraceEnabled()) {
                    log.trace("POSTHACOMMIT END");
                    for (int index = 0; index < m_allocs.size(); index++) {
                        final FixedAllocator xfa = m_allocs.get(index);
                        log.trace("Allocator " + index + ", startAddress: " + xfa.getStartAddr() + ", allocated: " + xfa.getAllocatedSlots());
                    }
                }

                if (log.isTraceEnabled())
                    log.trace("External Cache Pre Clear Size: " + m_externalCache.size());
                
                // If FixedAllocator.removeFreedWrites does its job then we do not
                //  need to clear the external cache                
                // m_externalCache.clear();
                
                assert m_nextAllocation != 0;
            } finally {
                innerLock.unlock();
            }
        
        } catch (IOException e) {
        
            throw new RuntimeException(e);
            
        } finally {
            outerLock.unlock();
        }
        
        // FIXME: Remove once allocators are synced
//      log.error("Complete implementation of postHACommit()");
//      
//      resetFromHARootBlock(rbv);
//      
//      log.warn("POSTHACOMMIT AFTER RESET");
//      for (int index = 0; index < m_allocs.size(); index++) {
//          final FixedAllocator xfa = m_allocs.get(index);
//          log.warn("Allocator " + index + ", startAddress: " + xfa.getStartAddr() + ", allocated: " + xfa.getAllocatedSlots());
//      }
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
//      private final ArrayList<String> m_dupData = new ArrayList<String>();
            
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
//                  final int latchedAddr = m_duplicates.get(i);
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
        final int sze = (int) blockAddr & 0xFFFFFFFF; // Resolution for BLZG-1236 (recycler error)

        if (log.isTraceEnabled())
            log.trace("freeDeferrals at " + physicalAddress(addr) + ", size: "
                    + sze + " releaseTime: " + commitTime);

        final byte[] buf = new byte[sze + 4]; // allow for checksum
        getData(addr, buf);
        final DataInputStream strBuf = new DataInputStream(
                new ByteArrayInputStream(buf));
        m_allocationWriteLock.lock();
//      int totalFreed = 0;
        try {
            int nxtAddr = strBuf.readInt();

//          int cnt = 0;

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
                        log.warn("" + commitTime + " " + nxtAddr
                                + " FREE DUP");
                    }
                } else {
                    stats.m_freed.put(nxtAddr, nxtAddr);
                    if (writeAll) {
                    	log.warn("" + commitTime + " " + nxtAddr
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
            m_allocationWriteLock.unlock();
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

        final Lock outerLock = m_allocationReadLock;

        try {

            final FixedAllocator alloc = getBlockByAddress(nxtAddr);

            final byte[] data = new byte[alloc.m_size];

            final ByteBuffer bb = ByteBuffer.wrap(data);

            final int offset = getOffset(nxtAddr);

            final long paddr = alloc.getPhysicalAddress(offset);

            // Guard IO against concurrent file extension.
            final Lock innerLock = m_extensionLock.readLock();

            try {
            
                FileChannelUtility.readAll(m_reopener, bb, paddr);
            
            } finally {

                innerLock.unlock();
                
            }

            return data;

        } finally {

            outerLock.unlock();

        }

    }

    /**
     * @see IHABufferStrategy#getBlockSequence()
     */
    public long getBlockSequence() {

        return lastBlockSequence;

    }

    private long lastBlockSequence = 0;

    /**
     * @see IHABufferStrategy#getCurrentBlockSequence()
     */
    public long getCurrentBlockSequence() {
        
        final WriteCacheService tmp = m_writeCacheService;

        if (tmp == null) {

            /*
             * Either this is not an HA strategy mode -or- we are in abort() and
             * the value temporarily [null]. If there is an abort(), then the
             * counter will be reset to 0L.
             */
            
            return 0L;
            
        }

        return tmp.getSequence();
        
    }
    
//  private HARebuildRequest m_rebuildRequest = null;
    
     //    /**
//     * Only blacklist the addr if not already available, in other words
//     * a blacklisted address only makes sense if it for previously 
//     * committed data and not instantly recyclable.
//     */
//    public void blacklistAddress(int addr, final String info) {
//      if (m_blacklist == null) {
//          // add delay/synchronization to emulate blacklist delay?
//          return;
//      }
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

        if(true) {

            computeDigestOld(snapshot, digest);
            
        } else {
            
            computeDigestAlt(snapshot, digest);
            
        }
        
    }

    private void computeDigestOld(final Object snapshot, final MessageDigest digest)
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

//            // A byte[] with the same capacity as that ByteBuffer.
//            final byte[] a = new byte[b.capacity()];
            
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

                if (log.isTraceEnabled())
                    log.trace("Computing digest: sequence=" + sequence
                            + ", offset=" + offset + ", nbytes=" + nbytes);

                // Setup for read.
                b.position(0);
                b.limit(nbytes);

                // read block
                readRaw(/*nbytes,*/ offset, b);

//                // Copy data into our byte[].
//                final byte[] c = BytesUtil.toArray(b, false/* forceCopy */, a);

                // update digest
                //digest.update(c, 0/* off */, nbytes/* len */);
                digest.update(b);

                remaining -= nbytes;

                offset += nbytes;
                
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

    /**
     * This alternative implementation checks only the live allocations
     * 
     * @param snapshot
     * @param digest
     * @throws DigestException
     * @throws IOException
     */
    private void computeDigestAlt(final Object snapshot, final MessageDigest digest)
            throws DigestException, IOException {
        if (snapshot != null)
            throw new UnsupportedOperationException();

        m_allocationWriteLock.lock();
        try {
            // FIXME add digest for RootBlocks!
            
            for (FixedAllocator fa : m_allocs) {
                fa.computeDigest(snapshot, digest);
            }
        } finally {
            m_allocationWriteLock.unlock();
        }
        
        {
            final byte[] data = digest.digest();
            final StringBuffer sb = new StringBuffer();
            for (byte b : data) {
                if (sb.length() > 0)
                    sb.append(",");
                sb.append(b);
            }
            
            log.warn("STORE DIGEST: " + sb.toString());
            log.warn("Free Deferrals: " + this.m_deferredFreeOut.getBytesWritten());
        }
    }
    
    /**
     * Used as part of the rebuild protocol
     * @throws IOException 
     */
    public void writeRaw(final long offset, final ByteBuffer transfer) throws IOException {
    	
    	if (log.isDebugEnabled())
    		log.debug("writeRaw: " + offset);
    	
        // Guard IO against concurrent file extension.
        final Lock lock = m_extensionLock.readLock();
        
        lock.lock();
        
        try {

            FileChannelUtility.writeAll(m_reopener, transfer, offset);
            
        } finally {
            
            lock.unlock();
            
        }

    }

	private String showAllocatorList() {
		final StringBuilder sb = new StringBuilder();
		
        for (int index = 0; index < m_allocs.size(); index++) {
            final FixedAllocator xfa = m_allocs.get(index);
            sb.append("Allocator " + index + ", size: " + xfa.m_size + ", startAddress: " + xfa.getStartAddr() + ", allocated: " + xfa.getAllocatedSlots() + "\n");
        }
        
        return sb.toString();
	}
//    /**
//     * 
//     * @return whether WCS is flushed
//     * 
//     * @see IBufferStrategy#isFlushed()
//     */
//	public boolean isFlushed() {
//		return this.m_writeCacheService.isFlushed();
//	}

	public static class RWStoreState implements StoreState {

		/**
		 * Generated ID
		 */
		private static final long serialVersionUID = 4315400143557397323L;
		
		/*
		 * Transient state necessary for consistent ha leader transition
		 */
		private final int m_fileSize;
		private final int m_nextAllocation;
		private final int m_committedNextAllocation;
		private final long m_minReleaseAge;
		private final long m_lastDeferredReleaseTime;
		private final long m_storageStatsAddr;
		private final int m_allocsSize;
		private final int m_metaBitsAddr;
		private final int m_metaBitsSize;

        private RWStoreState(final RWStore store) {
            m_fileSize = store.m_fileSize;
            m_nextAllocation = store.m_nextAllocation;
            m_committedNextAllocation = store.m_committedNextAllocation;
            m_minReleaseAge = store.m_minReleaseAge;
            m_lastDeferredReleaseTime = store.m_lastDeferredReleaseTime;
            m_storageStatsAddr = store.m_storageStatsAddr;
            m_allocsSize = store.m_allocs.size();
            m_metaBitsAddr = store.m_metaBitsAddr;
            m_metaBitsSize = store.m_metaBits.length;
		}
		
        @Override
		public boolean equals(final Object obj) {
			if (obj == null || !(obj instanceof RWStoreState))
				return false;
			final RWStoreState other = (RWStoreState) obj;
			return m_fileSize == other.m_fileSize
					&& m_nextAllocation == other.m_nextAllocation
					&& m_committedNextAllocation == other.m_committedNextAllocation
					&& m_minReleaseAge == other.m_minReleaseAge
					&& m_lastDeferredReleaseTime == other.m_lastDeferredReleaseTime
					&& m_storageStatsAddr == other.m_storageStatsAddr
					&& m_allocsSize == other.m_allocsSize
					&& m_metaBitsAddr == other.m_metaBitsAddr
					&& m_metaBitsSize == other.m_metaBitsSize;
		}
		
		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder();
			
			sb.append("RWStoreState\n");
			sb.append("fileSize: " + m_fileSize + "\n");
			sb.append("nextAllocation: " + m_nextAllocation + "\n");
			sb.append("committedNextAllocation: " + m_committedNextAllocation + "\n");
			sb.append("minReleaseAge: " + m_minReleaseAge + "\n");
			sb.append("lastDeferredReleaseTime: " + m_lastDeferredReleaseTime + "\n");
			sb.append("storageStatsAddr: " + m_storageStatsAddr + "\n");
			sb.append("allocsSize: " + m_allocsSize + "\n");
			sb.append("metaBitsAddr: " + m_metaBitsAddr + "\n");
			sb.append("metaBitsSize: " + m_metaBitsSize + "\n");
			
			return sb.toString();
		}
	}
	
	/**
	 * Can be used to determine if an address is within an allocated slot.
	 * 
	 * @param addr
	 * @return whether addr is within slot allocated area
	 */
	public boolean verifyAllocatedAddress(final long addr) {
        for (int index = 0; index < m_allocs.size(); index++) {
            final FixedAllocator xfa = m_allocs.get(index);
            if (xfa.verifyAllocatedAddress(addr))
            	return true;
        }
		
        return false;
	}
	
	public StoreState getStoreState() {
		final RWStoreState ret = new RWStoreState(this);
		
		return ret;
	}

	/**
	 * Forces a reset of the metabits allocation on the next commit.
	 * <p>
	 * Note that a side-effect of this is that there will be a memory leak
	 * of either a FixedAllocation slot or an existing demi-space.
	 * <p>
	 * @param useDemispace
	 * @return whether the storage has been modified.
	 */
	public boolean ensureMetabitsDemispace(final boolean useDemispace) {
		final boolean isDemispace = m_metaBitsAddr > 0;
		
		if (isDemispace != useDemispace || m_useMetabitsDemispace != useDemispace) {
			m_useMetabitsDemispace = useDemispace;
	
			m_metaBitsAddr = 0;
			
			m_recentAlloc = true; // force commit
			
			return true;
		} else {
			return false;
		}
	}
	
	public boolean isUsingDemiSpace() {
		return m_metaBitsAddr > 0;
	}

	/**
	 * Add the address/byte[] to the snapshot representing the metabits allocaiton data
	 * 
	 * @throws IOException 
	 */
	public void snapshotMetabits(final ISnapshotData tm) throws IOException {
		final long mba;
		if (m_metaBitsAddr < 0) {
			mba = physicalAddress((int) m_metaBitsAddr);
		} else {
        // long ret = physicalAddress((int) m_metaBitsAddr);
			mba = convertAddr(-m_metaBitsAddr); // maximum 48 bit address range
		}

		tm.put(mba, genMetabitsData());
	}

	/**
	 * Add the address/allocator associated with each FixedAllocator to the snapshot map
	 */
	public void snapshotAllocators(final ISnapshotData tm) {
		for(FixedAllocator alloc : m_allocs) {
			alloc.snapshot(tm);
		}
	}
	
	class AllocationContext implements IAllocationContext {
		
		boolean m_active = true;
		final boolean m_isolated;
		
		public AllocationContext(boolean isolated) {
			m_isolated = isolated;
		}

		final public void checkActive() {
			if (!m_active) {
				throw new IllegalStateException();
			}
		}
		
		final public void release() {
			checkActive();
			
			m_active = false;
		}

		@Override
		public boolean isIsolated() {
			return m_isolated;
		}
	}
	
	public IAllocationContext newAllocationContext(final boolean isolated) {
		m_allocationWriteLock.lock();
		try {
			final IAllocationContext ret = new AllocationContext(isolated);
			if (isolated) {
				final ContextAllocation ca = new ContextAllocation(this,
						m_freeFixed.length, null, ret);
	
				if (m_contexts.put(ret, ca) != null) {
	
					throw new AssertionError();
	
				}
			}
			return ret;
		} finally {
			m_allocationWriteLock.unlock();
		}
	}

//  public void prepareForRebuild(final HARebuildRequest req) {
//      assert m_rebuildRequest == null;
//      
//      m_rebuildRequest = req;
//  }
//
//  public void completeRebuild(final HARebuildRequest req, final IRootBlockView rbv) {
//      assert m_rebuildRequest != null;
//      
//      assert m_rebuildRequest.equals(req);
//      
//      // TODO: reinit from file
//      this.resetFromHARootBlock(rbv);
//      
//      m_rebuildRequest = null;
//  }

}
