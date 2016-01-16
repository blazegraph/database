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
/*
 * Created on Feb 10, 2010
 */

package com.bigdata.io.writecache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.ha.HAPipelineGlue;
import com.bigdata.ha.QuorumPipeline;
import com.bigdata.io.ChecksumUtility;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.IBufferAccess;
import com.bigdata.io.IReopenChannel;
import com.bigdata.io.writecache.WriteCache.ReadCache;
import com.bigdata.io.writecache.WriteCache.RecordMetadata;
import com.bigdata.journal.AbstractBufferStrategy;
import com.bigdata.journal.IBufferStrategy;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RWStrategy;
import com.bigdata.journal.WORMStrategy;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumMember;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rwstore.RWStore;
import com.bigdata.util.ChecksumError;
import com.bigdata.util.DaemonThreadFactory;
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.Computable;
import com.bigdata.util.concurrent.Memoizer;

/**
 * A {@link WriteCacheService} is provisioned with some number of
 * {@link WriteCache} buffers and a writer thread. Caller's populate
 * {@link WriteCache} instances. When they are full, they are transferred to a
 * queue which is drained by the thread writing on the local disk. Hooks are
 * provided to wait until the current write set has been written (e.g., at a
 * commit point when the cached writes must be written through to the backing
 * channel). This implementation supports high availability using a write
 * replication pipeline.
 * <p>
 * A pool of {@link WriteCache} instances is used. Readers test all of the
 * {@link WriteCache} using a shared {@link ConcurrentMap} and will return
 * immediately the desired record or <code>null</code> if the record is not in
 * any of the {@link WriteCache} instances. Write caches remain available to
 * readers until they need to be recycled as the current write cache (the one
 * servicing new writes).
 * <p>
 * The {@link WriteCacheService} maintains a dirty list of {@link WriteCache}
 * instances. A single thread handle writes onto the disk and onto the write
 * replication pipeline (for HA). When the caller calls flush() on the write
 * cache service it flush() the current write cache is transferred to the dirty
 * list and then wait until the write cache instances now on the dirty list have
 * been serviced. In order to simplify the design and the provide boundary
 * conditions for HA decision making, writers block during
 * {@link #flush(boolean, long, TimeUnit)}.
 * <p>
 * Instances of this class are used by both the {@link RWStrategy} and the
 * {@link WORMStrategy}. These classes differ in how they allocate space on the
 * backing file and in the concurrency which they permit for writers.
 * <dl>
 * <dt>{@link WORMStrategy}</dt>
 * <dd>The {@link WORMStrategy} serializes all calls to
 * {@link #writeChk(long, ByteBuffer, int)} since it must guarantee the precise
 * offset at which each record is written onto the backing file. As a
 * consequence of its design, each {@link WriteCache} is a single contiguous
 * chunk of data and is transferred directly to a known offset on the disk. This
 * append only strategy makes for excellent transfer rates to the disk.</dd>
 * <dt>{@link RWStrategy}</dt>
 * <dd>The {@link RWStrategy} only needs to serialize the decision making about
 * the offset at which the records are allocated. Since the records may be
 * allocated at any location in the backing file, each {@link WriteCache}
 * results in a scattered write on the disk.</dd>
 * </dl>
 * Both the {@link WORMStrategy} and the {@link RWStrategy} implementations need
 * to also establish a read-write lock to prevent changes in the file extent
 * from causing corrupt data for concurrent read or write operations on the
 * file. See {@link #writeChk(long, ByteBuffer, int)} for more information on
 * this issue (it is a workaround for a JVM bug).
 * 
 * <h2>Checksums</h2>
 * 
 * The WORM and RW buffer strategy implementations, the WriteCacheService, and
 * the WriteCache all know whether or not checksums are in use. When they are,
 * the buffer strategy computes the checksum and passes it down (otherwise it
 * passes down a 0, which will be ignored since checksums are not enabled). The
 * WriteCache adjusts its capacity by -4 when checksums are enabled and adds the
 * checksum when transferring the caller's data into the WriteCache. On read,
 * the WriteCache will verify the checksum if it exists and returns a new
 * allocation backed by a byte[] showing only the caller's record.
 * <p>
 * {@link IAddressManager#getByteCount(long)} must be the actual on the disk
 * record length, not the size of the record when it reaches the application
 * layer. This on the disk length is the adjusted size after optional
 * compression and with the optional checksum. Applications which assume that
 * lengthOf(addr) == byte[].length will break, but that's life.
 * 
 * <h2>ReadCache</h2>
 * 
 * Without a hotList the readCache is managed naively by clearing any new
 * readCache. This potentially results in frequently accessed records being lost
 * to the cache.
 * 
 * <h2>HotCache</h2>
 * 
 * With the HotCache evicted readCaches hot records get transferred to hotList
 * and 'old' hotCaches get added to end of readCache. Pattern is needed to pluck
 * reserve hotCache from readList so that it is always possible to transfer hot
 * records from the readList.
 * <p>
 * Start with hotCache AND hotReserve.
 * 
 * If new reserve needed, because existing one is now used, try and compress new
 * readCache into current hotCache - if won't fit, then call resetWith and lose
 * those writes, cycle again, moving front hotCache to readList and compress
 * that one.
 * <p>
 * LIMIT: If we begin with full caches with above threshold hitCounts then the
 * whole list will cycle around until we hit original cache which will contain
 * records with zero hitCounts - for practical purposes ignoring any concurrent
 * reads.
 * 
 * @see WriteCache
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo There needs to be a unit test which verifies overwrite of a record in
 *       the {@link WriteCache} (a write at the same offset in the backing file,
 *       but at a different position in the {@link WriteCache} buffer). It is
 *       possible for this to occur with the {@link RWStore} if a record is
 *       written, deleted, and the immediately reallocated. Whether or not this
 *       is a likely event depends on how aggressively the {@link RWStore}
 *       reallocates addresses which were allocated and then deleted within the
 *       same native transaction.
 * 
 * @todo When compression is enabled, it is applied above the level of the
 *       {@link WriteCache} and {@link WriteCacheService} (which after all
 *       require the caller to pass in the checksum of the compressed record).
 *       It is an open question as to whether the caller or the store handles
 *       record compression. Note that the B+Tree leaf and node records may
 *       require an uncompressed header to allow fixup of the priorAddr and
 *       nextAddr fields.
 */
abstract public class WriteCacheService implements IWriteCache {

    protected static final Logger log = Logger.getLogger(WriteCacheService.class);

    /**
     * Logger for HA events.
     */
    private static final Logger haLog = Logger.getLogger("com.bigdata.ha");

    /**
     * <code>true</code> until the service is {@link #close() closed}.
     */
//  private volatile boolean open = true;
    private final AtomicBoolean open = new AtomicBoolean(true);

    /**
     * <code>true</code> iff record level checksums are enabled.
     */
    final private boolean useChecksum;

    /**
     * A single threaded service which writes dirty {@link WriteCache}s onto the
     * backing store.
     */
    final private ExecutorService localWriteService;

    /**
     * The {@link Future} of the task running on the {@link #localWriteService}.
     * 
     * @see WriteTask
     * @see #reset()
     */
    private Future<Void> localWriteFuture;

    /**
     * The {@link Future} of the task running on the {@link #remoteWriteService}
     * .
     * <p>
     * Note: Since this is <em>volatile</em> you MUST guard against concurrent
     * clear to <code>null</code> by {@link #reset()}.
     * 
     * @see WriteTask
     * @see #reset()
     */
    private volatile Future<?> remoteWriteFuture = null;

    /**
     * A list of clean buffers. By clean, we mean not needing to be written.
     * Once a dirty write cache has been flushed, it is placed onto the
     * {@link #cleanList}. Clean buffers can be taken at any time for us as the
     * current buffer.
     */
    final private LinkedBlockingDeque<WriteCache> cleanList;

    /**
     * Lock for the {@link #cleanList} allows us to notice when it becomes empty
     * and not-empty.
     */
    final private ReentrantLock cleanListLock = new ReentrantLock();

    /**
     * Condition <code>!cleanList.isEmpty()</code>
     * <p>
     * Note: If you wake up from this condition you MUST also test {@link #halt}.
     */
    final private Condition cleanListNotEmpty = cleanListLock.newCondition();

    /**
     * The read lock allows concurrent {@link #acquireForWriter()}s while the
     * write lock prevents {@link #acquireForWriter()} when we must either reset
     * the {@link #current} cache buffer or change the {@link #current}
     * reference. E.g., {@link #flush(boolean, long, TimeUnit)}.
     * <p>
     * Note: {@link #read(long)} is non-blocking. It does NOT use this lock!!!
     */
    final private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * A list of dirty buffers. Writes from these may be combined, but not
     * across {@link #flush(boolean)}.
     */
    final private BlockingQueue<WriteCache> dirtyList;

    /**
     * Lock for the {@link #dirtyList} allows us to notice when it becomes empty
     * and not-empty.
     */
    final private ReentrantLock dirtyListLock = new ReentrantLock();

    /**
     * Lock used to put cache buffers onto the {@link #dirtyList}. This lock is
     * required in order for {@link #flush(boolean, long, TimeUnit)} to have
     * atomic semantics, otherwise new cache buffers could be added to the dirty
     * list. This lock is distinct from the {@link #lock} because we do not want
     * to yield that lock when awaiting the {@link #dirtyListEmpty} condition.
     * <p>
     * Note: If you wake up from this condition you MUST also test {@link #halt}.
     * 
     * @see #dirtyListLock.
     */
    final private Condition dirtyListEmpty = dirtyListLock.newCondition();

    /**
     * Condition signaled whenever content is added to the dirty list.
     * <p>
     * Note: If you wake up from this condition you MUST also test {@link #halt}.
     */
    final private Condition dirtyListChange = dirtyListLock.newCondition();

    /**
     * Used to compact sparsely utilized {@link WriteCache}.
     */
    private final AtomicReference<WriteCache> compactingCacheRef = new AtomicReference<WriteCache>();
    
    /**
     * Maintained to guarantee that compaction is possible. This is always a
     * clean cache. 
     */
    private final AtomicReference<WriteCache> compactingReserveRef = new AtomicReference<WriteCache>();
    
    /**
     * Disable {@link WriteCache} compaction when <code>false</code>.
     * <p>
     * Note: This is set to <code>false</code> when
     * {@link #compactionThreshold} is 100.
     */
    private final boolean compactionEnabled;
    
    /**
     * The minimum percentage of empty space that could be recovered before we
     * will attempt to compact a {@link WriteCache} buffer (in [0:100]).
     */
    private final int compactionThreshold = 20; 
    
    /**
     * The current buffer. Modification of this value and reset of the current
     * {@link WriteCache} are protected by the write lock of {@link #lock()}.
     */
    final private AtomicReference<WriteCache> current = new AtomicReference<WriteCache>();

    /**
     * The current read cache.
     */
    final private AtomicReference<ReadCache> readCache = new AtomicReference<ReadCache>();

    /**
     * Flag set if {@link WriteTask} encounters an error. The cause is set
     * on {@link #firstCause} as well.
     * <p>
     * Note: Error handling MUST cause the write cache service buffers to be
     * {@link #reset()} and make sure the HA write pipeline is correctly
     * configured. This is handled by a high-level abort() on the journal. It is
     * NOT Ok to simply re-try writes of partly filled buffers since they may
     * already have been partly written to the disk. A high-level abort() is
     * necessary to ensure that we discard any bad writes. The abort() will need
     * to propagate to all members of the {@link Quorum} so they are all reset
     * to the last commit point and have reconfigured write cache services and
     * write pipelines.
     */
    private volatile boolean halt = false;

    /**
     * The first cause of an error within the asynchronous
     * {@link WriteTask}.
     */
    private final AtomicReference<Throwable> firstCause = new AtomicReference<Throwable>();

    /**
     * The capacity of the cache buffers. This is assumed to be the same for
     * each buffer.
     */
    final private int capacity;

//  /**
//   * Object knows how to (re-)open the backing channel.
//   */
//  final private IReopenChannel<? extends Channel> opener;

    /**
     * A map from the offset of the record on the backing file to the cache
     * buffer on which that record was written.
     */
    final private ConcurrentMap<Long/* offset */, WriteCache> serviceMap;

    /**
     * An immutable array of the {@link WriteCache} buffer objects owned by the
     * {@link WriteCacheService} (in contract to those owner by the caller but
     * placed onto the {@link #dirtyList} by
     * {@link #writeChk(long, ByteBuffer, int)}).
     */
    final private WriteCache[] writeBuffers;
    
    /**
     * An immutable array of the {@link WriteCache} buffer objects owned by the
     * {@link WriteCacheService}.  These buffers are used for the readCache.
     */
    final private ReadCache[] readBuffers;
    
    /**
     * Debug arrays to chase down write/removal errors.
     * 
     * Toggle comment appropriately to activate/deactivate
     */
	// final long[] addrsUsed = new long[4024 * 1024];
	// private int addrsUsedCurs = 0;
	// final char[] addrActions = new char[addrsUsed.length];
	// final int[] addrLens = new int[addrsUsed.length];
	private final long[] addrsUsed = null;
	private int addrsUsedCurs = 0;
	private final char[] addrActions = null;
	private final int[] addrLens = null;
    
    /**
     * The backing reader that can be used when a cache read misses.
     */
    final private IBackingReader reader;
    
    /**
     * The current file extent.
     */
    final private AtomicLong fileExtent = new AtomicLong(-1L);

//  /**
//   * The environment in which this object participates
//   */
//  protected final Environment environment;

    /**
     * The object which manages {@link Quorum} state changes on the behalf of
     * this service.
     */
    final private Quorum<HAPipelineGlue, QuorumMember<HAPipelineGlue>> quorum;

//    /**
//     * The {@link UUID} of the highly available service.
//     */
//    final private UUID serviceId;
    
    /**
     * The {@link Quorum} token under which this {@link WriteCacheService}
     * instance is valid. This is fixed for the life cycle of the
     * {@link WriteCacheService}. This ensures that all writes are buffered
     * under a consistent quorum meet.
     */
    final private long quorumToken;
    
    final private int replicationFactor;
    
    /**
     * The object which manages {@link Quorum} state changes on the behalf of
     * this service.
     */
    protected Quorum<HAPipelineGlue, QuorumMember<HAPipelineGlue>> getQuorum() {

        return quorum;
        
    }

    /**
     * Allocates N buffers from the {@link DirectBufferPool}.
     * 
     * @param nwriteBuffers
     *            The #of {@link WriteCache} buffers.
     * @param minCleanListSize
     *            The maximum #of {@link WriteCache} buffers on the
     *            {@link #dirtyList} before we start to evict {@link WriteCache}
     *            buffers to the disk -or- ZERO (0) to use a default value. <br>
     *            Note: As a rule of thumb, you should set
     *            <code>maxDirtyListSize LTE nbuffers-4</code> such that we have
     *            at least: (1) for [current], (1) for [compactingCache], (1)
     *            for reserve and (1) buffer left available on the
     *            {@link #cleanList}.
     * @param prefixWrites
     *            When <code>true</code>, the {@link WriteCacheService} is
     *            supporting an RWS mode store and each {@link WriteCache}
     *            buffer will directly encode the fileOffset of each record
     *            written onto the {@link WriteCache}. When <code>false</code>,
     *            the {@link WriteCacheService} is supporting a WORM mode store
     *            and the {@link WriteCache} buffers contain the exact data to
     *            be written onto the backing store.
     * @param compactionThreshold
     *            The minimum percentage of space that could be reclaimed before
     *            we will attempt to coalesce the records in a
     *            {@link WriteCache} buffer. When <code>100</code>, compaction
     *            is explicitly disabled.
     *            <p>
     *            Note: This is ignored for WORM mode backing stores since we
     *            can not compact the buffer in that mode.
     * @param useChecksum
     *            <code>true</code> iff record level checksums are enabled.
     * @param fileExtent
     *            The current extent of the backing file.
     * @param opener
     *            The object which knows how to (re-)open the channel to which
     *            cached writes are flushed.
     * @param quorumManager
     *            The object which manages {@link Quorum} state changes on the
     *            behalf of this service.
     * 
     * @throws InterruptedException
     */
    public WriteCacheService(final int nwriteBuffers, int minCleanListSize,
    		final int nreadBuffers,
            final boolean prefixWrites, final int compactionThreshold,
            final int hotCacheSize, final int hotCacheThreshold,
            final boolean useChecksum, final long fileExtent,
            final IReopenChannel<? extends Channel> opener, final Quorum quorum,
            final IBackingReader reader)
            throws InterruptedException {

        if (nwriteBuffers <= 0)
            throw new IllegalArgumentException();

        if (minCleanListSize == 0) { // default

            /*
             * Setup a reasonable default if no value was specified.
             * Just need to make sure we have a few spare buffers to
             * prevent latency on acquiring a clean buffer for writing.
	     *
	     * The default here is 5% of the write cache buffers. This
	     * is based on historical experience that we do better with
	     * 50MB of dirty list when there are 2000 write cache buffers,
	     * which is 2.5%.  It seems a reasonable thing to give over
	     * 5%.  If you want more write elision, then just increase
	     * the number of write cache buffers.  95% of them will be 
	     * used to defer writes and elide writes.  5% of them will
	     * be available to drive the disk with random write IOs.
	     *
	     * See BLZG-1589 (Modify the default behavior for setting
the clear/dirty list threshold)
             */
            
            minCleanListSize = Math.max(4, (int) (nwriteBuffers*.003));

        }
        
        if (minCleanListSize > nwriteBuffers)
            minCleanListSize = nwriteBuffers;

        if (minCleanListSize < 0)
            throw new IllegalArgumentException();

        if (compactionThreshold <= 0)
            throw new IllegalArgumentException();

        if (compactionThreshold > 100)
            throw new IllegalArgumentException();

        if (fileExtent < 0L)
            throw new IllegalArgumentException();

        if (opener == null)
            throw new IllegalArgumentException();

//        if (quorum == null)
//            throw new IllegalArgumentException();

        this.useChecksum = useChecksum;

        /**
         * FIXME WCS compaction fails!
         * 
         * CORRECTION, it is NOT clearly established that WCS compaction fails
         * although some failures appear to correlate with it being enabled.
         * It may be that with compaction enabled other errors are more likely
         * that are not directly associated with the compaction; for example
         * as a result of denser data content.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/674" >
         *      WCS write cache compaction causes errors in RWS postHACommit()
         *      </a>
         */
        this.compactionEnabled = canCompact() && compactionThreshold < 100;
        
        if (log.isInfoEnabled())
            log.info("Compaction Enabled: " + compactionEnabled
                    + " @ threshold=" + compactionThreshold);

//      this.opener = opener;

        // the token under which the write cache service was established.
        if ((this.quorum = quorum) != null) {
            this.quorumToken = quorum.token();
            this.replicationFactor = quorum.replicationFactor();
        } else {
            // Not HA.
            this.quorumToken = Quorum.NO_QUORUM;
            this.replicationFactor = 1;
        }
        
        this.reader = reader;
        
        dirtyList = new LinkedBlockingQueue<WriteCache>();

        cleanList = new LinkedBlockingDeque<WriteCache>();

        writeBuffers = new WriteCache[nwriteBuffers];

        /*
         * Configure the desired dirtyListThreshold.
         */
        if (compactionEnabled) {
            /*
             * Setup the RWS dirtyListThreshold.
             * 
             * allow for compacting cache and reserve
             */
            m_dirtyListThreshold = Math.max(1, nwriteBuffers - minCleanListSize - 2); 
        } else {
            /*
             * Note: We always want a threshold of ONE (1) for the WORM since:
             * 1) We can not compact cache buffers for that store mode.
             * 2) We still want to write data to the file even if it will
             * 	never be read (as in the case of "deleted" data in same transaction
             * 	as it was allocated).
             */
            m_dirtyListThreshold = 1;
        }
        assert m_dirtyListThreshold >= 1;
        assert m_dirtyListThreshold <= writeBuffers.length;
        
        // Setup ReadCache
        this.readListSize = nreadBuffers;
        this.readList = new LinkedBlockingDeque<ReadCache>();
        
        readBuffers = new ReadCache[nreadBuffers];
        
        // pre-allocate all ReadCache
        for (int i = 0; i < readBuffers.length; i++) {
        	readBuffers[i] = new ReadCache(null);
        }
        
       /*
         * Hot cache setup
         * 
         * Let's aim for a 1/10 of the readCache, but hotListSize must be at least 3 
         * to function
         */
        {
        	if (hotCacheSize < (readListSize * 0.8) && hotCacheSize > 2) {
        		hotListSize = hotCacheSize;
        	} else {
        		hotListSize = 0;
        	}
        }
        hotList = new LinkedBlockingDeque<ReadCache>();
        
        this.hotCacheThreshold = hotCacheThreshold;
        
        // pre-populate hotList and readList
        for (int i = 0; i < hotListSize; i++) {
        	hotList.add(readBuffers[i]);
        }

        for (int i = hotListSize; i < readListSize; i++) {
        	readList.add(readBuffers[i]);
        }
        
        // set initial read cache
        hotCache = hotList.poll();        
        hotReserve = hotList.poll();        
        readCache.set(readList.poll());
        {
        	final ReadCache curReadCache = readCache.get();
        	if (curReadCache != null) {
        		curReadCache.incrementReferenceCount();
        	}
        }
        
        if (log.isInfoEnabled())
            log.info("nbuffers=" + nwriteBuffers + ", dirtyListThreshold="
                    + m_dirtyListThreshold + ", compactionThreshold="
                    + compactionThreshold + ", compactionEnabled="
                    + compactionEnabled + ", prefixWrites=" + prefixWrites
                    + ", hotListSize=" + hotListSize
                    + ", useChecksum=" + useChecksum + ", quorum=" + quorum);

        // save the current file extent.
        this.fileExtent.set(fileExtent);

        // Add [current] WriteCache.
        current.set(writeBuffers[0] = newWriteCache(null/* buf */,
                useChecksum, false/* bufferHasData */, opener, fileExtent));
//        if (nbuffers > 1) {
//            readCache.set(buffers[1] = newWriteCache(null/* buf */,
//                useChecksum, false/* bufferHasData */, opener, fileExtent));
//            
//            buffers[1].incrementReferenceCount(); // for readCache
//            buffers[1].closeForWrites();
//        }
        
        // add remaining buffers.
        for (int i = 1; i < nwriteBuffers; i++) {

            final WriteCache tmp = newWriteCache(null/* buf */, useChecksum,
                    false/* bufferHasData */, opener, fileExtent);

            writeBuffers[i] = tmp;
            
             cleanList.add(tmp);

        }


        // Set the same counters object on each of the write cache instances.
        final WriteCacheServiceCounters counters = new WriteCacheServiceCounters(
        		nwriteBuffers, m_dirtyListThreshold, compactionThreshold);

        for (int i = 0; i < writeBuffers.length; i++) {
        
            writeBuffers[i].setCounters(counters);
            
        }
        
        this.counters = new AtomicReference<WriteCacheServiceCounters>(counters);

        // assume capacity is the same for each buffer instance.
        capacity = current.get().capacity();

        // set initial capacity based on an assumption of 1k buffers.
        serviceMap = new ConcurrentHashMap<Long, WriteCache>(nwriteBuffers
                * (capacity / 1024));

        /*
         * Memoizer used to install reads into the cache on a cache miss.
         */
        memo = new ReadMemoizer(loadChild);

        // start service to write on the backing channel.
        localWriteService = Executors
                .newSingleThreadExecutor(new DaemonThreadFactory(getClass()
                        .getName()));

        // run the write task
        localWriteFuture = localWriteService.submit(newWriteTask());
        
    }
    
    /**
     * Return <code>true</code> iff we are allowed to compact buffers. The
     * default implementation of the {@link WriteCache} is for a Worm and can
     * never compact.
     * <p>
     * Note: This method is package private for access by
     * {@link WriteCacheService}.
     */
    protected boolean canCompact() {

        return false;
        
    }
    
    /**
     * Called from {@link IBufferStrategy#commit()} and {@link #reset()} to
     * reset WriteCache sequence for HA synchronization. The return value winds
     * up propagated to the {@link IRootBlockView#getBlockSequence()} field in
     * the {@link IRootBlockView}s.
     * 
     * @return The value of the counter before this method was called.
     */
    public long resetSequence() {
     
        return cacheSequence.getAndSet(0L);
        
    }
    private final AtomicLong cacheSequence = new AtomicLong(0);

    /**
     * Return the then current write cache block sequence number.
     */
    public long getSequence() {

        return cacheSequence.get();
        
    }
    
    /**
     * Determines how long the dirty list should grow until the
     * {@link WriteCache} buffers are coalesced and/or written to disk.
     * <p>
     * Note: For the WORM there is no advantage to any buffering, but the
     * RWStore may recycle storage, so: 1) Writes can be avoided if delayed 2)
     * Buffers could potentially be compacted, further delaying writes.
     * <p>
     * Note: This MUST BE GTE ONE (1) since WriteTask.call() will otherwise drop
     * through without actually taking anything off of the dirtyList.
     */
    private final int m_dirtyListThreshold;
    
    /**
     * The readCache is managed separately from the writeCache.
     * <p>
     * If active then the readCache may optionally be managed together
     * with a hotList, to which frequently read buffers are transferred.
     * <p>
     * Data is added to the readCache:
     * <li>after an evicted WriteCache is written to disk/HA
     * <li>on a cache miss, disk reads are added to the cache
     * <p>
     * Data is added to the hotList when a readCache is evicted from the
     * readList.  resetWith uses the hitCount associated with live data
     * records to determine which data is transferred to the hotList.
     * <p>
     * When a readCache is evicted from the hotList, the entire cache
     * is moved to the readList.
     */
    private final int readListSize;
    /**
     * The readList - maximum of readListSize
     */
    final private BlockingQueue<ReadCache> readList;

    /**
     * Determines the size of the HIRS cache (will be zero if disabled)
     * <p>
     * Where HIRS captures High inter-reference vs Low inter-reference of
     * LIRS.
     * <p>
     * The HIRS cache is used in conjunction with the readCache which a naive
     * copying strategy would be a kind of LIRS cache.  Instead, cache hits
     * from "older" read cache records are copied to the HIRS cache which
     * should be recycled more slowly.
     * <p>
     * Once the HIRS cache is full (maximum number of buffers in use) then
     * then the per record hit count is used to determine which records are
     * transferred to be maintained.
     */
    private final int hotListSize;
    
     /**
     * The hotList - maximum of hirsSize - populated lazily from cleanList
     */
    final private BlockingQueue<ReadCache> hotList;

    /**
     * The current hotCache.
     * <p>
     * Note: Guarded by the {@link #readCache} reference.
     */
    private ReadCache hotCache = null;

    /**
     * Current hotCacheThreshold above which readCache records are 
     * transferred to the hotCache.
     */
	final private int hotCacheThreshold;

    /**
     * The current hotReserve.
     * <p>
     * Note: Guarded by the {@link #readCache} reference.
     */
    private ReadCache hotReserve = null;

//    /**
//     * Computes modular distance of a circular number list.
//     * 
//     * eg start: 1, end:5, mod: 20 = 5-1 = ((5+20)-1)%20 = 4
//     * or start:15, end:3, mod: 20 = (3+20)-15 = 8
//     * 
//     * Used to determine the position of a cache from front of
//     * the clean list
//     */
//    private int modDistance(final int start, final int end, final int mod) {
//     	return ((end + mod) - start) % mod;
//    }
        
    /**
     * When <code>true</code>, dirty buffers are immediately drained, compacted,
     * and then written out to the backing media and (in HA mode) to the
     * followers.
     */
    private volatile boolean flush = false;

    /**
     * When <code>true</code> any dirty buffers are written directly and never compacted.
     * This is only used in flush() when adding any compactingCache to the dirty list.
     */
    private volatile boolean directWrite = false;

    protected Callable<Void> newWriteTask() {

        return new WriteTask();

    }

    /**
     * The task responsible for writing dirty buffers onto the backing channel
     * and onto the downstream {@link Quorum} member if the service is highly
     * available.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    class WriteTask implements Callable<Void> {

        private ByteBuffer checksumBuffer;
        
        /**
         * Note: If there is an error in this thread then it needs to be
         * propagated to the threads write()ing on the cache or awaiting flush()
         * and from there back to the caller and an abort(). We do not need to
         * bother the readers since the read() methods all allow for concurrent
         * close() and will return null rather than bad data. The reprovisioning
         * of the write cache service (e.g., by reset()) must hold the writeLock
         * so as to occur when there are no outstanding reads executing against
         * the write cache service.
         * 
         * @todo If resynchronization rolls back the lastCommitTime for a store,
         *       then we need to interrupt or otherwise invalidate any readers
         *       with access to historical data which is no longer part of the
         *       quorum.
         */
        public Void call() throws Exception {
            try {
                if (quorum != null) {
                    // allocate heap byte buffer for whole buffer checksum.
                    checksumBuffer = ByteBuffer.allocate(writeBuffers[0].peek()
                            .capacity());
                } else {
                    checksumBuffer = null;
                }
                doRun();
                return null;
            } catch (InterruptedException t) {
                /*
                 * This task can only be interrupted by a thread with its
                 * Future (or by shutting down the thread pool on which it
                 * is running), so this interrupt is a clear signal that the
                 * write cache service is closing down.
                 */
                return null;
            } catch (Throwable t) {
                if (InnerCause.isInnerCause(t,
                        AsynchronousCloseException.class)) {
                    /*
                     * The service was shutdown. We do not want to log an
                     * error here since this is normal shutdown. close()
                     * will handle all of the Condition notifies.
                     */
                    return null;
                }
                /*
                 * Anything else is an error and halts processing. Error
                 * processing MUST a high-level abort() and MUST do a
                 * reset() if this WriteCacheService instance will be
                 * reused.
                 * 
                 * Note: If a WriteCache was taken from the dirtyList above
                 * then it will have been dropped. However, all of the
                 * WriteCache instances owned by the WriteCacheService are
                 * in [buffers] and reset() is written in terms of [buffers]
                 * precisely so we do not loose buffers here.
                 */
                if (firstCause.compareAndSet(null/* expect */, t/* update */)) {
                    halt = true;
                }
                /*
                 * Signal anyone blocked on the dirtyList or cleanList
                 * Conditions. They need to notice the change in [halt] and
                 * wrap and rethrow [firstCause].
                 */
                dirtyListLock.lock();
                try {
                    dirtyListEmpty.signalAll();
                    dirtyListChange.signalAll();
                } finally {
                    dirtyListLock.unlock();
                }
                cleanListLock.lock();
                try {
                    cleanListNotEmpty.signalAll();
                } finally {
                    cleanListLock.unlock();
                }
                log.error(t, t);
                /*
                 * Halt processing. The WriteTask must be restarted by
                 * reset.
                 */
                return null;
            } finally {
                /*
                 * Clear compactingCache reference now that the WriteTask is
                 * known to be terminated.
                 */
                compactingCacheRef.set(null); // clear reference.
                checksumBuffer = null;
            }
        } // call()

        private void doRun() throws Exception {

            while (true) {

            	/*
            	 * Replace assert !halt; since it is set in WriteCacheService.close()
            	 */
                if (halt) {
                    throw new RuntimeException(firstCause.get());
                }

                // Await dirty cache buffer.
                final WriteCache cache = awaitDirtyBuffer();

                boolean didCompact = false;
                boolean didWrite = false;

                /*
                 * Note: When using a large number of write cache buffers and a
                 * bulk data load, it is not uncommon for all records to be
                 * recycled by the time we take something from the dirtyList, in
                 * which case the cache will be (logically) empty.
                 * 
                 * Note: This test (WriteCache.isEmpty()) is not decisive
                 * because we are not holding any locks across it and the
                 * subsequent actions. Therefore, it is possible that the cache
                 * will become empty after it has been tested through concurrent
                 * clearWrite() invocations. That should not be a problem. We
                 * want to leave the cache open (versus closing it against
                 * writes) in case we decide to compact the cache rather than
                 * evicting it. The cache MUST NOT be closed for writes when we
                 * compact it or we will lose the ability to clear recycled
                 * records out of that WriteCache.
                 */

                final boolean wasEmpty = cache.isEmpty();

                if (!wasEmpty) {

                    final int percentEmpty = cache.potentialCompaction();

                    if (compactionEnabled && !directWrite 
                            && percentEmpty >= compactionThreshold) {

                        if (log.isDebugEnabled())
                            log.debug("percentEmpty=" + percentEmpty + "%");

                        // Attempt to compact cache block.
                        if (compactCache(cache)) {

                            // [cache] is clean and empty.
                            assert cache.isEmpty();

                        } else {

                            // Write cache block if did not compact.
                            writeCacheBlock(cache);
                            
                            didWrite = true;
                            
                        }
                        
                        didCompact = true;

                    } else {

                        // Write cache block.
                        writeCacheBlock(cache);

                        didWrite = true;

                    }

                }

                // Now written/compacted, remove from dirtyList.
                if (dirtyList.take() != cache)
                    throw new AssertionError();
                counters.get().ndirty--;

                dirtyListLock.lockInterruptibly();
                try {
                    if (dirtyList.isEmpty()) {
                        /*
                         * Signal Condition when we release the
                         * dirtyListLock.
                         */
                        dirtyListEmpty.signalAll();
                    }
                } finally {
                    dirtyListLock.unlock();
                }

                addClean(cache, false/* addFirst */);

                if (!wasEmpty && log.isInfoEnabled()) {
                    final WriteCacheServiceCounters tmp = counters.get();
                    final long nhit = tmp.nhit.get();
                    final long ntests = nhit + tmp.nmiss.get();
                    final int hitRate = (int) (100 * ((ntests == 0L ? 0d
                            : (double) nhit / ntests)));
                    final WriteCacheServiceCounters c = counters.get();
                    log.info("WriteCacheService: bufferCapacity="
                            + writeBuffers[0].capacity() + ",nbuffers="
                            + tmp.nbuffers + ",nclean=" + tmp.nclean
                            + ",ndirty=" + tmp.ndirty + ",maxDirty="
                            + tmp.maxdirty + ",hitRate=" + hitRate + ",empty="
                            + wasEmpty + ",didCompact=" + didCompact
                            + ",didWrite=" + didWrite + ",ncompact="
                            + c.ncompact + ",nbufferEvictedToChannel="
                            + c.nbufferEvictedToChannel);
                }

            } // while(true)
            
        } // doRun()
        
        /**
         * We choose here whether to compact the cache.
         * 
         * 1) Reserve extra clean buffer, if none available do NOT attempt
         * compaction 2) Compact to "current" compacting buffer avoiding
         * contention with writing threads 3) If required replace current
         * compacting buffer with reserved, adding compacting buffer to dirty
         * list 4) Release compacted
         * 
         * @return <code>true</code> iff we compacted the cache.
         * 
         * @throws InterruptedException
         */
        private boolean compactCache(final WriteCache cache)
                throws InterruptedException, Exception {

            /*
             * The cache should not be closed against writes. If it were closed
             * for writes, then we would no longer be able to capture cleared
             * writes in the RecordMap. However, if we compact the cache, we
             * want any cleared writes to be propagated into the compacted
             * cache.
             */
            assert !cache.isClosedForWrites();

            if (compactingReserveRef.get() == null) {

                final WriteCache tmp = getDirectCleanCache();

                if (tmp == null)
                    return false; // cannot guarantee compaction

                tmp.resetWith(serviceMap); // should be NOP!

                compactingReserveRef.set(tmp);

            }
                
            /*
             * We can be certain to be able to compact.
             */
            
            /*
             * Grab the [compactingCache] (if any).
             */
            WriteCache curCompactingCache = null;
            dirtyListLock.lockInterruptibly();
            try {
                // Might be null.
                curCompactingCache = compactingCacheRef.getAndSet(null);
//            } finally {
//                dirtyListLock.unlock();
//            }
//            try {
                boolean done = false;
                if (curCompactingCache != null) {
                    if (log.isTraceEnabled())
                        log.trace("Transferring to curCompactingCache");
                    
                    done = WriteCache.transferTo(cache/* src */,
                            curCompactingCache/* dst */, serviceMap, 0/*threshold*/);
                    if (done) {
                        // Everything was compacted.  Send just the address metadata (empty cache block).
                        sendAddressMetadata(cache);
                        
                        if (log.isDebugEnabled())
                            log.debug("RETURNING RESERVE: curCompactingCache.bytesWritten="
                                    + curCompactingCache.bytesWritten());

                        return true;
                    }
                    /*
                     * The [curCompactingCache] is full.
                     */
                    if (flush) {
                        /*
                         * Send out the full cache block.
                         */
                        writeCacheBlock(curCompactingCache);
                        addClean(curCompactingCache, true/* addFirst */);
                        if (log.isTraceEnabled())
                            log.trace("Flushed curCompactingCache");
                    } else {
                        /*
                         * Add current compacting cache to dirty list.
                         */
                        dirtyList.add(curCompactingCache);
                        if (log.isTraceEnabled())
                            log.trace("Added curCompactingCache to dirtyList");
                    }
                    // fall through. fill in the reserve cache next.
                    curCompactingCache = null;
                }

                /*
                 * Clear the state on the reserve buffer and remove from
                 * cacheService map.
                 */
                if (log.isTraceEnabled())
                    log.trace("Setting curCompactingCache to reserve");

                curCompactingCache = compactingReserveRef.getAndSet(null);
                {
                    final WriteCache tmp = getDirectCleanCache();
                    if (tmp != null) {
                        tmp.resetWith(serviceMap); // should be NOP!
                        compactingReserveRef.set(tmp);
                    }
                }
                
                if (log.isTraceEnabled())
                    log.trace("Transferring to curCompactingCache");
                done = WriteCache.transferTo(cache/* src */,
                        curCompactingCache/* dst */, serviceMap, 0/*threshold*/);

                if (!done) {
                    throw new AssertionError(
                            "We must be able to compact the cache");
                }
                if (log.isDebugEnabled())
                    log.debug("USING RESERVE: curCompactingCache.bytesWritten="
                            + curCompactingCache.bytesWritten());
                sendAddressMetadata(cache);
                // Buffer was compacted.
                return true;
            } finally {
//                dirtyListLock.lock();
                try {
                    // Now reset compactingCache with dirtyListLock held
                    compactingCacheRef.set(curCompactingCache);
                    counters.get().ncompact++;
                } finally {
                    dirtyListLock.unlock();
                }
            }

        } // compactCache()

        /**
         * In HA, we need to notify a downstream RWS of the addresses that have
         * been allocated on the leader in the same order in which the leader
         * made those allocations. This information is used to infer the order
         * in which the allocators for the different allocation slot sizes are
         * created. This method will synchronously send those address notices and
         * and also makes sure that the followers see the recycled addresses
         * records so they can keep both their allocators and the actual
         * allocations synchronized with the leader.
         * 
         * @param cache
         *            A {@link WriteCache} whose data has been transfered into
         *            another {@link WriteCache} through a "compact" operation.
         * 
         * @throws IllegalStateException
         * @throws InterruptedException
         * @throws ExecutionException
         * @throws IOException
         * 
         * @see <a href="http://trac.blazegraph.com/ticket/721"> HA1 </a>
         */
        private void sendAddressMetadata(final WriteCache cache)
                throws IllegalStateException, InterruptedException,
                ExecutionException, IOException {

            if (quorum == null) { //|| !quorum.isHighlyAvailable()
//                    || !quorum.getClient().isLeader(quorumToken)) {
                return;
            }

            if (cache.prepareAddressMetadataForHA()) {

                writeCacheBlock(cache);

            }

        }

         /**
         * Get a dirty cache buffer. Unless we are flushing out the buffered
         * writes, we will allow the dirtyList to grow to the desired threshold
         * before we attempt to compact anything.
         * <p>
         * Note: This DOES NOT remove the {@link WriteCache} from the
         * {@link #dirtyList}. It uses a peek(). The {@link WriteCache} will
         * remain on the {@link #dirtyList} until it has been handled by
         * {@link #doRun()}.
         * 
         * @return A dirty {@link WriteCache}.
         */
        private WriteCache awaitDirtyBuffer() throws InterruptedException {

            dirtyListLock.lockInterruptibly();
            try {
                assert m_dirtyListThreshold >= 1
                        && m_dirtyListThreshold <= writeBuffers.length : "dirtyListThreshold="
                        + m_dirtyListThreshold
                        + ", #buffers="
                        + writeBuffers.length;
                /*
                 * Wait for a dirty buffer.
                 * 
                 * Note: [flush] and [m_dirtyListThreshold] can change
                 * during this loop!
                 */
                while (true) {
                    if (!flush) {
                        // Let dirtyList grow up to threshold.
                        if (dirtyList.size() < m_dirtyListThreshold
                                && !halt) {
                            dirtyListChange.await();
                        } else
                            break;
                    } else {
                        // We need to flush things out.
                        if (dirtyList.isEmpty() && !halt) {
                            dirtyListChange.await();
                        } else
                            break;
                    }
                }
                if (halt)
                    throw new RuntimeException(firstCause.get());

                // update counters.
                final WriteCacheServiceCounters c = counters.get();
                c.ndirty = dirtyList.size();
                if (c.maxdirty < c.ndirty)
                    c.maxdirty = c.ndirty;

                // Guaranteed available.
                final WriteCache cache = dirtyList.peek();
                if (cache == null)
                    throw new AssertionError();
                
                // System.err.println(cache.toString());

                return cache;

            } finally {
        
                dirtyListLock.unlock();
                
            }

        }

        /**
         * Write the {@link WriteCache} onto the disk and the HA pipeline.
         * 
         * @param cache
         *            The {@link WriteCache}.
         * 
         * @throws InterruptedException
         * @throws ExecutionException
         * @throws IOException
         */
        private void writeCacheBlock(final WriteCache cache)
                throws InterruptedException, ExecutionException, IOException {

            /**
             * IFF HA and this is the quorum leader.
             * 
             * Note: This is true for HA1 as well. The code path enabled by this
             * is responsible for writing the HALog files.
             * 
             * @see <a href="http://trac.blazegraph.com/ticket/721"> HA1 </a>
             */
            final boolean isHALeader = quorum != null
                    && quorum.getClient().isLeader(quorumToken);

            /*
             * Ensure nothing will modify this buffer before written to disk or
             * HA pipeline.
             * 
             * Note: Do NOT increment the cacheSequence here. We need to decide
             * whether or not the buffer is empty first, and it needs to be
             * closed for writes before we can make that decision.
             */

            // Must be closed for writes.
            cache.closeForWrites();
            
            /*
             * Test for an empty cache.
             * 
             * Note: We can not do this until the cache has been closed for
             * writes.
             */
            {
                final ByteBuffer b = cache.peek();
                if (b.position() == 0) {
                    // Empty cache.
                    return;
                }
            }

            // Increment WriteCache sequence.
            final long thisSequence = cacheSequence.getAndIncrement();
//            cache.setSequence(thisSequence);

            // Set the current file extent on the WriteCache.
            cache.setFileExtent(fileExtent.get());

            if (isHALeader) {//quorum != null && quorum.isHighlyAvailable()) {

                // Verify quorum still valid and we are the leader.
                quorum.assertLeader(quorumToken);

                /*
                 * Replicate from the leader to the first follower. Each
                 * non-final follower will receiveAndReplicate the write cache
                 * buffer. The last follower will receive the buffer.
                 */

                // send to 1st follower.
                @SuppressWarnings("unchecked")
                final QuorumPipeline<HAPipelineGlue> quorumMember = (QuorumPipeline<HAPipelineGlue>) quorum
                        .getMember();

                assert quorumMember != null : "Not quorum member?";

                final WriteCache.HAPackage pkg = cache.newHAPackage(//
                        quorumMember.getStoreUUID(),//
                        quorumToken,//
                        quorumMember.getLastCommitCounter(),//
                        quorumMember.getLastCommitTime(),//
                        thisSequence,//
                        replicationFactor,//
                        checksumBuffer
                        );

                assert pkg.getData().remaining() > 0 : "Empty cache: " + cache;

                /*
                 * Start the remote asynchronous IO before the local synchronous
                 * IO.
                 * 
                 * Note: In HA with replicationFactor=1, this should still
                 * attempt to replicate the write cache block in case there is
                 * someone else in the write pipeline (for example, off-site
                 * replication).
                 */
                /*
                 * FIXME There may be a problem with doing the async IO first.
                 * Track this down and document the nature of the problem,
                 * then clean up the documentation here (see the commented
                 * out version of this line below).
                 */
                quorumMember.logWriteCacheBlock(pkg.getMessage(), pkg.getData().duplicate());

                /*
                 * TODO Do we want to always support the replication code path
                 * when a quorum exists (that is, also for HA1) in case there
                 * are pipeline listeners that are not HAJournalServer
                 * instances? E.g., for offsite replication?
                 */
                if (quorum.replicationFactor() > 1) {

                    // ASYNC MSG RMI + NIO XFER.
                    remoteWriteFuture = quorumMember.replicate(null/* req */,
                            pkg.getMessage(), pkg.getData().duplicate());

                    counters.get().nsend++;

                }

                /*
                 * The quorum leader logs the write cache block here. For the
                 * followers, the write cache blocks are currently logged by
                 * HAJournalServer.
                 */
//                quorumMember.logWriteCacheBlock(msg, b.duplicate());

            }

            /*
             * Do the local IOs (concurrent w/ remote replication).
             * 
             * Note: This will not throw out an InterruptedException unless this
             * thread is actually interrupted. The local storage managers all
             * trap asynchronous close exceptions arising from the interrupt of
             * a concurrent IO operation and retry until they succeed.
             */
            {

                if (log.isDebugEnabled())
                    log.debug("Writing to file: " + cache.toString());

                final long begin = System.nanoTime();
                final long nrecs = cache.recordMap.size(); // #of records in the write cache block.

                try {
                
                    // Flush WriteCache buffer to channel (write on disk)
                    cache.flush(false/* force */);
                    
                } finally {

                    // See BLZG-1589 (new latency-oriented counters)
                    final long elapsed = System.nanoTime() - begin;
                    
                    final WriteCacheServiceCounters c = counters.get();
                    
                    c.nbufferEvictedToChannel++;
                    c.nrecordsEvictedToChannel += nrecs;
                    c.elapsedBufferEvictedToChannelNanos += elapsed;
    
                }
                
            }

            /*
             * Wait for the downstream IOs to finish.
             * 
             * Note: Only the leader is doing replication of the WriteCache
             * blocks from this thread and only the leader will have a non-null
             * value for the [remoteWriteFuture]. The followers are replicating
             * to the downstream nodes in QuorumPipelineImpl. Since the WCS
             * absorbs a lot of latency, replication from QuorumPipelineImpl
             * should be fine.
             */
            if (remoteWriteFuture != null) {

                // Wait for the downstream IOs to finish.
                remoteWriteFuture.get();
                
            }

        } // writeCacheBlock()

    } // class WriteTask

    /**
     * Factory for {@link WriteCache} implementations.
     * 
     * @param buf
     *            The backing buffer (optional).
     * @param useChecksum
     *            <code>true</code> iff record level checksums are enabled.
     * @param bufferHasData
     *            <code>true</code> iff the buffer has data to be written onto
     *            the local persistence store (from a replicated write).
     * @param opener
     *            The object which knows how to re-open the backing channel
     *            (required).
     * @param fileExtent
     *            The then current extent of the backing file.
     * 
     * @return A {@link WriteCache} wrapping that buffer and able to write on
     *         that channel.
     * 
     * @throws InterruptedException
     */
    abstract public WriteCache newWriteCache(IBufferAccess buf,
            boolean useChecksum, boolean bufferHasData,
            IReopenChannel<? extends Channel> opener, final long fileExtent)
            throws InterruptedException;

    /**
     * {@inheritDoc}
     * <p>
     * All dirty buffers are reset and transferred to the head of the clean
     * list. The buffers on the clean list are NOT reset since they may contain
     * valid cached reads (data which is known to be on the disk). We do not
     * want to discard the read cache on reset().
     * <p>
     * Note: This approach deliberately does not cause any buffers belonging to
     * the caller of {@link #writeChk(long, ByteBuffer, int)} to become part of
     * the {@link #cleanList}.
     * <p>
     * Note: <strong>You MUST set the {@link #setExtent(long) file extent}
     * </strong> after {@link #reset() resetting} the {@link WriteCacheService}.
     * This is necessary in order to ensure that the correct file extent is
     * communicated along the write replication pipeline when high availability
     * is enabled.
     * <p>
     * Note: {@link #reset()} MUST NOT interrupt readers. It should only reset
     * those aspects of the write cache state that are associated with writes.
     * On the other hand, {@link #close()} must close all buffers and must not
     * permit readers to read from closed buffers.
     */
    public void reset() throws InterruptedException {
        final WriteLock writeLock = lock.writeLock();
        writeLock.lockInterruptibly();
        try {
            if (!open.get()) {
                // Reset can not recover from close().
                throw new IllegalStateException(firstCause.get());
            }

            /*
             * Note: The WriteTask must use lockInterruptably() so it will
             * notice when it is interrupted by cancel().
             */

            // cancel the current WriteTask.
            localWriteFuture.cancel(true/* mayInterruptIfRunning */);
            final Future<?> rwf = remoteWriteFuture;
            if (rwf != null) {
                // Note: Cancel of remote Future is RMI!
                try {
                    rwf.cancel(true/* mayInterruptIfRunning */);
                } catch (Throwable t) {
                    log.warn(t, t);
                }
            }

            /*
             * Drain and reset the dirty cache buffers, dropping them onto the
             * cleanList.
             */
            drainAndResetDirtyList();

            /*
             * Now that we have sent all the signal()s we know how to send, go
             * ahead and wait for the WriteTask to notice and terminate.
             */
            try {
                // wait for it
                localWriteFuture.get();
            } catch (Throwable t) {
                // ignored.
            } finally {

                /*
                 * Once more, drain and reset the dirty cache buffers, dropping
                 * them onto the cleanList.
                 * 
                 * Note: This is intended to handle the case where there might
                 * be concurrency in WriteTask.call() such that we did not get
                 * all of the dirty buffers the first time we called this method
                 * above.
                 * 
                 * Note: This will ignore the [compactingReserve]. That
                 * WriteCache is always clean and can stay where it is.
                 */
                drainAndResetDirtyList();

                /*
                 * Verify some post-conditions once the WriteTask is terminated.
                 */
 
                dirtyListLock.lockInterruptibly();
                try {
                    if (!dirtyList.isEmpty())
                        throw new AssertionError();
                } finally {
                    dirtyListLock.unlock();
                }
                if (compactingCacheRef.get() != null)
                    throw new AssertionError();

                // ensure cleanList is not empty after WriteTask terminates, handling single buffer case
                cleanListLock.lockInterruptibly();
                try {
                    if (writeBuffers.length > 1 && cleanList.isEmpty())
                        throw new AssertionError();
                } finally {
                    cleanListLock.unlock();
                }

            }

            /*
             * Note: DO NOT clear the service record map. This still has valid
             * cache entries (the read cache).
             */
//            // clear the service record map.
//            recordMap.clear();
//
//            // reset each buffer.
//            for (WriteCache t : buffers) {
//                t.reset();
//            }

            /*
             * Make sure the [current] is reset and non-null.
             */
            {

                final WriteCache x = current.get();

                if (x != null) {

                    // reset if found.
                    x.resetWith(serviceMap);

                    // addClean(x, true/* addFirst */);

                } else {

                    // Non-blocking take.
                    final WriteCache t = cleanList.poll();

                    if (t == null)
                        throw new AssertionError();

                    if (!current.compareAndSet(null/* expect */, t/* update */)) {

                        // Concurrently set.
                        throw new AssertionError();

                    }

                }

            }
            
//            // set readCache
//            if (buffers.length > 1) {
//                readCache.set(buffers[1]);
//                buffers[1].closeForWrites();
//            }
//
//            // re-populate the clean list with remaining buffers
//            for (int i = 2; i < buffers.length; i++) {
//                cleanList.put(buffers[i]);
//            }

            // reset the counters.
            {
                final WriteCacheServiceCounters c = counters.get();
                c.ndirty = 0;
                c.nclean = writeBuffers.length-1;
                c.nreset++;
            }
            
            // reset cacheSequence for HA
            resetSequence();

            /*
             * Restart the WriteTask
             * 
             * Note: don't do Future#get() for the remote Future. The task was
             * cancelled above and we don't want to wait on RMI (for the remote
             * Future). The remote service will have to handle any problems on
             * its end when resynchronizing if it was disconnected and did not
             * see our cancel() message.
             */
            // if (rwf != null) {
            // try {
            // rwf.get();
            // } catch (Throwable t) {
            // // ignored.
            // }
            // }
            this.localWriteFuture = localWriteService.submit(newWriteTask());
            this.remoteWriteFuture = null;

            // clear the file extent to an illegal value.
            fileExtent.set(-1L);

            counters.get().nreset++;

            flush = false;
            
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Drain the dirty list; reset each dirty cache buffer, and then add the
     * reset buffers to the front of the cleanList (since they are known to be
     * empty).
     * 
     * @throws InterruptedException
     */
    private void drainAndResetDirtyList() throws InterruptedException {

        final List<WriteCache> c = new LinkedList<WriteCache>();

        // drain the dirty list.
        dirtyListLock.lockInterruptibly();
        try {
            dirtyList.drainTo(c);
            dirtyListEmpty.signalAll();
            dirtyListChange.signalAll(); // NB: you must verify
                                         // Condition once signaled!
        } finally {
            dirtyListLock.unlock();
        }
        
        // Reset dirty cache buffers and add to cleanList.
        cleanListLock.lockInterruptibly();
        try {
            for (WriteCache x : c) {
                x.resetWith(serviceMap);
                 cleanList.addFirst(x);
            }
            
            assert !cleanList.isEmpty();
            
            cleanListNotEmpty.signalAll();
            counters.get().nclean = cleanList.size();
        } finally {
            cleanListLock.unlock();
        }

    }
    
    public void close() { //throws InterruptedException {

        if (!open.compareAndSet(true/* expect */, false/* update */)) {
            // Already closed, so this is a NOP.
            return;
        }

        /*
         * Set [firstCause] and [halt] to ensure that other threads report
         * errors.
         * 
         * Note: If the firstCause has not yet been set, then we set it now to a
         * stack trace which will indicate that the WriteCacheService was
         * asynchronously closed (that is, it was closed by another thread).
         */
        if (firstCause.compareAndSet(null/* expect */,
                new AsynchronousCloseException()/* update */)) {
            halt = true;
        }
        
        // Interrupt the write task.
        localWriteFuture.cancel(true/* mayInterruptIfRunning */);
        final Future<?> rwf = remoteWriteFuture;
        if (rwf != null) {
            // Note: Cancel of remote Future is RMI!
            try {
                rwf.cancel(true/* mayInterruptIfRunning */);
            } catch (Throwable t) {
                log.warn(t, t);
            }
        }

        // Immediate shutdown of the write service.
        localWriteService.shutdownNow();

//          // Immediate shutdown of the remote write service (if running).
//          if (remoteWriteService != null) {
//              remoteWriteService.shutdownNow();
//          }

        boolean interrupted = false;

        // Note: Possible code to ensure Futures are terminated....
//        // Wait for the Futures.
//        try {
//            localWriteFuture.get();
//        } catch (Throwable t) {
//            if (InnerCause.isInnerCause(t, InterruptedException.class)) {
//                interrupted = true;
//            }
//        }
//        if (rwf != null) {
//            try {
//                rwf.get();
//            } catch (Throwable t) {
//                if (InnerCause.isInnerCause(t, InterruptedException.class)) {
//                    interrupted = true;
//                }
//            }
//        }
        
        /*
         * Ensure that the WriteCache buffers are close()d in a timely
         * manner.
         */

        // reset buffers on the dirtyList.
        dirtyListLock.lock/*Interruptibly*/();
        try {
            dirtyList.drainTo(new LinkedList<WriteCache>());
            dirtyListEmpty.signalAll();
            dirtyListChange.signalAll();
        } finally {
            dirtyListLock.unlock();
        }

        // close() buffers on the cleanList.
        cleanListLock.lock/*Interruptibly*/();
        try {
            cleanList.drainTo(new LinkedList<WriteCache>());
        } finally {
            cleanListLock.unlock();
        }

        /*
         * Note: The lock protects the [current] reference.
         */
        final WriteLock writeLock = lock.writeLock();
        writeLock.lock/*Interruptibly*/();
        try {

            // close all buffers.
            for (WriteCache t : writeBuffers) {
                try {
                    t.close();
                } catch (InterruptedException ex) {
                    interrupted = true;
                    continue;
                }
            }

            // and any ReadCache buffers
            for (ReadCache t : readBuffers) {
                try {
                    t.close();
                } catch (InterruptedException ex) {
                    interrupted = true;
                    continue;
                }
            }
            
            // clear reference to the current buffer.
            current.getAndSet(null);

            // clear reference to the compactingCache buffer.
            compactingCacheRef.getAndSet(null);

            // clear reference to the readCache buffer.
            readCache.getAndSet(null);
            synchronized (readCache) {
                hotCache = null;
                hotReserve = null;
            }

            // clear the service record map.
            serviceMap.clear();

            // clear the file extent to an illegal value.
            fileExtent.set(-1L);

            if(interrupted)
                Thread.currentThread().interrupt();

        } finally {
            writeLock.unlock();
        }
    
        if (log.isInfoEnabled())
            log.info(counters.get().toString());

    }

    /**
     * Ensures that {@link #close()} is eventually invoked so the buffers can be
     * returned to the {@link DirectBufferPool}.
     * 
     * @throws Throwable
     */
    protected void finalized() throws Throwable {

        close();

    }

    /**
     * This method is called ONLY by write threads and verifies that the service
     * is {@link #open}, that the {@link WriteTask} has not been
     * {@link #halt halted}, and that the {@link WriteTask} is still
     * executing (in case any uncaught errors are thrown out of
     * {@link WriteTask#call()}.
     * <p>
     * Note: {@link #read(long)} DOES NOT throw an exception if the service is
     * closed, asynchronously closed, or even just plain dead. It just returns
     * <code>null</code> to indicate that the desired record is not available
     * from the cache.
     * 
     * @throws IllegalStateException
     *             if the service is closed.
     * @throws RuntimeException
     *             if the {@link WriteTask} has failed.
     */
    private void assertOpenForWriter() {

        if (!open.get())
            throw new IllegalStateException(firstCause.get());

        if (halt)
            throw new RuntimeException(firstCause.get());

        if (localWriteFuture.isDone()) {

            /*
             * If the write task terminates abnormally then throw the exception
             * out here.
             */

            try {
                // @todo don't do get() all the time...?
                localWriteFuture.get();

            } catch (Throwable t) {

                throw new RuntimeException(t);

            }

        }

    }

    /**
     * Return the current buffer to a write thread. Once they are done, the
     * caller MUST call {@link #release()}.
     * 
     * @return The buffer.
     * 
     * @throws InterruptedException
     * @throws IllegalStateException
     *             if the {@link WriteCacheService} is closed.
     * @throws RuntimeException
     *             if the service has been {@link #halt halted}
     */
    private WriteCache acquireForWriter() throws InterruptedException, IllegalStateException {

        final ReadLock readLock = lock.readLock();

        readLock.lockInterruptibly();

        try {

            /*
             * We only want to throw errors from the WriteTask out of write()
             * and flush(). However, this method is NOT invoked by read() which
             * uses a different non-blocking protocol to access the record if it
             * is in a cache buffer.
             */
            assertOpenForWriter();

            /*
             * Note: acquire() does not block since it holds the ReadLock.
             * Methods which change [current] MUST hold the WriteLock across
             * that operation to ensure that [current] is always non-null since
             * acquire() will not block once it acquires the ReadLock.
             */
            final WriteCache tmp = current.get();

            if (tmp == null) {

                throw new RuntimeException();

            }

            // Note: The ReadLock is still held!
            return tmp;

        } catch (Throwable t) {

            /*
             * Note: release the lock only on the error path.
             */

            readLock.unlock();

            if (t instanceof InterruptedException)
                throw (InterruptedException) t;

            if (t instanceof IllegalStateException)
                throw (IllegalStateException) t;

            throw new RuntimeException(t);

        }

    }

    /**
     * Release the latch on an acquired buffer.
     */
    private void release() {

        /*
         * Note: This is releasing the ReadLock which was left open by
         * acquire().
         */
        lock.readLock().unlock();

    }

    /**
     * Flush the current write set through to the backing channel.
     * 
     * @throws InterruptedException
     */
    public void flush(final boolean force) throws InterruptedException {

        try {

            if (!flush(force, Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {

                throw new RuntimeException();

            }

        } catch (TimeoutException e) {

            throw new RuntimeException(e);

        }

    }

    /**
     * {@inheritDoc}
     * <p>
     * flush() is a blocking method. At most one flush() operation may run at a
     * time. The {@link #current} buffer is moved to the {@link #dirtyList}
     * while holding the {@link WriteLock} and flush() then waits until the
     * dirtyList becomes empty, at which point all dirty records have been
     * written through to the backing file.
     * <p>
     * Note: Any exception thrown from this method MUST trigger error handling
     * resulting in a high-level abort() and {@link #reset()} of the
     * {@link WriteCacheService}.
     * 
     * TODO flush() is currently designed to block concurrent writes() in
     * order to give us clean decision boundaries for the HA write pipeline and
     * also to simplify the internal locking design. Once we get HA worked out
     * cleanly we should explore whether or not we can relax this constraint
     * such that writes can run concurrently with flush(). That would have
     * somewhat higher throughput since mutable B+Tree evictions would no longer
     * cause concurrent tasks to block during the commit protocol or the file
     * extent protocol. [Perhaps by associating each write set with a distinct
     * sequence counter (that is incremented by both commit and abort)?]
     * 
     * TODO Flush should order ALL {@link WriteCache}'s on the dirtyList by
     * their fileOffset and then evict them in that order. This reordering will
     * maximize the opportunity for locality during the IOs. With a large write
     * cache (multiple GBs) this reordering could substantially reduce the
     * IOWait associated with flush() for a large update. Note: The reordering
     * should only be performed by the leader in HA mode - the followers will
     * receive the {@link WriteCache} blocks in the desired order and can just
     * drop them onto the dirtyList.
     * 
     * @see WriteTask
     * @see #dirtyList
     * @see #dirtyListEmpty
     */
    public boolean flush(final boolean force, final long timeout,
            final TimeUnit units) throws TimeoutException, InterruptedException {

        if (haLog.isInfoEnabled()) {
            /*
             * Note: This is an important event for HA. The write cache is
             * flushed to ensure that the entire write set is replicated on the
             * followers. Once that has been done, HA will do a 2-phase commit
             * to verify that there is a quorum that agrees to write the root
             * block. Writing the root block is the only thing that the nodes in
             * the quorum need to do once the write cache has been flushed.
             */
            haLog.info("Flushing the write cache: seq=" + cacheSequence);
        }

        final long begin = System.nanoTime();
        final long nanos = units.toNanos(timeout);
        long remaining = nanos;

        final WriteLock writeLock = lock.writeLock();
        if (!writeLock.tryLock(remaining, TimeUnit.NANOSECONDS))
            throw new TimeoutException();
        try {
            final WriteCache tmp = current.getAndSet(null);
//            if (tmp.remaining() == 0) {
//                /*
//                 * Handle an empty buffer by waiting until the dirtyList is
//                 * empty.
//                 */
//                // remaining := (total - elapsed).
//                remaining = nanos - (System.nanoTime() - begin);
//                if (!dirtyListLock.tryLock(remaining, TimeUnit.NANOSECONDS))
//                    throw new TimeoutException();
//                try {
//                    while (!dirtyList.isEmpty() && !halt) {
//                        // remaining := (total - elapsed).
//                        remaining = nanos - (System.nanoTime() - begin);
//                        if (!dirtyListEmpty.await(remaining,
//                                TimeUnit.NANOSECONDS)) {
//                            throw new TimeoutException();
//                        }
//                    }
//                    if (halt)
//                        throw new RuntimeException(firstCause.get());
//                } finally {
//                    dirtyListLock.unlock();
//                }
//                return true;
//            }
//            /*
//             * Otherwise, the current buffer is non-empty.
//             */
            // remaining := (total - elapsed).
            remaining = nanos - (System.nanoTime() - begin);
            if (!dirtyListLock.tryLock(remaining, TimeUnit.NANOSECONDS))
                throw new TimeoutException();
            
            try {
                /*
                 * Force WriteTask.call() to evict anything in the cache.
                 * 
                 * Note: We need to wait until the dirtyList has been evicted
                 * before writing out the compacting cache (if any) and then
                 * finally drop the compactingCache onto the cleanList. Or have
                 * a 2-stage flush.
                 */
                flush = true;
                
                /*
                 * Wait until the dirtyList has been emptied.
                 * 
                 * Note: [tmp] may be empty, but there is basically zero cost in
                 * WriteTask to process an empty buffer and, done this way, the
                 * code is much less complex here.
                 */
                dirtyList.add(tmp);
                counters.get().ndirty++;
                dirtyListChange.signalAll();
                while (!dirtyList.isEmpty() && !halt) {
                    // remaining := (total - elapsed).
                    remaining = nanos - (System.nanoTime() - begin);
                    if (!dirtyListEmpty.await(remaining, TimeUnit.NANOSECONDS)) {
                        throw new TimeoutException();
                    }
                }
                /*
                 * Add the [compactingCache] (if any) to dirty list and spin it
                 * down again.
                 * 
                 * Note: We can not drop the compactingCache onto the dirtyList
                 * until the dirtyList has been spun down to empty.
                 * 
                 * Note: We have introduced the directWrite state variable to indicate
                 * that the compactingCache must not be compacted or it may not be
                 * written.
                 */
                final WriteCache tmp2 = compactingCacheRef.getAndSet(null/* newValue */);
                if (tmp2 != null) {
                	directWrite = true;
                	try {
	                    if (log.isInfoEnabled()) {
	                        log.info("Adding compacting cache");
	                    }
	                    dirtyList.add(tmp2);
	                    counters.get().ndirty++;
	                    dirtyListChange.signalAll();
	                    while (!dirtyList.isEmpty() && !halt) {
	                        // remaining := (total - elapsed).
	                        remaining = nanos - (System.nanoTime() - begin);
	                        if (!dirtyListEmpty.await(remaining, TimeUnit.NANOSECONDS)) {
	                            throw new TimeoutException();
	                        }
	                    }
                	} finally {
                		directWrite = false;
                	}
                }
                if (halt)
                    throw new RuntimeException(firstCause.get());
            } finally {
                flush = false;
                try {
                    if(!halt) {
                        /*
                         * Check assertions for clean WCS after flush().
                         * 
                         * Note: Can not check assertion if there is an existing
                         * exception.
                         */
                        assert dirtyList.size() == 0;
                        assert compactingCacheRef.get() == null;
                        assert current.get() == null;
                    }
                } finally {
                    dirtyListLock.unlock();
                }
            }
            /*
             * Replace [current] with a clean cache buffer.
             */
            // remaining := (total - elapsed).
            remaining = nanos - (System.nanoTime() - begin);
            if (!cleanListLock.tryLock(remaining, TimeUnit.NANOSECONDS))
                throw new TimeoutException();
            try {
                // Note: use of Condition let's us notice [halt].
                while (cleanList.isEmpty() && !halt) {
                    // remaining := (total - elapsed).
                    remaining = nanos - (System.nanoTime() - begin);
                    if (!cleanListNotEmpty.await(remaining, TimeUnit.NANOSECONDS)) {
                        throw new TimeoutException();
                    }
                    if (halt)
                        throw new RuntimeException(firstCause.get());
                }
                // Guaranteed available hence non-blocking.
                final WriteCache nxt = cleanList.take();
                counters.get().nclean--;
                
                // Note: should already be pristine
                nxt.resetWith(serviceMap);//, fileExtent.get());
                current.set(nxt);
                if (haLog.isInfoEnabled())
                    haLog.info("Flushed the write cache: seq=" + cacheSequence);
                return true;
            } finally {
                cleanListLock.unlock();
            }
        } finally {
            writeLock.unlock();
        }
    }
    
    /**
     * Set the extent of the file on the current {@link WriteCache}. The then
     * current value of the extent will be communicated together with the rest
     * of the {@link WriteCache} state if it is written onto another service
     * using the write replication pipeline (HA only). The receiver will use the
     * value read from the {@link WriteCache} message to adjust the extent of
     * its backing file.
     * <p>
     * Note: Changes in the file extent for persistence store implementations
     * MUST (a) be mutually exclusive with reads and writes on the backing file
     * (due to a JVM bug); and (b) force the file data and the file metadata to
     * the disk. Thus any change in the {@link #fileExtent} MUST be followed by
     * a {@link #flush(boolean, long, TimeUnit)}.
     * <p>
     * Note: You MUST set the file extent each time you invoke {@link #reset()}
     * so the {@link WriteCacheService} is always aware of the correct file
     * extent.
     * 
     * @throws InterruptedException
     * @throws IllegalStateException
     */
    public void setExtent(final long fileExtent) throws IllegalStateException,
            InterruptedException {

        if (fileExtent < 0L)
            throw new IllegalArgumentException();

//        final WriteCache cache = acquireForWriter();
//
//        try {
            if (log.isDebugEnabled())
                log.debug("Set fileExtent: " + fileExtent);

            // make a note of the current file extent.
            this.fileExtent.set(fileExtent);

//            // set the current file extent on the WriteCache.
//            cache.setFileExtent(fileExtent);
//
//        } finally {
//
//            release();
//
//        }

    }

    @Override
    public boolean write(final long offset, final ByteBuffer data, final int chk)
            throws InterruptedException, IllegalStateException {
     
        return write(offset, data, chk, useChecksum, 0/* latchedAddr */);
        
    }
    
    /**
     * Write the record onto the cache. If the record is too large for the cache
     * buffers, then it is written synchronously onto the backing channel.
     * Otherwise it is written onto a cache buffer which is lazily flushed onto
     * the backing channel. Cache buffers are written in order once they are
     * full. This method does not impose synchronization on writes which fit the
     * capacity of a cache buffer.
     * <p>
     * When integrating with the {@link RWStrategy} or the {@link WORMStrategy}
     * there needs to be a read/write lock such that file extension is mutually
     * exclusive with file read/write operations (due to a Sun bug). The caller
     * can override {@link #newWriteCache(ByteBuffer, IReopenChannel)} to
     * acquire the necessary lock (the read lock of a {@link ReadWriteLock}).
     * This is even true when the record is too large for the cache since we
     * delegate the write to a temporary {@link WriteCache} wrapping the
     * caller's buffer.
     * <p>
     * Note: Any exception thrown from this method MUST trigger error handling
     * resulting in a high-level abort() and {@link #reset()} of the
     * {@link WriteCacheService}.
     * 
     * @param latchedAddr The latched address (RWStore only).
     * 
     * @return <code>true</code> since the record is always accepted by the
     *         {@link WriteCacheService} (unless an exception is thrown).
     * 
     * @see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6371642
     * 
     * @todo The WORM serializes invocations on this method because it must put
     *       each record at a specific offset into the user extent of the file.
     *       However, the RW store does not do this. Therefore, for the RW store
     *       only, we could use a queue with lost cost access and scan for best
     *       fit packing into the write cache buffer. When a new buffer is set
     *       as [current], we could pack the larger records in the queue onto
     *       that buffer first. This might provide better throughput for the RW
     *       store but would require an override of this method specific to that
     *       implementation.
     *       
     * See BLZG-1589 (new latency-oriented counters)
     */
    public boolean write(final long offset, final ByteBuffer data, final int chk, final boolean useChecksum,final int latchedAddr)
            throws InterruptedException, IllegalStateException {

        final long begin = System.nanoTime();
        
        try {

            return write_timed(offset, data, chk, useChecksum, latchedAddr);
            
        } finally {
            
            final long elapsed = System.nanoTime() - begin;
            
            final WriteCacheServiceCounters c = counters.get();
            
            c.ncacheWrites++; // maintain nwrites
            c.elapsedCacheWriteNanos += elapsed;
            
        }
        
    }
    
    private boolean write_timed(final long offset, final ByteBuffer data, final int chk, final boolean useChecksum,final int latchedAddr)
                throws InterruptedException, IllegalStateException {

      	if (log.isTraceEnabled()) {
            log.trace("offset: " + offset + ", length: " + data.limit()
                    + ", chk=" + chk + ", useChecksum=" + useChecksum);
        }
        
        if (!open.get())
            throw new IllegalStateException(firstCause.get());

        if (offset < 0)
            throw new IllegalArgumentException();

        if (data == null)
            throw new IllegalArgumentException(
                    AbstractBufferStrategy.ERR_BUFFER_NULL);

        // #of bytes in the record.
        final int remaining = data.remaining();

        // #of bytes to be written.
        final int nwrite = remaining + (useChecksum ? 4 : 0);

        if (remaining == 0)
            throw new IllegalArgumentException(
                    AbstractBufferStrategy.ERR_BUFFER_EMPTY);

        if (nwrite > capacity) {

            /*
             * Handle large records.
             */
            return writeLargeRecord(offset, data, chk, useChecksum);

        }

        /*
         * The record can fit into a cache instance, so try and acquire one and
         * write the record onto it.
         * 
         * @todo this could be refactored to use moveBufferToDirtyList()
         */
        {

            final WriteCache cache = acquireForWriter();

            try {
                debugAddrs(offset, data.remaining(), 'A');

                // write on the cache.
                if (cache.write(offset, data, chk, useChecksum, latchedAddr)) {
                	
                 	final WriteCache old = serviceMap.put(offset, cache);
                    // There should be no duplicate address in the record
                    //  map since these entries should be removed, although
                    //  write data may still exist in an old WriteCache.
                    // A duplicate may also be indicative of an allocation
                    //  error, which we need to be pretty strict about!
                    if (old == cache) {
                        throw new AssertionError("Record already in cache: offset=" + offset + " " + addrDebugInfo(offset));
                    }

                    return true;

                }

            } finally {

                release();

            }

        }

        /*
         * The record did not fit into the current buffer but it is small enough
         * to fit into an empty buffer. Grab the write lock and then try again.
         * If it still does not fit, then put the current buffer onto the dirty
         * list and take a buffer from the clean list and then write the record
         * onto that buffer while we are holding the lock. This last step must
         * succeed since the buffer will be empty and the record can fit into an
         * empty buffer.
         */
        {

            final Lock writeLock = lock.writeLock();

            writeLock.lockInterruptibly();

            try {

                /*
                 * While holding the write lock, see if the record can fit into
                 * the current buffer. Note that the buffer we acquire here MAY
                 * be a different buffer since a concurrent write could have
                 * already switched us to a new buffer. In that case, the record
                 * might fit into the new buffer.
                 */

                // Acquire a buffer. Maybe the same one, maybe different.
                WriteCache cache = acquireForWriter();

                try {

                    // While holding the write lock, see if the record fits.
                    if (cache.write(offset, data, chk, useChecksum, latchedAddr)) {

                        /*
                         * It fits: someone already changed to a new cache,
                         * which is fine.
                         */
                        if (serviceMap.put(offset, cache) != null) {
                            // The record should not already be in the cache.
                            throw new AssertionError("Record already in cache: offset=" + offset + " "  + addrDebugInfo(offset));
                        }
                        
                        return true;

                    }

                    /*
                     * There is not enough room in the current buffer for this
                     * record, so put the buffer onto the dirty list. Then take
                     * a new buffer from the clean list (block), reset the
                     * buffer to clear the old writes, and set it as current. At
                     * that point, the record should always fit.
                     * 
                     * Note: When we take a cache instances from the cleanList
                     * we need to remove any entries in our recordMap which are
                     * in its record map.
                     * 
                     * Note: We move the current buffer to the dirty list before
                     * we take a buffer from the clean list. This is absolutely
                     * necessary since the code will otherwise deadlock if there
                     * is only one buffer.
                     * 
                     * Note: Do NOT yield the WriteLock here. That would make it
                     * possible for another thread to acquire() the current
                     * buffer, which has already been placed onto the dirtyList
                     * by this thread!!!
                     */

                    /*
                     * Move the current buffer to the dirty list.
                     * 
                     * Note: The lock here is not required to give flush() atomic
                     * semantics with regard to the set of dirty write buffers
                     * when flush() gained the writeLock [in fact, we only need
                     * the dirtyListLock for the dirtyListEmpty Condition].
                     */
                    if (!current
                            .compareAndSet(cache/* expect */, null/* update */)) {
                        throw new AssertionError();
                    }
                    dirtyListLock.lockInterruptibly();
                    try {
                        dirtyList.add(cache);
                        dirtyListChange.signalAll();
                    } finally {
                        dirtyListLock.unlock();
                    }

                    /*
                     * Take the buffer from the cleanList and set it has the
                     * [current] buffer.
                     */
                    
                    // Grab buffer from clean list.
                    final WriteCache newBuffer = takeFromClean();
                    
                    counters.get().nclean--;
                    // Clear the state on the new buffer and remove from
                    // cacheService map
                    newBuffer.resetWith(serviceMap);//, fileExtent.get());

                    // Set it as the new buffer.
                    current.set(cache = newBuffer);

                    // Try to write on the new buffer.
                    if (cache.write(offset, data, chk, useChecksum, latchedAddr)) {

                        // This must be the only occurrence of this record.
                        if (serviceMap.put(offset, cache) != null) {
                            throw new AssertionError("Record already in cache: offset=" + offset + " " + addrDebugInfo(offset));
                        }

                        
                        return true;

                    }

                    /*
                     * Should never happen.
                     */
                    throw new AssertionError("Unable to write into current WriteCache " + offset + " " + addrDebugInfo(offset));

                } finally {

                    release();

                }

            } finally {

                writeLock.unlock();

            }

        }

    }
    
	private WriteCache takeFromClean() throws InterruptedException {
		cleanListLock.lockInterruptibly();

		try {

			while (true) {

				if (log.isInfoEnabled() && cleanList.isEmpty())
					log.info("Waiting for clean buffer");

				/*
				 * Note: We use the [cleanListNotEmpty] Condition so we can
				 * notice a [halt].
				 */
				while (cleanList.isEmpty() && !halt) {
					cleanListNotEmpty.await();
				}

				if (halt)
					throw new RuntimeException(firstCause.get());

				// Poll() rather than take() since other methods poll() the list
				// unprotected.
				final WriteCache ret = cleanList.poll();

				if (ret != null) {
					return ret;
				}

			}

		} finally {
			cleanListLock.unlock();
		}
	}
    

//    /**
//     * Caches data read from disk (or even read from "older" cache).
//     * The assumption is that we do not need a "reserve" buffer.
//     * 
//     * @param addr
//     * @param bb
//     * @throws InterruptedException
//     */
//    public void cache(final long addr, final ByteBuffer bb)
//			throws InterruptedException {
//		// I think this is fine!
//		synchronized (readCache) {
//			final WriteCache cache = readCache.get();
//			if (cache != null && !cache.cache(addr, bb)) {
//				// add existing non-null cache to clean list
//				if (cache != null)
//					addClean(cache, false /* add first */);
//
//				// fetch new readCache from clean list
//				final WriteCache ncache = getDirectCleanCache();
//
//				// should not be null
//				assert ncache != null;
//
//				// if we decide it CAN be null then we simply do not cache the
//				// read
//				if (ncache == null)
//					return;
//
//				// remove any global references to existing data
//				ncache.resetWith(recordMap);
//				// only closed caches can cache reads
//				ncache.closeForWrites();
//
//				readCache.set(ncache);
//				ncache.closeForWrites();
//				ncache.cache(addr, bb);
//
//				if (recordMap.put(addr, ncache) != null) {
//					throw new AssertionError("Record already in cache: offset="
//							+ addr + " " + addrDebugInfo(addr));
//				}
//			} else if (cache != null) {
//				if (recordMap.put(addr, cache) != null) {
//					throw new AssertionError("Record already in cache: offset="
//							+ addr + " " + addrDebugInfo(addr));
//				}
//			}
//
//			// we've written the byte buffer, so flip it!
//			bb.flip();
//		}
//	}
    
    public void debugAddrs(long offset, int length, char c) {
        if (addrsUsed != null) {
            addrsUsed[addrsUsedCurs] = offset;
            addrActions[addrsUsedCurs] = c;
            addrLens[addrsUsedCurs] = length;
            
            addrsUsedCurs++;
            if (addrsUsedCurs >= addrsUsed.length) {
                addrsUsedCurs = 0;
            }
        }
    }

    /**
     * Write a record whose size (when combined with the optional checksum) is
     * larger than the capacity of an individual {@link WriteCache} buffer. This
     * operation is synchronous (to protect the ByteBuffer from concurrent
     * modification by the caller). It will block until the record has been
     * written.
     * <p>
     * This implementation will write the record onto a sequence of
     * {@link WriteCache} objects and wait until all of those objects have been
     * written through to the backing file and the optional HA write pipeline. A
     * checksum will be appended after the last chunk of the record. This
     * strategy works for the WORM since the bytes will be laid out in a
     * contiguous region on the disk.
     * <p>
     * Note: For the WORM, this code MUST NOT allow the writes to proceed out of
     * order or the data will not be laid out correctly on the disk !!!
     * <p>
     * Note: The RW store MUST NOT permit individual allocations whose size on
     * the disk is greater than the capacity of an individual {@link WriteCache}
     * buffer (@todo Or is this Ok? Perhaps it is if the RW store holds a lock
     * across the write for a large record? Maybe if we also add a low-level
     * method for inserting an entry into the record map?)
     * <p>
     * Note: This method DOES NOT register the record with the shared
     * {@link #serviceMap}. Since the record spans multiple {@link WriteCache}
     * objects it can not be directly recovered without reading it from the
     * backing file.
     * 
     * <h2>Dialog on large records</h2>
     * 
     * It seems to me that the RW store is designed to break up large records
     * into multiple allocations. If we constrain the size of the largest
     * allocation slot on the RW store to be the capacity of a WriteCache buffer
     * (including the bytes for the checksum and other record level metadata)
     * then we do not have a problem with breaking up large records for it in
     * the WriteCacheService and it will automatically benefit from HA using the
     * write replication logic.
     * <p>
     * The WORM does not have these limits on the allocation size, so it seems
     * likely that breaking it up across multiple WriteCache buffer instances
     * would have to be done inside of the WriteCacheService in order to prevent
     * checksums from being interleaved with each WriteCache worth of data it
     * emits for a large record. We can't raise this out of the
     * WriteCacheService because the large record would not be replicated for
     * HA.
     */
    protected boolean writeLargeRecord(final long offset, final ByteBuffer data, final int chk, final boolean useChecksum)
            throws InterruptedException, IllegalStateException {

        if (log.isTraceEnabled()) {
            log.trace("offset: " + offset + ", length: " + data.limit() + ", chk=" + chk + ", useChecksum="
                    + useChecksum);
        }

        if (offset < 0)
            throw new IllegalArgumentException();

        if (data == null)
            throw new IllegalArgumentException(AbstractBufferStrategy.ERR_BUFFER_NULL);

        // #of bytes in the record.
        final int remaining = data.remaining();

        if (remaining == 0)
            throw new IllegalArgumentException(AbstractBufferStrategy.ERR_BUFFER_EMPTY);

        // Small records should not take this code path.
        if (remaining < capacity)
            throw new AssertionError();

        /*
         * Put as much into each WriteCache instance as well fit, then transfer
         * the WriteCache onto the dirtyList, take a new WriteCache from the
         * cleanList, and continue until all data as been transferred. If
         * checksums are enabled, add a 4 byte checksum afterwards.
         * 
         * Note: We hold the WriteLock across this operation since we will be
         * changing out [current] each time it fills up. This has the
         * side-effect of guaranteeing that the writes are emitted without
         * intervening writes of other record.
         * 
         * while(r > 0) {
         * 
         * cache = acquire();
         * 
         * copy up to [r] bytes into the buffer.
         * 
         * if the buffer is full, then transfer it to the dirty list.
         * 
         * release()
         * 
         * }
         * 
         * write checksum on buffer
         */

        final Lock writeLock = lock.writeLock();
        writeLock.lockInterruptibly();
        try {
            // the offset of the next byte to transfer to a cache buffer.
            int p = 0;
            // #of bytes remaining in the large record (w/o the checksum).
            int r = remaining;
            while (r > 0) {
                // Acquire a buffer.
                final WriteCache cache = acquireForWriter();
                try {
                    // #of bytes to copy onto the write cache.
                    final int ncpy = Math.min(r, cache.remaining());
                    if (ncpy > 0) {
                        // create view of the data to be copied.
                        final ByteBuffer tmp = data.duplicate();
                        tmp.limit(p + ncpy);
                        tmp.position(p);
                        // Note: For WORM, this MUST NOT add the checksum except
                        // for the last chunk!
                        if (!cache.write(offset + p, tmp, chk, false/* writeChecksum */,0/*latchedAddr*/))
                            throw new AssertionError();
                        r -= ncpy;
                        p += ncpy;
                    }
                    if (cache.remaining() == 0) {
                        moveBufferToDirtyList();
                    }
                } finally {
                    release();
                }
            } // while( remaining > 0 )
            /*
             * Now we need to write out the optional checksum. We do not have to
             * flush this write through. The buffer can remain partly full.
             */
            if (useChecksum) {
                // Acquire a buffer.
                final WriteCache cache = acquireForWriter();
                try {
                    // Allocate a small buffer
                    final ByteBuffer t = ByteBuffer.allocate(4);
                    // Add in the record checksum.
                    t.putInt(chk);
                    // Prepare for reading.
                    t.flip();
                    // Note: [t] _is_ the checksum.
                    if (!cache.write(offset + p, t, chk, false/* writeChecksum */,0/*latchedAddr*/))
                        throw new AssertionError();
                } finally {
                    release();
                }
            }
            /*
             * If the current cache buffer is dirty then we need to move it to
             * the dirty list since the caller MUST be able to read the record
             * back from the file by the time this method returns.
             */
            final WriteCache cache = acquireForWriter();
            try {
                if (!cache.isEmpty()) {
                    moveBufferToDirtyList();
                }
            } finally {
                release();
            }
            /*
             * In order to guarantee that the caller can read the record back
             * from the file we now flush the dirty list to the backing store.
             * When this method returns, the record will be on the disk and can
             * be read back safely from the disk.
             */
            if (log.isTraceEnabled())
                log.trace("FLUSHING LARGE RECORD");
            
            flush(false/* force */);
            // done.
            return true;
        } finally {
            writeLock.unlock();
        }

    }

    /**
     * Move the {@link #current} buffer to the dirty list and await a clean
     * buffer. The clean buffer is set as the {@link #current} buffer and
     * returned to the caller.
     * <p>
     * Note: If there is buffer available on the {@link #cleanList} then this
     * method can return immediately. Otherwise, this method will block until a
     * clean buffer becomes available.
     * 
     * @return A clean buffer.
     * 
     * @throws InterruptedException
     * @throws IllegalMonitorStateException
     *             unless the current thread is holding the {@link WriteLock}
     *             for {@link #lock}.
     */
    private WriteCache moveBufferToDirtyList() throws InterruptedException {

        if (!lock.isWriteLockedByCurrentThread())
            throw new IllegalMonitorStateException();

        final WriteCache cache = current.getAndSet(null);
        assert cache != null;
        
        /*
         * Note: The lock here is required to give flush() atomic semantics with
         * regard to the set of dirty write buffers when flush() gained the
         * writeLock [in fact, we only need the dirtyListLock for the
         * dirtyListEmpty Condition].
         */
        dirtyListLock.lockInterruptibly();
        try {
            dirtyList.add(cache);
            dirtyListChange.signalAll();
        } finally {
            dirtyListLock.unlock();
        }

        /*
         * Take the buffer from the cleanList and set it as the [current]
         * buffer.
         * 
         * Note: We use the [cleanListNotEmpty] Condition so we can notice a
         * [halt].
         */
        cleanListLock.lockInterruptibly();

        try {

            while (cleanList.isEmpty() && !halt) {
                cleanListNotEmpty.await();
            }

            if (halt)
                throw new RuntimeException(firstCause.get());

            // Take a buffer from the cleanList (guaranteed avail).
            final WriteCache newBuffer = cleanList.take();

            counters.get().nclean--;
            
            // Clear state on new buffer and remove from cacheService map
            newBuffer.resetWith(serviceMap);//, fileExtent.get());

            // Set it as the new buffer.
            current.set(newBuffer);

            return newBuffer;

        } finally {

            cleanListLock.unlock();

        }

    }
   
    /**
     * Add to the cleanList.
     * <p>
     * Since moving to an explicit readCache, we now call resetWith before
     * adding the the cleanList.  Potentially removing latency on acquiring
     * a new cache from the clean list.
     * <p>
     * If a readCache is in operation then we will transfer to the read cache
     */
    private void addClean(final WriteCache cache, final boolean addFirst)
            throws InterruptedException {
        if (cache == null)
            throw new IllegalArgumentException();
        
        
        if (this.readListSize > 0) { // if there is a readCache
        	installReads(cache);
        } else {
        	cache.resetWith(serviceMap);
        }
        
        cleanListLock.lockInterruptibly();
        try {
            assert cache.isEmpty() || cache.isClosedForWrites();
            if (addFirst) {
                cleanList.addFirst(cache);
            } else  {
                cleanList.addLast(cache);
                
            }
            cleanListNotEmpty.signalAll();
            counters.get().nclean = cleanList.size();
        } finally {
            cleanListLock.unlock();
        }
    }
    
    public boolean installReads(final WriteCache cache) throws InterruptedException {
    	if (readListSize == 0)
    		return false;
    	
    	synchronized (readCache) {
    		final ReadCache rcache = readCache.get();
    		if (!WriteCache.transferTo(cache, rcache, serviceMap, 0)) {
    			// full readCache
    			readCache.set(null);
    			if (rcache.decrementReferenceCount()==0) {
    				readList.add(rcache);
    			}
    			
    			final ReadCache ncache = getDirectReadCache();
    			if (ncache == null) {
    				throw new AssertionError();
    			}
    			
    			// remaining must be >= to announced capacity after getDirectReadCache
    			if (ncache.remaining() < ncache.capacity())
    				throw new AssertionError("New Cache, remaining() < capacity(): " + ncache.remaining() + " < " + ncache.capacity());
    			
    			// Now transfer remaining to new readCache
    			if (!WriteCache.transferTo(cache, ncache, serviceMap, 0)) {
    				throw new AssertionError("Unable to complete transfer to new cache with remaining: " + ncache.remaining());
    			}
    			
    			ncache.incrementReferenceCount();
    			readCache.set(ncache);
    		}
    	}

    	return true;
    }

    /**
     * Pool the {@link #cleanList} and return the {@link WriteCache} from the
     * head of the {@link #cleanList} IFF one is available and otherwise
     * <code>null</code>.
     * 
     * @return The {@link WriteCache} iff one was available.
     * 
     * @throws InterruptedException
     */
    private WriteCache getDirectCleanCache() throws InterruptedException {

        final WriteCache tmp = cleanList.poll();

        if (tmp != null) {
        
            counters.get().nclean--;
            
        }

        return tmp;

    }

    /**
     * Non-blocking take of a {@link ReadCache}. If successful, the returned
     * {@link ReadCache} will be clean. Otherwise return <code>null</code>.
     */
    private ReadCache getDirectReadCache() throws InterruptedException {

        // Non-blocking take.
        ReadCache tmp = readList.poll();

        if (tmp == null)
            return null;

        try {

            /*
             * Attempt to reset the record.
             */
            synchronized (readCache) {
                if (hotCache == null) {
                    tmp.resetWith(serviceMap);
                    return tmp;
                }
                int cycles = 0;
                while (tmp != null) {
                    if (log.isDebugEnabled() && !tmp.isEmpty()) {
                        /*
                         * Just debug stuff.
                         */
                        int hitRecords = 0;
                        int hotRecords = 0;
                        int totalRecords = 0;
                        final Iterator<RecordMetadata> values = tmp.recordMap
                                .values().iterator();
                        while (values.hasNext()) {
                            final RecordMetadata md = values.next();
                            totalRecords++;
                            if (md.getHitCount() > 0) {
                                hitRecords++;
                                if (md.getHitCount() > hotCacheThreshold)
                                    hotRecords++;
                            }
                        }
                        log.debug("Recycled ReadCache, hot(>" + hotCacheThreshold + "): " + hotRecords + ", hit: " + hitRecords + " of " + totalRecords);
                    }

                    if (WriteCache.transferTo(tmp, hotCache, serviceMap,
                            hotCacheThreshold)) {
                        if (!tmp.isEmpty())
                            throw new AssertionError();

                        tmp.reset();
                        break;
                    }

                    if (log.isDebugEnabled())
                        log.debug("Cycle HOTCACHE: " + ++cycles);

                    // transfer not completed, so:
                    // move current hotCache to end of HotList
                    // move head of HotList to end of ReadList
                    // make hotReserve new hotCache
                    // complete transfer to new hotCache
                    // make now empty tmp new hotReserve
                    hotList.add(hotCache);
                    readList.add(hotList.poll().resetHitCounts());
                    if (!hotReserve.isEmpty())
                        throw new AssertionError();

                    hotCache = hotReserve;
                    hotReserve = null;
                    if (!WriteCache.transferTo(tmp, hotCache, serviceMap,
                            hotCacheThreshold)) {
                        throw new AssertionError();
                    }
                    tmp.reset();
                    hotReserve = tmp;

                    tmp = readList.poll();
                } // while (tmp != null)
            } // synchronized(readCache)
        } catch (InterruptedException ex) {
            /*
             * If interrupted, then return the ReadCache to the list and
             * propagate the interrupt to the caller. This makes the operation
             * safe with respect to an interrupt. Either the operation succeeds
             * fully, or we return [null] to the caller and propagate restore
             * the interrupt status on the current Thread.
             */
            readList.put(tmp);
            // Propagate the interrupt status.
            Thread.currentThread().interrupt();
            // ReadCache is not available.
            return null;
        }

        return tmp;

    }

    /**
     * This is a non-blocking query of all write cache buffers (current, clean
     * and dirty).
     * <p>
     * This implementation DOES NOT throw an {@link IllegalStateException} if
     * the service is already closed NOR if there is an asynchronous close of
     * the service. Instead it just returns <code>null</code> to indicate a
     * cache miss.
     */
    public ByteBuffer read(final long offset, final int nbytes)
            throws InterruptedException, ChecksumError {

        // Check the cache.
        final ByteBuffer tmp = _readFromCache(offset, nbytes);

        if (tmp != null) {
        	
        	if (tmp.remaining() == 0)
        		throw new AssertionError();

            // Cache hit.
            return tmp;

        }

        // Cache miss.
        counters.get().nmiss.increment();
        
        if (reader != null) {
            
            /*
             * Read through to the disk and install the record into cache.
             */
            final ByteBuffer ret = loadRecord(offset, nbytes);
            
            if (ret != null && ret.remaining() == 0)
            	throw new AssertionError();
            
            return ret;

        } else {
            
            /*
             * No reader. Return null. Caller is responsible for reading through
             * to the disk.
             */
            
            return null;
            
        }

    }

    /**
     * Attempt to read record from cache (either write cache or read cache
     * depending on the service map state).
     */
    private ByteBuffer _readFromCache(final long offset, final int nbytes)
            throws ChecksumError, InterruptedException {
    
        if (nbytes > capacity) {
            /*
             * Note: Writes larger than a single write cache buffer are NOT
             * cached.
             */
            return null;
        }

        final Long off = Long.valueOf(offset);

        while (true) {

            if (!open.get()) {

                /*
                 * Not open. Return [null] rather than throwing an exception per
                 * the contract for this implementation.
                 */

                return null;

            }

            final WriteCache cache = serviceMap.get(off);

            if (cache == null) {
             
                // Cache miss.
                break;
                
            }

            /*
             * Ask the cache buffer if it has the record still. It will not
             * if the cache buffer has been concurrently reset.
             */
            try {

                final ByteBuffer ret = cache.read(off.longValue(), nbytes);

                if (ret == null && serviceMap.get(off) == cache) {

                    throw new IllegalStateException(
                            "Inconsistent cache for offset: " + off);
                    
                }

                if (ret == null && log.isDebugEnabled()) {

                    log.debug("WriteCache out of sync with WriteCacheService");

                }

                if (ret != null)
                    return ret;

                // May have been transferred to another Cache!
                //
                // Fall through.
                continue;
                
            } catch (IllegalStateException ex) {
                /*
                 * The write cache was closed. Per the API for this method,
                 * return [null] so that the caller will read through to the
                 * backing store.
                 */
                assert !open.get();
                return null;

            }

        }
        
        // Cache miss.
        return null;

    }
    
    /**
     * Helper class models a request to load a record from the backing store.
     * <p>
     * Note: This class must implement equals() and hashCode() since it is used
     * within the {@link Memoizer} pattern.
     */
    private static class LoadRecordRequest {

        final WriteCacheService service;
        final long offset;
        final int nbytes;

        public LoadRecordRequest(final WriteCacheService service,
                final long offset, final int nbytes) {

            this.service = service;

            this.offset = offset;

            this.nbytes = nbytes;

        }

        /**
         * Equals returns true iff the request has the same parameters.
         */
        public boolean equals(final Object o) {

            if (!(o instanceof LoadRecordRequest))
                return false;

            final LoadRecordRequest r = (LoadRecordRequest) o;

            return service == r.service && offset == r.offset
                    && nbytes == r.nbytes;

        }

        /**
         * The hashCode() implementation assumes that the <code>offset</code>'s
         * hashCode() is well distributed.
         */
        public int hashCode() {
            
            return (int) (offset ^ (offset >>> 32));
            
        }
        
    }

    /**
     * Helper loads a child node from the specified address by delegating
     * {@link WriteCacheService#_getRecord(long, int)}.
     */
    final private static Computable<LoadRecordRequest, ByteBuffer> loadChild = new Computable<LoadRecordRequest, ByteBuffer>() {

        /**
         * Loads a record from the specified address.
         * 
         * @return A heap {@link ByteBuffer} containing the data for that
         *         record.
         * 
         * @throws IllegalArgumentException
         *             if addr is {@link IRawStore#NULL}.
         */
        public ByteBuffer compute(final LoadRecordRequest req)
                throws InterruptedException {

			try {

				final ByteBuffer ret = req.service._getRecord(req.offset, req.nbytes);
				
				if (ret != null && ret.remaining() == 0)
					throw new AssertionError();
				
				return ret;

			} finally {

				/*
				 * Clear the future task from the memoizer cache.
				 * 
				 * Note: This is necessary in order to prevent the cache from
				 * retaining a hard reference to each child materialized for the
				 * B+Tree.
				 * 
				 * Note: This does not depend on any additional synchronization.
				 * The Memoizer pattern guarantees that only one thread actually
				 * call ft.run() and hence runs this code.
				 */

				req.service.memo.removeFromCache(req);

			}

		}
        
    };

    /**
     * A {@link Memoizer} subclass which exposes an additional method to remove
     * a {@link FutureTask} from the internal cache.
     */
    private static class ReadMemoizer extends
            Memoizer<LoadRecordRequest/* request */, ByteBuffer/* child */> {

        /**
         * @param c
         */
        public ReadMemoizer(final Computable<LoadRecordRequest, ByteBuffer> c) {

            super(c);

        }

        /**
         * The approximate size of the cache (used solely for debugging to
         * detect cache leaks).
         */
        int size() {
            
            return cache.size();
            
        }

        /**
         * Called by the thread which atomically installs the record into the
         * cache and updates the service record map. At that point the record is
         * available from the service record map.
         * 
         * @param req
         *            The request.
         */
        void removeFromCache(final LoadRecordRequest req) {

            if (cache.remove(req) == null) {

                throw new AssertionError();
                
            }

        }

//        /**
//         * Called from {@link AbstractBTree#close()}.
//         * 
//         * @todo should we do this?  There should not be any reads against the
//         * the B+Tree when it is close()d.  Therefore I do not believe there 
//         * is any reason to clear the FutureTask cache.
//         */
//        void clear() {
//            
//            cache.clear();
//            
//        }
        
    };

    /**
     * Used to materialize records with at most one thread reading the
     * record from disk for a given address. Other threads desiring the
     * same record will wait on the {@link Future} for the thread doing
     * the work.
     */
    private final ReadMemoizer memo;

    /**
     * Enter the memoizer pattern.
     */
    private ByteBuffer loadRecord(final long offset, final int nbytes) {
        
        try {

            counters.get().memoCacheSize.set(memo.size());

            final ByteBuffer ret = memo.compute(new LoadRecordRequest(this, offset, nbytes));
            
            // Duplicate buffer since memoizer may return same ByteBuffer to multiple callers
            //	resulting in problems of concurrent read
            return ret.duplicate();

        } catch (InterruptedException e) {

            /*
             * Note: This exception will be thrown iff interrupted while
             * awaiting the FutureTask inside of the Memoizer.
             */

            throw new RuntimeException(e);

        }
        
    }

    /**
     * Method invoked from within the memoizer pattern to read the record from
     * the backing store and install it into the cache. The method must first
     * verify that the record is not in the cache.
     * 
     * @param offset
     * @param nbytes
     * @return A heap byte buffer containing the read record.
     * @throws IllegalStateException
     * @throws InterruptedException
     */
    private ByteBuffer _getRecord(final long offset, final int nbytes)
            throws IllegalStateException, InterruptedException {

		/*
		 * On entry, this thread will either install the read into the cache or
		 * the record will already be in the cache. We are protected by the
		 * memoizer pattern here. No other thread will be attempting to install
		 * the same record (the record for that offset) into the cache.
		 */

		ByteBuffer tmp = _readFromCache(offset, nbytes);

		if (tmp != null) {

		    // Already in the read cache.
			if (tmp.remaining() == 0)
				throw new AssertionError();
			
			return tmp;

		}
		
		final boolean largeRecord = nbytes > capacity;
		final boolean directRead = largeRecord || this.readListSize == 0;

		if (directRead) {
        
            // No free buffer to install the read (OR largeRecord)
            final ByteBuffer ret = _readFromLocalDiskIntoNewHeapByteBuffer(offset, nbytes);
            
            if (ret != null && ret.remaining() == 0)
            	throw new AssertionError();
      
            return ret;
        }

        /*
         * The reader threads co-operatively manage the readCache on behalf of
         * the WCS. The allocation attempt for a cache buffer is serialized and
         * when an allocation fails a new readCache is initialized and the
         * previous cache reference is decremented (no longer referenced as the
         * current read cache).
         * 
         * When a cache is selected to buffer a read, the reference is
         * incremented while the read is active.
         * 
         * When the reference is finally decremented to zero (either at the end
         * of a read or after a failed allocation) the cache can be returned to
         * the clean list.
         */

        // The cache block into which we will install the record.
		ReadCache theCache = null;
		// The buffer slice into which we will install the record.
		ByteBuffer bb = null;
        /*
         * Set true iff we will install a record and have incremented the
         * reference count for the cache. if true, then this Thread MUST
         * decrement the reference count by any code path that leaves this
         * method. (If a obtain an allocation but do not set this flag, then we
         * will not actually perform the installation and the cache block will
         * not be pinned.)
         */
        boolean willInstall = false;
        try {
            synchronized (readCache) {
                theCache = readCache.get();
                if (theCache != null) {
                    /*
                     * Attempt to allocate record on current read cache.
                     */
                    assert theCache.getReferenceCount() > 0;
                    bb = theCache.allocate(nbytes); // intr iff can't lock().
                    if (bb != null) {
                        // increment while readCache synchronized
                        theCache.incrementReferenceCount();
                        willInstall = true;
                    } else {
                        /*
                         * At this point, the current [readCache] does not have
                         * enough room to install the record. We will clear the
                         * [readCache] reference and transfer it to the
                         * [readList].
                         * 
                         * *** CRITICAL SECTION ***
                         * 
                         * We MUST transfer cache once reference is cleared or
                         * the buffer will be lost!
                         * 
                         * Note: Anything on the readList MUST have
                         * referenceCount==0 since we do not transfer to the
                         * readList until that condition is met.
                         */
                        readCache.set(null);
                        if (theCache.decrementReferenceCount() == 0) {
                            readList.add(theCache);
                        }
                    }
                }
                if (bb == null) {
                    /*
                     * Either no [readCache] on entry or no room in current
                     * [readCache] and [readCache] was set to [null].
                     */
                    assert readCache.get() == null; // pre-condition.
                    final ReadCache newCache = getDirectReadCache(); // non-blocking take
                    if (newCache != null) {
                        assert newCache.getReferenceCount() == 0;
                        { // CRITICAL SECTION
                            // Pre-increment the new [readCache].
                            newCache.incrementReferenceCount();
                            // Set read cache reference.
                            readCache.set(newCache/* newValue */);
                        }
                        // guaranteed to succeed unless interrupted
                        bb = newCache.allocate(nbytes);
                        theCache = newCache;
                        { // CRITICAL SECTION.
                            // increment while readCache synchronized
                            theCache.incrementReferenceCount();
                            willInstall = true;
                        }
                    }
				}
			} // synchronized(readCache)
    
            if (bb == null) {
                /*
                 * No free buffer to install the read. Read directly into a heap
                 * ByteBuffer and return that to the caller.
                 */
                assert willInstall == false;
                return _readFromLocalDiskIntoNewHeapByteBuffer(offset, nbytes);
    		}

            /*
             * [bb] is a view onto an allocation on [theCache] into which we can
             * install the read.
             */
            
            // The offset into [bb] of the allocation.
		    final int pos = bb.position();
		    
		    // Read the record from the disk into NIO buffer.
		    final ByteBuffer ret = reader.readRaw(offset, bb);
		
		    // must copy to heap buffer from cache, allowing for checksum
            final byte[] b = new byte[nbytes - 4];
            ret.get(b);
            
            // calculate checksum from readRaw before adding to readCache!
            {
            	final int datalen = nbytes - 4;
	            final int chk = ret.getInt(pos + datalen);
	
	            if (chk != ChecksumUtility.threadChk.get().checksum(b, 0/* offset */, datalen)) {
	
	                throw new ChecksumError();
	
	            }
            }
          
		    // update record maps
		    theCache.commitToMap(offset, pos, nbytes);
		    serviceMap.put(offset, theCache);

            return ByteBuffer.wrap(b);

        } catch (Throwable t) {
        	t.printStackTrace(System.err);
        	
        	throw new RuntimeException(t);
        } finally {
            /*
             * CRITICAL SECTION. If [willInstall] then we are responsible for
             * this ReadCache and MUST decrement the counter.
             */
            if (willInstall && theCache.decrementReferenceCount() == 0) {
                readList.add(theCache);
                // END CRITICAL SECTION.
                if (theCache == readCache.get())
                    throw new AssertionError();
            }
        }
 
	}

    /**
     * Read through to the backing file.
     * 
     * @param offset
     *            The byte offset of the record on the backing file.
     * @param nbytes
     *            The #of bytes to be read.
     * 
     * @return The installed record in a newly allocated heap {@link ByteBuffer}
     *         .
     */
    private final ByteBuffer _readFromLocalDiskIntoNewHeapByteBuffer(
            final long offset, final int nbytes) {

        if (log.isDebugEnabled())
            log.debug("Allocating direct, nbytes: " + nbytes);

        final ByteBuffer ret = reader.readRaw(offset,
                ByteBuffer.allocate(nbytes));

        final int chk = ChecksumUtility.getCHK().checksum(ret.array(),
                0/* offset */, nbytes - 4/* len */); // read checksum

        final int tstchk = ret.getInt(nbytes - 4);
        
        if (chk != tstchk)
            throw new ChecksumError("offset=" + offset + ",nbytes=" + nbytes
                    + ",expected=" + tstchk + ",actual=" + chk);
        
        ret.limit(nbytes - 4);
        
        if (ret.remaining() == 0)
        	throw new AssertionError();

        // This read was not installed into the read cache.
        counters.get().nreadNotInstalled.increment();
        
        return ret;
    
    }
    
    /**
     * Read the data from the backing file.
     * 
     * We need to know the size of the data so we can allocate the buffer.
     * 
     * @param offset
     * @return
     * @throws InterruptedException
     * @throws IllegalStateException
     */
//    private ByteBuffer readBacking(final long offset, final int nbytes)
//            throws IllegalStateException, InterruptedException {
//        if (reader == null)
//            return null;
//
//        if (nbytes > readCache.get().capacity()) // not possible to cache
//            return null;
//
//        // allocate space in readCache and retrieve buffer into which we'll
//        // read the data
//
//        ByteBuffer bb = null;
//        WriteCache installCache;
//        synchronized (readCache) {
//            final WriteCache cache = readCache.get();
//            bb = cache.allocate(offset, nbytes);
//            if (bb == null) { // return readCache to clean list
//                addClean(cache, false/* add to front */);
//                installCache = getDirectCleanCache();
//                readCache.set(installCache);
//                installCache.closeForWrites();
//
//                bb = installCache.allocate(offset, nbytes);
//
//                assert bb != null;
//            } else {
//                installCache = cache;
//            }
//        }
//
//        // must return new byte[] since original ByteBuffer will be updated
//        final byte[] ret = new byte[nbytes - 4];
//
//        // DEBUG readRaw into non-direct byte buffer
//        // final ByteBuffer trans = ByteBuffer.wrap(ret);
//        // reader.readRaw(offset, trans);
//
//        reader.readRaw(offset, bb);
//
//        recordMap.put(offset, installCache);
//
//        // copy WriteCache data into return buffer
//        bb.get(ret);
//
//        return ByteBuffer.wrap(ret);
//    }

    /**
     * Called to check if a write has already been flushed. This is only made if
     * a write has been made to previously committed data (in the current RW
     * session).
     * <p>
     * If dirty {@link WriteCache}s are flushed in order then it does not
     * matter, however, if we want to be able to combine {@link WriteCache}s
     * then it makes sense that there are no duplicate writes.
     * <p>
     * On reflection this is more likely needed since for the {@link RWStore},
     * depending on session parameters, the same cached area could be
     * overwritten. We could still maintain multiple writes but we need a
     * guarantee of order when retrieving data from the write cache (newest
     * first).
     * <p>
     * So the question is, whether it is better to keep cache consistent or to
     * constrain with read order?
     * 
     * @param offset
     *            the address to check
     */
    public boolean clearWrite(final long offset, final int latchedAddr) {
        try {
            counters.get().nclearAddrRequests++;
            while (true) {
                final WriteCache cache = serviceMap.get(offset);
                if (cache == null) {
                    // Not found.
                    return false;
                }
                cache.transferLock.lock();
                try {
//                    /**
//                     * Note: The tests below require us to take the read lock on
//                     * the WriteCache before we test the serviceMap again in
//                     * order to guard against a concurrent reset() of the
//                     * WriteCache.
//                     * 
//                     * @see <a href=
//                     *      "https://sourceforge.net/apps/trac/bigdata/ticket/654"
//                     *      Rare AssertionError in WriteCache.clearAddrMap()
//                     *      </a>
//                     */
//                    cache.acquire();
//                    try {
                    final WriteCache cache2 = serviceMap.get(offset);
                    if (cache2 != cache) {
                        /*
                         * Not found in this WriteCache.
                         * 
                         * Record was (re-)moved before we got the lock.
                         * 
                         * Note: We need to retry. WriteCache.transferTo() could
                         * have just migrated the record to another WriteCache.
                         */
                        continue;
                    }
                    
                    // Remove entry from the recordMap.
                    final WriteCache oldValue = serviceMap.remove(offset);
                    if (oldValue == null) {
                        /**
                         * Note: The [WriteCache.transferLock] protects the
                         * WriteCache against a concurrent transfer of a record
                         * in WriteCache.transferTo(). However,
                         * WriteCache.resetWith() does NOT take the
                         * transferLock. Therefore, it is possible (and valid)
                         * for the [recordMap] entry to be cleared to [null] for
                         * this record by a concurrent resetWith() call.
                         * 
                         * @see <a href=
                         *      "https://sourceforge.net/apps/trac/bigdata/ticket/654"
                         *      Rare AssertionError in WriteCache.clearAddrMap()
                         *      </a>
                         */
                        continue;
                    }
					if (oldValue != cache) {
						/*
						 * Concurrent modification!
						 */
						throw new AssertionError("oldValue=" + oldValue
						+ ", cache=" + cache + ", offset=" + offset
						+ ", latchedAddr=" + latchedAddr);
					}

                    /*
                     * Note: clearAddrMap() is basically a NOP if the WriteCache
                     * has been closedForWrites().
                     */
                    if (cache.clearAddrMap(offset, latchedAddr)) {
                        // Found and cleared.
                        counters.get().nclearAddrCleared++;
                        debugAddrs(offset, 0, 'F');
                        return true;
                    }
//                    } finally {
//                        cache.release();
//                    }
                } finally {
                    cache.transferLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    
//    /**
//     * Debug method to verify that the {@link WriteCacheService} has flushed all
//     * {@link WriteCache} buffers.
//     * 
//     * @return whether there are no outstanding writes buffered
//     */
//    public boolean isFlushed() {
//        
//        final boolean clear = 
//    			dirtyList.size() == 0
//    			&& compactingCacheRef.get() == null
//    			&& (current.get() == null || current.get().isEmpty());
//    	
//        return clear;
//        
//    }
    
    /**
     * An array of writeCache actions is maintained that can be used
     * to provide a breadcrumb of how that address has been written, saved,
     * freed or removed.
     * <p>
     * Write errors often show up as a checksum error, so the length of
     * data written to the address cab be crucial information in determining the
     * root of any problem.
     * 
     * @param address for which info requested
     * @return summary of writeCache actions
     */
    public String addrDebugInfo(final long paddr) {
        if (addrsUsed == null) {
            return "No WriteCache debug info";
        }
        
        final StringBuffer ret = new StringBuffer();
//      // first see if address was ever written
//      boolean written = false;
        for (int i = 0; i < addrsUsed.length; i++) {
            if (i == addrsUsedCurs) {
                ret.append("|...|");
            }
            if (addrsUsed[i] == paddr) {
                ret.append(addrActions[i]);
                if (addrActions[i]=='A') {
                    ret.append("[" + addrLens[i] + "]");
                }
            }
        }
        /*
         * Note: I've added in the write cache service counters here for
         * information about the maximum #of buffers from the pool which have
         * been in use, #of flushes, etc.
         */
        ret.append(":");
        ret.append(getCounters().toString());
        return ret.toString();
    }

    /**
     * Return <code>true</code> iff the address is in the write
     * cache at the moment which the write cache is checked.
     * <p>
     * Note: Unless the caller is holding an appropriate lock
     * across this operation, the result is NOT guaranteed to
     * be correct at any time other than the moment when the
     * cache was tested. 
     */
    public boolean isPresent(final long addr) {
        // System.out.println("Checking address: " + addr);
        
        return serviceMap.get(addr) != null;
    }
    
    /**
     * Note: Atomic reference is used so the counters may be imposed from
     * outside.
     */
    private final AtomicReference<WriteCacheServiceCounters> counters;

    /**
     * Return the performance counters for the {@link WriteCacheService}.
     */
    public CounterSet getCounters() {

        return counters.get().getCounters();

    }
    
    /**
     * Return the #of {@link WriteCache} blocks sent by the quorum leader to
     * the first downstream follower.
     */
    public long getSendCount() {

        return counters.get().nsend;

    }

    /**
     * An instance of this exception is thrown if a thread notices that the
     * {@link WriteCacheService} was closed by a concurrent process.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public static class AsynchronousCloseException extends IllegalStateException {

        private static final long serialVersionUID = 1L;
        
    }

}
