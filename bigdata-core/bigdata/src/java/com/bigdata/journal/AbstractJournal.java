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
 * Created on Mar 13, 2007
 */

package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.rmi.RemoteException;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.log4j.Logger;

import com.bigdata.BigdataStatics;
import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.ICheckpointProtocol;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexInconsistentError;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.ICUVersionRecord;
import com.bigdata.btree.view.FusedView;
import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.cache.ConcurrentWeakValueCacheWithTimeout;
import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.concurrent.FutureTaskMon;
import com.bigdata.config.Configuration;
import com.bigdata.config.IValidator;
import com.bigdata.config.IntegerRangeValidator;
import com.bigdata.config.IntegerValidator;
import com.bigdata.config.LongRangeValidator;
import com.bigdata.config.LongValidator;
import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.CAT;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSetAccess;
import com.bigdata.counters.Instrument;
import com.bigdata.ha.CommitRequest;
import com.bigdata.ha.CommitResponse;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.HAStatusEnum;
import com.bigdata.ha.HATXSGlue;
import com.bigdata.ha.IHAPipelineResetRequest;
import com.bigdata.ha.IHAPipelineResetResponse;
import com.bigdata.ha.IIndexManagerCallable;
import com.bigdata.ha.IJoinedAndNonJoinedServices;
import com.bigdata.ha.JoinedAndNonJoinedServices;
import com.bigdata.ha.PrepareRequest;
import com.bigdata.ha.PrepareResponse;
import com.bigdata.ha.QuorumService;
import com.bigdata.ha.RunState;
import com.bigdata.ha.msg.HANotifyReleaseTimeResponse;
import com.bigdata.ha.msg.HAReadResponse;
import com.bigdata.ha.msg.HARootBlockRequest;
import com.bigdata.ha.msg.HARootBlockResponse;
import com.bigdata.ha.msg.HAWriteSetStateResponse;
import com.bigdata.ha.msg.IHA2PhaseAbortMessage;
import com.bigdata.ha.msg.IHA2PhaseCommitMessage;
import com.bigdata.ha.msg.IHA2PhasePrepareMessage;
import com.bigdata.ha.msg.IHAAwaitServiceJoinRequest;
import com.bigdata.ha.msg.IHADigestRequest;
import com.bigdata.ha.msg.IHADigestResponse;
import com.bigdata.ha.msg.IHAGatherReleaseTimeRequest;
import com.bigdata.ha.msg.IHALogDigestRequest;
import com.bigdata.ha.msg.IHALogDigestResponse;
import com.bigdata.ha.msg.IHALogRequest;
import com.bigdata.ha.msg.IHALogRootBlocksRequest;
import com.bigdata.ha.msg.IHALogRootBlocksResponse;
import com.bigdata.ha.msg.IHANotifyReleaseTimeRequest;
import com.bigdata.ha.msg.IHANotifyReleaseTimeResponse;
import com.bigdata.ha.msg.IHAReadRequest;
import com.bigdata.ha.msg.IHAReadResponse;
import com.bigdata.ha.msg.IHARebuildRequest;
import com.bigdata.ha.msg.IHARemoteRebuildRequest;
import com.bigdata.ha.msg.IHARootBlockRequest;
import com.bigdata.ha.msg.IHARootBlockResponse;
import com.bigdata.ha.msg.IHASendState;
import com.bigdata.ha.msg.IHASendStoreResponse;
import com.bigdata.ha.msg.IHASnapshotDigestRequest;
import com.bigdata.ha.msg.IHASnapshotDigestResponse;
import com.bigdata.ha.msg.IHASnapshotRequest;
import com.bigdata.ha.msg.IHASnapshotResponse;
import com.bigdata.ha.msg.IHASyncRequest;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.ha.msg.IHAWriteSetStateRequest;
import com.bigdata.ha.msg.IHAWriteSetStateResponse;
import com.bigdata.ha.msg.Mock2PhaseCommitProtocolException;
import com.bigdata.htree.HTree;
import com.bigdata.io.ChecksumUtility;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.IDataRecord;
import com.bigdata.io.IDataRecordAccess;
import com.bigdata.io.SerializerUtil;
import com.bigdata.io.writecache.WriteCacheService;
import com.bigdata.journal.Name2Addr.Entry;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.JournalMetadata;
import com.bigdata.quorum.AsynchronousQuorumCloseException;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumActor;
import com.bigdata.quorum.QuorumException;
import com.bigdata.quorum.QuorumMember;
import com.bigdata.quorum.QuorumTokenTransitions;
import com.bigdata.rawstore.IAllocationContext;
import com.bigdata.rawstore.IAllocationManagerStore;
import com.bigdata.rawstore.IPSOutputStream;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.rawstore.WormAddressManager;
import com.bigdata.relation.locator.IResourceLocator;
import com.bigdata.resources.ResourceManager;
import com.bigdata.rwstore.IAllocationManager;
import com.bigdata.rwstore.IHistoryManager;
import com.bigdata.rwstore.IRWStrategy;
import com.bigdata.rwstore.RWStore;
import com.bigdata.rwstore.sector.MemStrategy;
import com.bigdata.rwstore.sector.MemoryManager;
import com.bigdata.service.AbstractHATransactionService;
import com.bigdata.service.AbstractTransactionService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.util.BytesUtil;
import com.bigdata.util.ClocksNotSynchronizedException;
import com.bigdata.util.NT;
import com.bigdata.util.StackInfoReport;

/**
 * <p>
 * The journal is a persistence capable data structure supporting atomic commit,
 * named indices, and full transactions. The {@link BufferMode#DiskRW} mode
 * provides an persistence scheme based on reusable allocation slots while the
 * {@link BufferMode#DiskWORM} mode provides an append only persistence scheme.
 * Journals may be configured in highly available quorums.
 * </p>
 * <p>
 * This class is an abstract implementation of the {@link IJournal} interface
 * that does not implement the {@link IConcurrencyManager},
 * {@link IResourceManager}, or {@link ITransactionService} interfaces. The
 * {@link Journal} provides a concrete implementation that may be used for a
 * standalone database complete with concurrency control and transaction
 * management.
 * </p> <h2>Limitations</h2>
 * <p>
 * The {@link IIndexStore} implementation on this class is NOT thread-safe. The
 * basic limitation is that the mutable {@link BTree} is NOT thread-safe. The
 * {@link #getIndex(String)} method exposes this mutable {@link BTree}. If you
 * use this method to access the mutable {@link BTree} then YOU are responsible
 * for avoiding concurrent writes on the returned object.
 * </p>
 * <p>
 * See {@link IConcurrencyManager#submit(AbstractTask)} for a thread-safe API
 * that provides suitable concurrency control for both isolated and unisolated
 * operations on named indices. Note that the use of the thread-safe API does
 * NOT protect against applications that directly access the mutable
 * {@link BTree} using {@link #getIndex(String)}.
 * </p>
 * <p>
 * The {@link IRawStore} interface on this class is thread-safe. However, this
 * is a low-level API that is not used by directly by most applications. The
 * {@link BTree} class uses this low-level API to read and write its nodes and
 * leaves on the store. Applications generally use named indices rather than the
 * {@link IRawStore} interface.
 * </p>
 * <p>
 * Note: transaction processing MAY occur be concurrent since the write set of a
 * each transaction is written on a distinct {@link TemporaryStore}. However,
 * without additional concurrency controls, each transaction is NOT thread-safe
 * and MUST NOT be executed by more than one concurrent thread. Again, see
 * {@link IConcurrencyManager#submit(AbstractTask)} for a high-concurrency API
 * for both isolated operations (transactions) and unisolated operations. Note
 * that the {@link TemporaryStore} backing a transaction will spill
 * automatically from memory onto disk if the write set of the transaction grows
 * too large.
 * </p>
 * <h2>Commit processing</h2>
 * <p>
 * The journal maintains two root blocks. Commit updates the root blocks using
 * the Challis algorithm. (The root blocks are updated using an alternating
 * pattern and "timestamps" are recorded at the head and tail of each root block
 * to detect partial writes. See {@link IRootBlockView} and
 * {@link RootBlockView}.) When the journal is backed by a disk file, the data
 * are {@link Options#FORCE_ON_COMMIT optionally flushed to disk on commit}. If
 * desired, the writes may be flushed before the root blocks are updated to
 * ensure that the writes are not reordered - see {@link Options#DOUBLE_SYNC}.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @todo There are lots of annoying ways in which asynchronously closing the
 *       journal, e.g., using {@link #close()} or {@link #shutdown()} can cause
 *       exceptions to be thrown out of concurrent threads. It would be nice if
 *       we could throw a single exception that indicated that the journal had
 *       been asynchronously closed.
 */
public abstract class AbstractJournal implements IJournal/* , ITimestampService */
, IAllocationManager, IAllocationManagerStore
{

	/**
	 * Logger.
	 */
	private static final Logger log = Logger.getLogger(AbstractJournal.class);

    /**
     * @see http://sourceforge.net/apps/trac/bigdata/ticket/443 (Logger for
     *      RWStore transaction service and recycler)
     */
    private static final Logger txLog = Logger.getLogger("com.bigdata.txLog");

    /**
     * Logger for HA events.
     */
    protected static final Logger haLog = Logger.getLogger("com.bigdata.haLog");

    /**
	 * The index of the root address containing the address of the persistent
	 * {@link Name2Addr} mapping names to {@link BTree}s registered for the
	 * store.
	 */
	public static transient final int ROOT_NAME2ADDR = 0;

    /**
     * The index of the root address where the root block copy from the previous
     * commit is stored.
     */
	public static transient final int PREV_ROOTBLOCK = 1;

	/**
	 * The index of the root address of the delete blocks associated with
	 * this transaction.
	 */
	public static transient final int DELETEBLOCK = 2;

    /**
     * The index of the root address containing the {@link ICUVersionRecord}.
     * That record specifies the ICU version metadata which was in force when
     * the journal was created.
     */
    public static transient final int ROOT_ICUVERSION = 3;
    
	/**
	 * A clone of the properties used to initialize the {@link Journal}.
	 */
	final protected Properties properties;

    /**
     * The #of open journals (JVM wide). This is package private. It is used to
     * chase down unit tests which are not closing() the Journal.
     */
    final static AtomicInteger nopen = new AtomicInteger();

    /**
     * The #of closed journals (JVM wide). This is package private. It is used
     * to chase down unit tests which are not {@link #close() closing} the
     * Journal.
     */
    final static AtomicInteger nclose = new AtomicInteger();

    /**
     * The #of destroyed journals (JVM wide). This is package private. It is
     * used to chase down unit tests which are not {@link #destroy() destroying}
     * the journal.
     */
    final static AtomicInteger ndestroy = new AtomicInteger();
    
	/**
	 * The directory that should be used for temporary files.
	 */
	final public File tmpDir;

	/**
	 * The object used by the journal to compute the checksums of its root
	 * blocks (this object is NOT thread-safe so there is one instance per
	 * journal).
	 */
	private final ChecksumUtility checker = new ChecksumUtility();

	// /*
	// * These fields were historically marked as [final] and set by the
	// * constructor. With the introduction of high availability these fields
	// can
	// * not be final because the CREATE of the journal must be deferred until a
	// * quorum leader has been elected.
	// *
	// * The pattern for these fields is that they are assigned by create() and
	// * are thereafter immutable. The fields are marked as [volatile] so the
	// * state change when they are set will be visible without explicit
	// * synchronization (many methods use volatile reads on these fields).
	// */

	/**
	 * The metadata for a pre-existing journal -or- <code>null</code> if the
	 * journal was created for the first time.
	 */
	private FileMetadata fileMetadata;

	/** package private method exported to {@link DumpJournal}. */
	FileMetadata getFileMetadata() {
		return fileMetadata;
	}

	/**
	 * The implementation logic for the current {@link BufferMode}.
	 */
	private final IBufferStrategy _bufferStrategy;

    /**
     * A description of the journal as a resource.
     * <p>
     * Note: For HA, this is updated if new root blocks are installed onto the
     * journal. This is necessary since that operation changes the {@link UUID}
     * of the backing store, which is one of the things reported by the
     * {@link JournalMetadata} class.
     * 
     * @see #installRootBlocks(IRootBlockView, IRootBlockView)
     */
	private final AtomicReference<JournalMetadata> journalMetadata = new AtomicReference<JournalMetadata>();

	/*
     * 
     */

	/**
	 * The current root block. This is updated each time a new root block is
	 * written.
	 */
	private volatile IRootBlockView _rootBlock;

	/**
	 * The registered committers for each slot in the root block.
	 */
	private volatile ICommitter[] _committers = new ICommitter[ICommitRecord.MAX_ROOT_ADDRS];

	/*
	 * 
	 */
	
	/**
	 * This lock is used to prevent clearing or setting of critical fields while
	 * a concurrent thread is seeking to operate on the referenced objects. A
	 * read-write lock was chosen because nearly all operations are "reads" (the
	 * read the field reference). For this purpose the "writer" is a thread
	 * which causes the value of either of these fields to be changed. The
	 * "writers" are {@link #abort()}, {@link #closeForWrites(long)}, etc. These
	 * methods are invoked relatively infrequently in comparison with "read"
	 * access to these fields.
	 */
	private final ReentrantReadWriteLock _fieldReadWriteLock = new ReentrantReadWriteLock(false/* fair */);

    /**
     * This lock is needed to synchronize serviceJoin of a <em>follower</em>
     * with a GATHER. Specifically it ensures that if a service is trying to
     * join during a replicated write set, then the GATHER and the SERVICE JOIN
     * are MUTEX.
     */
    private final Lock _gatherLock = new ReentrantLock();
	
    /**
     * Used to cache the most recent {@link ICommitRecord} -- discarded on
     * {@link #abort()}; set by {@link #commitNow(long)}.
     * <p>
     * Note: This is set in the constructor and modified by {@link #_abort()}
     * but (once set by the constructor) it is never <code>null</code> until the
     * store is closed.
     */
	private volatile ICommitRecord _commitRecord;

    /**
     * BTree mapping commit timestamps to the address of the corresponding
     * {@link ICommitRecord}. The keys are timestamps (long integers). The
     * values are the address of the {@link ICommitRecord} with that commit
     * timestamp.
     * <p>
     * Note: The {@link CommitRecordIndex} object is NOT systematically
     * protected by <code>synchronized</code> within this class. Therefore it is
     * NOT safe for use by outside classes and CAN NOT be made safe simply by
     * synchronizing their access on the {@link CommitRecordIndex} object
     * itself. This is mainly for historical reasons and it may be possible to
     * systematically protect access to this index within a synchronized block
     * and then expose it to other classes.
     * <p>
     * Note: This is set in the constructor and modified by {@link #_abort()}
     * but (once set by the constructor) it is never <code>null</code> until the
     * store is closed.
     */
	private volatile CommitRecordIndex _commitRecordIndex;

	/**
	 * The {@link ICUVersionRecord} iff known.
	 */
	private volatile ICUVersionRecord _icuVersionRecord;
	
	/**
	 * The configured capacity for the {@link HardReferenceQueue} backing the
	 * index cache maintained by the "live" {@link Name2Addr} object.
	 * 
	 * @see Options#LIVE_INDEX_CACHE_CAPACITY
	 */
	private final int liveIndexCacheCapacity;

	/**
	 * The configured timeout in milliseconds for stale entries in the
	 * {@link HardReferenceQueue} backing the index cache maintained by the
	 * "live" {@link Name2Addr} object.
	 * 
	 * @see Options#LIVE_INDEX_CACHE_TIMEOUT
	 */
	private final long liveIndexCacheTimeout;

	/**
	 * The configured capacity for the LRU backing the
	 * {@link #historicalIndexCache}.
	 * 
	 * @see Options#HISTORICAL_INDEX_CACHE_CAPACITY
	 */
	private final int historicalIndexCacheCapacity;

	/**
	 * The configured timeout for stale entries in the
	 * {@link HardReferenceQueue} backing the {@link #historicalIndexCache}.
	 * 
	 * @see Options#HISTORICAL_INDEX_CACHE_TIMEOUT
	 */
	private final long historicalIndexCacheTimeout;

    /**
     * A cache that is used by the {@link AbstractJournal} to provide a
     * <em>canonicalizing</em> mapping from an address to the instance of a
     * read-only historical object loaded from that address and which indirectly
     * controls how long the journal will "keep open" historical index objects
     * by prevent them from being swept by the garbage collector.
     * <p>
     * Note: the "live" version of an object MUST NOT be placed into this cache
     * since its state will continue to evolve with additional writes while the
     * cache is intended to provide a canonicalizing mapping to the historical
     * committed states of the object. This means that objects such as indices
     * and the {@link Name2Addr} index MUST NOT be inserted into the cache if
     * the are being read from the store for "live" use. For this reason
     * {@link Name2Addr} uses its own caching mechanisms.
     * <p>
     * Note: {@link #abort()} discards the contents of this cache in order to
     * ensure that partial writes are discarded.
     * 
     * @see Options#HISTORICAL_INDEX_CACHE_CAPACITY
     * @see Options#HISTORICAL_INDEX_CACHE_TIMEOUT
     * 
     *      TODO This should have {@link ICheckpointProtocol} values. We have to
     *      update the
     *      {@link IRWStrategy#registerExternalCache(ConcurrentWeakValueCache, int)}
     *      method in order to make that change.
     */
	// final private WeakValueCache<Long, ICommitter> historicalIndexCache;
	final private ConcurrentWeakValueCache<Long, ICommitter> historicalIndexCache;

    /**
     * A cache that is used to avoid lookups against the
     * {@link CommitRecordIndex} and {@link Name2Addr} for historical index
     * views.
     * <p>
     * Note: This cache is in front of the {@link #historicalIndexCache} as the
     * latter is only tested once we have the {@link ICommitRecord} and have
     * resolved the entry in {@link Name2Addr}. This cache allows us to avoid
     * both of those steps.
     * <p>
     * Note: The {@link #historicalIndexCache} imposes a canonicalizing mapping.
     * It remains necessary, even with the introduction of the
     * {@link #indexCache}.
     * 
     * @see #getIndex(String, long)
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/546" > Add
     *      cache for access to historical index views on the Journal by name
     *      and commitTime. </a>
     */
    final private ConcurrentWeakValueCacheWithTimeout<NT, ICheckpointProtocol> indexCache;

	/**
	 * The "live" BTree mapping index names to the last metadata record
	 * committed for the named index. The keys are index names (unicode
	 * strings). The values are the names and the last known address of the
	 * named btree.
	 * <p>
	 * The "live" name2addr index is required for unisolated writers regardless
	 * whether they are adding an index, dropping an index, or just recovering
	 * the "live" version of an existing named index.
	 * <p>
	 * Operations that read on historical {@link Name2Addr} objects can of
	 * course be concurrent. Those objects are loaded from an
	 * {@link ICommitRecord}. See {@link #getIndex(String, ICommitRecord)}.
	 * <p>
	 * Note: access to the "live" {@link Name2Addr} index MUST be bracketed with
	 * <code>synchronized({@link #_name2Addr})</code>.
	 * 
	 * @see #getName2Addr()
	 */
	private volatile Name2Addr _name2Addr;
	
	/**
	 * An atomic state specifying whether a clean abort is required.  This is set
	 * to true by critical section code in the _abort if it does not complete cleanly.
	 * <p>
	 * It is checked in the commit() method ensure updates are protected.
	 * 
	 * @see #1021 (Add critical section protection to AbstractJournal.abort() and BigdataSailConnection.rollback())
	 */
	private final AtomicBoolean abortRequired = new AtomicBoolean(false);

	/**
	 * Return the "live" BTree mapping index names to the last metadata record
	 * committed for the named index. The keys are index names (unicode
	 * strings). The values are the names and the last known address of the
	 * named btree.
	 * <p>
	 * The "live" name2addr index is required for unisolated writers regardless
	 * whether they are adding an index, dropping an index, or just recovering
	 * the "live" version of an existing named index.
	 * <p>
	 * Operations that read on historical {@link Name2Addr} objects can of
	 * course be concurrent. Those objects are loaded from an
	 * {@link ICommitRecord}. See {@link #getIndex(String, ICommitRecord)}.
	 * <p>
	 * Note: the "live" {@link Name2Addr} index is a mutable {@link BTree}. All
	 * access to the object MUST be synchronized on that object.
	 */
	protected Name2Addr _getName2Addr() {

		final ReadLock lock = _fieldReadWriteLock.readLock();

		lock.lock();

		try {

			final Name2Addr tmp = _name2Addr;

			if (tmp == null)
				throw new AssertionError();

			return tmp;

		} finally {

			lock.unlock();

		}

	}

	/**
	 * A read-only view of the {@link Name2Addr} object mapping index names to
	 * the most recent committed {@link Entry} for the named index. The keys are
	 * index names (unicode strings). The values are {@link Entry}s containing
	 * the names, commitTime, and last known {@link Checkpoint} address of the
	 * named {@link BTree} on the {@link Journal}.
	 */
	public IIndex getName2Addr() {

		final ReadLock lock = _fieldReadWriteLock.readLock();

		lock.lock();

		try {

			final long checkpointAddr;

			if (_name2Addr == null) {

				checkpointAddr = getRootAddr(ROOT_NAME2ADDR);

			} else {

				checkpointAddr = _name2Addr.getCheckpoint().getCheckpointAddr();

			}

			/*
			 * Note: This uses the canonicalizing mapping to get an instance
			 * that is distinct from the live #name2Addr object while not
			 * allowing more than a single such distinct instance to exist for
			 * the current name2Addr object.
			 */
            final BTree btree = (BTree) getIndexWithCheckpointAddr(checkpointAddr);
            
            // References must be distinct.
            if (_name2Addr == btree)
                throw new AssertionError();

//			/*
//			 * Wrap up in a read-only view since writes MUST NOT be allowed.
//			 */
//			return new ReadOnlyIndex(btree);

            /*
             * Set the last commit time on the Name2Addr view.
             * 
             * TODO We can not reliably set the lastCommitTime on Name2Addr in
             * this method. It will typically be the commitTime associated last
             * commit point on the store, but it *is* possible to have a commit
             * on the database where no named indices were dirty and hence no
             * update was made to Name2Addr. In this (rare, but real) case, the
             * factual Name2Addr lastCommitTime would be some commit time
             * earlier than the lastCommitTime reported by the Journal.
             */
            final long lastCommitTime = getLastCommitTime();

            if (lastCommitTime != 0L) {

                btree.setLastCommitTime(lastCommitTime);

            }

            return btree;
            
		} finally {

			lock.unlock();

		}

	}

	/**
	 * Return a read-only view of the {@link Name2Addr} object as of the
	 * specified commit time.
	 * 
	 * @param commitTime
	 *            A commit time.
	 * 
	 * @return The read-only view -or- <code>null</code> if there is no commit
	 *         record for that commitTime.
	 * 
	 * @see #getName2Addr()
	 */
	public IIndex getName2Addr(final long commitTime) {

		final ICommitRecord commitRecord = getCommitRecord(commitTime);

		if (commitRecord == null) {

			return null;

		}

		final long checkpointAddr = commitRecord.getRootAddr(ROOT_NAME2ADDR);

        final Name2Addr n2a = 
                (Name2Addr) getIndexWithCheckpointAddr(checkpointAddr);

//        return new ReadOnlyIndex(n2a);

        /*
         * Set the last commit time on the Name2Addr index view.
         * 
         * TODO We can not reliably set the lastCommitTime on Name2Addr in this
         * method. It will typically be the commitTime associated with the
         * [commitRecord] that we resolved above, but it *is* possible to have a
         * commit on the database where no named indices were dirty and hence no
         * update was made to Name2Addr. In this (rare, but real) case, the
         * factual Name2Addr lastCommitTime would be some commit time earlier
         * than the commitTime reported by the [commitRecord].
         */

        final long commitTime2 = commitRecord.getTimestamp();
        
        n2a.setLastCommitTime(commitTime2);

        return n2a;

    }

//	/**
//	 * Return the root block view associated with the commitRecord for the
//	 * provided commit time.  This requires accessing the next commit record
//	 * since the previous root block is stored with each record.
//	 * 
//	 * @param commitTime
//	 *            A commit time.
//	 * 
//	 * @return The root block view -or- <code>null</code> if there is no commit
//	 *         record for that commitTime.
//	 */
//	@Deprecated // This method is unused and lacks a unit test.
//	IRootBlockView getRootBlock(final long commitTime) {
//
//        /*
//         * Note: getCommitRecordStrictlyGreaterThan() uses appropriate
//         * synchronization for the CommitRecordIndex.
//         */
//        final ICommitRecord commitRecord = getCommitRecordStrictlyGreaterThan(commitTime);
//
//		if (commitRecord == null) {
//
//		    return null;
//		    
//		}
//
//		final long rootBlockAddr = commitRecord.getRootAddr(PREV_ROOTBLOCK);
//		
//		if (rootBlockAddr == 0) {
//			
//		    return null;
//		    
//		} else {
//
//		    final ByteBuffer bb = read(rootBlockAddr);
//			
//			return new RootBlockView(true /* rb0 - WTH */, bb, checker);
//			
//		}
//
//	}
//	
//	/**
//	 * 
//	 * @param startTime from which to begin iteration
//	 * 
//	 * @return an iterator over the committed root blocks
//	 */
//    @Deprecated
//    /*
//     * This is UNUSED AND NOT SAFE (used only in test suite by
//     * StressTestConcurrentTx, which I have commented out) and not safe (because
//     * it lacks the necessary locks to access the CommitRecordIndex). The code
//     * is also wrong since it visits GT the commitTime when it should visit GTE
//     * the commitTime.
//     */
//	Iterator<IRootBlockView> getRootBlocks(final long startTime) {
//		return new Iterator<IRootBlockView>() {
//			ICommitRecord commitRecord = getCommitRecordIndex().findNext(startTime);
//
//			public boolean hasNext() {
//				return commitRecord != null;
//			}
//			
//			public IRootBlockView next() {
//				final long rootBlockAddr = commitRecord.getRootAddr(PREV_ROOTBLOCK);
//				
//				commitRecord = getCommitRecordIndex().findNext(commitRecord.getTimestamp());
//				
//				if (rootBlockAddr == 0) {
//					return null;
//				} else {
//					ByteBuffer bb = read(rootBlockAddr);
//					
//					return new RootBlockView(true /* rb0 - WTH */, bb, checker);
//				}
//			}
//
//			public void remove() {
//				throw new UnsupportedOperationException();
//			}
//			
//		};
//	}

	/**
	 * True iff the journal was opened in a read-only mode.
	 */
	private final boolean readOnly;

	/**
	 * Option controls whether the journal forces application data to disk
	 * before updating the root blocks.
	 */
	protected final boolean doubleSync;

	/**
	 * Option controls how the journal behaves during a commit.
	 */
	protected final ForceEnum forceOnCommit;

	/**
	 * Option set by the test suites causes the file backing the journal to be
	 * deleted when the journal is closed.
	 */
	protected final boolean deleteOnClose;

	/**
	 * The maximum extent before a {@link #commit()} will {@link #overflow()}.
	 * In practice, overflow tries to trigger before this point in order to
	 * avoid extending the journal.
	 * 
	 * @see Options#MAXIMUM_EXTENT
	 */
	private final long maximumExtent;
	private final long initialExtent;
	private final long minimumExtension;

	private RootBlockCommitter m_rootBlockCommitter;

	/**
	 * The maximum extent before a {@link #commit()} will {@link #overflow()}.
	 * In practice, overflow tries to trigger before this point in order to
	 * avoid extending the journal.
	 * 
	 * @see Options#MAXIMUM_EXTENT
	 */
	final public long getMaximumExtent() {

		return maximumExtent;

	}

	/**
	 * Resolves the property value (static variant for ctor initialization).
	 * 
	 * @see Configuration#getProperty(IIndexManager, Properties, String, String,
	 *      String)
	 */
	static protected String getProperty(final Properties properties, final String name, final String defaultValue) {

		return Configuration.getProperty(null/* indexManager */, properties, ""/*
																			 * no
																			 * namespace
																			 */, name, defaultValue);

	}

	/**
	 * Resolves the property value.
	 * 
	 * @see Configuration#getProperty(IIndexManager, Properties, String, String,
	 *      String)
	 */
	protected String getProperty(final String name, final String defaultValue) {

		return Configuration.getProperty(this, properties, ""/* no namespace */, name, defaultValue);

	}

	/**
	 * Resolves, parses, and validates the property value.
	 * 
	 * @param name
	 *            The property name.
	 * @param defaultValue
	 *            The default value.
	 * @return
	 */
	protected <E> E getProperty(final String name, final String defaultValue, IValidator<E> validator) {

		return Configuration.getProperty(this, properties, ""/* no namespace */, name, defaultValue, validator);

	}

    /**
     * Create or re-open a journal.
     * 
     * @param properties
     *            The properties as defined by {@link Options}.
     * 
     * @throws RuntimeException
     *             If there is a problem when creating, opening, or reading from
     *             the journal file.
     * 
     * @see Options
     */
    protected AbstractJournal(final Properties properties) {

        this(properties, null/* quorum */);

    }

    /**
     * Create or re-open a journal as part of a highly available {@link Quorum}.
     * 
     * @param properties
     *            The properties as defined by {@link Options}.
     * @param quorum
     *            The quorum with which the journal will join (HA mode only).
     * 
     * @throws RuntimeException
     *             If there is a problem when creating, opening, or reading from
     *             the journal file.
     * 
     * @see Options
     */
    protected AbstractJournal(Properties properties,
            final Quorum<HAGlue, QuorumService<HAGlue>> quorum) {

        if (properties == null)
            throw new IllegalArgumentException();

        this.properties = properties = (Properties) properties.clone();

        // null unless in HA mode.
		this.quorum = quorum;
		
		/*
		 * Set various 'final' properties.
		 */
		{

			historicalIndexCacheCapacity = getProperty(Options.HISTORICAL_INDEX_CACHE_CAPACITY,
					Options.DEFAULT_HISTORICAL_INDEX_CACHE_CAPACITY, IntegerValidator.GT_ZERO);

			historicalIndexCacheTimeout = getProperty(Options.HISTORICAL_INDEX_CACHE_TIMEOUT,
					Options.DEFAULT_HISTORICAL_INDEX_CACHE_TIMEOUT, LongValidator.GTE_ZERO);

			// historicalIndexCache = new WeakValueCache<Long, ICommitter>(
			// new LRUCache<Long, ICommitter>(historicalIndexCacheCapacity));

			// Cache by addr
			historicalIndexCache = new ConcurrentWeakValueCacheWithTimeout<Long, ICommitter>(historicalIndexCacheCapacity,
					TimeUnit.MILLISECONDS.toNanos(historicalIndexCacheTimeout));

			// Cache by (name,commitTime). This cache is in front of the cache by addr.
			indexCache = new ConcurrentWeakValueCacheWithTimeout<NT, ICheckpointProtocol>(historicalIndexCacheCapacity,
	                    TimeUnit.MILLISECONDS.toNanos(historicalIndexCacheTimeout));

			liveIndexCacheCapacity = getProperty(Options.LIVE_INDEX_CACHE_CAPACITY,
					Options.DEFAULT_LIVE_INDEX_CACHE_CAPACITY, IntegerValidator.GT_ZERO);

			liveIndexCacheTimeout = getProperty(Options.LIVE_INDEX_CACHE_TIMEOUT,
					Options.DEFAULT_LIVE_INDEX_CACHE_TIMEOUT, LongValidator.GTE_ZERO);

		}

		initialExtent = getProperty(Options.INITIAL_EXTENT, Options.DEFAULT_INITIAL_EXTENT, new LongRangeValidator(
				Options.minimumInitialExtent, Long.MAX_VALUE));

		maximumExtent = getProperty(Options.MAXIMUM_EXTENT, Options.DEFAULT_MAXIMUM_EXTENT, new LongRangeValidator(
				initialExtent, Long.MAX_VALUE));

		minimumExtension = getProperty(Options.MINIMUM_EXTENSION, Options.DEFAULT_MINIMUM_EXTENSION,
				new LongRangeValidator(Options.minimumMinimumExtension, Long.MAX_VALUE));

        readOnly = (Boolean.parseBoolean(getProperty(Options.READ_ONLY, Options.DEFAULT_READ_ONLY)));

		forceOnCommit = ForceEnum.parse(getProperty(Options.FORCE_ON_COMMIT, Options.DEFAULT_FORCE_ON_COMMIT));

		doubleSync = Boolean.parseBoolean(getProperty(Options.DOUBLE_SYNC, Options.DEFAULT_DOUBLE_SYNC));

		deleteOnClose = Boolean.parseBoolean(getProperty(Options.DELETE_ON_CLOSE, Options.DEFAULT_DELETE_ON_CLOSE));

		// "tmp.dir"
		{

			tmpDir = new File(getProperty(Options.TMP_DIR, System.getProperty("java.io.tmpdir")));

			if (!tmpDir.exists()) {

				if (!tmpDir.mkdirs()) {

					throw new RuntimeException("Could not create directory: " + tmpDir.getAbsolutePath());

				}

			}

			if (!tmpDir.isDirectory()) {

				throw new RuntimeException("Not a directory: " + tmpDir.getAbsolutePath());

			}

		}

		/*
		 * Create the appropriate IBufferStrategy object.
		 * 
		 * Note: the WriteLock is obtained here because various methods such as
		 * _getCommitRecord() assert that the caller is holding the write lock
		 * in order to provide runtime safety checks.
		 */
		final WriteLock lock = _fieldReadWriteLock.writeLock();

		lock.lock();

		try {

            /*
             * Peek at the buffer mode as configured in the properties. If it is
             * a memory-only buffer mode, then we will take one code path. If it
             * is a disk-backed buffer mode, then FileMetadata will figure out
             * what the actual mode should be. If it is a new file, then it is
             * just the mode configured in the Properties. If the file exists,
             * then it is the mode as recorded in the file.
             */
	        
            if (BufferMode.valueOf(
                    getProperty(Options.BUFFER_MODE,
                            Options.DEFAULT_BUFFER_MODE)).isFullyBuffered()) {

				/*
				 * Memory only buffer modes.
				 */

				if (readOnly) {

                    throw new RuntimeException(
                            "readOnly not supported for transient journals.");

				}

				fileMetadata = null;

                final long createTime = Long.parseLong(getProperty(
                        Options.CREATE_TIME, "" + System.currentTimeMillis()));

                // Note: Only used by WORM mode stores.
                final int offsetBits = getProperty(
                        Options.OFFSET_BITS,
                        Integer.toString((this instanceof Journal ? WormAddressManager.SCALE_UP_OFFSET_BITS
                                : WormAddressManager.SCALE_OUT_OFFSET_BITS)),
                        new IntegerRangeValidator(
                                WormAddressManager.MIN_OFFSET_BITS,
                                WormAddressManager.MAX_OFFSET_BITS));

                final BufferMode bufferMode = BufferMode.valueOf(
                        getProperty(Options.BUFFER_MODE, 
                                    Options.DEFAULT_BUFFER_MODE)
                                    );

                switch (bufferMode) {
                case Transient: {

                    final boolean useDirectBuffers = Boolean
                            .parseBoolean(getProperty(
                                    Options.USE_DIRECT_BUFFERS,
                                    Options.DEFAULT_USE_DIRECT_BUFFERS));

                    _bufferStrategy = new TransientBufferStrategy(offsetBits,
                            initialExtent, 0L/*
                                              * soft limit for maximumExtent
                                              */, useDirectBuffers);
                    break;
                }
                case MemStore: {

                    _bufferStrategy = new MemStrategy(new MemoryManager(
                            DirectBufferPool.INSTANCE,
                            Integer.MAX_VALUE/* maxSectors */, true/*blocking*/, properties));

                    break;

                }
                default:
                    throw new AssertionError("bufferMode=" + bufferMode);
                }
                
				/*
				 * setup the root blocks.
				 */
				final int nextOffset = 0;
				final long firstCommitTime = 0L;
				final long lastCommitTime = 0L;
				final long commitCounter = 0L;
				final long commitRecordAddr = 0L;
				final long commitRecordIndexAddr = 0L;
				final UUID uuid = UUID.randomUUID(); // Journal's UUID.
				final long closedTime = 0L;
                final long blockSequence = IRootBlockView.NO_BLOCK_SEQUENCE;
                final StoreTypeEnum storeType = bufferMode.getStoreType();
                if (createTime == 0L) {
                    throw new IllegalArgumentException(
                            "Create time may not be zero.");
                }
                final IRootBlockView rootBlock0 = new RootBlockView(true,
                        offsetBits, nextOffset, firstCommitTime,
                        lastCommitTime, commitCounter, commitRecordAddr,
                        commitRecordIndexAddr, uuid, //
                        blockSequence,//
                        quorumToken,//
                        0L, // metaStartAddr
                        0L, // metaStartBits
                        storeType,//
                        createTime, closedTime, RootBlockView.currentVersion,
                        checker);
                final IRootBlockView rootBlock1 = new RootBlockView(false,
                        offsetBits, nextOffset, firstCommitTime,
                        lastCommitTime, commitCounter, commitRecordAddr,
                        commitRecordIndexAddr, uuid, //
                        blockSequence,//
                        quorumToken,//
                        0L, // metaStartAddr
                        0L, // metaStartBits
                        storeType,//
                        createTime, closedTime, RootBlockView.currentVersion,
                        checker);
                
                _bufferStrategy.writeRootBlock(rootBlock0, ForceEnum.No);
				_bufferStrategy.writeRootBlock(rootBlock1, ForceEnum.No);

				this._rootBlock = rootBlock1;
			
				/*
				 * End memory backed modes.
				 */
				
			} else {

			    /*
			     * Disk backed modes.
			     */
			    
				/*final FileMetadata*/ fileMetadata = FileMetadata.createInstance(
						properties, !(this instanceof Journal), quorumToken);

                /*
                 * Note: Use the BufferMode as reported by FileMetadata. This
                 * will be the right mode on a restart as it checks what is
                 * actually in the store header / root blocks.
                 */
                final BufferMode bufferMode = fileMetadata.bufferMode;

//				/*
//				 * Note: The caller SHOULD specify an explicit [createTime] when
//				 * its value is critical. The default assigned here does NOT
//				 * attempt to use a clock that is consistent with the commit
//				 * protocol or even a clock that assigns unique timestamps.
//				 */
//				final long createTime = Long
//						.parseLong(getProperty(Options.CREATE_TIME, "" + System.currentTimeMillis()));
//
//				assert createTime != 0L;

				switch (bufferMode) {

				case Direct: {

					/*
					 * Setup the buffer strategy.
					 */

					_bufferStrategy = new DirectBufferStrategy(0L/*
																 * soft limit
																 * for
																 * maximumExtent
																 */, fileMetadata);

					this._rootBlock = fileMetadata.rootBlock;

					break;

				}

				case Mapped: {

					/*
					 * Setup the buffer strategy.
					 */

					/*
					 * Note: the maximumExtent is a hard limit in this case only
					 * since resize is not supported for mapped files.
					 */
					_bufferStrategy = new MappedBufferStrategy(maximumExtent /*
																			 * hard
																			 * limit
																			 * for
																			 * maximum
																			 * extent
																			 */, fileMetadata);

					this._rootBlock = fileMetadata.rootBlock;

					break;

				}

//				case Disk: {
//
//					/*
//					 * Setup the buffer strategy.
//					 */
//
//					_bufferStrategy = new DiskOnlyStrategy(0L/*
//															 * soft limit for
//															 * maximumExtent
//															 */,
//					// minimumExtension,
//							fileMetadata);
//
//					this._rootBlock = fileMetadata.rootBlock;
//
//					break;
//
//				}
				case Disk:
				case DiskWORM: {

					/*
					 * Setup the buffer strategy.
					 */

					_bufferStrategy = new WORMStrategy(
					        0L,// soft limit for maximumExtent
					        minimumExtension,//
                            fileMetadata, //
                            quorum//
                            );

					this._rootBlock = fileMetadata.rootBlock;

					break;

				}

				case DiskRW: {

					/*
					 * Setup the buffer strategy.
					 */

					_bufferStrategy = new RWStrategy(fileMetadata, quorum);

					this._rootBlock = fileMetadata.rootBlock;
					
					break;

				}

				case TemporaryRW: {

					/*
					 * Setup the buffer strategy.
					 */

					_bufferStrategy = new RWStrategy(fileMetadata, quorum);

					this._rootBlock = fileMetadata.rootBlock;
					
					break;

				}

				case Temporary: {

					/*
					 * Setup the buffer strategy.
					 * 
					 * FIXME Add test suite for this buffer mode. It should
					 * support MRMW but is not restart-safe.
					 */

					// FIXME Change BufferMode.Temporary to use WORMStrategy
					_bufferStrategy = new DiskOnlyStrategy(0L/*
															 * soft limit for
															 * maximumExtent
															 */,
					// minimumExtension,
							fileMetadata);

					this._rootBlock = fileMetadata.rootBlock;

					break;

				}

				default:

					throw new AssertionError();

				}

			}
			
            /*
             * Note: Creating a new journal registers some internal indices but
             * does NOT perform a commit. Those indices will become restart safe
             * with the first commit.
             */
			
			// Save resource description (sets value returned by getUUID()).
            this.journalMetadata.set(new JournalMetadata(this));

			// new or reload from the store root block.
			this._commitRecord = _getCommitRecord();

			// new or re-load commit record index from store via root block.
			this._commitRecordIndex = _getCommitRecordIndex();
			
			/**
			 * If the store can recycle storage then we must provide a hook to
			 * allow the removal of cached data when it is available for recycling.
			 */
			if (_bufferStrategy instanceof IHistoryManager) {

			    final int checkpointRecordSize = getByteCount(_commitRecordIndex
                        .getCheckpoint().getCheckpointAddr());

			    ((IHistoryManager) _bufferStrategy).registerExternalCache(
                        historicalIndexCache, checkpointRecordSize);
			    
            }

            // new or re-load from the store.
			this._icuVersionRecord = _getICUVersionRecord();

			// verify the ICU version.
            if (this._icuVersionRecord != null
                    && !ICUVersionRecord.newInstance().equals(
                            this._icuVersionRecord)) {

                final boolean update = Boolean.valueOf(properties.getProperty(
                        Options.UPDATE_ICU_VERSION, "false"));

                if (!update) {

                    throw new RuntimeException("ICUVersionChange: store="
                            + this._icuVersionRecord + ", runtime="
                            + ICUVersionRecord.newInstance());

                }
                
			}
			
			// Give the store a chance to set any committers that it defines.
			setupCommitters();

			// report event.
			ResourceManager.openJournal(getFile() == null ? null : getFile().toString(), size(), getBufferStrategy()
					.getBufferMode());
			
            if (txLog.isInfoEnabled())
                txLog.info("OPEN-JOURNAL: uuid=" + getUUID() + ", file="
                        + getFile() + ", bufferMode="
                        + getBufferStrategy().getBufferMode());
            
		} finally {

			lock.unlock();

		}

		nopen.incrementAndGet();
		
	}

	/**
	 * @todo consider making the properties restart safe so that they can be
	 *       read from the journal. This will let some properties be specified
	 *       on initialization while letting others default or be overridden on
	 *       restart. This is trivially accomplished by dedicating a root slot
	 *       to a Properties object, or a flattened Properties object serialized
	 *       as key-value pairs, in which case the data could just be loaded
	 *       into a btree and the btree api could be used to change the
	 *       persistent properties as necessary.
	 */
    @Override
	final public Properties getProperties() {

		return new Properties(properties);

	}

	/**
	 * Return the delegate that implements the {@link BufferMode}.
	 * <p>
	 * Note: this method MUST NOT check to see whether the journal is open since
	 * we need to use it if we want to invoke
	 * {@link IBufferStrategy#deleteResources()} and we can only invoke that
	 * method once the journal is closed.
	 */
	public IBufferStrategy getBufferStrategy() {

		return _bufferStrategy;

	}

	/**
	 * Service for running arbitrary tasks in support of
	 * {@link IResourceLocator}. There is no concurrency control associated with
	 * this service, but tasks run here may submit tasks to the
	 * {@link ConcurrencyManager}.
	 */
    @Override
	abstract public ExecutorService getExecutorService();

	/**
	 * Shutdown the journal (running tasks will run to completion, but no new
	 * tasks will start).
	 * <p>
	 * Note: You SHOULD use this method rather than {@link #close()} for normal
	 * shutdown of the journal.
	 * 
	 * @see #shutdownNow()
	 */
    @Override
	synchronized public void shutdown() {

		// Note: per contract for shutdown.
		if (!isOpen())
			return;

		if (log.isInfoEnabled())
			log.info("");

		// close immediately.
		_close();

		if (log.isInfoEnabled())
			log.info("Shutdown complete.");

	}

	/**
	 * Immediate shutdown (running tasks are canceled rather than being
	 * permitted to complete).
	 * 
	 * @see #shutdown()
	 */
    @Override
	synchronized public void shutdownNow() {

		// Note: per contract for shutdownNow()
		if (!isOpen())
			return;

		if (log.isInfoEnabled())
			log.info("");

		// close immediately.
		_close();

		if (log.isInfoEnabled())
			log.info("Shutdown complete.");

	}

	/**
	 * Closes out the journal iff it is still open.
	 */
    @Override
    protected void finalize() throws Throwable {
        
		if (_bufferStrategy.isOpen()) {

			if (log.isInfoEnabled())
				log.info("Closing journal: " + getFile());

			shutdownNow();

		}

	}

	/**
	 * Return counters reporting on various aspects of the journal.
	 */
    @Override
	public CounterSet getCounters() {

		return CountersFactory.getCounters(this);

	}

	/**
	 * Note: A combination of a static inner class and a weak reference to the
	 * outer class are used to avoid the returned {@link CounterSet} having a
	 * hard reference to the outer class while retaining the ability to update
	 * the {@link CounterSet} dynamically as long as the referenced object
	 * exists.
	 * <p>
	 * Note: one-shot counter are NOT used so that the LBS can aggregate the
	 * different values which this counter takes on across different live
	 * journal instances for the same data service. For example, the createTime
	 * for each live journal or the name of the file backing the current live
	 * journal.
	 */
	private static class CountersFactory {

		static public CounterSet getCounters(final AbstractJournal jnl) {

			final CounterSet counters = new CounterSet();

			final WeakReference<AbstractJournal> ref = new WeakReference<AbstractJournal>(jnl);

			counters.addCounter("file", new Instrument<String>() {
                @Override
				public void sample() {
					final AbstractJournal jnl = ref.get();
					if (jnl != null) {
                        final File file = jnl.getFile();
                        if (file != null)
                            setValue(file.toString());
                    }
				}
			});
         
         counters.addCounter("bufferMode", new Instrument<String>() {
            @Override
            public void sample() {
               final AbstractJournal jnl = ref.get();
               if (jnl != null) {
                  final IBufferStrategy bufferStrategy = jnl.getBufferStrategy();
                  if (bufferStrategy != null) {
                     final BufferMode bufferMode = bufferStrategy.getBufferMode();
                     if (bufferMode != null) {
                        setValue(bufferMode.toString());
                     }
                  }
               }
            }
         });
			
         counters.addCounter("groupCommit", new Instrument<Boolean>() {
            @Override
            public void sample() {
               final AbstractJournal jnl = ref.get();
               if (jnl != null) {
                  setValue(jnl.isGroupCommit());
                  }
            }
         });

			// counters.addCounter("file", new OneShotInstrument<String>(""
			// + jnl.getFile()));

			counters.addCounter("createTime", new Instrument<Long>() {
			    @Override
				public void sample() {
					final AbstractJournal jnl = ref.get();
					if (jnl != null) {
						final IRootBlockView rootBlock = jnl._rootBlock;
						if (rootBlock != null) {
							setValue(rootBlock.getCreateTime());
						}
					}
				}
			});

			counters.addCounter("closeTime", new Instrument<Long>() {
                @Override
				public void sample() {
					final AbstractJournal jnl = ref.get();
					if (jnl != null) {
						final IRootBlockView rootBlock = jnl._rootBlock;
						if (rootBlock != null) {
							setValue(rootBlock.getCloseTime());
						}
					}
				}
			});

			counters.addCounter("commitCount", new Instrument<Long>() {
                @Override
				public void sample() {
					final AbstractJournal jnl = ref.get();
					if (jnl != null) {
						final IRootBlockView rootBlock = jnl._rootBlock;
						if (rootBlock != null) {
							setValue(rootBlock.getCommitCounter());
						}
					}
				}
			});

			counters.addCounter("historicalIndexCacheSize", new Instrument<Integer>() {
                @Override
				public void sample() {
					final AbstractJournal jnl = ref.get();
					if (jnl != null) {
						setValue(jnl.historicalIndexCache.size());
					}
				}
			});

            counters.addCounter("indexCacheSize", new Instrument<Integer>() {
                @Override
                public void sample() {
                    final AbstractJournal jnl = ref.get();
                    if (jnl != null) {
                        setValue(jnl.indexCache.size());
                    }
                }
            });

            counters.addCounter("liveIndexCacheSize", new Instrument<Integer>() {
                @Override
				public void sample() {
					final AbstractJournal jnl = ref.get();
					if (jnl != null) {
						final Name2Addr name2Addr = jnl._name2Addr;
						if (name2Addr != null) {
							setValue(name2Addr.getIndexCacheSize());
						}
					}
				}
			});

            // backing strategy performance counters.
            counters.attach(jnl._bufferStrategy.getCounters());

            // commit protocol performance counters.
            counters.makePath("commit")
                    .attach(jnl.commitCounters.getCounters());

            return counters;

        }

	}

//    /**
//     * Return the live index counters maintained by the unisolated
//     * {@link Name2Addr} index iff they are available. These counters are not
//     * available for a read-only journal. They are also not available if the
//     * journal has been concurrently shutdown (since the {@link Name2Addr}
//     * reference will have been cleared).
//     * <p>
//     * Note: This is exposed to the {@link Journal} which reports this
//     * information.
//     * 
//     * @return The live index counters and <code>null</code> iff they are not
//     *         available.
//     */
//    protected CounterSet getLiveIndexCounters() {
//        if (!isReadOnly()) {
//            /*
//             * These index counters are only available for the unisolated
//             * Name2Addr view. If this is a read-only journal, then we can not
//             * report out that information.
//             */
//            final Name2Addr tmp = _name2Addr;
//            if (tmp != null) {
//                /*
//                 * Only if the Name2Addr index exists at the moment that we look
//                 * (avoids problems with reporting during concurrent shutdown).
//                 */
//                return tmp.getIndexCounters();
//            }
//        }
//        return null;
//    }

    /**
     * Return a {@link CounterSet} reflecting the named indices that are open or
     * which have been recently opened.
     * <p>
     * Note: This method prefers the live view of the index since this gets us
     * the most recent metadata for the index depth, #of nodes, #of leaves, etc.
     * However, when the live view is not currently open, we prefer the most
     * recent view of the index.
     * <p>
     * Note: This scans the live {@link Name2Addr}s internal index cache for the
     * live indices and then scans the {@link #historicalIndexCache} for the
     * read-only index views. Only a single {@link CounterSet} is reported for
     * any given named index.
     * 
     * @return A new {@link CounterSet} reflecting the named indices that were
     *         open as of the time that this method was invoked.
     * 
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/626">
     *      Expose performance counters for read-only indices </a>
     */
    protected final CounterSet getIndexCounters() {
    
        // If we find a live view of an index, we put its name in this set.
        final Set<String/* name */> foundLive = new HashSet<String>();

        final CounterSet tmp = new CounterSet();

        /*
         * First, search Name2Addr's internal cache.
         */
        {
            final Name2Addr n2a = _name2Addr;

            if (n2a != null) {

                // Get the live views from n2a.
                n2a.getIndexCounters(tmp, foundLive);

            }
            
        }

        /*
         * Now snapshot the [indexCache], creating a map (name,ndx) mapping. We
         * will use the most recent view of each named index.
         * 
         * Note: There are potentially many views of a given named index opened
         * against different commit points. Each of these can have distinct
         * metadata for the writeRetentionQueue, depth, #of nodes, #of leaves,
         * etc. We are prefering the most recent view.
         * 
         * TODO Note the #of open views for the index (injected field, but then
         * we need to do this for all views, including the live view).
         */

        final Map<String/* commitTime */, ICheckpointProtocol> map = new HashMap<String/* name */, ICheckpointProtocol>();
        {

            final Iterator<Map.Entry<NT, WeakReference<ICheckpointProtocol>>> itr = indexCache
                    .entryIterator();

            while (itr.hasNext()) {

                final Map.Entry<NT, WeakReference<ICheckpointProtocol>> e = itr
                        .next();

                final NT nt = e.getKey();

                final ICheckpointProtocol newVal = e.getValue().get();

                if (newVal == null) {

                    // Reference was cleared.
                    continue;

                }

                final String name = nt.getName();

                final ICheckpointProtocol oldVal = map.get(name);

                if (oldVal != null) {

                    final long oldTime = oldVal.getLastCommitTime();

                    final long newTime = newVal.getLastCommitTime();

                    if (newTime > oldTime) {

                        // Prefer the most recent index view.
                        map.put(name, newVal);

                    }
                    
                } else {

                    // There is no entry set for this name index.
                    map.put(name, newVal);

                }
                
            }

        }

        /*
         * Now include the CounterSet for each selected read-only index view.
         */

        for (Map.Entry<String/* name */, ICheckpointProtocol> e : map
                .entrySet()) {

            final String path = e.getKey(); // name

            final CounterSet aCounterSet = e.getValue().getCounters();

            // Attach the CounterSet
            tmp.makePath(path).attach(aCounterSet);

        }

        return tmp;

    }

    @Override
	final public File getFile() {

		final IBufferStrategy tmp = getBufferStrategy();

		if (tmp == null)
			return null;

		return tmp.getFile();

    }

//    /**
//     * The HA log directory.
//     * 
//     * @see HAJournal.Options#HA_LOG_DIR
//     * 
//     * @throws UnsupportedOperationException
//     *             always.
//     */
//    public File getHALogDir() {
//	    
//	    throw new UnsupportedOperationException();
//
//	}

    /**
     * Assert that <code>t1</code> LT <code>t2</code>, where <code>t1</code> and
     * <code>t2</code> are timestamps obtain such that this relation will be
     * <code>true</code> if the clocks on the nodes are synchronized.
     * <p>
     * Note: Clock synchronization errors can arise across nodes if the nodes
     * are not using a common network time source.
     * <p>
     * Note: Synchronization errors can arise on a single node if the clock is
     * changed on that node - specifically if the clock is move backwards to
     * before the most recent commit timestamp. For example, if the timezone is
     * changed.
     * 
     * @param serviceId1
     *            The service that reported the timestamp <code>t1</code>.
     * @param serviceId2
     *            The service that reported the timestamp <code>t2</code>.
     * @param t1
     *            A timestamp from one service.
     * @param t2
     *            A timestamp from the another service.
     * 
     * @throws ClocksNotSynchronizedException
     * 
     * @see ClocksNotSynchronizedException
     */
    protected void assertBefore(final UUID serviceId1, final UUID serviceId2,
            final long t1, final long t2) throws ClocksNotSynchronizedException {

        // Maximum allowed clock skew.
        final long maxSkew = getMaximumClockSkewMillis();

        ClocksNotSynchronizedException.assertBefore(serviceId1, serviceId2, t1,
                t2, maxSkew);

    }
    
    /**
     * The maximum error allowed (milliseconds) in the clocks. This is used by
     * {@link #assertBefore(UUID, UUID, long, long)} to verify that the clocks
     * are within some acceptable skew of one another. It is also used by
     * {@link #nextCommitTimestamp()} where it specifies the maximum clock skew
     * that will be corrected without operator intervention.
     * <p>
     * Note: This is overridden by the HAJournal.
     * 
     * @see #assertBefore(UUID, UUID, long, long)
     */
    protected long getMaximumClockSkewMillis() {

        throw new UnsupportedOperationException();
        
    }

    /**
     * The HA timeout in milliseconds for a 2-phase prepare.
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
    public long getHAPrepareTimeout() {
        
        throw new UnsupportedOperationException();

    }

    /**
     * The HA timeout in milliseconds for the release time consensus protocol.
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
    public long getHAReleaseTimeConsensusTimeout() {

        throw new UnsupportedOperationException();
    
    }

    /**
	 * Core implementation of immediate shutdown handles event reporting.
	 */
	protected void _close() {

		assertOpen();

//		if (log.isInfoEnabled())
//			log.info("file=" + getFile());

		_bufferStrategy.close();

        // Stop watching for quorum related events.
        if (quorum != null)
            quorum.terminate();

		// report event.
		ResourceManager.closeJournal(getFile() == null ? null : getFile().toString());

        if (txLog.isInfoEnabled())
            txLog.info("CLOSE-JOURNAL: uuid=" + getUUID() + ", file="
                    + getFile());

//		if (LRUNexus.INSTANCE != null) {
//
//			try {
//
//				LRUNexus.INSTANCE.deleteCache(getUUID());
//
//			} catch (Throwable t) {
//
//				log.error(t, t);
//
//			}
//
//		}

		if (deleteOnClose) {

			/*
			 * This option is used by the test suite and MUST NOT be used with
			 * live data.
			 */

			deleteResources();

		}
        
        nclose.incrementAndGet();
	}

	/**
	 * Deletes the backing file(s) (if any).
	 * <p>
	 * Note: This is the core implementation of delete and handles event
	 * reporting.
	 * 
	 * @exception IllegalStateException
	 *                if the journal is open.
	 */
	@Override
	public void deleteResources() {

		if (isOpen())
			throw new IllegalStateException();

		if (log.isInfoEnabled())
			log.info("");

		final IBufferStrategy bufferStrategy = getBufferStrategy();

		if (bufferStrategy != null) {

			bufferStrategy.deleteResources();

//          @see BLZG-1501 (remove LRUNexus)
//			if (LRUNexus.INSTANCE != null) {
//
//				try {
//
//					LRUNexus.INSTANCE.deleteCache(getUUID());
//
//				} catch (Throwable t) {
//
//					log.error(t, t);
//
//				}
//
//			}

		}

		ResourceManager.deleteJournal(getFile() == null ? null : getFile().toString());

	}

	/**
	 * Truncate the backing buffer such that there is no remaining free space in
	 * the journal.
	 * <p>
	 * Note: The caller MUST have exclusive write access to the journal. When
	 * the {@link ConcurrencyManager} is used, that means that the caller MUST
	 * have an exclusive lock on the {@link WriteExecutorService}.
	 * <p>
	 * Note: The {@link BufferMode#DiskRW} does NOT support this operation. This
	 * is because it stores meta-allocation information at the end of the file,
	 * which makes it impossible to shrink the file. Therefore this method will
	 * return without causing the file on disk to be shrunk for the RWStore.
	 */
	public void truncate() {

		assertOpen();

		if (isReadOnly())
			throw new IllegalStateException();

		final IBufferStrategy backingBuffer = getBufferStrategy();

		switch (backingBuffer.getBufferMode()) {
		case DiskRW:
			/*
			 * Operation is not supported for the RWStore.
			 */
			return;
		default:
			break;
		}
		
		final long oldExtent = backingBuffer.getExtent();

		final long newExtent = backingBuffer.getHeaderSize() + backingBuffer.getNextOffset();

		backingBuffer.truncate(newExtent);

		if (log.isInfoEnabled())
			log.info("oldExtent=" + oldExtent + ", newExtent=" + newExtent);

	}

	/**
	 * Make sure that the journal has at least the specified number of bytes of
	 * unused capacity remaining in the user extent.
	 * <p>
	 * Note: You need an exclusive write lock on the journal to extend it.
	 * 
	 * @param minFree
	 *            The minimum #of bytes free for the user extent.
	 * 
	 * @return The #of bytes of free space remaining in the user extent.
	 */
	public long ensureMinFree(final long minFree) {

		assertOpen();

		if (minFree < 0L)
			throw new IllegalArgumentException();

		final IBufferStrategy buf = _bufferStrategy;

		final long remaining = buf.getUserExtent() - buf.getNextOffset();

		if (remaining < minFree) {

			buf.truncate(buf.getExtent() + minFree);

		}

		return buf.getUserExtent() - buf.getNextOffset();

	}

	/**
	 * Restart safe conversion of the store into a read-only store with the
	 * specified <i>closeTime</i>.
	 * <p>
	 * This implementation sets the "closeTime" on the root block such that the
	 * journal will no longer accept writes, flushes all buffered writes, and
	 * releases any write cache buffers since they will no longer be used. This
	 * method is normally used when one journal is being closed out for writes
	 * during synchronous overflow processing and new writes will be buffered on
	 * a new journal. This has advantages over closing the journal directly
	 * including that it does not disturb concurrent readers.
	 * <p>
	 * Note: The caller MUST have exclusive write access to the journal. When
	 * the {@link ConcurrencyManager} is used, that means that the caller MUST
	 * have an exclusive lock on the {@link WriteExecutorService}.
	 * <p>
	 * Note: This does NOT perform a commit - any uncommitted writes will be
	 * discarded.
	 * 
	 * @throws IllegalStateException
	 *             If there are no commits on the journal.
	 * 
	 * @todo There should also be an option to convert a journal from
	 *       {@link BufferMode#Direct} to {@link BufferMode#Disk}. We would want
	 *       to do that not when the journal is sealed but as soon as
	 *       asynchronous overflow processing is done. Ideally this will not
	 *       require us to close and reopen the journal since that will disturb
	 *       concurrent readers.
	 */
	public void closeForWrites(final long closeTime) {

		final WriteLock lock = _fieldReadWriteLock.writeLock();

		lock.lock();

		try {

			final long lastCommitTime = _rootBlock.getLastCommitTime();

			if (log.isInfoEnabled())
				log.info("Closing journal for further writes: closeTime=" + closeTime + ", lastCommitTime="
						+ lastCommitTime);

			if (log.isDebugEnabled())
				log.debug("before: " + _rootBlock);

			final IRootBlockView old = _rootBlock;

			if (old.getCommitCounter() == 0L) {

				throw new IllegalStateException("No commits on journal");

			}

			// release any unused space.
			truncate();

			/*
			 * Create the final root block.
			 * 
			 * Note: We MUST bump the commitCounter in order to have the new
			 * root block be selected over the old one!
			 * 
			 * Note: This will throw an error if nothing has ever been committed
			 * on the journal. The problem is that the root block does not
			 * permit a non-zero commitCounter unless the commitRecordAddr and
			 * perhaps some other stuff are non-zero as well.
			 */
			final long metaStartAddr = _bufferStrategy.getMetaStartAddr();
			final long metaBitsAddr = _bufferStrategy.getMetaBitsAddr();
			final IRootBlockView newRootBlock = new RootBlockView(//
					!old.isRootBlock0(), old.getOffsetBits(), old.getNextOffset(), old.getFirstCommitTime(), old
							.getLastCommitTime(), //
					old.getCommitCounter() + 1, //
					old.getCommitRecordAddr(), //
					old.getCommitRecordIndexAddr(), //
					old.getUUID(), //
					0L, // blockSequence (writes are discarded)
					quorumToken, //
					metaStartAddr, //
					metaBitsAddr, //
					old.getStoreType(), //
					old.getCreateTime(), closeTime, //
					old.getVersion(), checker);

			/*
			 * Write it on the store.
			 * 
			 * Note: We request that the write is forced to disk to ensure that
			 * all buffered writes are forced to the disk. This is necessary in
			 * order to make sure that the updated root block (and anything left
			 * in the write cache for the disk buffer) get forced through onto
			 * the disk. We do not need to specify ForceMetadata here since the
			 * file size is unchanged by this operation.
			 */
			_bufferStrategy.writeRootBlock(newRootBlock, ForceEnum.Force);

			// discard write cache and make store read-only.
			_bufferStrategy.closeForWrites();

			// replace the root block reference.
			_rootBlock = newRootBlock;

			if (log.isDebugEnabled())
				log.debug("after: " + _rootBlock);

			// discard current commit record and re-read from the store.
			_commitRecord = _getCommitRecord();

			/*
			 * FIXME Verify that we can safely convert the writeRetentionQueue
			 * and readOnly flags on the BTree and then re-enable this code
			 * block. The tricky issue is the safe publication of the change to
			 * the writeRetentionQueue field (and populating it with the old
			 * queue's data) and the readOnly field. If those changes are not
			 * safely published then I also need to consider what the side
			 * effects of inconsistent views of those fields might be. One way
			 * to handle the safe publication is using AtomicBoolean and
			 * AtomicReference for those fields.
			 */
			// /* Convert all of the unisolated BTree objects into
			// read-historical
			// * BTrees as of the lastCommitTime on the journal. This is done in
			// * order to benefit from any data buffered by those BTrees since
			// * buffered data is data that we don't need to read from disk and
			// we
			// * don't need to de-serialize. This is especially important for
			// * asynchronous overflow processing which performs full index
			// scans
			// * of the BTree's shortly after synchronous overflow process (and
			// * which is the main reason why closeForWrites() exists).
			// *
			// * Note: The caller already promises that they hold the exclusive
			// * write lock so we don't really need to synchronize on
			// [name2Addr].
			// *
			// * Note: If we find a dirty mutable BTree then we ignore it rather
			// * than repurposing it. This allows the possibility that there are
			// * uncommitted writes.
			// */
			// synchronized (_name2Addr) {
			//
			// final Iterator<Map.Entry<String, WeakReference<BTree>>> itr =
			// _name2Addr
			// .indexCacheEntryIterator();
			//
			// while (itr.hasNext()) {
			//
			// final java.util.Map.Entry<String, WeakReference<BTree>> entry =
			// itr
			// .next();
			//
			// final String name = entry.getKey();
			//
			// final BTree btree = entry.getValue().get();
			//
			// if (btree == null) {
			//
			// // Note: Weak reference was cleared.
			// continue;
			//
			// }
			//
			// if (btree.needsCheckpoint()) {
			//
			// // Note: Don't convert a dirty BTree.
			// continue;
			//
			// }
			//
			// // Recover the Entry which has the last checkpointAddr.
			// final Name2Addr.Entry _entry = _name2Addr.getEntry(name);
			//
			// if (_entry == null) {
			//
			// /*
			// * There must be an Entry for each index in Name2Addr's
			// * cache.
			// */
			//
			// throw new AssertionError("No entry: name=" + name);
			//
			// }
			//
			// /*
			// * Mark the index as read-only (the whole journal no longer
			// * accepts writes) before placing it in the historical index
			// * cache (we don't want concurrent requests to be able to
			// * obtain a BTree that is not marked as read-only from the
			// * historical index cache).
			// */
			//
			// btree.convertToReadOnly();
			//
			// /*
			// * Put the BTree into the historical index cache under that
			// * checkpointAddr.
			// *
			// * Note: putIfAbsent() avoids the potential problem of
			// * having more than one object for the same checkpointAddr.
			// */
			//
			// historicalIndexCache.putIfAbsent(_entry.checkpointAddr,
			// btree);
			//
			// } // next index.
			//
			// // discard since no writers are allowed.
			// _name2Addr = null;
			//
			// }
			// close();
		} finally {

			lock.unlock();

		}

	}

	/**
	 * Invokes {@link #shutdownNow()}.
	 */
    @Override
	synchronized public void close() {

		// Note: per contract for close().
		if (!isOpen())
			throw new IllegalStateException();

		if (log.isInfoEnabled())
			log.info("");

		shutdownNow();

	}

	@Override
	synchronized public void destroy() {

		if (log.isInfoEnabled())
			log.info("");

		if (isOpen())
			shutdownNow();

		if (!deleteOnClose) {

			/*
			 * Note: if deleteOnClose was specified then the resource was
			 * already deleted by _close().
			 */

			deleteResources();

		}

		ndestroy.incrementAndGet();
		
	}

	/**
	 * Assert that the store is open.
	 * <p>
	 * Note: You can see an {@link IllegalStateException} thrown out of here if
	 * there are tasks running during {@link #shutdown()} and one of the various
	 * task services times out while awaiting termination. Such exceptions are
	 * normal since the store was closed asynchronously while task(s) were still
	 * running.
	 * 
	 * @exception IllegalStateException
	 *                if the store is closed.
	 */
	protected void assertOpen() {

		if (_bufferStrategy != null && !_bufferStrategy.isOpen()) {

            throw new IllegalStateException((getFile()==null?"transient":"file=" + getFile()));

		}

	}

    @Override
	final public UUID getUUID() {

		return journalMetadata.get().getUUID();

	}

	@Override
	final public IResourceMetadata getResourceMetadata() {

		return journalMetadata.get();

	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Note: This will report <code>false</code> for a new highly available
	 * journal until the quorum has met and {@link #init()} has been invoked for
	 * the {@link Quorum}.
	 */
    @Override
	public boolean isOpen() {

		return _bufferStrategy != null && _bufferStrategy.isOpen();

	}

    /**
     * Return <code>true</code> if the journal was opened in a read-only mode or
     * if {@link #closeForWrites(long)} was used to seal the journal against
     * further writes.
     */
    @Override
    public boolean isReadOnly() {

        if (readOnly) {
            // Opened in a read-only mode.
            return true;
        }

        if (getRootBlockView().getCloseTime() != 0L) {
            // Closed for writes.
            return true;
        }

        final long token = this.quorumToken;

        /*
         * This code path is too expensive and has been observed to deadlock.
         * Turn this into a non-blocking code path through correct maintenance
         * of the haReadyToken and the haStatus fields.
         */
//        if (token != Quorum.NO_QUORUM) {
//            
//          // Quorum exists, are we the leader for that token?
//          final boolean isLeader = quorum.getClient().isLeader(token);
//
//          // read-only unless this is the leader.
//          return !isLeader;
//
//        }

        /*
         * This code path is completely non-blocking. It relies on volatile
         * writes on [quorumToken] and [haStatus].
         * 
         * The rule is read-only if there is a met quorum unless this is the
         * leader and read/write if there is no quorum or if the quorum is not
         * met.
         */
        if (token != Quorum.NO_QUORUM) {

            switch (haStatus) {
            case Leader: // read/write
                return false;
            case Follower: // read-only
                return true;
            case NotReady: 
                /*
                 * This case is considered "read-only" locally, but not
                 * available for reads by the HA layer (REST API, SPARQL, etc).
                 */
                return true;
            default:
                throw new AssertionError();
            }

        }

        /*
         * Note: Default for HA permits read/write access when the quorum is not
         * met. This allows us to make local changes when setting up the
         * service, doing resync, etc.
         */

        return false;

    }

    /**
     * Assert that the journal is readable.
     * 
     * @throws IllegalStateException
     *             if the journal is not writable at this time.
     */
    protected void assertCanRead() {

        if (_bufferStrategy == null) {
            // only possible during the constructor call.
            throw new IllegalStateException();
        }

        if (!_bufferStrategy.isOpen()) {
            throw new IllegalStateException();
        }

    }

    /**
     * Assert that the journal is writable.
     * 
     * @throws IllegalStateException
     *             if the journal is not writable at this time.
     */
    protected void assertCanWrite() {

        if (_bufferStrategy == null) {
            // only possible during the constructor call.
            throw new IllegalStateException();
        }

        if (!_bufferStrategy.isOpen()) {
            throw new IllegalStateException();
        }

        if (_bufferStrategy.isReadOnly()) {
            throw new IllegalStateException();
        }

        if (abortRequired.get()) {
            /**
             * Do not permit mutation if an abort must be performed.
             * 
             * @see http://jira.blazegraph.com/browse/BLZG-181 (Add critical
             *      section protection to AbstractJournal.abort() and
             *      BigdataSailConnection.rollback())
             * @see http://jira.blazegraph.com/browse/BLZG-1236 (Recycler error
             *      in 1.5.1)
             */
            throw new AbortRequiredException();
        }
        
    }

    @Override
	public boolean isStable() {

	    return _bufferStrategy.isStable();

	}

    @Override
	public boolean isFullyBuffered() {

		return _bufferStrategy.isFullyBuffered();

	}

	public boolean isDoubleSync() {

		return doubleSync;

	}

	/**
	 * Return <code>true</code> if the persistence store uses record level
	 * checksums. When <code>true</code>, the store will detect bad reads by
	 * comparing the record as read from the disk against the checksum for that
	 * record.
	 */
	public boolean isChecked() {

		return _bufferStrategy.useChecksums();

	}

//    /**
//     * Return <code>true</code> if the journal is configured for high
//     * availability.
//     * 
//     * @see Quorum#isHighlyAvailable()
//     */
//	public boolean isHighlyAvailable() {
//
//        return quorum == null ? false : quorum.isHighlyAvailable();
//
//	}

    /**
     * {@inheritDoc}
     * <p>
     * Returns the current root block (immediate, non-blocking peek).
     * <p>
     * Note: The root block reference can be <code>null</code> until the journal
     * has been initialized. Once it has been set, the root block will always be
     * non-<code>null</code>. Since this method does not obtain the inner lock,
     * it is possible for another thread to change the root block reference
     * through a concurrent {@link #abort()} or {@link #commitNow(long)}. The
     * {@link IRootBlockView} itself is an immutable data structure.
     */
    @Override
	final public IRootBlockView getRootBlockView() {

//		final ReadLock lock = _fieldReadWriteLock.readLock();
//
//		lock.lock();
//
//		try {

			if (_rootBlock == null) {

				/*
				 * This can happen before the journal file has been created.
				 * Once it has been created the root block will always be
				 * non-null when viewed while holding the lock.
				 */

				throw new IllegalStateException();

			}

			return _rootBlock;

//		} finally {
//
//			lock.unlock();
//
//		}

	}
	
    /**
     * Variant of {@link #getRootBlockView()} that takes the internal lock in
     * order to provide an appropriate synchronization barrier when installing
     * new root blocks onto an empty journal in HA.
     * 
     * @see #installRootBlocks(IRootBlockView, IRootBlockView)
     */
    final public IRootBlockView getRootBlockViewWithLock() {

        final ReadLock lock = _fieldReadWriteLock.readLock();

        lock.lock();

        try {

            if (_rootBlock == null) {

                /*
                 * This can happen before the journal file has been created.
                 * Once it has been created the root block will always be
                 * non-null when viewed while holding the lock.
                 */

                throw new IllegalStateException();

            }

            return _rootBlock;

        } finally {

            lock.unlock();

        }

    }

    @Override
    final public long getLastCommitTime() {

//		final ReadLock lock = _fieldReadWriteLock.readLock();
//
//		lock.lock();
//
//		try {

			return _rootBlock.getLastCommitTime();

//		} finally {
//
//			lock.unlock();
//
//		}

	}

	/**
	 * Set a persistence capable data structure for callback during the commit
	 * protocol.
	 * <p>
	 * Note: the committers must be reset after restart or whenever the
	 * committers are discarded (the committers are themselves transient
	 * objects).
	 * 
	 * @param rootSlot
	 *            The slot in the root block where the address of the
	 *            {@link ICommitter} will be recorded.
	 * 
	 * @param committer
	 *            The commiter.
	 */
    @Override
	final public void setCommitter(final int rootSlot, final ICommitter committer) {

		assertOpen();

		_committers[rootSlot] = committer;

	}

	/**
	 * Notify all registered committers and collect their reported root
	 * addresses in an array.
	 * 
	 * @param commitTime
	 *            The timestamp assigned to the commit.
	 * 
	 * @return The array of collected root addresses for the registered
	 *         committers.
	 */
	final private long[] notifyCommitters(final long commitTime) {

		assert commitTime > 0L;

//		int ncommitters = 0;

		final long[] rootAddrs = new long[_committers.length];

		for (int i = 0; i < _committers.length; i++) {

		    final ICommitter committer = _committers[i];
		    
			if (committer == null)
				continue;

			final long addr = committer.handleCommit(commitTime);

			rootAddrs[i] = addr;

//			ncommitters++;

		}

		return rootAddrs;

	}

	@Override
	public void abort() {

		final WriteLock lock = _fieldReadWriteLock.writeLock();

		lock.lock();

		try {

            if (quorum != null) {

                try {

                    // HA mode
                    quorum.getClient().abort2Phase(quorumToken);

                } catch (Throwable t) {

                    haLog.error(
                            "2-Phase abort failure.  Will do local abort. cause="
                                    + t, t);

                    // Low-level abort.
                    doLocalAbort();
                    
                }

            } else {

                // Non-HA mode.
                doLocalAbort();
                
            }

		} catch (Throwable e) {
			
			throw new RuntimeException(e);

		} finally {

			lock.unlock();

		}

	}

	/**
	 * Discards any unisolated writes since the last {@link #commitNow(long)()}
	 * and also discards the unisolated (aka live) btree objects, reloading them
	 * from the current {@link ICommitRecord} on demand.
	 * <p>
	 * Note: The {@link WriteExecutorService} handles commit group and uses an
	 * index {@link Checkpoint} strategy so that it is able to abort individual
	 * tasks simply by discarding their changes and without interrupting
	 * concurrent writers. An {@link #abort()} is therefore an action of last
	 * resort and is generally triggered by things such as running out of disk
	 * space or memory within the JVM.
	 * <p>
	 * If a {@link Thread}s is interrupted in the midst of an IO operation on a
	 * {@link Channel} then the channel will be asynchronously closed by the
	 * JDK. Since some {@link IBufferStrategy}s use a {@link FileChannel} to
	 * access the backing store, this means that we need to re-open the backing
	 * store transparently so that we can continue operations after the commit
	 * group was aborted. This is done automatically when we re-load the current
	 * {@link ICommitRecord} from the root blocks of the store.
	 */// TODO Could merge with doLocalAbort().
	private void _abort() {

		// @see #1021 (Add critical section protection to AbstractJournal.abort() and BigdataSailConnection.rollback())
		boolean success = false;
		
		final WriteLock lock = _fieldReadWriteLock.writeLock();

		lock.lock();

		try {

            if (log.isInfoEnabled())
                log.info("ABORT", new StackInfoReport("ABORT"));

            // Clear
            gatherFuture.set(null/* newValue */);

			if (_bufferStrategy == null) {

				// Nothing to do.
				success = true;
				
				return;

			}

			txLog.info("ABORT");

//          @see BLZG-1501 (remove LRUNexus)
//			if (LRUNexus.INSTANCE != null) {
//
//				/*
//				 * Discard the LRU for this store. It may contain writes which
//				 * have been discarded. The same addresses may be reissued by
//				 * the WORM store after an abort, which could lead to incorrect
//				 * reads from a dirty cache.
//				 * 
//				 * FIXME An optimization would essentially isolate the writes on
//				 * the cache per BTree or between commits. At the commit point,
//				 * the written records would be migrated into the "committed"
//				 * cache for the store. The caller would read on the uncommitted
//				 * cache, which would read through to the "committed" cache.
//				 * This would prevent incorrect reads without requiring us to
//				 * throw away valid records in the cache. This could be a
//				 * significant performance gain if aborts are common on a
//				 * machine with a lot of RAM.
//				 */
//
//				LRUNexus.getCache(this).clear();
//
//			}
			
			invalidateCommitters();

			/*
			 * The buffer strategy has a hook which is used to discard buffered
			 * writes. This is both an optimization (it ensures that those
			 * writes are not asynchronously laid down on the disk) and a
			 * requirement for the WORM store.
			 * 
			 * Note: The WriteCache for the WORM store has internal state giving
			 * the firstOffset of the records in the internal buffer - if we do
			 * not call reset() on that write cache then the firstOffset will be
			 * incorrect and the records will be laid down on the wrong offsets
			 * on the store. This correctness issue does not arise for the RW
			 * store because the WriteCache has the actual offset at which each
			 * record will be written and ordered writes are used to lay down
			 * those records. However, for the WORM store we use a single large
			 * write and require that the data in the buffer exactly matches the
			 * target state on the backing file.
			 */
			_bufferStrategy.abort();

            /*
             * The Name2Addr reference will be discarded below. This should be
             * sufficient to ensure that any index requested by the methods on
             * the AbstractJournal will be re-read from disk using the commit
             * record which we re-load below. This is necessary in order to
             * discard any checkpoints that may have been written on indices
             * since the last commit (dirty indices that have not been
             * checkpointed to the disk are discarded when we discard
             * Name2Addr).
             * 
             * Note: Historical index references should NOT be discarded on
             * abort as they remain valid. Discarding them admits the
             * possibility of a non-canonicalizing cache for the historical
             * indices since an existing historical index reference will
             * continue to be held but a new copy of the index will be loaded on
             * the next request if we clear the cache here.
             */
			// historicalIndexCache.clear();
			
			// discard the commit record and re-read from the store.
			_commitRecord = _getCommitRecord();

			/*
			 * Re-load the commit record index from the address in the current
			 * root block.
			 * 
			 * Note: This may not be strictly necessary since the only time we
			 * write on this index is a single record during each commit. So, it
			 * should be valid to simply catch an error during a commit and
			 * discard this index forcing its reload. However, doing this here
			 * is definitely safer.
			 * 
			 * Note: This reads on the store. If the backing channel for a
			 * stable store was closed by an interrupt, e.g., during an abort,
			 * then this will cause the backing channel to be transparent
			 * re-opened. At that point both readers and writers will be able to
			 * access the channel again.
			 */

			// clear reference and reload from the store.
			_commitRecordIndex = _getCommitRecordIndex();
			
			// clear reference and reload from the store.
			_icuVersionRecord = _getICUVersionRecord();
			
			// clear the array of committers.
			_committers = new ICommitter[_committers.length];

			// discard any hard references that might be cached.
			discardCommitters();

            /*
             * Setup new committers, e.g., by reloading from their last root
             * addr.
             */

            setupCommitters();

            if (quorum != null) {

                /*
                 * In HA, we need to tell the QuorumService that the database
                 * has done an abort() so it can discard any local state
                 * associated with the current write set (the HALog file and the
                 * last live HA message).
                 */

                QuorumService<HAGlue> localService = null;
                try {
                
                    localService = quorum.getClient();
                    
                } catch (IllegalStateException ex) {
                    
                    /*
                     * Note: Thrown if the QuorumService is not running.
                     */
                    
                    // ignore.
                }

                if (localService != null) {

                    localService.discardWriteSet();

                }

            }
            
			if (log.isInfoEnabled())
				log.info("done");
			
			success = true; // mark successful abort.

		} catch (Throwable e) {			
			log.error("ABORT FAILED!", e);
			
			throw new RuntimeException("ABORT FAILED", e);
		} finally {
			// @see #1021 (Add critical section protection to AbstractJournal.abort() and BigdataSailConnection.rollback())
			abortRequired.set(!success);

			lock.unlock();

		}

    }

	/**
	 * Rollback a journal to its previous commit point.
	 * <p>
	 * Note: You MUST have an exclusive write lock on the journal.
	 * <p>
	 * Note: To restore the last root block we copy the alternative root block
	 * over the current root block. That gives us two identical root blocks and
	 * restores us to the root block that was in effect before the last commit.
	 * 
	 * @deprecated Do not use this method. HA provides point in time restore. Use
	 * that.  Or you can open a journal using the alternate root block by specifying
	 * {@link Options#ALTERNATE_ROOT_BLOCK}
	 */
	public void rollback() {

		final WriteLock lock = _fieldReadWriteLock.writeLock();

		lock.lock();

		try {

			assertOpen();

			if (isReadOnly())
				throw new IllegalStateException();

			txLog.warn("ROLLBACK");

			/*
			 * Read the alternate root block (NOT the current one!).
			 */
			final ByteBuffer buf = _bufferStrategy.readRootBlock(!_rootBlock.isRootBlock0());

			/*
			 * Create a view from the alternate root block, but using the SAME
			 * [rootBlock0] flag state as the current root block so that this
			 * will overwrite the current root block.
			 */
			final IRootBlockView newRootBlock = new RootBlockView(_rootBlock.isRootBlock0(), buf, checker);

			/*
			 * Overwrite the current root block on the backing store with the
			 * state of the alternate root block.
			 */
			_bufferStrategy.writeRootBlock(newRootBlock, forceOnCommit);

			// Use the new root block.
			_rootBlock = newRootBlock;

			/*
			 * Discard all in-memory state - it will need be re-loaded from the
			 * restored root block.
			 */
			abort();
			
			/**
			 * Ensure cache is clear to prevent access to invalid BTree from last
			 * commit point
			 */
			historicalIndexCache.clear();
			indexCache.clear();

		} finally {

			lock.unlock();

		}

	}

//	/**
//	 * Return the object providing the {@link AbstractLocalTransactionManager}
//	 * for this journal.
//	 */
//	abstract public AbstractLocalTransactionManager getLocalTransactionManager();

    @Override
    public boolean isDirty() {

        return _bufferStrategy.isDirty();

    }

    /**
     * Get timestamp that will be assigned to this commit point.
     * <P>
     * Note: This will spin until commit time advances over
     * <code>lastCommitTime</code>, but not for more than N milliseconds. This
     * will allow us to ensure that time moves forward when the leader fails
     * over to another node with modest clock skew. If there is a large clock
     * skew, the operator intervention will be required.
     * <p>
     * Note: This also makes sense for a non-HA deployment since we still want
     * time to move forward at each commit point.
     * 
     * TODO This also makes sense when the Journal is opened since we often
     * issue queries against historical commit points on the journal based on
     * the clock. [Unit test for this in standalone and HA modes?]
     */
    private long nextCommitTimestamp() {
        final IRootBlockView rootBlock = _rootBlock;
        final long lastCommitTime = rootBlock.getLastCommitTime();
        if (lastCommitTime < 0)
            throw new RuntimeException(
                    "Last commit time is invalid in rootBlock: " + rootBlock);
        final long commitTime;
        {
            final ILocalTransactionManager transactionManager = getLocalTransactionManager();

            boolean warned = false;
            while (true) {
                final long t = transactionManager.nextTimestamp();
                if (t > lastCommitTime) {
                    /*
                     * We have a distinct timestamp. Time is moving forward.
                     */
                    commitTime = t;
                    break;
                }
                /*
                 * Time is going backwards.  Figure out by how much.
                 * 
                 * Note: delta is in ms.
                 */
                final long delta = Math.abs(t - lastCommitTime);
                if (delta > getMaximumClockSkewMillis()/* ms */)
                    throw new ClocksNotSynchronizedException("Clocks off by "
                            + delta + " ms: lastCommitTime=" + lastCommitTime
                            + ", but localTimestamp=" + t);
                if (!warned) {
                    log.warn("Clocks off by " + delta + " ms: lastCommitTime="
                            + lastCommitTime + ", but localTimestamp=" + t);
                    warned = true;
                }
                try {
                    // Wait for the delta to expire.
                    Thread.sleep(delta/* ms */);
                } catch (InterruptedException ex) {
                    // Propagate interrupt.
                    Thread.currentThread().interrupt();
                }
            }
        }
        return commitTime;
    }
    
    @Override
	public long commit() {
    	
		// The timestamp to be assigned to this commit point.
		final long commitTime = nextCommitTimestamp();

        // do the commit.
        final IRootBlockView lastRootBlock = _rootBlock;
        final long commitTime2;
        try {
            commitTime2 = commitNow(commitTime);
        } catch (Throwable t) {
            throw new RuntimeException(t.getLocalizedMessage()
                    + ": lastRootBlock=" + lastRootBlock, t);
        }

		if (commitTime2 == 0L) {

			// Nothing to commit.

			return 0L;

		}

		// commitNow() should return either 0L or the commitTime we gave it.
		assert commitTime2 == commitTime;

		/*
		 * Now that we have committed the data we notify the federation that it
		 * should advance its global lastCommitTime.
		 * 
		 * @todo we could use IBufferStrategy#rollback() if the notice failed,
		 * e.g., due to a service outage.
		 */

        getLocalTransactionManager().notifyCommit(commitTime);

		return commitTime;

	}

    /**
     * Performance counters for the journal-level commit operations.
     */
    private static class CommitCounters implements ICounterSetAccess {
        /**
         * Elapsed nanoseconds for the {@link ICommitter#handleCommit(long)}
         * (flushing dirty pages from the indices into the write cache service).
         */
        private final CAT elapsedNotifyCommittersNanos = new CAT();
        /**
         * Elapsed nanoseconds for {@link CommitState#writeCommitRecord()}.
         * Note: This is also responsible for recycling the deferred frees for
         * {@link IHistoryManager} backends.
         */
        private final CAT elapsedWriteCommitRecordNanos = new CAT();
        /**
         * Elapsed nanoseconds for flushing the write set from the write cache
         * service to the backing store (this is the bulk of the disk IO unless
         * the write cache service fills up during a long running commit, in
         * which case there is also incremental eviction).
         */
        private final CAT elapsedFlushWriteSetNanos = new CAT();
        /**
         * Elapsed nanoseconds for the simple atomic commit (non-HA). This
         * consists of sync'ing the disk (iff double-sync is enabled), writing
         * the root block, and then sync'ing the disk.
         */
        private final CAT elapsedSimpleCommitNanos = new CAT();
        /**
         * Elapsed nanoseconds for the entire commit protocol.
         */
        private final CAT elapsedTotalCommitNanos = new CAT();

        //
        // HA counters
        //
        
        /**
         * Elapsed nanoseconds for GATHER (consensus release time protocol : HA
         * only).
         */
        private final CAT elapsedGatherNanos = new CAT();
        /**
         * Elapsed nanoseconds for PREPARE (2-phase commit: HA only).
         */
        private final CAT elapsedPrepare2PhaseNanos = new CAT();
        /**
         * Elapsed nanoseconds for COMMIT2PHASE (2-phase commit: HA only).
         */
        private final CAT elapsedCommit2PhaseNanos = new CAT();

        @Override
        public CounterSet getCounters() {

            final CounterSet root = new CounterSet();

            root.addCounter("notifyCommittersSecs", new Instrument<Double>() {
                @Override
                public void sample() {
                    final double secs = (elapsedNotifyCommittersNanos.get() / 1000000000.);
                    setValue(secs);
                }
            });
            
            root.addCounter("writeCommitRecordSecs", new Instrument<Double>() {
                @Override
                public void sample() {
                    final double secs = (elapsedWriteCommitRecordNanos.get() / 1000000000.);
                    setValue(secs);
                }
            });
            
            root.addCounter("flushWriteSetSecs", new Instrument<Double>() {
                @Override
                public void sample() {
                    final double secs = (elapsedFlushWriteSetNanos.get() / 1000000000.);
                    setValue(secs);
                }
            });
            
            root.addCounter("simpleCommitSecs", new Instrument<Double>() {
                @Override
                public void sample() {
                    final double secs = (elapsedSimpleCommitNanos.get() / 1000000000.);
                    setValue(secs);
                }
            });
            
            root.addCounter("totalCommitSecs", new Instrument<Double>() {
                @Override
                public void sample() {
                    final double secs = (elapsedTotalCommitNanos.get() / 1000000000.);
                    setValue(secs);
                }
            });
            
            //
            // HA
            //
            
            root.addCounter("gatherSecs", new Instrument<Double>() {
                @Override
                public void sample() {
                    final double secs = (elapsedGatherNanos.get() / 1000000000.);
                    setValue(secs);
                }
            });
            
            root.addCounter("prepare2PhaseSecs", new Instrument<Double>() {
                @Override
                public void sample() {
                    final double secs = (elapsedPrepare2PhaseNanos.get() / 1000000000.);
                    setValue(secs);
                }
            });

            root.addCounter("commit2PhaseSecs", new Instrument<Double>() {
                @Override
                public void sample() {
                    final double secs = (elapsedCommit2PhaseNanos.get() / 1000000000.);
                    setValue(secs);
                }
            });

            return root;
            
        }
    }
    final private CommitCounters commitCounters = new CommitCounters();
    
    /**
     * Class to which we attach all of the little pieces of state during
     * {@link AbstractJournal#commitNow(long)}.
     * <p>
     * The non-final fields in this class are laid directly below the method
     * which set those fields. The methods in the class are laid out in the
     * top-to-bottom order in which they are executed by commitNow().
     */
    static private class CommitState {
        
        /**
         * The timestamp at which the commit began.
         */
        private final long beginNanos;

        /**
         * The backing store. 
         */
        private final AbstractJournal store;

        /**
         * The backing {@link IBufferStrategy} for the {@link #store}.
         */
        private final IBufferStrategy _bufferStrategy;
        
        /**
         * The quorum iff HA and <code>null</code> otherwise.
         */
        private final Quorum<HAGlue, QuorumService<HAGlue>> quorum;

        /**
         * Local HA service implementation (non-Remote) and <code>null</code> if
         * not in an HA mode..
         */
        private final QuorumService<HAGlue> quorumService;
        
        /**
         * The commit time either of a transaction or of an unisolated commit.
         * Note that when mixing isolated and unisolated commits you MUST use
         * the same {@link ITimestampService} for both purposes.
         */
        private final long commitTime;
        
        /**
         * The current root block on the journal as of the start of the commit
         * protocol.
         */
        private final IRootBlockView old;

        /**
         * The quorum token associated with this commit point.
         */
        private final long commitToken;
        
//        /** The #of bytes on the journal as of the previous commit point. */
//        private final long byteCountBefore;
        
        /**
         * The commit counter that will be assigned to the new commit point.
         */
        private final long newCommitCounter;
        
        /**
         * 
         * @param store
         *            The backing store.
         * @param commitTime
         *            The commit time either of a transaction or of an
         *            unisolated commit. Note that when mixing isolated and
         *            unisolated commits you MUST use the same
         *            {@link ITimestampService} for both purposes.
         */
        public CommitState(final AbstractJournal store, final long commitTime) {

            if (store == null)
                throw new IllegalArgumentException();
            
            this.beginNanos = System.nanoTime();

            this.store = store;

            this.commitTime = commitTime;

            this._bufferStrategy = store._bufferStrategy;
            
            // Note: null if not HA.
            this.quorum = store.quorum;

            /*
             * Local HA service implementation (non-Remote).
             * 
             * Note: getClient() throws IllegalStateException if quorum exists
             * and is not not running.
             */
            this.quorumService = quorum == null ? null : quorum.getClient();

            this.old = store._rootBlock;

//            // #of bytes on the journal as of the previous commit point.
//            this.byteCountBefore = store._rootBlock.getNextOffset();

            this.newCommitCounter = old.getCommitCounter() + 1;

            this.commitToken = store.quorumToken;
            
            store.assertCommitTimeAdvances(commitTime);
            
        }

        /**
         * Notify {@link ICommitter}s to flush out application data. This sets
         * the {@link #rootAddrs} for the {@link ICommitRecord}.
         * 
         * @return <code>true</code> if the store is dirty and the commit should
         *         proceed and <code>false</code> otherwise.
         */
        private boolean notifyCommitters() {

            final long beginNanos = System.nanoTime();
            
            /*
             * First, run each of the committers accumulating the updated root
             * addresses in an array. In general, these are btrees and they may
             * have dirty nodes or leaves that needs to be evicted onto the
             * store. The first time through, any newly created btrees will have
             * dirty empty roots (the btree code does not optimize away an empty
             * root at this time). However, subsequent commits without
             * intervening data written on the store should not cause any
             * committers to update their root address.
             * 
             * Note: This also checkpoints the deferred free block list.
             */
            rootAddrs = store.notifyCommitters(commitTime);

            /*
             * See if anything has been written on the store since the last
             * commit.
             */
            if (!_bufferStrategy.requiresCommit(store._rootBlock)) {

                /*
                 * Will not do commit.
                 * 
                 * Note: No data was written onto the store so the commit can
                 * not achieve any useful purpose.
                 */

                return false;
                
            }
            
            /*
             * Explicitly call the RootBlockCommitter
             * 
             * Note: This logs the current root block and set the address of
             * that root block in the as a root address in the commitRecord.
             * This is of potential use solely in disaster recovery scenarios
             * where your root blocks are toast, but good root blocks can be
             * found elsewhere in the file. Once you find a root block, you can
             * get the commitRecordIndex and then find earlier root blocks using
             * that root addr. Or you can just scan the file looking for valid
             * root blocks and then use the most recent one that you can find.
             */
            rootAddrs[PREV_ROOTBLOCK] = store.m_rootBlockCommitter
                    .handleCommit(commitTime);

            store.commitCounters.elapsedNotifyCommittersNanos.add(System
                    .nanoTime() - beginNanos);
            
            // Will do commit.
            return true;

        }

        /**
         * The new root addresses for the {@link ICommitRecord}.
         * 
         * @see #notifyCommitters()
         */
        private long[] rootAddrs;
        
        /**
         * Write out the {@link ICommitRecord}, noting the
         * {@link #commitRecordAddr}, add the {@link ICommitRecord} to the
         * {@link CommitRecordIndex}. Finally, checkpoint the
         * {@link CommitRecordIndex} setting the {@link #commitRecordIndexAddr}.
         * <p>
         * Note: This is also responsible for recycling the deferred frees for
         * {@link IHistoryManager} backends.
         */
        private void writeCommitRecord() {

            final long beginNanos = System.nanoTime();
            
            /*
             * Before flushing the commitRecordIndex we need to check for
             * deferred frees that will prune the index.
             * 
             * This is responsible for recycling the deferred frees (RWS).
             * 
             * Do this BEFORE adding the new commit record since that commit
             * record will otherwise be immediately removed if no history is
             * retained.
             */
            if (_bufferStrategy instanceof IHistoryManager) {

                ((IHistoryManager) _bufferStrategy)
                        .checkDeferredFrees(store);

            }

            final ICommitRecord commitRecord = new CommitRecord(commitTime,
                    newCommitCounter, rootAddrs);

            this.commitRecordAddr = store.write(ByteBuffer
                    .wrap(CommitRecordSerializer.INSTANCE
                            .serialize(commitRecord)));

            /*
             * Add the commit record to an index so that we can recover
             * historical states efficiently.
             */
            store._commitRecordIndex.add(commitRecordAddr, commitRecord);
            
            /*
             * Flush the commit record index to the store and stash the address
             * of its metadata record in the root block.
             * 
             * Note: The address of the root of the CommitRecordIndex itself
             * needs to go right into the root block. We are unable to place it
             * into the commit record since we need to serialize the commit
             * record, get its address, and add the entry to the
             * CommitRecordIndex before we can flush the CommitRecordIndex to
             * the store.
             */
            commitRecordIndexAddr = store._commitRecordIndex
                    .writeCheckpoint();

            store.commitCounters.elapsedWriteCommitRecordNanos.add(System.nanoTime()
                    - beginNanos);
            
        }
        
        /**
         * The address of the {@link ICommitRecord}.
         * 
         * @see #writeCommitRecord()
         */
        private long commitRecordAddr;
        
        /**
         * The address of the {@link CommitRecordIndex} once it has been
         * checkpointed against the backing store.
         * <p>
         * Note: The address of the root of the {@link CommitRecordIndex} needs
         * to go right into the {@link IRootBlockView}. We are unable to place
         * it into the {@link ICommitRecord} since we need to serialize the
         * {@link ICommitRecord}, get its address, and add the entry to the
         * {@link CommitRecordIndex} before we can flush the
         * {@link CommitRecordIndex} to the store.
         * 
         * @see #writeCommitRecord()
         */
        private long commitRecordIndexAddr;
        
        /**
         * Call commit on {@link IBufferStrategy} prior to creating the new
         * {@link IRootBlockView}. This will flush the {@link WriteCacheService}
         * . For HA, that ensures that the write set has been replicated to the
         * followers.
         * <p>
         * Note: required for {@link RWStore} since the metaBits allocations are
         * not made until commit, leading to invalid addresses for recent store
         * allocations.
         * <p>
         * Note: After this, we do not write anything on the backing store other
         * than the root block. The rest of this code is dedicated to creating a
         * properly formed root block. For a non-HA deployment, we just lay down
         * the root block. For an HA deployment, we do a 2-phase commit.
         * <p>
         * Note: In HA, the followers lay down the replicated writes
         * synchronously. Thus, they are guaranteed to be on local storage by
         * the time the leader finishes WriteCacheService.flush(). This does not
         * create much latency because the WriteCacheService drains the
         * dirtyList in a seperate thread.
         */
        private void flushWriteSet() {

            final long beginNanos = System.nanoTime();

            _bufferStrategy.commit();

            store.commitCounters.elapsedFlushWriteSetNanos.add(System.nanoTime()
                    - beginNanos);

        }

        /**
         * Create the new root block.
         */
        private void newRootBlock() {

            /*
             * The next offset at which user data would be written. Calculated,
             * after commit!
             */
            final long nextOffset = _bufferStrategy.getNextOffset();

            final long blockSequence;
            if (_bufferStrategy instanceof IHABufferStrategy) {

                // always available for HA.
                blockSequence = ((IHABufferStrategy) _bufferStrategy)
                        .getBlockSequence();

            } else {

                blockSequence = old.getBlockSequence();

            }

            /*
             * Update the firstCommitTime the first time a transaction commits
             * and the lastCommitTime each time a transaction commits (these are
             * commit timestamps of isolated or unisolated transactions).
             */

            final long firstCommitTime = (old.getFirstCommitTime() == 0L ? commitTime
                    : old.getFirstCommitTime());

            final long priorCommitTime = old.getLastCommitTime();

            if (priorCommitTime != 0L) {

                /*
                 * This is a local sanity check to make sure that the commit
                 * timestamps are strictly increasing. An error will be reported
                 * if the commit time for the current (un)isolated transaction
                 * is not strictly greater than the last commit time on the
                 * store as read back from the current root block.
                 */

                assertPriorCommitTimeAdvances(commitTime, priorCommitTime);

            }

            final long lastCommitTime = commitTime;
            final long metaStartAddr = _bufferStrategy.getMetaStartAddr();
            final long metaBitsAddr = _bufferStrategy.getMetaBitsAddr();

            // Create the new root block.
            newRootBlock = new RootBlockView(!old.isRootBlock0(),
                    old.getOffsetBits(), nextOffset, firstCommitTime,
                    lastCommitTime, newCommitCounter, commitRecordAddr,
                    commitRecordIndexAddr,
                    old.getUUID(), //
                    blockSequence,
                    commitToken,//
                    metaStartAddr, metaBitsAddr, old.getStoreType(),
                    old.getCreateTime(), old.getCloseTime(), old.getVersion(),
                    store.checker);

        }

        /**
         * The new {@link IRootBlockView}.
         * 
         * @see #newRootBlock()
         */
        private IRootBlockView newRootBlock;
        

        /**
         * Run the GATHER consensus protocol (iff HA).
         */
        private void gatherPhase() {

            final long beginNanos = System.nanoTime();

            /*
             * If not HA, do not do GATHER.
             */

            if (!(_bufferStrategy instanceof IHABufferStrategy))
                return;

            if (quorum == null)
                return;

            if (!quorum.isHighlyAvailable()) {
                // Gather and 2-phase commit are not used in HA1.
                return;
            }

            /**
             * CRITICAL SECTION. We need obtain a distributed consensus for the
             * services joined with the met quorum concerning the earliest
             * commit point that is pinned by the combination of the active
             * transactions and the minReleaseAge on the TXS. New transaction
             * starts during this critical section will block (on the leader or
             * the folllower) unless they are guaranteed to be allowable, e.g.,
             * based on the current minReleaseAge, the new tx would read from
             * the most recent commit point, the new tx would ready from a
             * commit point that is already pinned by an active transaction on
             * that node, etc.
             * 
             * Note: Lock makes this section MUTEX with awaitServiceJoin().
             * 
             * @see <a href=
             *      "https://docs.google.com/document/d/14FO2yJFv_7uc5N0tvYboU-H6XbLEFpvu-G8RhAzvxrk/edit?pli=1#"
             *      > HA TXS Design Document </a>
             * 
             * @see <a
             *      href="https://sourceforge.net/apps/trac/bigdata/ticket/623"
             *      > HA TXS / TXS Bottleneck </a>
             */

            store._gatherLock.lock();

            try {

                // Atomic decision point for GATHER re joined services.
                gatherJoinedAndNonJoinedServices = new JoinedAndNonJoinedServices(
                        quorum);

                // Run the GATHER protocol.
                consensusReleaseTime = ((AbstractHATransactionService) store
                        .getLocalTransactionManager().getTransactionService())
                        .updateReleaseTimeConsensus(newCommitCounter,
                                commitTime, gatherJoinedAndNonJoinedServices
                                        .getJoinedServiceIds(), store
                                        .getHAReleaseTimeConsensusTimeout(),
                                TimeUnit.MILLISECONDS);

            } catch (Exception ex) {

                log.error(ex, ex);

                // Wrap and rethrow.
                throw new RuntimeException(ex);

            } finally {

                store._gatherLock.unlock();

                store.commitCounters.elapsedGatherNanos.add(System.nanoTime()
                        - beginNanos);

            }

        }

        /**
         * Set by {@link #gatherPhase()} IFF HA.
         */
        private IJoinedAndNonJoinedServices gatherJoinedAndNonJoinedServices = null;
        /**
         * Set by {@link #gatherPhase()} IFF HA.
         */
        private IHANotifyReleaseTimeResponse consensusReleaseTime = null;

        /**
         * Simple (non-HA) commit.
         */
        private void commitSimple() {
            
            final long beginNanos = System.nanoTime();
            
            /*
             * Force application data to stable storage _before_
             * we update the root blocks. This option guarantees
             * that the application data is stable on the disk
             * before the atomic commit. Some operating systems
             * and/or file systems may otherwise choose an
             * ordered write with the consequence that the root
             * blocks are laid down on the disk before the
             * application data and a hard failure could result
             * in the loss of application data addressed by the
             * new root blocks (data loss on restart).
             * 
             * Note: We do not force the file metadata to disk.
             * If that is done, it will be done by a force()
             * after we write the root block on the disk.
             */
            if (store.doubleSync) {

                _bufferStrategy.force(false/* metadata */);

            }

            // write the root block on to the backing store.
            _bufferStrategy.writeRootBlock(newRootBlock, store.forceOnCommit);

            if (_bufferStrategy instanceof IRWStrategy) {

                /*
                 * Now the root blocks are down we can commit any transient
                 * state.
                 */

                ((IRWStrategy) _bufferStrategy).postCommit();

            }
            
            // set the new root block.
            store._rootBlock = newRootBlock;

            // reload the commit record from the new root block.
            store._commitRecord = store._getCommitRecord();

            if (quorum != null) {
                /**
                 * Write the root block on the HALog file, closing out that
                 * file.
                 * 
                 * @see <a href="http://trac.blazegraph.com/ticket/721"> HA1 </a>
                 */
                final QuorumService<HAGlue> localService = quorum.getClient();
                if (localService != null) {
                    // Quorum service not asynchronously closed.
                    try {
                        // Write the closing root block on the HALog file.
                        localService.logRootBlock(newRootBlock);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            
            if (txLog.isInfoEnabled())
                txLog.info("COMMIT: commitTime=" + commitTime);

            store.commitCounters.elapsedSimpleCommitNanos.add(System.nanoTime()
                    - beginNanos);

        }

        /**
         * HA mode commit (2-phase commit).
         */
        private void commitHA() {

            try {
                
                prepare2Phase();
                
                commit2Phase();
                
            } catch (Exception e) {

                // launder throwable.
                throw new RuntimeException(e);
                
            }
            
        }
        
        /**
         * PREPARE
         * <p>
         * Note: We need to make an atomic decision here regarding whether a
         * service is joined with the met quorum or not. This information will
         * be propagated through the HA 2-phase prepare message so services will
         * know how they must intepret the 2-phase prepare(), commit(), and
         * abort() requests. The atomic decision is necessary in order to
         * enforce a consistent role on a services that is resynchronizing and
         * which might vote to join the quorum and enter the quorum
         * asynchronously with respect to this decision point.
         * 
         * TODO If necessary, we could also explicitly provide the zk version
         * metadata for the znode that is the parent of the joined services.
         * However, we would need an expanded interface to get that metadata
         * from zookeeper out of the Quorum.
         * 
         * @throws IOException
         * @throws TimeoutException
         * @throws InterruptedException
         */
        private void prepare2Phase() throws InterruptedException,
                TimeoutException, IOException {
 
            final long beginNanos = System.nanoTime();
            boolean didPrepare = false;
            try {

                // Atomic decision point for joined vs non-joined services.
                prepareJoinedAndNonJoinedServices = new JoinedAndNonJoinedServices(
                        quorum);

                prepareRequest = new PrepareRequest(//
                        consensusReleaseTime,//
                        gatherJoinedAndNonJoinedServices,//
                        prepareJoinedAndNonJoinedServices,//
                        newRootBlock,//
                        quorumService.getPrepareTimeout(), // timeout
                        TimeUnit.MILLISECONDS//
                );

                // issue prepare request.
                prepareResponse = quorumService.prepare2Phase(prepareRequest);

                if (haLog.isInfoEnabled())
                    haLog.info(prepareResponse.toString());
                
                if (!prepareResponse.willCommit()) {
                    
                    // PREPARE rejected.  
                    throw new QuorumException("PREPARE rejected: nyes="
                            + prepareResponse.getYesCount() + ", replicationFactor="
                            + prepareResponse.replicationFactor());

                }
                didPrepare = true;

            } finally {

                if (!didPrepare) {

                    /*
                     * Something went wrong. Any services that were in the
                     * pipeline could have a dirty write set. Services that
                     * voted NO will have already discarded their dirty write
                     * set. We issue an abort2Phase() to tell the other services
                     * to discard the dirty write set as well.
                     * 
                     * TODO We only need to issue the 2-phase abort against
                     * those services that (a) were joined with the met quorum;
                     * and (b) voted YES in response to the PREPARE message (any
                     * service that voted NO already discarded its dirty write
                     * set).
                     * 
                     * TODO The service should only do the 2-phase abort if the
                     * commitToken and commitCounter are valid. If the quorum
                     * breaks, then the services will move into the Error state
                     * and will do a local abort as part of that transition.
                     */
                    
                    try {
                        quorumService.abort2Phase(commitToken);
                    } catch (Throwable t) {
                        log.warn(t, t);
                    }

                }
                
                store.commitCounters.elapsedPrepare2PhaseNanos.add(System
                        .nanoTime() - beginNanos);

            }
            
        }
        // Fields set by the method above.
        private IJoinedAndNonJoinedServices prepareJoinedAndNonJoinedServices;
        private PrepareRequest prepareRequest;
        private PrepareResponse prepareResponse;
        
        /**
         * COMMIT.
         * 
         * Pre-condition: PREPARE was successful on a majority of the services.
         */
        private void commit2Phase() throws Exception {
            
            final long beginNanos = System.nanoTime();
            boolean didCommit = false;
            try {

                /*
                 * Prepare was successful. COMMIT message has been formed. We
                 * will now commit.
                 * 
                 * Note: The overall commit will fail unless we can prove that a
                 * majority of the services successfully committed.
                 */
                
                commitRequest = new CommitRequest(prepareRequest,
                        prepareResponse);

                commitResponse = quorumService.commit2Phase(commitRequest);

                if (!store.quorum.isQuorum(commitResponse.getNOk())) {

                    /*
                     * Fail the commit.
                     * 
                     * Note: An insufficient number of services were able to
                     * COMMIT successfully.
                     * 
                     * Note: It is possible that a commit could be failed here
                     * when the commit is in fact stable on a majority of
                     * services. For example, with k=3 and 2 services running if
                     * both of them correctly update their root blocks but we
                     * lose network connectivity to the follower before the RMI
                     * returns, then we will fail the commit.
                     */
                    
                    // Note: Guaranteed to not return normally!
                    commitResponse.throwCauses();

                }

                didCommit = true;

            } finally {

                if (!didCommit) {

                    /*
                     * The quorum voted to commit, but something went wrong.
                     * 
                     * This forces the leader to fail over. The quorum can then
                     * meet up again around a new consensus.
                     * 
                     * Note: It is possible that a new consensus can not be
                     * formed. The 2-phase commit protocol does not handle some
                     * cases of compound failure. For example, consider the case
                     * where the HA cluster is running with bare majority of
                     * services. All services that are active vote YES, but one
                     * of those services fails before processing the COMMIT
                     * message. The quorum will not meet again unless a new
                     * consensus can be formed from the services that remain up
                     * and the services that were not running. The services that
                     * were not running will be at an earlier commit point so
                     * they can not form a consensus with the services that
                     * remain up. Further, there can not be a majority among the
                     * services that were not running (except in the edge case
                     * where the failed commit was the first commit since the
                     * last commit on one of the services that was down at the
                     * start of that failed commit). Situations of this type
                     * require operator intervention. E.g., explicitly rollback
                     * the database or copy HALog files from one machine to
                     * another such that it will apply those HALog files on
                     * restart and form a consensus with the other services.
                     */

                    quorumService.enterErrorState();

                }

                store.commitCounters.elapsedCommit2PhaseNanos.add(System
                        .nanoTime() - beginNanos);

            }

        }
        // Fields set by the method above.
        private CommitRequest commitRequest;
        private CommitResponse commitResponse;
        
    } // class CommitState.
    
	/**
	 * An atomic commit is performed by directing each registered
	 * {@link ICommitter} to flush its state onto the store using
	 * {@link ICommitter#handleCommit(long)}. The address returned by that
	 * method is the address from which the {@link ICommitter} may be reloaded
	 * (and its previous address if its state has not changed). That address is
	 * saved in the {@link ICommitRecord} under the index for which that
	 * committer was {@link #registerCommitter(int, ICommitter) registered}. We
	 * then force the data to stable store, update the root block, and force the
	 * root block and the file metadata to stable store.
	 * <p>
	 * Note: Each invocation of this method MUST use a distinct
	 * <i>commitTime</i> and the commitTimes MUST be monotonically increasing.
	 * These guarantees support both the database version history mechanisms and
	 * the High Availability mechanisms.
	 * 
	 * @param commitTime
	 *            The commit time either of a transaction or of an unisolated
	 *            commit. Note that when mixing isolated and unisolated commits
	 *            you MUST use the same {@link ITimestampService} for both
	 *            purposes.
	 * 
	 * @return The timestamp assigned to the commit record -or- 0L if there were
	 *         no data to commit.
	 */
    // Note: Overridden by StoreManager (DataService).
	protected long commitNow(final long commitTime) {
	    
	    final long beginNanos = System.nanoTime();
	    
        final WriteLock lock = _fieldReadWriteLock.writeLock();

        lock.lock();

        try {
            
			assertOpen();

            if (log.isInfoEnabled())
                log.info("commitTime=" + commitTime);

        	// Critical Section Check. @see #1021 (Add critical section protection to AbstractJournal.abort() and BigdataSailConnection.rollback())
        	if (abortRequired.get()) 
        		throw new AbortRequiredException();

            final CommitState cs = new CommitState(this, commitTime);

            /*
             * Flush application data, decide whether or not the store is dirty,
             * and return immediately if it is not dirty.
             */
            if (!cs.notifyCommitters()) {

                if (log.isInfoEnabled())
                    log.info("Nothing to commit");

                return 0L;

            }

            // Do GATHER (iff HA).
            cs.gatherPhase();

            /*
             * Flush deferred frees (iff RWS), write the commit record onto the
             * store, and write the commit record index onto the store.
             */
            cs.writeCommitRecord();

            if (quorum != null) {
                /*
                 * Verify that the last negotiated quorum is still valid.
                 */
                quorum.assertLeader(cs.commitToken);
            }

            /*
             * Conditionally obtain a lock that will protect the
             * commit()/postCommit() protocol.
             */
//            final long nextOffset;
            final Lock commitLock;
            if (_bufferStrategy instanceof IRWStrategy) {
                commitLock = ((IRWStrategy) _bufferStrategy).getCommitLock();
            } else {
                commitLock = null;
            }
            if (commitLock != null) {
                // Take the commit lock.
                commitLock.lock();
            }
            try {

                // Flush writes to the backing store / followers.
                cs.flushWriteSet();
                
                // Prepare the new root block.
                cs.newRootBlock();

                if (quorum == null || quorum.replicationFactor() == 1) {
                    
                    // Non-HA mode (including HA1).
                    cs.commitSimple();
    
                } else {

                    // HA mode commit (2-phase commit).
                    cs.commitHA();
                        
                } // else HA mode

            } finally {

                if (commitLock != null) {
                    /*
                     * Release the [commitLock] iff one was taken above.
                     */
                    commitLock.unlock();
                }

            }

			final long elapsedNanos = System.nanoTime() - cs.beginNanos;

			if (BigdataStatics.debug || log.isInfoEnabled()) {
                final String msg = "commit: commitTime=" + cs.commitTime
                        + ", commitCounter=" + cs.newCommitCounter
                        + ", latency="
                        + TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
//                        + ", nextOffset="
//                        + cs.newRootBlock.getNextOffset()
//                        + ", byteCount="
//                        + (cs.newRootBlock.getNextOffset() - cs.byteCountBefore);
                if (BigdataStatics.debug)
					System.err.println(msg);
				else if (log.isInfoEnabled())
					log.info(msg);
//				if (BigdataStatics.debug && LRUNexus.INSTANCE != null) {
//					System.err.println(LRUNexus.INSTANCE.toString());
//				}
			}

			return cs.commitTime;

		} finally {

			lock.unlock();
			
            commitCounters.elapsedTotalCommitNanos.add(System.nanoTime()
                    - beginNanos);
			
        }
        
    }

//    /**
//     * (debug only) For the {@link RWStrategy}, scans the
//     * {@link #historicalIndexCache} and verifies that there are no checkpoint
//     * addresses present which are "locked".
//     */
//    private boolean assertHistoricalIndexCacheIsClean() {
//        
//        if (true) return true; // disable
//
//        if (!(getBufferStrategy() instanceof RWStrategy))
//            return true;
//
//        final RWStrategy bufferStrategy = (RWStrategy) getBufferStrategy();
//
//        final Iterator<Map.Entry<Long, WeakReference<BTree>>> itr = historicalIndexCache
//                .entryIterator();
//
//        while (itr.hasNext()) {
//
//            final Map.Entry<Long, WeakReference<BTree>> e = itr.next();
//
//            bufferStrategy.assertNotLocked(e.getKey());
//
//            final BTree btree = e.getValue().get();
//
//            if (btree != null) {
//
//                bufferStrategy.assertNotLocked(btree.getCheckpoint()
//                        .getCheckpointAddr());
//            }
//
//        }
//
//        return true;
//
//    }
    
    /**
	 * Method verifies that the commit time strictly advances on the local store
	 * by checking against the current root block.
	 * 
	 * @param commitTime
	 *            The proposed commit time.
	 * 
	 * @throws IllegalArgumentException
	 *             if the <i>commitTime</i> is LTE the value reported by
	 *             {@link IRootBlockView#getLastCommitTime()}.
	 */
	protected void assertCommitTimeAdvances(final long commitTime) {

		if (commitTime <= _rootBlock.getLastCommitTime()) {

			/*
			 * The commit times must strictly advance.
			 */

			throw new IllegalArgumentException();

		}

	}

    /**
    * Method verifies that the commit time strictly advances on the local store
    * by checking against the current root block.
    * 
    * @param currentCommitTime
    * @param priorCommitTime
    * 
    * @throws IllegalArgumentException
    *            if the <i>commitTime</i> is LTE the value reported by
    *            {@link IRootBlockView#getLastCommitTime()}.
    */
    static protected void assertPriorCommitTimeAdvances(
            final long currentCommitTime, final long priorCommitTime) {

        if (currentCommitTime <= priorCommitTime) {

            throw new RuntimeException("Time goes backwards: commitTime="
                    + currentCommitTime + ", but lastCommitTime="
                    + priorCommitTime + " on the current root block");

        }

    }

    @Override
	public void force(final boolean metadata) {

		assertOpen();

		_bufferStrategy.force(metadata);

	}

	@Override
	public long size() {

		return _bufferStrategy.size();

	}

    @Override
    public ByteBuffer read(final long addr) {
            assertOpen();

        assertCanRead();
            
        return _bufferStrategy.read(addr);
            
	}
    
    @Override
    public long write(final ByteBuffer data) {

        assertCanWrite();

        return _bufferStrategy.write(data);
	
    }

    @Override
    public long write(final ByteBuffer data, final IAllocationContext context) {

        assertCanWrite();

        if (_bufferStrategy instanceof IRWStrategy) {

            return ((IRWStrategy) _bufferStrategy).write(data, context);
            
        } else {

            return _bufferStrategy.write(data);

        }
        
    }

	@Override
	public IPSOutputStream getOutputStream() {
        assertCanWrite();

		return _bufferStrategy.getOutputStream();
	}

	@Override
	public IPSOutputStream getOutputStream(final IAllocationContext context) {

		assertCanWrite();

		if (_bufferStrategy instanceof IRWStrategy) {

			return ((IRWStrategy) _bufferStrategy).getOutputStream(context);

		} else {

			return _bufferStrategy.getOutputStream();

		}

	}
	
	@Override
	public InputStream getInputStream(long addr) {
		return _bufferStrategy.getInputStream(addr);
	}


	// Note: NOP for WORM. Used by RW for eventual recycle protocol.
    @Override
    public void delete(final long addr) {

        assertCanWrite();

        _bufferStrategy.delete(addr);

    }

    @Override
    public void delete(final long addr, final IAllocationContext context) {

        assertCanWrite();

        if(_bufferStrategy instanceof IRWStrategy) {
        
            ((IRWStrategy) _bufferStrategy).delete(addr, context);
            
        } else {
            
            _bufferStrategy.delete(addr);
            
        }

    }
    
    @Override
    public void detachContext(final IAllocationContext context) {
        
        assertCanWrite();

        if(_bufferStrategy instanceof IRWStrategy) {

            ((IRWStrategy) _bufferStrategy).detachContext(context);
            
        }
    	
    }

    @Override
    public void abortContext(final IAllocationContext context) {
        
        assertCanWrite();

        if(_bufferStrategy instanceof IRWStrategy) {

            ((IRWStrategy) _bufferStrategy).abortContext(context);
            
        }
        
    }

//    @Override
//    public void registerContext(final IAllocationContext context) {
//        
//        assertCanWrite();
//
//        if(_bufferStrategy instanceof IRWStrategy) {
//
//            ((IRWStrategy) _bufferStrategy).registerContext(context);
//            
//        }
//        
//    }
    
    @Override
	final public long getRootAddr(final int index) {

		final ReadLock lock = _fieldReadWriteLock.readLock();

		lock.lock();

		try {

			assertOpen();

			final ICommitRecord commitRecord = _commitRecord;

			if (commitRecord == null)
				throw new AssertionError();

			return commitRecord.getRootAddr(index);

		} finally {

			lock.unlock();

		}

	}

    /**
     * Resolve the {@link ICommitRecord} for the earliest visible commit point
     * based on the caller's <i>releaseTime</i>.
     * <p>
     * Note: This method is used for HA. The caller provides a releaseTime based
     * on the readsOnCommitTime of the earliestActiveTx and the minReleaseAge
     * rather than {@link ITransactionService#getReleaseTime()} since the latter
     * is only updated by the release time consensus protocol during a 2-phase
     * commit.
     */
    protected ICommitRecord getEarliestVisibleCommitRecordForHA(
            final long releaseTime) {

        final ReadLock lock = _fieldReadWriteLock.readLock();

        lock.lock();

        try {

            final long commitCounter = _rootBlock.getCommitCounter();

            final long lastCommitTime = _rootBlock.getLastCommitTime();
            
            if (commitCounter == 0L) {

                if (log.isTraceEnabled())
                    log.trace("No commit points");

                // Nothing committed yet.
                return null;
                
            }

            if (releaseTime >= lastCommitTime) {

                /*
                 * The caller is querying with an effective releaseTime GTE the
                 * lastCommitTime. It is not valid to have a releaseTime GTE the
                 * current committed state.
                 */
                
                throw new IllegalArgumentException("releaseTime(" + releaseTime
                        + ") >= lastCommitTime(" + lastCommitTime + ")");

            }
            
            final CommitRecordIndex commitRecordIndex = _commitRecordIndex;

            if (commitRecordIndex == null)
                throw new AssertionError();

            /*
             * Note: The commitRecordIndex does not allow us to probe with a
             * commitTime of ZERO. Therefore, when the releaseTime is ZERO, we
             * probe with a commitTime of ONE. Since the commitTimes are
             * timestamps, there will never be a record with a commitTime of ONE
             * and this will return us the first record in the
             * CommitRecordIndex.
             */

            final long effectiveTimestamp = releaseTime == 0L ? 1 : releaseTime;
            
            final ICommitRecord commitRecord = commitRecordIndex
                    .findNext(effectiveTimestamp);

            if (commitRecord == null)
                throw new AssertionError("commitCounter=" + commitCounter
                        + " but no commitRecord for releaseTime=" + releaseTime
                        + ", effectiveTimestamp=" + effectiveTimestamp + " :: "
                        + commitRecordIndex);

            if (log.isTraceEnabled())
                log.trace("releaseTime=" + releaseTime + ",commitRecord="
                        + commitRecord);

            return commitRecord;

//        } catch (IOException e) {
//
//            // Note: Should not be thrown. Local method call.
//            throw new RuntimeException(e);

        } finally {

            lock.unlock();
            
        }

    }
	
	/**
	 * Returns a read-only view of the most recently committed
	 * {@link ICommitRecord} containing the root addresses.
	 * 
	 * @return The current {@link ICommitRecord} and never <code>null</code>.
	 */
	public ICommitRecord getCommitRecord() {

		final ReadLock lock = _fieldReadWriteLock.readLock();

		lock.lock();

		try {

			assertOpen();

			final ICommitRecord commitRecord = _commitRecord;

			if (commitRecord == null)
				throw new AssertionError();

			return commitRecord;

		} finally {

			lock.unlock();

		}

	}

	/**
	 * Return the commit record, either new or read from the root block.
	 */
	private ICommitRecord _getCommitRecord() {

		assert _fieldReadWriteLock.writeLock().isHeldByCurrentThread();

		// the address of the current commit record from the root block.
		final long commitRecordAddr = _rootBlock.getCommitRecordAddr();
		
		if (log.isInfoEnabled())
			log.info("Reading commit record from: " + commitRecordAddr);
		
		if (commitRecordAddr == NULL) {

			// No commit record on the store yet.
			return new CommitRecord();

		} else {

			// Read the commit record from the store.
            return CommitRecordSerializer.INSTANCE.deserialize(_bufferStrategy
                    .read(commitRecordAddr));

		}

	}

	/**
	 * This method is invoked to mark any persistence capable data structures
	 * as invalid (in an error state). This ensures that dirty committers are
	 * not accidentally flushed through after a call to abort().
	 * 
	 * @see https://jira.blazegraph.com/browse/BLZG-1953
	 */
	protected void invalidateCommitters() {

	    if(log.isDebugEnabled())
	        log.debug("invalidating commiters for: " + this + ", lastCommitTime: " + this.getLastCommitTime());
	    
	    assert _fieldReadWriteLock.writeLock().isHeldByCurrentThread();

	    final Throwable t = new StackInfoReport("ABORT journal " + this + ", lastCommitTime: " + this.getLastCommitTime());
	    
        for (ICommitter committer : _committers) {
            if (committer != null)
                committer.invalidate(t);
        }

        // @see BLZG-2023, BLZG-2041.  Discard unisolated views from the resource locator cache.
        getResourceLocator().clearUnisolatedCache();

	}
	
	/**
	 * This method is invoked by {@link #abort()} when the store must discard
	 * any hard references that it may be holding to objects registered as
	 * {@link ICommitter}s.
	 * <p>
	 * The default implementation discards the btree mapping names to named
	 * btrees.
	 * <p>
	 * Subclasses MAY extend this method to discard their own committers but
	 * MUST NOT override it completely.
	 */
	protected void discardCommitters() {

		assert _fieldReadWriteLock.writeLock().isHeldByCurrentThread();

		// discard.
		_name2Addr = null;

	}

	/**
	 * Invoked when a journal is first created, re-opened, or when the
	 * committers have been {@link #discardCommitters() discarded}.
	 * <p>
	 * The basic implementation sets up the btree that is responsible for
	 * resolving named btrees.
	 * <p>
	 * Subclasses may extend this method to setup their own committers.
	 */
	protected void setupCommitters() {

		assert _fieldReadWriteLock.writeLock().isHeldByCurrentThread();

		if (!isReadOnly()) {

            /*
             * Only the leader can accept writes so only the leader will
             * register the Name2Addr object. Followers can access the
             * historical Name2Addr objects from the CommitRecordIndex for
             * historical commit points, but not the live Name2Addr object,
             * which is only on the leader.
             */
	        setupName2AddrBTree(getRootAddr(ROOT_NAME2ADDR));

	        /**
			 * Do not register committer to write previous root block, but
			 * instead just create it and call explicitly when required.  This
			 * is a workaround to allow "void" transactions.
			 */
			m_rootBlockCommitter = new RootBlockCommitter(this);
			
			/**
			 * If the strategy is a RWStrategy, then register the delete
			 * block committer to store the deferred deletes for each
			 * commit record.
			 */
			if (_bufferStrategy instanceof IRWStrategy)
				setCommitter(DELETEBLOCK, new DeleteBlockCommitter((IRWStrategy) _bufferStrategy));

            /*
             * Responsible for writing the ICUVersionRecord exactly once onto
             * the backing store, e.g., when the store is created or when it is
             * open with the "update" option specified for ICU.
             */
            setCommitter(ROOT_ICUVERSION, new ICUVersionCommitter());

		}

	}

    /**
     * Return the {@link ICUVersionRecord} from the current
     * {@link ICommitRecord} -or- a new instance for the current runtime
     * environment if the root address for {@link #ROOT_ICUVERSION} is
     * {@link #NULL}.
     */
    private ICUVersionRecord _getICUVersionRecord() {

        assert _fieldReadWriteLock.writeLock().isHeldByCurrentThread();

        final long addr = getRootAddr(ROOT_ICUVERSION);

        final ICUVersionRecord r;
        if (addr == NULL) {
            // New instance for the current runtime environment.
            r = ICUVersionRecord.newInstance();
        } else {
            // Existing instance from the store.
            r = (ICUVersionRecord) SerializerUtil.deserialize(read(addr));
        }
        return r;
        
    }

    /**
     * Writes the {@link ICUVersionRecord} onto the store iff either (a) it does
     * not exist; or (b) it exists, it differs from the last persistent record,
     * and the update flag was specified.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * 
     * @see Options#UPDATE_ICU_VERSION
     */
    private class ICUVersionCommitter implements ICommitter {

        private boolean update;
        
        private long lastAddr;

        private volatile Throwable error = null;

        private ICUVersionCommitter() {
            
            // the "update" option.
            update = Boolean.valueOf(properties.getProperty(
                    Options.UPDATE_ICU_VERSION, "false"));
            
            // lookup the address of the ICU version record (may be NULL).
            lastAddr = getRootAddr(ROOT_ICUVERSION);
            
        }

        /**
         * Commits a new {@link ICUVersionRecord} IF none is defined -OR- IF one
         * is defined, it is a different version of ICU, and the update flag is
         * set.
         */
        @Override
        public long handleCommit(final long commitTime) {

            if (error != null)
                throw new IndexInconsistentError(error);

            if(!update && lastAddr != NULL) {
                
                // Nothing changed.
                return lastAddr;
                
            }

            /*
             * Note: The Journal only validates the persistent ICU version
             * record in its constructor. By the time the code reaches this
             * point, it is either in agreement or will be written.
             */

            final ICUVersionRecord r = ICUVersionRecord.newInstance();

            if (lastAddr == NULL || !(r.equals(_icuVersionRecord) && update)) {

                if (_icuVersionRecord != null && update)
                    log.warn("Updating ICUVersion: old=" + _icuVersionRecord
                            + ", new=" + r);

                // do not update next time.
                update = false;
                
                // write ICU version record onto the store.
                lastAddr = write(ByteBuffer.wrap(SerializerUtil.serialize(r)));
            
                // return address of the ICU version record.
                return lastAddr;
                
            }
            
            // Nothing changed.
            return lastAddr;
            
        }

        @Override
        public void invalidate(final Throwable t) {

            if (t == null)
                throw new IllegalArgumentException();

            if (error == null)
                error = t;

        }
        
	}

	/*
	 * named indices.
	 */

	/**
	 * Setup the btree that resolves named indices. This is invoke when the
	 * journal is opened and by {@link #abort()} .
	 * 
	 * @param addr
	 *            The root address of the btree -or- 0L iff the btree has not
	 *            been defined yet.
	 * 
	 * @see Options#LIVE_INDEX_CACHE_CAPACITY
	 */
	Name2Addr setupName2AddrBTree(final long addr) {

		assert _fieldReadWriteLock.writeLock().isHeldByCurrentThread();

		assert _name2Addr == null;

		if (addr == 0L) {

			/*
			 * Create btree mapping names to addresses.
			 * 
			 * The btree has either never been created or if it had been created
			 * then the store was never committed and the btree had since been
			 * discarded. In any case we create a new btree now.
			 * 
			 * Note: if the journal is read-only the we create the commit record
			 * index on an in-memory store in order to avoid triggering a write
			 * exception on the journal.
			 */

			if (log.isInfoEnabled())
				log.info("New " + Name2Addr.class.getName());

			_name2Addr = Name2Addr.create((isReadOnly() ? new SimpleMemoryRawStore() : this));

		} else {

			/*
			 * Reload the mutable btree from its checkpoint address.
			 * 
			 * Note: This is the live view of the B+Tree. In this specific case
			 * we DO NOT use the canonicalizing mapping since we do not want
			 * anyone else to have access to this same instance of the B+Tree.
			 */

			if (log.isInfoEnabled())
				log.info("Loading " + Name2Addr.class.getName() + " from " + addr);

			_name2Addr = (Name2Addr) BTree.load(this, addr, false/* readOnly */);

		}

		_name2Addr.setupCache(liveIndexCacheCapacity, liveIndexCacheTimeout);

		// register for commit notices.
		setCommitter(ROOT_NAME2ADDR, _name2Addr);

		return _name2Addr;

	}

    /**
     * Return a read-only view of the last committed state of the
     * {@link CommitRecordIndex}.
     * 
     * @return The read-only view of the {@link CommitRecordIndex}.
     */
	public CommitRecordIndex getReadOnlyCommitRecordIndex() {

	    final ReadLock lock = _fieldReadWriteLock.readLock();

        lock.lock();

        try {

            assertOpen();

            final CommitRecordIndex commitRecordIndex = getCommitRecordIndex(
                    _rootBlock.getCommitRecordIndexAddr(), true/* readOnly */);

//            return new ReadOnlyIndex(commitRecordIndex);
            return commitRecordIndex;

        } finally {

            lock.unlock();

        }
 
	}

//    /**
//     * I have removed this method since the returned {@link CommitRecordIndex}
//     * was being used without appropriate synchronization. There is a
//     * {@link #getReadOnlyCommitRecordIndex()} which may be used in place of
//     * this method.
//     */
//	protected CommitRecordIndex getCommitRecordIndex() {
//
//		final ReadLock lock = _fieldReadWriteLock.readLock();
//
//		lock.lock();
//
//		try {
//
//			assertOpen();
//
//			final long commitRecordIndexAddr = _rootBlock.getCommitRecordIndexAddr();
//			
//			final CommitRecordIndex commitRecordIndex = getCommitRecordIndex(addr);
//
//			if (commitRecordIndex == null)
//				throw new AssertionError();
//
//			return commitRecordIndex;
//
//		} finally {
//
//			lock.unlock();
//
//		}
//
//	}

	/**
	 * Read and return the {@link CommitRecordIndex} from the current root
	 * block.
	 * 
	 * @return The {@link CommitRecordIndex} and never <code>null</code>.
	 */
	private CommitRecordIndex _getCommitRecordIndex() {

		assert _fieldReadWriteLock.writeLock().isHeldByCurrentThread();

		assert _rootBlock != null;

		final long addr = _rootBlock.getCommitRecordIndexAddr();

		try {

            if (log.isDebugEnabled())
                log.debug("Loading from addr=" + addr);
		    
            // Load the live index from the disk.
            return getCommitRecordIndex(addr, false/* readOnly */);

		} catch (RuntimeException ex) {

			/*
			 * Log the root block for post-mortem.
			 */
			log.fatal("Could not read the commit record index:\n" + _rootBlock, ex);

			throw ex;

		}

	}

    /**
     * Create or load and return the index that resolves timestamps to
     * {@link ICommitRecord}s. This method is capable of returning either the
     * live {@link CommitRecordIndex} or a read-only view of any committed
     * version of that index.
     * 
     * <strong>CAUTION: DO NOT EXPOSE THE LIVE COMMIT RECORD INDEX OUTSIDE OF
     * THIS CLASS. IT IS NOT POSSIBLE TO HAVE CORRECT SYNCHRONIZATION ON THAT
     * INDEX IN ANOTHER CLASS.</code>
     * 
     * @param addr
     *            The root address of the index -or- 0L if the index has not
     *            been created yet. When addr is non-{@link #NULL}, each
     *            invocation will return a distinct {@link CommitRecordIndex}
     *            object.
     * 
     * @param readOnly
     *            When <code>false</code> the returned is NOT cached.
     * 
     * @return The {@link CommitRecordIndex} for that address or a new index if
     *         <code>0L</code> was specified as the address.
     * 
     * @see #_commitRecordIndex
     */
    protected CommitRecordIndex getCommitRecordIndex(final long addr,
            final boolean readOnly) {

		if (log.isInfoEnabled())
			log.info("addr=" + toString(addr));

		final CommitRecordIndex ndx;

		if (addr == NULL) {

			/*
			 * The btree has either never been created or if it had been created
			 * then the store was never committed and the btree had since been
			 * discarded. In any case we create a new btree now.
			 * 
			 * Note: if the journal is read-only then we create the commit
			 * record index on an in-memory store in order to avoid triggering a
			 * write exception on the journal.
			 * 
			 * Note: if the journal is not the quorum leader then it is
			 * effectively read-only.
			 */
            if (isReadOnly() || readOnly) {

				ndx = CommitRecordIndex.createTransient();

			} else {

				ndx = CommitRecordIndex.create(this);

			}

		} else {

            if (readOnly) {

                /*
                 * Read only view of the most CommitRecordIndex having
                 * that checkpointAddr.
                 */
                
		        ndx = (CommitRecordIndex) getIndexWithCheckpointAddr(addr);
		        
		    } else {
	            
                /*
                 * Reload the mutable btree from its root address.
                 * 
                 * Note: For this code path we DO NOT cache the index view.
                 */

		        ndx = (CommitRecordIndex) BTree.load(this, addr, false/* readOnly */);
		        
		    }
		}

		assert ndx != null;

		return ndx;

	}

    /**
     * Note: There are some bigdata releases (such as 1.0.4) where the commit
     * record index was not pruned when deferred deletes were recycled. By
     * maintaining this test, we will correctly refuse to return a commit record
     * for a commit point whose deferred deletes have been recycled, even when
     * the commit record is still present in the commit record index.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/480
     */
	private boolean isHistoryGone(final long commitTime) {

	    if (this._bufferStrategy instanceof IHistoryManager) {

	        final long lastReleaseTime = ((IHistoryManager) _bufferStrategy)
                    .getLastReleaseTime();
	        
            if (commitTime <= lastReleaseTime) {
            
                if (log.isDebugEnabled())
                    log.info("History gone: commitTime=" + commitTime);
                
                return true; // no index available
                
            }
            
	    }
	    
	    return false;
	    
	}
	
    /**
     * {@inheritDoc}
     * 
     * @todo the {@link CommitRecordIndex} is a possible source of thread
     *       contention since transactions need to use this code path in order
     *       to locate named indices but the {@link WriteExecutorService} can
     *       also write on this index. I have tried some different approaches to
     *       handling this.
     */
    @Override
    public ICommitRecord getCommitRecord(final long commitTime) {

        if (isHistoryGone(commitTime))
            return null;

        final ReadLock lock = _fieldReadWriteLock.readLock();

		lock.lock();

		try {

			assertOpen();

			final CommitRecordIndex commitRecordIndex = _commitRecordIndex;

			if (commitRecordIndex == null)
				throw new AssertionError();

			return commitRecordIndex.find(commitTime);

		} finally {

			lock.unlock();

		}

	}

	/**
	 * Return the first commit record whose timestamp is strictly greater than
	 * the given commitTime.
	 * 
	 * @param commitTime
	 *            The commit time.
	 * 
	 * @return The commit record -or- <code>null</code> if there is no commit
	 *         record whose timestamp is strictly greater than
	 *         <i>commitTime</i>.
	 */
	public ICommitRecord getCommitRecordStrictlyGreaterThan(final long commitTime) {

		final ReadLock lock = _fieldReadWriteLock.readLock();

		lock.lock();

		try {

			assertOpen();

			final CommitRecordIndex commitRecordIndex = _commitRecordIndex;

			if (commitRecordIndex == null)
				throw new AssertionError();

			return commitRecordIndex.findNext(commitTime);

		} finally {

			lock.unlock();

		}

	}

    /**
     * {@inheritDoc}
     * <p>
     * Note: Transactions should pass in the timestamp against which they are
     * reading rather than the transaction identifier (aka startTime). By
     * providing the timestamp of the commit point, the transaction will hit the
     * {@link #indexCache}. If the transaction passes the startTime instead,
     * then all startTimes will be different and the cache will be defeated.
     * 
     * @throws UnsupportedOperationException
     *             If you pass in {@link ITx#UNISOLATED},
     *             {@link ITx#READ_COMMITTED}, or a timestamp that corresponds
     *             to a read-write transaction since those are not "commit
     *             times".
     * 
     * @see #indexCache
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/546" > Add
     *      cache for access to historical index views on the Journal by name
     *      and commitTime. </a>
     * 
     *      FIXME GIST Reconcile API tension with {@link IIndex} and
     *      {@link ICheckpointProtocol}, however this method is overridden by
     *      {@link Journal} and is also implemented by
     *      {@link IBigdataFederation}. The central remaining tensions are
     *      {@link FusedView} and the local/remote aspect. {@link FusedView}
     *      could probably be "fixed" by extending {@link AbstractBTree} rather
     *      than having an inner delegate for the mutable view. The local/remote
     *      issue is more complex.
     */
    @Override
	public IIndex getIndex(final String name, final long commitTime) {

        return (BTree) getIndexLocal(name, commitTime);

    }

	/**
     * Core implementation for access to historical index views.
     * <p>
     * Note: Transactions should pass in the timestamp against which they are
     * reading rather than the transaction identifier (aka startTime). By
     * providing the timestamp of the commit point, the transaction will hit the
     * {@link #indexCache}. If the transaction passes the startTime instead,
     * then all startTimes will be different and the cache will be defeated.
     * 
     * @throws UnsupportedOperationException
     *             If you pass in {@link ITx#UNISOLATED},
     *             {@link ITx#READ_COMMITTED}, or a timestamp that corresponds
     *             to a read-write transaction since those are not "commit
     *             times".
     * 
     * @see #indexCache
     * @see #getIndex(String, long)
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/546" > Add
     *      cache for access to historical index views on the Journal by name
     *      and commitTime. </a>
     */
	@Override
	final public ICheckpointProtocol getIndexLocal(final String name,
            final long commitTime) {

        if (commitTime == ITx.UNISOLATED || commitTime == ITx.READ_COMMITTED
                || TimestampUtility.isReadWriteTx(commitTime)) {

            throw new UnsupportedOperationException("name=" + name
                    + ",commitTime=" + TimestampUtility.toString(commitTime));

        }

        ICheckpointProtocol ndx = null;
        
        /*
         * Form the key for the cache.
         * 
         * Note: In order to avoid cluttering the cache, a read-only or
         * read/write transaction MUST pass the timestamp of the actual commit
         * point against which it is reading into this method. If it passes in
         * abs(txId) instead, then the cache will be cluttered since each tx
         * will have a distinct key for the same index against the same commit
         * point.
         */
        final NT nt = new NT(name, commitTime);

        // Test the cache.
        ndx = indexCache.get(nt);

        if (ndx != null) {

            if (isHistoryGone(commitTime)) {
            	
                if (log.isTraceEnabled())
                    log.trace("Removing entry from cache: " + name);

                /*
                 * No longer visible.
                 * 
                 * Note: If you are using a transaction, then the transaction
                 * will have a read lock which prevents the commit point against
                 * which it is reading from being released. Thus, a transaction
                 * can not hit this code path. However, it can be hit by
                 * historical reads which are not protected by a transaction.
                 */
 
                indexCache.remove(nt);

                return null;            
                
            }

            // Cache hit.
            return ndx;

        }

        /*
         * Cache miss.
         */
        
		final ReadLock lock = _fieldReadWriteLock.readLock();

		lock.lock();

		try {

			assertOpen();

            // Resolve the commit record.
			final ICommitRecord commitRecord = getCommitRecord(commitTime);

			if (commitRecord == null) {

//              if (log.isInfoEnabled())
                log.warn("No commit record: name=" + name + ", timestamp="
                        + commitTime);

				return null;

			}

			// Resolve the index against that commit record.
            ndx = (ICheckpointProtocol) getIndexWithCommitRecord(name, commitRecord);

            if (ndx == null) {

                // Not found
                return null;
                
            }

            // Add the index to the cache.
            final ICheckpointProtocol ndx2 = indexCache.putIfAbsent(nt, ndx);

            if (ndx2 != null) {

                /*
                 * Lost a data race. Use the winner's version of the index.
                 * 
                 * Note: Both index objects SHOULD be the same reference.
                 * getIndex(name,commitRecord) will go through a canonicalizing
                 * mapping to ensure that.
                 */

                ndx = ndx2;

            }

            // Found it and cached it.
            return ndx;

		} finally {

			lock.unlock();

		}

	}

	/**
	 * The size of the cache from (name,timestamp) to {@link IIndex}.
	 */
	protected int getIndexCacheSize() {
	    
	    return indexCache.size();
	    
	}
	
	/**
	 * The size of the canonicalizing cache from addr to {@link IIndex}.
	 */
	protected int getHistoricalIndexCacheSize() {

	    return historicalIndexCache.size();
	    
	}
	
//	/**
//     * Returns a read-only named index loaded from a {@link ICommitRecord}. The
//     * {@link BTree} will be marked as read-only, it will NOT permit writes, and
//     * {@link BTree#getLastCommitTime(long)} will report the value associated
//     * with {@link Entry#commitTime} for the historical {@link Name2Addr}
//     * instance for that {@link ICommitRecord}.
//     * 
//     * @return The named index -or- <code>null</code> iff the named index did
//     *         not exist as of that commit record.
//     * 
//     * @deprecated by {@link #getIndexWithCommitRecord(String, ICommitRecord)}
//     */
//	public BTree getIndex(final String name, final ICommitRecord commitRecord) {
//
//        return (BTree) getIndexWithCommitRecord(name, commitRecord);
//
//	}
	
    /**
     * Returns a read-only named index loaded from a {@link ICommitRecord}. The
     * index will be marked as read-only, it will NOT permit writes, and
     * {@link ICheckpointProtocol#getLastCommitTime(long)} will report the value
     * associated with {@link Entry#commitTime} for the historical
     * {@link Name2Addr} instance for that {@link ICommitRecord}.
     * <p>
     * Note: This method should be preferred to
     * {@link #getIndexWithCheckpointAddr(long)} for read-historical indices
     * since it will explicitly mark the index as read-only and specifies the
     * <i>lastCommitTime</i> on the returned index based on
     * {@link Name2Addr.Entry#commitTime}, which is the actual commit time for
     * the last update to the index.
     * 
     * @return The named index -or- <code>null</code> iff the named index did
     *         not exist as of that commit record.
     */
    final public ICheckpointProtocol getIndexWithCommitRecord(
            final String name, final ICommitRecord commitRecord) {

        if (name == null)
            throw new IllegalArgumentException();

        if (commitRecord == null)
            throw new IllegalArgumentException();

        final ReadLock lock = _fieldReadWriteLock.readLock();

        lock.lock();

        try {

            assertOpen();

            /*
             * The address of an historical Name2Addr mapping used to resolve
             * named indices for the historical state associated with this
             * commit record.
             */
            final long checkpointAddr = commitRecord
                    .getRootAddr(ROOT_NAME2ADDR);

            if (checkpointAddr == 0L) {

                log.warn("No name2addr entry in this commit record: "
                        + commitRecord);

                return null;

            }

            /*
             * Resolve the address of the historical Name2Addr object using the
             * canonicalizing object cache. This prevents multiple historical
             * Name2Addr objects springing into existence for the same commit
             * record.
             */
            final Name2Addr name2Addr = (Name2Addr) getIndexWithCheckpointAddr(checkpointAddr);

            /*
             * The address at which the named index was written for that
             * historical state.
             */
            final Name2Addr.Entry entry = name2Addr.getEntry(name);

            if (entry == null) {

                // No such index by name for that historical state.

                return null;

            }

            /*
             * Resolve the named index using the object cache to impose a
             * canonicalizing mapping on the historical named indices based on
             * the address on which it was written in the store.
             */

            final ICheckpointProtocol index = getIndexWithCheckpointAddr(entry.checkpointAddr);

            assert entry.commitTime != 0L : "Entry=" + entry;

            // Set the last commit time on the btree.
            index.setLastCommitTime(entry.commitTime);

            return index;

        } finally {

            lock.unlock();

        }

	}

//	/**
//	 * A canonicalizing mapping for <em>historical</em> {@link BTree}s.
//	 * <p>
//	 * Note: This method imposes a canonicalizing mapping and ensures that there
//	 * will be at most one instance of the historical index at a time. This
//	 * guarentee is used to facilitate buffer management. Writes on indices
//	 * returned by this method are NOT allowed.
//	 * <p>
//	 * Note: This method marks the {@link BTree} as read-only but does not set
//	 * {@link BTree#setLastCommitTime(long)} since it does not have access to
//	 * the {@link Entry#commitTime}, only the {@link BTree}s checkpointAddr and
//	 * {@link Checkpoint} record. See {@link #getIndex(String, ICommitRecord)}
//	 * which does set {@link BTree#setLastCommitTime(long)}.
//	 * <p>
//	 * Note: The canonicalizing mapping for unisolated {@link BTree}s is
//	 * maintained by the {@link ITx#UNISOLATED} {@link Name2Addr} instance.
//	 * 
//	 * @param checkpointAddr
//	 *            The address of the {@link Checkpoint} record for the
//	 *            {@link BTree}.
//	 * 
//	 * @return The {@link BTree} loaded from that {@link Checkpoint}.
//	 * 
//	 * @see Options#HISTORICAL_INDEX_CACHE_CAPACITY
//	 * 
//	 * @deprecated by {@link #getIndexWithCheckpointAddr(long)}
//	 */
//	final public BTree getIndex(final long checkpointAddr) {
//
//	    return (BTree) getIndexWithCheckpointAddr(checkpointAddr);
//	    
//	}

//    /**
//     * A canonicalizing mapping for <em>historical</em> {@link HTree}s.
//     * <p>
//     * Note: This method imposes a canonicalizing mapping and ensures that there
//     * will be at most one instance of the historical index at a time. This
//     * guarentee is used to facilitate buffer management. Writes on indices
//     * returned by this method are NOT allowed.
//     * <p>
//     * Note: This method marks the {@link BTree} as read-only but does not set
//     * {@link BTree#setLastCommitTime(long)} since it does not have access to
//     * the {@link Entry#commitTime}, only the {@link BTree}s checkpointAddr and
//     * {@link Checkpoint} record. See {@link #getIndex(String, ICommitRecord)}
//     * which does set {@link BTree#setLastCommitTime(long)}.
//     * <p>
//     * Note: The canonicalizing mapping for unisolated {@link BTree}s is
//     * maintained by the {@link ITx#UNISOLATED} {@link Name2Addr} instance.
//     * 
//     * @param checkpointAddr
//     *            The address of the {@link Checkpoint} record for the
//     *            {@link HTree}.
//     * 
//     * @return The {@link HTree} loaded from that {@link Checkpoint}.
//     * 
//     * @see Options#HISTORICAL_INDEX_CACHE_CAPACITY
//     */
//    final public HTree getHTree(final long checkpointAddr) {
//
//        return (HTree) getIndexWithCheckpointAddr(checkpointAddr);
//        
//    }

    /**
     * A canonicalizing mapping for <em>historical</em> (read-only) views of
     * persistence capable data structures (core impl).
     * <p>
     * Note: This method imposes a canonicalizing mapping and ensures that there
     * will be at most one object providing a view of the historical data
     * structure as of the specified timestamp. This guarentee is used to
     * facilitate buffer management.
     * <p>
     * Note: The canonicalizing mapping for unisolated views of persistence
     * capable data structures is maintained by the {@link ITx#UNISOLATED}
     * {@link Name2Addr} instance.
     * 
     * @param checkpointAddr
     *            The address of the {@link Checkpoint} record.
     * 
     * @return The read-only persistence capable data structure associated with
     *         that {@link Checkpoint}.
     * 
     * @see Options#HISTORICAL_INDEX_CACHE_CAPACITY
     */
    public final ICheckpointProtocol getIndexWithCheckpointAddr(
            final long checkpointAddr) {

        /*
         * Note: There are potentially three IO operations here. Reading the
         * Checkpoint record, reading the IndexMetadata record, then reading the
         * root node/leaf of the BTree.
         * 
         * Note: We use putIfAbsent() here rather than the [synchronized]
         * keyword for higher concurrency with atomic semantics.
         * 
         * DO NOT use the checkpointAddr but rather the physical address
         * without the length, this will enable the RWStore to clear the
         * cache efficiently without mocking up an address (which requires
         * access to the checkpointAddr size).
         */
        
        final long offset  = getPhysicalAddress(checkpointAddr);
        
        ICommitter ndx = historicalIndexCache.get(offset);
        
        if (ndx == null) {

            /*
             * Load index from the store.
             * 
             * Note: Does not set lastCommitTime.
             */
            ndx = Checkpoint
                    .loadFromCheckpoint(this, checkpointAddr, true/* readOnly */);

            if (log.isTraceEnabled())
                log.trace("Adding checkpoint to historical index at "
                        + checkpointAddr);

        } else {

            if (log.isTraceEnabled())
                log.trace("Found historical index at " + checkpointAddr
                        + ", historicalIndexCache.size(): "
                        + historicalIndexCache.size());
            
        }

        // Note: putIfAbsent is used to make concurrent requests atomic.
        ICommitter oldval = historicalIndexCache.putIfAbsent(offset, ndx);

        if (oldval != null) {

            /*
             * If someone beat us to it then use the BTree instance that they
             * loaded.
             */
            ndx = oldval;

        }
        
        return (ICheckpointProtocol) ndx;

    }
    
	/**
	 * Registers a named index. Once registered the index will participate in
	 * atomic commits.
	 * <p>
	 * Note: A named index must be registered outside of any transaction before
	 * it may be used inside of a transaction.
	 * <p>
	 * Note: You MUST {@link #commit()} before the registered index will be
	 * either restart-safe or visible to new transactions.
	 */
    @Override
	final public void registerIndex(final IndexMetadata metadata) {

		if (metadata == null)
			throw new IllegalArgumentException();

		final String name = metadata.getName();

		if (name == null)
			throw new IllegalArgumentException();

		// Note: Old code path was B+Tree specific.
//		registerIndex(name, metadata);
		
        validateIndexMetadata(name, metadata);

        // Note: Generic index create code path.
        final ICheckpointProtocol ndx = Checkpoint.create(this, metadata);

        // Note: Generic index registration code path.
        _register(name, ndx);

	}
    
    /**
     * Provides an opportunity to validate some aspects of the
     * {@link IndexMetadata} for an index partition.
     */
    protected void validateIndexMetadata(final String name, final IndexMetadata metadata) {

        // NOP, but extended by the ManagedJournal.

    }

    /**
     * {@inheritDoc}
     * <p>
     * Once registered the index will participate in atomic commits.
     * <p>
     * Note: A named index must be registered outside of any transaction before
     * it may be used inside of a transaction.
     * <p>
     * Note: You MUST {@link #commit()} before the registered index will be
     * either restart-safe or visible to new transactions.
     * 
     * @deprecated by {@link #register(String, IndexMetadata)}
     */
    @Override
	final public BTree registerIndex(final String name, final IndexMetadata metadata) {

		validateIndexMetadata(name, metadata);

		final BTree btree = BTree.create(this, metadata);

		return registerIndex(name, btree);

	}

	/**
     * Variant method creates and registered a named persistence capable data
     * structure but does not assume that the data structure will be a
     * {@link BTree}.
     * 
     * @param store
     *            The backing store.
     * @param metadata
     *            The metadata that describes the data structure to be created.
     * 
     * @return The persistence capable data structure.
     * 
     * @see Checkpoint#create(IRawStore, IndexMetadata)
     */
    @Override
    public ICheckpointProtocol register(final String name,
            final IndexMetadata metadata) {

        final ICheckpointProtocol ndx = Checkpoint.create(this, metadata);

        _register(name, ndx);

        return ndx;

    }

    @Override
	final public BTree registerIndex(final String name, final BTree ndx) {

	    _register(name, ndx);
	    
	    return ndx;
	    
    }

    final public void registerIndex(final String name, final HTree ndx) {

        _register(name, ndx);
        
    }

    /**
     * Registers a named index (core impl). Once registered the index will
     * participate in atomic commits.
     * <p>
     * Note: A named index must be registered outside of any transaction before
     * it may be used inside of a transaction.
     * <p>
     * Note: You MUST {@link #commit()} before the registered index will be
     * either restart-safe or visible to new transactions.
     * 
     * @param name
     *            The name.
     * @param ndx
     *            The persistence capable data structure.
     */
    final private void _register(final String name, final ICheckpointProtocol ndx) {
        
        final ReadLock lock = _fieldReadWriteLock.readLock();

        lock.lock();

        try {

            assertOpen();

            synchronized (_name2Addr) {

                // add to the persistent name map.
                _name2Addr.registerIndex(name, ndx);

            }

        } finally {

            lock.unlock();

        }

	}
	
    /**
     * {@inheritDoc}
     * <p>
     * Drops the named index. The index will no longer participate in atomic
     * commits and will not be visible to new transactions. Storage will be
     * reclaimed IFF the backing store support that functionality.
     */
    @Override
	public void dropIndex(final String name) {

        final ICheckpointProtocol ndx = getUnisolatedIndex(name);

        if (ndx == null)
            throw new NoSuchIndexException(name);

//        final IndexTypeEnum indexType = ndx.getIndexMetadata().getIndexType();
//
        if (getBufferStrategy() instanceof IRWStrategy) {
            /*
             * Reclaim storage associated with the index.
             */
            ndx.removeAll();
//            switch (indexType) {
//            case BTree:
//                ((AbstractBTree) ndx).removeAll();
//                break;
//            case HTree:
//                ((HTree) ndx).removeAll();
//                break;
//            default:
//                throw new AssertionError("Unknown: " + indexType);
//            }
        }
	    
		final ReadLock lock = _fieldReadWriteLock.readLock();

		lock.lock();

		try {

			assertOpen();

			synchronized (_name2Addr) {

				// drop from the persistent name map.
				_name2Addr.dropIndex(name);

			}

		} finally {

			lock.unlock();

		}

	}

    @Override
    public Iterator<String> indexNameScan(final String prefix,
            final long timestamp) {

        if (timestamp == ITx.UNISOLATED) {

            /*
             * For the live Name2Addr index, we get the necessary locks to avoid
             * concurrent modifications, fully materialize the iterator into a
             * collection, and then return an iterator over that collection.
             * This is safe, but not as scaleable.
             */

            final ReadLock lock = _fieldReadWriteLock.readLock();

            lock.lock();

            try {

                final List<String> names = new LinkedList<String>();

                synchronized (_name2Addr) {

                    final Iterator<String> itr = Name2Addr.indexNameScan(
                            prefix, _name2Addr);

                    while (itr.hasNext()) {

                        names.add(itr.next());

                    }

                }

                return names.iterator();

            } finally {

                lock.unlock();

            }

        }

        final IIndex n2a;

        if (timestamp == ITx.READ_COMMITTED) {

            n2a = getName2Addr();

        } else if (TimestampUtility.isReadWriteTx(timestamp)) {

            final ITx tx = getLocalTransactionManager().getTx(timestamp);
  
            if (tx == null)
               throw new TransactionNotFoundException(timestamp);

            final long readsOnCommitTime = tx.getReadsOnCommitTime();

            n2a = getName2Addr(readsOnCommitTime);

        } else {

            n2a = getName2Addr(timestamp);

        }

        return Name2Addr.indexNameScan(prefix, n2a);

    }

    /**
     * Return the mutable view of the named index (aka the "live" or
     * {@link ITx#UNISOLATED} index). This object is NOT thread-safe. You MUST
     * NOT write on this index unless you KNOW that you are the only writer. See
     * {@link ConcurrencyManager}, which handles exclusive locks for
     * {@link ITx#UNISOLATED} indices.
     * 
     * @return The mutable view of the index.
     * 
     * @see #getLiveView(String, long)
     */
    @Override
    final public BTree getIndex(final String name) {

        return (BTree) getUnisolatedIndex(name);
        
    }

//    /**
//     * Return the mutable view of the named index (aka the "live" or
//     * {@link ITx#UNISOLATED} index). This object is NOT thread-safe. You MUST
//     * NOT write on this index unless you KNOW that you are the only writer. See
//     * {@link ConcurrencyManager}, which handles exclusive locks for
//     * {@link ITx#UNISOLATED} indices.
//     * 
//     * @return The mutable view of the index.
//     * 
//     * @see #getUnisolatedIndex(String)
//     * 
//     * @deprecated Use {@link #getUnisolatedIndex(String)}
//     */
//    @Deprecated
//    final public HTree getHTree(final String name) {
//        
//        return (HTree) getUnisolatedIndex(name);
//        
//    }

//    /**
//     * Return the mutable view of the named index (aka the "live" or
//     * {@link ITx#UNISOLATED} index). This object is NOT thread-safe. You MUST
//     * NOT write on this index unless you KNOW that you are the only writer. See
//     * {@link ConcurrencyManager}, which handles exclusive locks for
//     * {@link ITx#UNISOLATED} indices.
//     * 
//     * @return The mutable view of the index.
//     * 
//     * @see #getLiveView(String, long)
//     */
//    final public Stream getStream(final String name) {
//        
//        return (Stream) getUnisolatedIndex(name);
//        
//    }
        
    /**
     * Return the mutable view of the named persistence capable data structure
     * (aka the "live" or {@link ITx#UNISOLATED} view).
     * 
     * @return The mutable view of the persistence capable data structure.
     */
    @Override
    final public ICheckpointProtocol getUnisolatedIndex(final String name) {

        final ReadLock lock = _fieldReadWriteLock.readLock();

        lock.lock();

        try {

            assertOpen();

            if (name == null)
                throw new IllegalArgumentException();

            if (Thread.interrupted()) {

                throw new RuntimeException(new InterruptedException());

            }

            // Note: NullPointerException can be thrown here if asynchronously
            // closed (should be fixed by the ReadLock).
            synchronized (_name2Addr) {

                return _name2Addr.getIndex(name);

            }

        } finally {

            lock.unlock();

        }

    }
    
	/*
	 * IAddressManager
	 */

    @Override
	final public long getOffset(long addr) {
		return _bufferStrategy.getOffset(addr);
	}

    @Override
	final public long getPhysicalAddress(long addr) {
		return _bufferStrategy.getAddressManager().getPhysicalAddress(addr);
	}

    @Override
	final public int getByteCount(long addr) {
		return _bufferStrategy.getByteCount(addr);
	}

    @Override
	final public long toAddr(int nbytes, long offset) {
		return _bufferStrategy.toAddr(nbytes, offset);
	}

    @Override
	final public String toString(long addr) {
		return _bufferStrategy.toString(addr);
	}

	final public int getOffsetBits() {
		return _bufferStrategy.getOffsetBits();
	}

	/**
	 * The maximum length of a record that may be written on the store.
	 */
	final public int getMaxRecordSize() {
		return _bufferStrategy.getMaxRecordSize();
	}

	/*
	 * High Availability
	 */

    /**
     * The current quorum token or {@value Quorum#NO_QUORUM} if the node is not
     * part of a {@link Quorum}.
     * <p>
     * The state of the field changes when a new quorum is negotiated or when an
     * existing quorum is broken. However, state changes in this field MUST be
     * coordinated with the journal in order to cover transitions into the
     * quorum (blocked read/write requests must be released) and transitions
     * when the quorum breaks (the current write set must be discarded by the
     * master). In addition, when there is no quorum, the resynchronization
     * protocol may affect both the persistent state of the journal and any
     * state which the journal keeps buffered in memory (record cache, address
     * translation cache, etc).
     * <p>
     * Access to this field is protected by the {@link #_fieldReadWriteLock} but
     * MUST also be coordinated as described above.
     */
	private volatile long quorumToken = Quorum.NO_QUORUM;

    protected long getQuorumToken() {

        return quorumToken;
        
    }

    /**
     * This method will update the quorum token on the journal and the
     * associated values <em>as if</em> the service was not joined with a met
     * quorum. This allows us to handle conditions where we know that the
     * service will not be joined with the met quorum once it is able to observe
     * the associated quorum events. However, those events can not be delivered
     * until the service is connected to zookeeper and one of the common causes
     * for entering the error state for the HAJournalServer is because the zk
     * client connection has been closed, hence, no zk events.
     * 
     * @param newValue
     *            The new value.
     */
    protected void clearQuorumToken(final long newValue) {
        
        // Lie to it.
        final boolean isServiceJoined = false;

        setQuorumToken2(newValue, isServiceJoined);

    }

    protected void setQuorumToken(final long newValue) {
        
        // Protect for potential NPE
        if (quorum == null)
            return;

        // This quorum member.
        final QuorumService<HAGlue> localService = quorum.getClient();

        final boolean isServiceJoined = localService != null
                && localService.isJoinedMember(newValue);

        setQuorumToken2(newValue, isServiceJoined);
        
    }

    /**
     * Update the {@link #quorumToken}, {@link #haReadyToken}, and
     * {@link #hashCode()}.
     * 
     * @param newValue
     *            The new quorum token value.
     * @param isServiceJoined
     *            <code>true</code> iff this service is known to be a service
     *            that is joined with the met quorum.
     */
    private void setQuorumToken2(final long newValue,
            final boolean isServiceJoined) {
    	
        if (haLog.isInfoEnabled())
            log.info("current: " + quorumToken + ", new: " + newValue
                    + ", joined=" + isServiceJoined);

        // Protect for potential NPE
        if (quorum == null)
            return;

        // The HAQuorumService (if running).
        final QuorumService<HAGlue> localService;
        {
            QuorumService<HAGlue> t;
            try {
                t = quorum.getClient();
            } catch (IllegalStateException ex) {
                t = null;
            }
            localService = t;
        }

        // Figure out the state transitions involved.
        final QuorumTokenTransitions transitionState = new QuorumTokenTransitions(
                quorumToken, newValue, isServiceJoined, haReadyToken);

        if (haLog.isInfoEnabled())
            haLog.info(transitionState.toString());

        if (transitionState.didBreak) {
            /*
             * If the quorum broke then set the token immediately without
             * waiting for the lock.
             * 
             * Note: This is a volatile write. We want the break to become
             * visible as soon as possible in order to fail things which examine
             * the token.
             * 
             * TODO Why not clear the haReadyToken and haStatus here as well?
             * However, those changes are not going to be noticed by threads
             * awaiting a state change until we do signalAll() and that requires
             * the lock.
             * 
             * TODO Why not clear the haReadyToken and haStatus on a leave using
             * a volatile write?  Again, threads blocked in awaitHAReady() would
             * not notice until we actually take the lock and do signalAll().
             */
            this.quorumToken = Quorum.NO_QUORUM;
        }

        /*
         * Both a meet and a break require an exclusive write lock.
         * 
         * TODO: Is this lock synchronization a problem? With token update
         * delayed on a lock could a second thread process a new token based on
         * incorrect state since the first thread has not updated the token? For
         * example: NO_TOKEN -> valid token -> NO_TOKEN
         */
        final WriteLock lock = _fieldReadWriteLock.writeLock();
        
        lock.lock();

        try {

            /**
             * The following condition tests are slightly confusing, it is not
             * clear that they represent all real states.
             * 
             * <pre>
             * Essentially:
             * 	didBreak - abort
             * 	didLeaveMetQuorum - abort
             * 	didJoinMetQuorum - follower gets rootBlocks
             * 	didMeet - just sets token
             * </pre>
             * 
             * In addition, there is a case where a service is joined as
             * perceived by the ZKQuorum but not yet HAReady. If a 2-phase
             * commit is initiated, then the service will enter an error state
             * (because it is not yet HAReady). This net-zero change case is
             * explicitly handled below.
             */

            if (transitionState.didLeaveMetQuorum) {

                /*
                 * The service was joined with a met quorum.
                 */
                quorumToken = newValue; // volatile write.
                
                /*
                 * We also need to discard any active read/write tx since there
                 * is no longer a quorum. This will hit both read-only
                 * transactions running on any service (not necessarily the
                 * leader) and read/write transactions if this service was the
                 * old leader.
                 * 
                 * Note: We do not need to discard read-only tx since the
                 * committed state should remain valid even when a quorum is
                 * lost. However, it would be a bit odd to leave read-only
                 * transactions running if you could not start a new read-only
                 * because the quorum is not met.
                 */
                ((AbstractTransactionService) getLocalTransactionManager()
                        .getTransactionService()).abortAllTx();
                
                /**
                 * Local abort (no quorum, so 2-phase abort not required).
                 * 
                 * FIXME HA : Abort the unisolated connection? (esp for group
                 * commit and the NSS level SPARQL and REST API unisolated
                 * operations). Maybe we can wrap the execute of the UpdateTask
                 * and the execution of the REST Mutation API methods in a
                 * well-known ThreadGuard and then do interruptAll() to force
                 * the cancelation of any running task? We could also wrap any
                 * IIndexManagerCallable in HAGlue.submit() with a FutureTask
                 * implementation that uses the appropriate ThreadGuard to
                 * ensure that any unisolated tasks are cancelled (that is
                 * actually overkill since it would not differentiate TX based
                 * operations from unisolated operations - we could also use
                 * that ThreadGuard in AbstractTask). Add unit tests for both
                 * UPDATE and other REST mutation methods.
                 * 
                 * @see <a
                 *      href="https://sourceforge.net/apps/trac/bigdata/ticket/753"
                 *      (HA doLocalAbort() should interrupt NSS requests and
                 *      AbstractTasks </a>
                 */
                doLocalAbort(); 
                
                /*
                 * Note: We can not re-cast our vote until our last vote is
                 * widthdrawn. That is currently done by QuorumWatcherBase. So,
                 * we have to wait until we observe that to cast a new vote.
                 */

                haReadyToken = Quorum.NO_QUORUM; // volatile write.
                
                haStatus = HAStatusEnum.NotReady; // volatile write.
                
                haReadyCondition.signalAll(); // signal ALL.
                
            } else if (transitionState.didBreak) {
            	    
                /*
                 * Note: [didLeaveMetQuorum] was handled above. So, this else if
                 * only applies to a service that observes a quorum break but
                 * which was not joined with the met quorum. As we were not
                 * joined at the break there is nothing to do save for updating
                 * the token.
                 */
                
                quorumToken = Quorum.NO_QUORUM; // volatile write.
                
                haReadyToken = Quorum.NO_QUORUM; // volatile write.
                
                haStatus = HAStatusEnum.NotReady; // volatile write.
                
                haReadyCondition.signalAll(); // signal ALL.
                
            } else if (transitionState.didMeet
                    || transitionState.didJoinMetQuorum) {

                /**
                 * Either a quorum meet (didMeet:=true) or the service is
                 * joining a quorum that is already met (didJoinMetQuorum).
                 */

                final long tmp;

                quorumToken = newValue;

                boolean installedRBs = false;

                final long localCommitCounter = _rootBlock.getCommitCounter();

                final boolean isLeader;
                final boolean isFollower;

                if (localService.isFollower(newValue)) {

                    isLeader = false;
                    isFollower = true;

                    if (localCommitCounter == 0L) {
                        
                        /*
                         * Take the root blocks from the quorum leader and use
                         * them.
                         */

                        // Remote interface for the quorum leader.
                        final HAGlue leader = localService.getLeader(newValue);

                        haLog.info("Fetching root block from leader.");
                        final IRootBlockView leaderRB;
                        try {
                            leaderRB = leader
                                    .getRootBlock(
                                            new HARootBlockRequest(null/* storeUUID */))
                                    .getRootBlock();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        if (leaderRB.getCommitCounter() == 0L) {

                            /*
                             * Installs the root blocks and does a local abort.
                             * 
                             * Note: This code path is only taken when both the
                             * leader and the follower are at commitCounter==0L.
                             * This prevents us from accidentally laying down on
                             * a follower the root blocks corresponding to a
                             * leader that already has committed write sets.
                             */
                            localService
                                    .installRootBlocks(
                                            leaderRB.asRootBlock(true/* rootBlock0 */),
                                            leaderRB.asRootBlock(false/* rootBlock0 */));

                            installedRBs = true;

                        }

                    }

                    // ready as follower.
                    tmp = newValue;

                } else if (localService.isLeader(newValue)) {

                    isLeader = true;
                    isFollower = false;
                    
                    // ready as leader.
                    tmp = newValue;

                } else {

                    isLeader = false;
                    isFollower = false;
                    
                    // Not ready.
                    tmp = Quorum.NO_QUORUM;
                    
                }

                /*
                 * Note: These volatile writes need to occur before we do the
                 * local abort since the readOnly versus readWrite state of the
                 * journal is decided based on the [haStatus].
                 */
                
                this.haReadyToken = tmp; // volatile write.

                // volatile write.
                this.haStatus = isLeader ? HAStatusEnum.Leader
                        : isFollower ? HAStatusEnum.Follower
                                : HAStatusEnum.NotReady;
                
                if (!installedRBs) {

                    /**
                     * If we install the RBs, then a local abort was already
                     * done. Otherwise we need to do one now (this covers the
                     * case when setQuorumToken() is called on the leader as
                     * well as cases where the service is either not a follower
                     * or is a follower, but the leader is not at
                     * commitCounter==0L, etc.
                     * 
                     * If didJoinMetQuorum==true, then we MUST be leaving the
                     * Resync run state in the HAJournalServer, so should NOT
                     * need to complete a localAbort.
                     * 
                     * TODO We should still review this point. If we do not
                     * delete a committed HALog, then why is doLocalAbort() a
                     * problem here? Ah. It is because doLocalAbort() is hooked
                     * by the HAJournalServer and will trigger a serviceLeave()
                     * and a transition to the error state.
                     * 
                     * @see <a
                     *      href="https://sourceforge.net/apps/trac/bigdata/ticket/695">
                     *      HAJournalServer reports "follower" but is in
                     *      SeekConsensus and is not participating in
                     *      commits</a>
                     */

                    if (haLog.isInfoEnabled())
                        haLog.info("Calling localAbort if NOT didJoinMetQuorum: "
                                + transitionState.didJoinMetQuorum);

                    if (!transitionState.didJoinMetQuorum) {

                        doLocalAbort();

                    }

                }

                haReadyCondition.signalAll(); // signal ALL.
                
            } else {

                /*
                 * Did not (leave|break|meet|join).
                 */

                if (haReadyToken != Quorum.NO_QUORUM) {

                    /*
                     * We should not be here if this service is HAReady.
                     */
                    throw new AssertionError("VOID setToken");

                }

                /*
                 * We are not joined. No change in token or HAReadyToken.
                 * 
                 * Note: This can occur (for example) if we are not yet joined
                 * and an error occurs during our attempt to join with a met
                 * quorum. One observed example is when this service is in the
                 * joined[] for zookeeper and therefore is messaged as part of
                 * the GATHER or PREPARE protocols for a 2-phase commit, but the
                 * service is not yet HAReady and therefore enters an error
                 * state rather than completing the 2-phase commit protocol
                 * successfully. When setQuorumToken() is called from the error
                 * handling task, the haReadyToken is already cleared. Unless
                 * the quorum also breaks, the quorum token will be unchanged.
                 * Hence we did not (leave|break|meet|join).
                 */

                // Fall through.
                
            }
            
        } finally {

            lock.unlock();
            
        }
        if (haLog.isInfoEnabled())
            haLog.info("done: token=" + quorumToken + ", HAReady="
                    + haReadyToken + ", HAStatus=" + haStatus);
    }
    private final Condition haReadyCondition = _fieldReadWriteLock.writeLock().newCondition();
    private volatile long haReadyToken = Quorum.NO_QUORUM;
    /**
     * Updated with the {@link #haReadyToken}.
     */
    private volatile HAStatusEnum haStatus = HAStatusEnum.NotReady;
    
//    /**
//     * Await the service being ready to partitipate in an HA quorum. The
//     * preconditions include:
//     * <ol>
//     * <li>receiving notice of the quorum token via
//     * {@link #setQuorumToken(long)}</li>
//     * <li>The service is joined with the met quorum for that token</li>
//     * <li>If the service is a follower and it's local root blocks were at
//     * <code>commitCounter:=0</code>, then the root blocks from the leader have
//     * been installed on the follower.</li>
//     * <ol>
//     * 
//     * @return the quorum token for which the service became HA ready.
//     */
//    final public long awaitHAReady() throws InterruptedException,
//            AsynchronousQuorumCloseException, QuorumException {
//        final WriteLock lock = _fieldReadWriteLock.writeLock();
//        lock.lock();
//        try {
//            long t = Quorum.NO_QUORUM;
//            while (((t = haReadyToken) == Quorum.NO_QUORUM)
//                    && getQuorum().getClient() != null) {
//                haReadyCondition.await();
//            }
//            final QuorumService<?> client = getQuorum().getClient();
//            if (client == null)
//                throw new AsynchronousQuorumCloseException();
//           if (!client.isJoinedMember(t)) {
//                throw new QuorumException();
//            }
//            return t;
//        } finally {
//            lock.unlock();
//        }
//    }

//    /**
//     * Await the service being ready to partitipate in an HA quorum. The
//     * preconditions include:
//     * <ol>
//     * <li>receiving notice of the quorum token via
//     * {@link #setQuorumToken(long)}</li>
//     * <li>The service is joined with the met quorum for that token</li>
//     * <li>If the service is a follower and it's local root blocks were at
//     * <code>commitCounter:=0</code>, then the root blocks from the leader have
//     * been installed on the follower.</li>
//     * <ol>
//     * 
//     * @param timeout
//     *            The timeout to await this condition.
//     * @param units
//     *            The units for that timeout.
//     *            
//     * @return the quorum token for which the service became HA ready.
//     */
    @Override
    final public long awaitHAReady(final long timeout, final TimeUnit units)
            throws InterruptedException, TimeoutException,
            AsynchronousQuorumCloseException {
        final WriteLock lock = _fieldReadWriteLock.writeLock();
        final long begin = System.nanoTime();
        final long nanos = units.toNanos(timeout);
        long remaining = nanos;
        if (!lock.tryLock(remaining, TimeUnit.NANOSECONDS))
            throw new TimeoutException();
        try {
            // remaining = nanos - (now - begin) [aka elapsed]
            remaining = nanos - (System.nanoTime() - begin);
            long t = Quorum.NO_QUORUM;
            while (((t = haReadyToken) == Quorum.NO_QUORUM)
                    && getQuorum().getClient() != null && remaining > 0) {
                if (!haReadyCondition.await(remaining, TimeUnit.NANOSECONDS))
                    throw new TimeoutException();
                
                remaining = nanos - (System.nanoTime() - begin);
            }
            final QuorumService<?> client = getQuorum().getClient();
            if (client == null)
                throw new AsynchronousQuorumCloseException();
            if (remaining <= 0)
                throw new TimeoutException();
            if (!client.isJoinedMember(t)) {
                throw new QuorumException();
            }
            return t;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the current value of the <code>haReadyToken</code>
     * (non-blocking).
     */
    final public long getHAReady() {

        /*
         * Note: In order for this operation to be non-blocking and still ensure
         * proper visibility of the [haReadyToken], the token MUST be volatile,
         * the setQuorumToken() method MUST NOT change the value of the
         * [haReadyToken] until all internal actions have been taken. That is,
         * until it is willing to do haReadyCondition.signalAll() and release
         * the lock guarding that Condition.
         */

        return haReadyToken;
        
    }

    /**
     * A simplified summary of the HA status of the service. This may be used to
     * reliably decide whether the service is the {@link HAStatusEnum#Leader}, a
     * {@link HAStatusEnum#Follower}, or {@link HAStatusEnum#NotReady}. This is
     * exposed both here (an RMI interface) and by the REST API.
     * 
     * @return The {@link HAStatusEnum} or <code>null</code> if the store is not
     *         associated with a {@link Quorum}.
     * 
     * @see HAGlue#getHAStatus()
     */
    final public HAStatusEnum getHAStatus() {

        if (quorum == null) {

            // Not HA.
            return null;

        }

        return haStatus;
        
    }

    /**
     * Assert that the {@link #getHAReady()} token has the specified value.
     * 
     * @param token
     *            The specified value.
     */
    final public void assertHAReady(final long token) throws QuorumException {

        if (quorum == null)
            return;

        if (token != haReadyToken) {

            throw new QuorumException(HAStatusEnum.NotReady.toString());

        }

    }
    
    /**
     * Install identical root blocks on the journal. This is used for a few
     * different conditions in HA.
     * <ol>
     * <li>When the quorum meets for the first time, we need to take the root
     * block from the leader and use it to replace both of our root blocks (the
     * initial root blocks are identical). That will make the root blocks the
     * same on all quorum members.</li>
     * <li>REBUILD: When a service goes through an automated disaster recovery,
     * we need to install new root blocks in order to make the local journal
     * logically empty. This prevents the service from attempting to interpret
     * the data on the backing file if there is a restart part way through the
     * rebuild operation.</li>
     * </ol>
     * 
     * FIXME We should also verify the following:
     * 
     * <pre>
     * - the DirectBufferPool.INSTANCE has the same buffer
     * capacity (so there will be room for the write cache
     * data in the buffers on all nodes).
     * </pre>
     * 
     * @see QuorumService#installRootBlocks(IRootBlockView)
     */
    protected void installRootBlocks(final IRootBlockView rootBlock0,
            final IRootBlockView rootBlock1) {

        if (rootBlock0 == null)
            throw new IllegalArgumentException();
        if (rootBlock1 == null)
            throw new IllegalArgumentException();
        if (!rootBlock0.isRootBlock0())
            throw new IllegalArgumentException();
        if (rootBlock1.isRootBlock0())
            throw new IllegalArgumentException();
//        if (rootBlock0.getCommitCounter() != 0L)
//            throw new IllegalArgumentException();
//        if (rootBlock1.getCommitCounter() != 0L)
//            throw new IllegalArgumentException();
        if (!rootBlock0.getStoreType().equals(rootBlock1.getStoreType()))
            throw new IllegalArgumentException();
        if (!rootBlock0.getUUID().equals(rootBlock1.getUUID()))
            throw new IllegalArgumentException();

//        if (_rootBlock.getCommitCounter() != 0) {
//
//            throw new IllegalStateException();
//
//        }

//        final IRootBlockView rootBlock0 = rootBlock;
//
//        final IRootBlockView rootBlock1 = new RootBlockView(
//                false/* rootBlock0 */, rootBlock0.asReadOnlyBuffer(), checker);

        final WriteLock lock = _fieldReadWriteLock.writeLock();

        lock.lock();

        try {
            
        // Check the root blocks before we install them.
        {
            if (!_rootBlock.getStoreType().equals(rootBlock0.getStoreType())) {
                /*
                 * The StoreType must agree.
                 */
                throw new RuntimeException("Incompatible StoreType: expected="
                        + _rootBlock.getStoreType() + ", actual="
                        + rootBlock0.getStoreType());
            }

        }

        // write root block through to disk and sync.
        _bufferStrategy.writeRootBlock(rootBlock0, ForceEnum.Force);

        // write 2nd root block through to disk and sync.
        _bufferStrategy.writeRootBlock(rootBlock1, ForceEnum.Force);

        // Choose the "current" root block.
        _rootBlock = RootBlockUtility.chooseRootBlock(rootBlock0, rootBlock1);

        // Save resource description (sets value returned by getUUID()).
        journalMetadata.set(new JournalMetadata(this));

        haLog.warn("Installed new root blocks: rootBlock0=" + rootBlock0
                + ", rootBlock1=" + rootBlock1);

        // now reset the store with the root block
        if (_bufferStrategy instanceof IHABufferStrategy)
            ((IHABufferStrategy) _bufferStrategy)
                    .resetFromHARootBlock(_rootBlock);

        /*
         * We need to reset the backing store with the token for the new quorum.
         * There should not be any active writers since there was no quorum.
         * Thus, this should just cause the backing store to become aware of the
         * new quorum and enable writes.
         * 
         * Note: This is done using a local abort, not a 2-phase abort. Each
         * node in the quorum should handle this locally when it sees the quorum
         * meet event.
         * 
         * TODO This assumes that a service that is not joined with the quorum
         * will not go through an _abort(). Such a service will have to go
         * through the synchronization protocol. If the service is in the
         * pipeline when the quorum meets, even through it is not joined, and
         * votes the same lastCommitTime, then it MIGHT see all necessary
         * replicated writes and if it does, then it could synchronize
         * immediately. There is basically a data race here.
         */

        doLocalAbort();
        
        } finally {
            
            lock.unlock();
            
        }
    
    }

    /**
     * Local commit protocol (HA).  This exists to do a non-2-phase abort
     * in HA.
     */
    final public void doLocalAbort() {

        _abort();
        
    }
    
    /**
     * Local commit protocol (HA, offline).
     * <p>
     * Note: This is used to support RESTORE by replay of HALog files when
     * the HAJournalServer is offline.
     * 
     * TODO This method should be protected.  If we move the HARestore class
     * into this package, then it can be changed from public to protected or
     * package private.
     */
    final public void doLocalCommit(final IRootBlockView rootBlock) {
     
        doLocalCommit(null/* localService */, rootBlock);
        
    }

    /**
     * Local commit protocol (HA).
     * 
     * @param localService
     *            For HA modes only. When non-<code>null</code>, this is used to
     *            identify whether the service is the leader. When the service
     *            is not the leader, we need to do some additional work to
     *            maintain the {@link IRWStrategy} allocators in synch at each
     *            commit point.
     * @param rootBlock
     *            The new root block.
     */
    protected void doLocalCommit(final QuorumService<HAGlue> localService,
            final IRootBlockView rootBlock) {

        final WriteLock lock = _fieldReadWriteLock.writeLock();

        lock.lock();

        try {

            /*
             * Note: flush() is done by prepare2Phase(). The only conditions
             * under which it is not done already is (a) HARestore (when
             * localService is null) and (b) during RESTORE or RESYNC for the
             * HAJournalServer (when haStatus will be NotReady).
             */
            final boolean shouldFlush = localService == null
                    || (haStatus == null || haStatus == HAStatusEnum.NotReady);

            /*
             * Force application data to stable storage _before_ we update the
             * root blocks. This option guarantees that the application data is
             * stable on the disk before the atomic commit. Some operating
             * systems and/or file systems may otherwise choose an ordered write
             * with the consequence that the root blocks are laid down on the
             * disk before the application data and a hard failure could result
             * in the loss of application data addressed by the new root blocks
             * (data loss on restart).
             * 
             * Note: We do not force the file metadata to disk. If that is done,
             * it will be done by a force() after we write the root block on the
             * disk.
             * 
             * Note: [shouldFlush] is probably sufficient. This test uses
             * [shouldFlush||true] to err on the side of safety.
             */
            if ((shouldFlush || true) && doubleSync) {

                _bufferStrategy.force(false/* metadata */);

            }
            
            // The timestamp for this commit point.
            final long commitTime = rootBlock.getLastCommitTime();

            // write the root block on to the backing store.
            _bufferStrategy.writeRootBlock(rootBlock, forceOnCommit);

            // set the new root block.
            _rootBlock = rootBlock;

            final boolean leader = localService == null ? false : localService
                    .isLeader(rootBlock.getQuorumToken());
            
            if (leader) {
            
                if (_bufferStrategy instanceof IRWStrategy) {
                
                    /*
                     * Now the root blocks are down we can commit any transient
                     * state.
                     */
                    
                    ((IRWStrategy) _bufferStrategy).postCommit();
                    
                }

            } else {

                /*
                 * Ensure allocators are synced after commit. This is only done
                 * for the followers. The leader has been updating the in-memory
                 * allocators as it lays down the writes. The followers have not
                 * be updating the allocators.
                 */

                if (haLog.isInfoEnabled() && localService != null)
                    haLog.info("PostHACommit: serviceUUID="
                            + localService.getServiceId());

                /**
                 * Call to sync any transient state
                 */
                ((IHABufferStrategy) _bufferStrategy)
                        .postHACommit(rootBlock);

                /*
                 * Clear reference and reload from the store.
                 * 
                 * The leader does not need to do this since it is writing on
                 * the unisolated commit record index and thus the new commit
                 * record is already visible in the commit record index before
                 * the commit. However, the follower needs to do this since it
                 * will otherwise not see the new commit points.
                 */

                _commitRecordIndex = _getCommitRecordIndex();

            }

            // reload the commit record from the new root block.
            _commitRecord = _getCommitRecord();

            if (txLog.isInfoEnabled())
                txLog.info("COMMIT: commitCounter="
                        + rootBlock.getCommitCounter() + ", commitTime="
                        + commitTime);

        } finally {

            lock.unlock();
        }

    }

    /**
     * The current {@link Quorum} (if any).
     */
	private final Quorum<HAGlue,QuorumService<HAGlue>> quorum;

    /**
     * Used to pin the {@link Future} of the gather operation on the client
     * to prevent it from being finalized while the leader is still running
     * its side of the consensus protocol to update the release time for the
     * replication cluster.
     * 
     * @see #gatherMinimumVisibleCommitTime(IHAGatherReleaseTimeRequest)
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/673" >
     *      Native thread leak in HAJournalServer process </a>
     */
    private final AtomicReference<Future<IHANotifyReleaseTimeResponse>> gatherFuture = new AtomicReference<Future<IHANotifyReleaseTimeResponse>>();
    
//    /**
//     * The {@link Quorum} for this service -or- <code>null</code> if the service
//     * is not running with a quorum.
//     */
    @Override
	public Quorum<HAGlue,QuorumService<HAGlue>> getQuorum() {

		return quorum;

	}
	
    /**
     * Factory for the {@link HADelegate} object for this
     * {@link AbstractJournal}. The object returned by this method will be made
     * available using {@link QuorumMember#getService()}.
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
    protected HAGlue newHAGlue(final UUID serviceId) {

        throw new UnsupportedOperationException();
        
	}

    /**
     * Return both root blocks (atomically - used by HA).
     * <p>
     * Note: This takes a lock to ensure that the root blocks are consistent
     * with a commit point on the backing store.
     */
    protected IRootBlockView[] getRootBlocks() {

        final Lock lock = _fieldReadWriteLock.readLock();
        
        lock.lock();

        try {

            final ChecksumUtility checker = ChecksumUtility.getCHK();
            
            final IRootBlockView rb0 = new RootBlockView(
                    true/* rootBlock0 */, getBufferStrategy()
                            .readRootBlock(true/* rootBlock0 */), checker);
            
            final IRootBlockView rb1 = new RootBlockView(
                    false/* rootBlock0 */, getBufferStrategy()
                            .readRootBlock(false/* rootBlock0 */), checker);
            
            return new IRootBlockView[] { rb0, rb1 };
            
        } finally {
            
            lock.unlock();
            
        }

    }
    
    /**
     * With lock held to ensure that there is no concurrent commit, copy
     * key data atomically to ensure recovered snapshot is consistent with
     * the commit state when the snapshot is taken.  This atomic data snapshot
     * can be merged with the file data to ensure a valid new store copy.
     * <p>
     * If this is not done then it is possible for the allocation data - both
     * metabits and fixed allocator commit bits - to be overwritten and inconsistent
     * with the saved root blocks.
     * 
     * @throws IOException 
     */
	public ISnapshotData snapshotAllocationData(final AtomicReference<IRootBlockView> rbv) throws IOException {
		final Lock lock = _fieldReadWriteLock.readLock();

		lock.lock();
		try {
			final ISnapshotData tm = new SnapshotData();
			final IBufferStrategy bs = getBufferStrategy();
			
			// clone rootblocks
			final ByteBuffer rb0 = bs.readRootBlock(true/*is rb0*/);
			tm.put((long) FileMetadata.OFFSET_ROOT_BLOCK0, BytesUtil.toArray(rb0));
			final ByteBuffer rb1 = bs.readRootBlock(false/*is rb0*/);
			tm.put((long) FileMetadata.OFFSET_ROOT_BLOCK1, BytesUtil.toArray(rb1));
			
			// return last commitCounter
            final IRootBlockView rbv0 = new RootBlockView(true/* rootBlock0 */, rb0, checker);            
            final IRootBlockView rbv1 = new RootBlockView(false/* rootBlock0 */, rb1, checker);
            
            rbv.set(RootBlockUtility.chooseRootBlock(rbv0, rbv1));
			
            // Disabling this test allows demonstration of the need to atomically snapshot the metabits and allocators
            //	for the RWStore in conjunction with TestHA1SnapshotPolicy.test_snapshot_stressMultipleTx_restore_validate
			if (bs instanceof RWStrategy) {
				final RWStore rws = ((RWStrategy) bs).getStore();
				
				// get metabits
				rws.snapshotMetabits(tm);
				
				// get committed allocations
				rws.snapshotAllocators(tm);
			}
			
			
			return tm;
		} finally {
			lock.unlock();
		}
	}

    /**
     * Implementation hooks into the various low-level operations required to
     * support HA for the journal.
     */
	protected class BasicHA implements HAGlue  {

        private final UUID serviceId;
        private final InetSocketAddress writePipelineAddr;

        protected BasicHA(final UUID serviceId,
                final InetSocketAddress writePipelineAddr) {

            if (serviceId == null)
                throw new IllegalArgumentException();
            if (writePipelineAddr == null)
                throw new IllegalArgumentException();

            this.serviceId = serviceId;
            this.writePipelineAddr = writePipelineAddr;

        }

        /**
         * The most recent prepare request.
         */
        private final AtomicReference<IHA2PhasePrepareMessage> prepareRequest = new AtomicReference<IHA2PhasePrepareMessage>();
        
        /**
         * Whether or not we voted "yes" for the last prepare request.
         */
        private final AtomicBoolean vote = new AtomicBoolean(false);

        /**
         * Return the backing {@link IIndexManager} (non-RMI method).
         */
        public AbstractJournal getIndexManager() {

            return AbstractJournal.this;
            
        }
        
        @Override
        public UUID getServiceId() {

            return serviceId;
            
		}

        @Override
        public InetSocketAddress getWritePipelineAddr() {
            
            return writePipelineAddr;
            
        }

        @Override
        public int getNSSPort() {

            throw new UnsupportedOperationException();
            
        }

        @Override
        public RunState getRunState() {

            throw new UnsupportedOperationException();
            
        }

        @Override
        public String getExtendedRunState() {

            throw new UnsupportedOperationException();
            
        }

        @Override
        public HAStatusEnum getHAStatus() {

            return AbstractJournal.this.getHAStatus();
            
        }
        
        @Override
        public long awaitHAReady(final long timeout, final TimeUnit units)
                throws AsynchronousQuorumCloseException, InterruptedException,
                TimeoutException {

            return AbstractJournal.this.awaitHAReady(timeout, units);

        }

        /**
         * {@inheritDoc}
         * 
         * FIXME awaitServiceJoin() is failing to set the commitCounter on the
         * message. Either create a new message type or return the right
         * message. The joining service should be able to verify that the
         * release time is applicable for its commit counter (in
         * {@link AbstractHATransactionService#runWithBarrierLock(Runnable)}.
         */
        @Override
        public IHANotifyReleaseTimeResponse awaitServiceJoin(
                final IHAAwaitServiceJoinRequest req)
                throws AsynchronousQuorumCloseException, InterruptedException,
                TimeoutException {

            /*
             * Note: Lock makes this operation MUTEX with a critical section in
             * commitNow().
             */
            _gatherLock.lock();

            try {
                
                final UUID serviceUUID = req.getServiceUUID();

                final long begin = System.nanoTime();

                final long nanos = req.getUnit().toNanos(req.getTimeout());

                long remaining = nanos;

                while ((remaining = nanos - (System.nanoTime() - begin)) > 0) {

                    final UUID[] joined = getQuorum().getJoined();

                    for (UUID t : joined) {

                        if (serviceUUID.equals(t)) {

                            /*
                             * Found it.
                             * 
                             * FIXME This should be returning the commitCounter
                             * associated with the most recent gather, not -1L.
                             */

                            if (log.isInfoEnabled())
                                log.info("Found Joined Service: " + serviceUUID);

                            final JournalTransactionService ts = (JournalTransactionService) getLocalTransactionManager()
                                    .getTransactionService();

                            return new HANotifyReleaseTimeResponse(
                                    ts.getReleaseTime(), -1);

                        }

                    }

                    // remaining := nanos - elapsed
                    remaining = nanos - (System.nanoTime() - begin);

                    if (remaining > 0) {

                        final long sleepMillis = Math
                                .min(TimeUnit.NANOSECONDS.toMillis(remaining),
                                        10/* ms */);

                        if (sleepMillis <= 0) {

                            /*
                             * If remaining LT 1 ms, then fail fast.
                             */

                            throw new TimeoutException();

                        }

                        Thread.sleep(sleepMillis);

                    }

                }

                // timeout.
                throw new TimeoutException();
            
            } finally {
            
                _gatherLock.unlock();
                
            }
            
        }

        @Override
        public IHADigestResponse computeDigest(final IHADigestRequest req)
                throws IOException, NoSuchAlgorithmException, DigestException {

            throw new UnsupportedOperationException();

        }

        @Override
        public IHALogDigestResponse computeHALogDigest(
                final IHALogDigestRequest req) throws IOException,
                NoSuchAlgorithmException, DigestException {

            throw new UnsupportedOperationException();

        }

        @Override
        public IHASnapshotDigestResponse computeHASnapshotDigest(
                final IHASnapshotDigestRequest req) throws IOException,
                NoSuchAlgorithmException, DigestException {
        
            throw new UnsupportedOperationException();

        }

//        @Override
//        public Future<Void> globalWriteLock(final IHAGlobalWriteLockRequest req)
//                throws IOException, TimeoutException, InterruptedException {
//
//            throw new UnsupportedOperationException();
//
//        }

        @Override
        public Future<IHASnapshotResponse> takeSnapshot(
                final IHASnapshotRequest req) throws IOException {

            throw new UnsupportedOperationException();

        }
        
        @Override
        public Future<Void> rebuildFromLeader(IHARemoteRebuildRequest req)
                throws IOException {

            throw new UnsupportedOperationException();

        }
        
        /**
         * Return a proxy object for a {@link Future} suitable for use in an RMI
         * environment (the default implementation returns its argument).
         * 
         * @param future
         *            The future.
         * 
         * @return The proxy for that future.
         */
        final protected <E> Future<E> getProxy(final Future<E> future) {

            return getProxy(future, false/* asyncFuture */);

        }

        /**
         * Return a proxy object for a {@link Future} suitable for use in an RMI
         * environment (the default implementation returns its argument).
         * 
         * @param future
         *            The future.
         * @param asyncFuture
         *            When <code>true</code>, the service should not wait for
         *            the {@link Future} to complete but should return a proxy
         *            object that may be used by the client to monitor or cancel
         *            the {@link Future}. When <code>false</code>, the method
         *            should wait for the {@link Future} to complete and then
         *            return a "thick" {@link Future} which wraps the completion
         *            state but does not permit asynchronous monitoring or
         *            cancellation of the operation wrapped by the
         *            {@link Future}.
         * 
         * @return The proxy for that future.
         */
        protected <E> Future<E> getProxy(final Future<E> future,
                final boolean asyncFuture) {

            return future;
            
        }

        @Override
        public Future<Boolean> prepare2Phase(
                final IHA2PhasePrepareMessage prepareMessage) {

            if (prepareMessage == null)
                throw new IllegalArgumentException();

            final boolean isRootBlock0 = prepareMessage.isRootBlock0();

            final long timeout = prepareMessage.getTimeout();

            final TimeUnit unit = prepareMessage.getUnit();

            final IRootBlockView rootBlock = prepareMessage.getRootBlock();

            if (haLog.isInfoEnabled())
                haLog.info("isJoinedService="
                        + prepareMessage.isJoinedService() + ", isRootBlock0="
                        + isRootBlock0 + ", rootBlock=" + rootBlock
                        + ", timeout=" + timeout + ", unit=" + unit);

            // the quorum token from the leader is in the root block.
            final long prepareToken = rootBlock.getQuorumToken();

            // Do not prepare if the token is wrong.
			quorum.assertQuorum(prepareToken);

			assertHAReady(prepareToken);
			
			// Save off a reference to the prepare request.
			prepareRequest.set(prepareMessage);

            // Clear vote (assume NO unless proven otherwise).
            vote.set(false);

            // Note: Can throw IllegalStateException (if not running).
			final QuorumService<HAGlue> quorumService = quorum.getClient();

			// Note: as decided by the leader!
            final boolean isJoined = prepareMessage.isJoinedService();

            // true the token is valid and this service is the quorum leader
            final boolean isLeader = quorumService.isLeader(prepareToken);

            final FutureTask<Boolean> ft;

            if (!isJoined) {

                /*
                 * A NOP task if this service is not joined with the met quorum.
                 */

                ft = new FutureTaskMon<Boolean>(new VoteNoTask(quorumService));

            } else {

                /*
                 * A task to flush and sync if the service is joined with the
                 * met quorum.
                 * 
                 * Note: This code path is only when [isJoined := true].
                 */

                ft = new FutureTaskMon<Boolean>(new Prepare2PhaseTask(isLeader,
                        prepareMessage));

            }

            if (isLeader) {

	            /*
	             * Run in the caller's thread.
	             * 
	             * Note: In order to avoid deadlock, when the leader calls back to
	             * itself it MUST do so in the same thread in which it is already
	             * holding the writeLock. [Actually, we do not obtain the writeLock
	             * in prepare2Phase, but all the other quorum commit methods do.]
	             */

			    ft.run();
			    
			} else {

                /*
                 * We can't really handle the timeout in the leader's thread
                 * (and it would be very odd if the leader wound up waiting on
                 * itself!) but the followers can obey the timeout semantics for
                 * prepare() by execute()ing the FutureTask and then returning
                 * it immediately. The leader can wait on the Future up to the
                 * timeout and then cancel the Future.
                 */
                
			    // submit.
			    getExecutorService().execute(ft);
			    
			}
			
			return getProxy(ft);
			
        }
        
        /**
         * Task votes NO (unconditional).
         * <p>
         * Note: If we were not joined at the start of the 2-phase commit, then
         * we will not participate. This provides an atomic decision point with
         * respect to when a service that is rebuilding or resynchronizing will
         * participate in a 2-phase commit. By voting NO here, the
         * commit2Phase() operation will be a NOP for THIS service.
         * <p>
         * Note: The vote of a service that was not joined with the met quorum
         * at the time that we begin the 2-phase commit protocol is ignored.
         */
        protected class VoteNoTask implements Callable<Boolean>{

            private final QuorumService<HAGlue> localService;

            public VoteNoTask(final QuorumService<HAGlue> localService) {
            
                this.localService = localService;
                
            }
            
            @Override
            public Boolean call() throws Exception {

                // Vote NO.
                vote.set(false);

                final IHA2PhasePrepareMessage req = prepareRequest.get();

                doLocalAbort();

                if (req.isJoinedService()) {

                    /*
                     * Force a service that was joined at the atomic decision
                     * point of the 2-phase commit protocol to do a service
                     * leave.
                     */
                    
                    if (localService != null) {

                        localService.enterErrorState();
                        
                    }

                }

                return vote.get();

            }

        } // class VoteNoTask
        
        /**
         * Task prepares for a 2-phase commit (syncs to the disk) and votes YES
         * iff if is able to prepare successfully.
         * <p>
         * Note: This code path is only when [isJoined := true].
         */
        private class Prepare2PhaseTask implements Callable<Boolean> {

            private final boolean isLeader;
            private final IHA2PhasePrepareMessage prepareMessage;

            public Prepare2PhaseTask(//
                    final boolean isLeader,//
                    final IHA2PhasePrepareMessage prepareMessage) {

                if (prepareMessage == null)
                    throw new IllegalArgumentException();

                if (!prepareMessage.isJoinedService()) {

                    /*
                     * Only services that are joined as of the atomic decision
                     * point in commitNow() are sent a PREPARE message.
                     */

                    throw new AssertionError();
                    
                }
                
                this.isLeader = isLeader;

                this.prepareMessage = prepareMessage;
                
            }

            /**
             * Note: This code path is only when [isJoined := true].
             */
            @Override
            public Boolean call() throws Exception {

                QuorumService<HAGlue> localService = null;

                try {

                    /*
                     * Note: Throws IllegalStateException if quorum has been
                     * terminated. We can't PREPARE if the quorum is terminated.
                     */
                    localService = quorum.getClient();

                    return innerCall();

                } finally {

                    if (haLog.isInfoEnabled())
                        haLog.info("VOTE=" + vote.get());

                    if (!vote.get()) {

                        /**
                         * Since the service refuses to PREPARE we want it to
                         * enter an error state and then figure out whether it
                         * needs to resynchronize with the quorum.
                         * <p>
                         * Note: Entering the error state will cause the local
                         * abort and serviceLeave() actions to be taken, which
                         * is why then have been commented out above.
                         * 
                         * @see <a
                         *      href="https://sourceforge.net/apps/trac/bigdata/ticket/695">
                         *      HAJournalServer reports "follower" but is in
                         *      SeekConsensus and is not participating in
                         *      commits</a>
                         */
                        if (localService != null) {
                            
                            localService.enterErrorState();
                            
                        }
                        
                    }

                }

            }

            private Boolean innerCall() throws Exception {
                
                /*
                 * Get and clear the [gatherFuture]. A service which was
                 * joined at the atomic decision point for the GATHER will
                 * have a non-null Future here. A service which is newly
                 * joined and which joined *after* the GATHER will have a
                 * [null] Future here. If the service participated in the
                 * gather, then we will use this Future to decide if it
                 * should vote NO. If the service joined *after* the GATHER,
                 * then the Future will be [null] and we will ignore it.
                 * 
                 * Note: This is checked below.
                 */
                final Future<IHANotifyReleaseTimeResponse> oldFuture = gatherFuture
                        .getAndSet(null/* newValue */);

                if (haLog.isInfoEnabled())
                    haLog.info("gatherFuture=" + oldFuture);
                    
                final IRootBlockView rootBlock = prepareMessage.getRootBlock();

                if (haLog.isInfoEnabled())
                    haLog.info("preparedRequest=" + rootBlock + ", isLeader: " + isLeader);

                if (rootBlock == null)
                    throw new IllegalStateException();

                // Validate new root block against current root block.
                validateNewRootBlock(/* isJoined, */isLeader,
                        AbstractJournal.this._rootBlock, rootBlock);

                if (haLog.isInfoEnabled())
                    haLog.info("validated=" + rootBlock);

                /*
                 * Verify that the local release time is consisent with the
                 * GATHER.
                 */
                final IHANotifyReleaseTimeResponse consensusReleaseTime = prepareMessage
                        .getConsensusReleaseTime();
                
                {

                    if (oldFuture != null) {

                        /*
                         * If we ran the GATHER task, then we must await the
                         * outcome of the GATHER on this service before we
                         * can verify that the local consensus release time
                         * is consistent with the GATHER.
                         * 
                         * Note: If the oldFuture is null, then the service
                         * just joined and was explicitly handed the
                         * consensus release time and hence should be
                         * consistent here anyway.
                         */
                        
                        oldFuture.get();
                        
                    }

                    final long localReleaseTime = getLocalTransactionManager()
                            .getTransactionService().getReleaseTime();

                    // Note: Per the GatherTask (in Journal.java).
                    final long expectedReleaseTime = Math.max(0L,
                            consensusReleaseTime.getCommitTime() - 1);

                    if (localReleaseTime != expectedReleaseTime) {

                        throw new AssertionError(
                                "Local service does not agree with consensusReleaseTime: localReleaseTime="
                                        + localReleaseTime
                                        + ", expectedReleaseTime="
                                        + expectedReleaseTime
                                        + ", consensusReleaseTime="
                                        + consensusReleaseTime
                                        + ", serviceId=" + getServiceId());

                    }

                }

                /*
                 * if(follower) {...}
                 */
                if (/*isJoined &&*/ !isLeader) {

                    /**
                     * This is a follower.
                     * 
                     * Validate the release time consensus protocol was
                     * completed successfully on the follower.
                     * 
                     * @see <a
                     *      href="https://sourceforge.net/apps/trac/bigdata/ticket/673"
                     *      > Native thread leak in HAJournalServer process </a>
                     */

                    if (!prepareMessage.isGatherService()) {

                        /*
                         * This service did not participate in the GATHER.
                         * Instead, it joined after the GATHER but before
                         * the PREPARE.
                         */

                        // [gatherFuture] should have been [null].
                        assert oldFuture == null;
                        
                        vote.set(true);

                        // Done.
                        return vote.get();
                        
                    }

                    /**
                     * Note: We need to block here (on oldFuture.get()) in
                     * case the follower has not finished applying the
                     * updated release time.
                     */
                    try {

                        // Note: [oldFuture] MUST be non-null!
                        final IHANotifyReleaseTimeResponse tmp = oldFuture.get();
                        
                        if ((tmp.getCommitCounter() != consensusReleaseTime
                                .getCommitCounter())
                                || (tmp.getCommitTime() != consensusReleaseTime
                                        .getCommitTime())) {
                        
                            throw new AssertionError(
                                    "GatherTask reports different consensus: GatherTask="
                                            + tmp
                                            + ", consensusReleaseTime="
                                            + consensusReleaseTime);
                        }

                        /*
                         * Gather was successful - fall through.
                         */
                        
                    } catch (InterruptedException e) {
                        /*
                         * Note: Future isDone(). Caller should not block.
                         */
                        throw new AssertionError();
                    } catch (CancellationException e) {
                        /*
                         * Gather cancelled on the follower (e.g.,
                         * immediately above).
                         */
                        haLog.error("Gather cancelled on follower: serviceId="
                                + getServiceId() + " : " + e, e);
                        return vote.get();
                    } catch (ExecutionException e) {
                        // Gather failed on the follower.
                        haLog.error("Gather failed on follower: serviceId="
                                + getServiceId() + " : " + e, e);
                        return vote.get();
                    }

                }
                
                /*
                 * Call to ensure strategy does everything required for itself
                 * before final root block commit. At a minimum it must flush
                 * its write cache to the backing file (issue the writes).
                 */
                // _bufferStrategy.commit(); // lifted to before we
                // retrieve
                // RootBlock in commitNow
                /*
                 * Force application data to stable storage _before_ we update
                 * the root blocks. This option guarantees that the application
                 * data is stable on the disk before the atomic commit. Some
                 * operating systems and/or file systems may otherwise choose an
                 * ordered write with the consequence that the root blocks are
                 * laid down on the disk before the application data and a hard
                 * failure could result in the loss of application data
                 * addressed by the new root blocks (data loss on restart).
                 * 
                 * Note: We do not force the file metadata to disk. If that is
                 * done, it will be done by a force() after we write the root
                 * block on the disk.
                 */
                if (doubleSync) {

                    _bufferStrategy.force(false/* metadata */);

                }

                if (prepareMessage.voteNo()) {

                    /*
                     * Hook allows the test suite to force a NO vote.
                     */

                    throw new Mock2PhaseCommitProtocolException("Force NO vote");

                }

                // Vote YES.
                vote.set(true);

                return vote.get();

            }
            
        }

        /**
         * Validate the new root block against the current root block. This
         * method checks a variety of invariants:
         * <ul>
         * <li>The UUID of the store must be the same.</li>
         * <li>The commitTime must be strictly increasing.</li>
         * <li>The commitCounter must increase by ONE (1).</li>
         * <li></li>
         * </ul>
         * Note: This code path is only when [isJoined := true] (that is, the
         * service was joined with the met quorum at the atomic decision point
         * for the joined set for the 2-phase commit).
         * 
         * @param isLeader
         *            iff this service is the leader for this commit.
         * @param oldRB
         *            the old (aka current) root block.
         * @param newRB
         *            the new (aka proposed) root block.
         */
//        * @param isJoined
//        *            iff this service was joined at the atomic decision point
//        *            in the 2-phase commit protocol.
        protected void validateNewRootBlock(//final boolean isJoined,
                final boolean isLeader, final IRootBlockView oldRB,
                final IRootBlockView newRB) {

            if (oldRB == null)
                throw new IllegalStateException();

            if (newRB == null)
                throw new IllegalStateException();

            // Validate UUID of store is consistent.
            if (!newRB.getUUID().equals(oldRB.getUUID())) {

                /*
                 * The root block has a different UUID. We can not accept this
                 * condition.
                 */

                throw new IllegalStateException("Store UUID: old="
                        + oldRB.getUUID() + " != new=" + newRB.getUUID());

            }

            // Validate commit time is strictly increasing.
            if (newRB.getLastCommitTime() <= oldRB.getLastCommitTime()) {

                /*
                 * The root block has a commit time that is LTE the most recent
                 * commit on this Journal. We can not accept this condition.
                 */

                throw new IllegalStateException("lastCommitTime: old="
                        + oldRB.getLastCommitTime() + " > new="
                        + newRB.getLastCommitTime());

            }

            // Validate the new commit counter.
            {

                final long newcc = newRB.getCommitCounter();

                final long oldcc = oldRB.getCommitCounter();

                if (newcc != (oldcc + 1)) {

                    /*
                     * The new root block MUST have a commit counter that is ONE
                     * more than the current commit counter on this Journal. We
                     * can not accept any other value for the commit counter.
                     */

                    throw new IllegalStateException("commitCounter: ( old="
                            + oldcc + " + 1 ) != new=" + newcc);

                }

            }

            // The quorum token from the leader is in the root block.
            final long prepareToken = newRB.getQuorumToken();

            // Verify that the same quorum is still met.
            quorum.assertQuorum(prepareToken);

            // Verify HA ready for that token.
            assertHAReady(prepareToken);

            // Note: Throws IllegalStateException if quorum not running.
            final QuorumService<HAGlue> localService = quorum.getClient();

            if (isLeader) {
                
                /*
                 * Verify still the leader.
                 */
                if (!localService.isLeader(prepareToken))
                    throw new IllegalStateException("Not leader.");

                final HAStatusEnum st = getHAStatus();

                if (!HAStatusEnum.Leader.equals(st)) {

                    throw new IllegalStateException("HAStatusEnum: expected="
                            + HAStatusEnum.Leader + ", actual=" + st);

                }
                
            } else {

                /*
                 * Verify still a follower.
                 */
                if (!localService.isFollower(prepareToken))
                    throw new IllegalStateException("Not follower.");

                final HAStatusEnum st = getHAStatus();

                if (!HAStatusEnum.Follower.equals(st)) {

                    throw new IllegalStateException("HAStatusEnum: expected="
                            + HAStatusEnum.Follower + ", actual=" + st);

                }

            }
            
            final long tmp = getHAReady();

            if (prepareToken != tmp) {

                throw new IllegalStateException("HAReadyToken: expected="
                        + prepareToken + ", actual=" + tmp);

            }
            
        }
        
        @Override
        public Future<Void> commit2Phase(
                final IHA2PhaseCommitMessage commitMessage) {

            final FutureTask<Void> ft = new FutureTaskMon<Void>(
                    new Commit2PhaseTask(commitMessage), null/* Void */);

            /*
             * Run in the caller's thread.
             * 
             * Note: In order to avoid deadlock, when the leader calls back to
             * itself it MUST do so in the same thread in which it is already
             * holding the writeLock.
             */
            ft.run();
            
            return getProxy(ft);

		}
        
        /**
         * 2-Phase commit (service must have voted YES for the 2-phase prepare).
         */
        private class Commit2PhaseTask implements Runnable {

            private final IHA2PhaseCommitMessage commitMessage;

            public Commit2PhaseTask(final IHA2PhaseCommitMessage commitMessage) {

                if (commitMessage == null)
                    throw new IllegalArgumentException();

                this.commitMessage = commitMessage;
                
            }

            @Override
            public void run() {

                QuorumService<HAGlue> localService = null;

                _fieldReadWriteLock.writeLock().lock();

                try {

                    /*
                     * Note: Throws IllegalStateException if quorum has been
                     * terminated. We can't go through the 2-phase commit if the
                     * quorum is terminated.
                     */
                    localService = quorum.getClient();
                    
                    doInnerRun(localService);
                    
                } catch (Throwable t) {

                    try {

                        haLog.error("ERROR IN 2-PHASE COMMIT: " + t
                                + ", rootBlock="
                                + prepareRequest.get().getRootBlock(), t);

                    } catch (Throwable t2) {

                        log.error(t2, t2);

                    }

                    if (localService != null) {

                        localService.enterErrorState();
                        
                    }

                    // always rethrow the root cause exception.
                    throw new RuntimeException(t);

                } finally {

                    // Discard the prepare request.
                    prepareRequest.set(null/* discard */);

                    // Discard the vote.
                    vote.set(false);

                    _fieldReadWriteLock.writeLock().unlock();

                }

            }

            private void doInnerRun(final QuorumService<HAGlue> localService)
                    throws Exception {
                
                final IHA2PhasePrepareMessage prepareMessage = prepareRequest
                        .get();

                if (prepareMessage == null)
                    throw new IllegalStateException();

                if (!prepareMessage.isJoinedService()) {
                    /*
                     * Only services that are joined as of the atomic decision
                     * point should receive a PREPARE or COMMIT message.
                     */
                    throw new AssertionError();
                }

                // Note: Could throw ChecksumError.
                final IRootBlockView rootBlock = prepareMessage == null ? null
                        : prepareMessage.getRootBlock();

                final long commitTime = commitMessage.getCommitTime();

                if (rootBlock == null)
                    throw new IllegalStateException();

                if (haLog.isInfoEnabled())
                    haLog.info("commitTime="
                            + commitTime
                            + ", commitCounter="
                            + prepareMessage.getRootBlock()
                                    .getCommitCounter() + ", vote=" + vote);

                if (rootBlock.getLastCommitTime() != commitTime) {
                    /*
                     * The commit time does not agree with the root
                     * block from the prepare message.
                     */
                    throw new IllegalStateException();
                }

                if (!vote.get()) {

                    /*
                     * This service voted NO. It will not participate in the
                     * commit.
                     */

                    haLog.warn("IGNORING COMMIT2PHASE");

                    return;

                }
                
                // verify that the qourum has not changed.
                quorum.assertQuorum(rootBlock.getQuorumToken());

                if (commitMessage.failCommit_beforeWritingRootBlockOnJournal()) {

                    throw new Mock2PhaseCommitProtocolException();

                }
                
                /*
                 * Write the root block on the local journal.
                 */
                AbstractJournal.this.doLocalCommit(localService, rootBlock);

                if (commitMessage.failCommit_beforeClosingHALog()) {

                    throw new Mock2PhaseCommitProtocolException();

                }

                /*
                 * Write the root block on the HALog file, closing out that
                 * file.
                 */
                localService.logRootBlock(rootBlock);

                if (commitMessage.didAllServicesPrepare()) {

                    /*
                     * The HALog files are conditionally purged (depending
                     * on the IRestorePolicy) on each node any time the
                     * quorum is fully met and goes through a commit point.
                     * The current HALog always remains open.
                     * 
                     * Note: This decision needs to be made in awareness of
                     * whether all services voted to PREPARE. Otherwise we
                     * can hit a problem where some service did not vote to
                     * prepare, but the other services did, and we wind up
                     * purging the HALogs even though one of the services
                     * did not go through the commit2Phase(). This issue is
                     * fixed by the didAllServicesPrepare() flag.
                     */

                    localService.purgeHALogs(rootBlock.getQuorumToken());
                    
                }
            
            } // doInnerRun()
            
        } // Commit2PhaseTask

        @Override
        public Future<Void> abort2Phase(final IHA2PhaseAbortMessage abortMessage) {

            final FutureTask<Void> ft = new FutureTaskMon<Void>(
                    new Abort2PhaseTask(abortMessage), null/* Void */);

            /*
             * Run in the caller's thread.
             * 
             * Note: In order to avoid deadlock, when the leader calls back to
             * itself it MUST do so in the same thread in which it is already
             * holding the writeLock.
             */
            ft.run();

            return getProxy(ft);

        }

        /**
         * 2-Phase abort.
         */
        private class Abort2PhaseTask implements Runnable {

            private final IHA2PhaseAbortMessage abortMessage;

            public Abort2PhaseTask(final IHA2PhaseAbortMessage abortMessage) {

                if (abortMessage == null)
                    throw new IllegalArgumentException();

                this.abortMessage = abortMessage;
            }

            @Override
            public void run() {

                try {

                    // Discard the prepare request.
                    prepareRequest.set(null/* discard */);

                    // Discard the vote.
                    vote.set(false);

                    final long token = abortMessage.getQuorumToken();

                    if (haLog.isInfoEnabled())
                        haLog.info("token=" + token);

                    /*
                     * Note: even if the quorum breaks, we still need to discard
                     * our local state. Forcing doLocalAbort() here is MUCH
                     * safer than failing to invoke it because the quorum
                     * broken. If we do not invoke doLocalAbort() then we could
                     * have an old write set laying around on the journal and it
                     * might accidentally get flushed through with a local
                     * commit. if we always force the local abort here, then the
                     * worst circumstance would be if a 2-phase abort message
                     * for a historical quorum state were somehow delayed and
                     * arrived after we entered a new quorum state. Forcing an
                     * abort under that weird (perhaps impossible) circumstance
                     * will just cause this service to drop out of the quorum if
                     * it later observes a write with the wrote block sequence
                     * or commit counter. This seems like a safe decision.
                     */
                    //quorum.assertQuorum(token); // REMOVED per comment above.

                } finally {
                    
                    // ALWAYS go through the local abort.
                    doLocalAbort();

                }

            }

        }
        
        /**
         * {@inheritDoc}
         * 
         * @todo We should test the LRUNexus for failover reads and install
         *       records into the cache if there is a cache miss. Unfortunately
         *       the cache holds objects, some of which declare how to access
         *       the underlying {@link IDataRecord} using the
         *       {@link IDataRecordAccess} interface.
         * 
         * @todo Since these are rare events it may not be worthwhile to setup a
         *       separate low-level socket service to send/receive the data.
         */
        @Override
        public Future<IHAReadResponse> readFromDisk(
                final IHAReadRequest msg) {

            final long token = msg.getQuorumToken();
//            final UUID storeId = msg.getStoreUUID();
            final long addr = msg.getAddr();
            
            if (haLog.isInfoEnabled())
                haLog.info("token=" + token + ", addr=" + addr);

            final FutureTask<IHAReadResponse> ft = new FutureTask<IHAReadResponse>(
                    new Callable<IHAReadResponse>() {
                        
                @Override
			    public IHAReadResponse call() throws Exception {

		            if (haLog.isInfoEnabled())
		                haLog.info("token=" + token);
		            
				    quorum.assertQuorum(token);
				    
					// final ILRUCache<Long, Object> cache = (LRUNexus.INSTANCE
					// == null) ? null
					// : LRUNexus.getCache(jnl);
					//
					// Object obj = cache.get(addr);
					//                    
					// if(obj != null && obj instanceof IDataRecordAccess) {
					//                        
					// return ((IDataRecordAccess)obj).data();
					//                        
					// }

                    // read from the local store.
                    final ByteBuffer b = ((IHABufferStrategy) getBufferStrategy())
                            .readFromLocalStore(addr);

                    final byte[] a = BytesUtil.toArray(b);
                    
					// cache.putIfAbsent(addr, b);

					return new HAReadResponse(a);

				}
			});
			
			ft.run();
			
            return getProxy(ft);

		}

        /*
         * Delegated to HAQuorumService.
         */
        @Override
        public Future<Void> receiveAndReplicate(final IHASyncRequest req,
                final IHASendState snd, final IHAWriteMessage msg)
                throws IOException {

            if (haLog.isDebugEnabled())
                haLog.debug("req=" + req + ", msg=" + msg);

            final Future<Void> ft = quorum.getClient().receiveAndReplicate(req,
                    snd, msg);

            return getProxy(ft);

		}

        /*
         * This is implemented by HAJournal, which is responsible for
         * maintaining the HA Log files.
         */
        @Override
        public IHALogRootBlocksResponse getHALogRootBlocksForWriteSet(
                IHALogRootBlocksRequest msg) throws IOException {
            throw new UnsupportedOperationException();
        }

        /*
         * This is implemented by HAJournal, which is responsible for
         * maintaining the HA Log files.
         */
        @Override
        public Future<Void> sendHALogForWriteSet(IHALogRequest msg)
                throws IOException {
            throw new UnsupportedOperationException();
        }

        /*
         * This is implemented by HAJournal.
         */
        @Override
        public Future<IHASendStoreResponse> sendHAStore(IHARebuildRequest msg)
                throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public IHAWriteSetStateResponse getHAWriteSetState(
                final IHAWriteSetStateRequest req) {

            final long token = getQuorum().token();
            
            // Verify leader for that token.
            getQuorum().assertLeader(token);

            /*
             * Note: This lock will prevent concurrent commit so the
             * commitCounter is known to be valid for the blockSequence.
             */

            final IHAWriteSetStateResponse resp;
            
            final Lock lock = _fieldReadWriteLock.readLock();
            
            lock.lock();
            
            try {

                final IRootBlockView rb = _rootBlock;
                
                final long sequence = ((IHABufferStrategy) getBufferStrategy())
                        .getCurrentBlockSequence();

                resp = new HAWriteSetStateResponse(rb.getCommitCounter(),
                        rb.getLastCommitTime(), sequence);

            } finally {
                
                lock.unlock();
                
            }

            // Verify still leader for that token.
            getQuorum().assertLeader(token);
            
            return resp;
            
        }

        @Override
        public IHARootBlockResponse getRootBlock(
                final IHARootBlockRequest msg) {

            // storeId is optional (used in scale-out).
            final UUID storeId = msg.getStoreUUID();
//            if (storeId == null)
//                throw new IllegalArgumentException();

            if (haLog.isInfoEnabled())
                haLog.info("storeId=" + storeId);

            if (storeId != null && !getUUID().equals(storeId)) {

                // A request for a different journal's root block.
                throw new UnsupportedOperationException();
                
            }

            if (msg.isNonBlocking()) {

                // Non-blocking code path.
                
                return new HARootBlockResponse(
                        AbstractJournal.this.getRootBlockView());

            } else {

                // Blocking code path.
                
                final ReadLock lock = _fieldReadWriteLock.readLock();

                lock.lock();

                try {
                    
                    return new HARootBlockResponse(
                            AbstractJournal.this.getRootBlockView());

                } finally {
                
                    lock.unlock();
                    
                }
                
            }

        }

//        @Override
//        public Future<Void> bounceZookeeperConnection() {
//            final FutureTask<Void> ft = new FutureTaskMon<Void>(new Runnable() {
//                public void run() {
//                    // NOP (not implemented at this layer).
//                }
//            }, null);
//            ft.run();
//            return getProxy(ft);
//        }
//
//        @Override
//        public Future<Void> enterErrorState() {
//            final FutureTask<Void> ft = new FutureTaskMon<Void>(new Runnable() {
//                public void run() {
//                    // NOP (not implemented at this layer).
//                }
//            }, null);
//            ft.run();
//            return getProxy(ft);
//        }

        /**
         * {@inheritDoc}
         * <p>
         * This implementation does pipeline remove() followed by pipline add().
         */
        @Override
        public Future<Void> moveToEndOfPipeline() {
            final FutureTask<Void> ft = new FutureTaskMon<Void>(new Runnable() {
                public void run() {
                    if (haLog.isInfoEnabled())
                        haLog.info("");
                    final QuorumActor<?, ?> actor = quorum.getActor();
                    actor.pipelineRemove();
                    actor.pipelineAdd();
                }
            }, null/* result */);
            getExecutorService().execute(ft);
            return getProxy(ft);
        }

        @Override
        public Future<IHAPipelineResetResponse> resetPipeline(
                final IHAPipelineResetRequest req) throws IOException {
            final Future<IHAPipelineResetResponse> f = quorum.getClient()
                    .resetPipeline(req);
            return getProxy(f);
        }

        /*
         * HATXSGlue.
         * 
         * Note: API is mostly implemented by Journal/HAJournal.
         */
        
//        /**
//         * Clear the {@link #gatherFuture} and return <code>true</code> iff the
//         * {@link Future} was available, was already done, and the computation
//         * did not result in an error. Othewise return <code>false</code>.
//         * <p>
//         * Note: This is invoked from
//         * {@link #prepare2Phase(IHA2PhasePrepareMessage)} to determine whether
//         * the gather operation on the follower completed normally. It is also
//         * invoked from {@link AbstractJournal#doLocalAbort()} and from
//         * {@link #gatherMinimumVisibleCommitTime(IHAGatherReleaseTimeRequest)}
//         * to ensure that the outcome from a previous gather is cleared before a
//         * new one is attempted.
//         * 
//         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/673" >
//         *      Native thread leak in HAJournalServer process </a>
//         */
//        private boolean clearGatherOutcome() {
//            final Future<Void> oldFuture = gatherFuture
//                    .getAndSet(null/* newValue */);
//            if (oldFuture != null) {
//                if(!oldFuture.isDone()) {
//                    // Ensure cancelled.
//                    oldFuture.cancel(true/*mayInterruptIfRunning*/);
//                }
//                try {
//                    oldFuture.get();
//                    // Gather was successful.
//                    return true;
//                } catch (InterruptedException e) {
//                    // Note: Future isDone(). Caller will not block.
//                    throw new AssertionError();
//                } catch (ExecutionException e) {
//                    haLog.error("Gather failed on follower: serviceId="
//                            + getServiceId() + " : " + e, e);
//                    return false;
//                }
//            }
//            // Outcome was not available.
//            return false;
//        }
        
        /**
         * {@inheritDoc}
         * 
         * @see <a
         *      href="https://sourceforge.net/apps/trac/bigdata/ticket/673"
         *      > Native thread leak in HAJournalServer process </a>
         */
        @Override
        public void gatherMinimumVisibleCommitTime(
                final IHAGatherReleaseTimeRequest req) throws IOException {

            if (haLog.isInfoEnabled())
                haLog.info("req=" + req);

            {

                /*
                 * Clear the old outcome. Reference SHOULD be null. Ensure not
                 * running.
                 */
                final Future<IHANotifyReleaseTimeResponse> oldFuture = gatherFuture
                        .getAndSet(null);

                if (oldFuture != null && !oldFuture.isDone())
                    oldFuture.cancel(true/* mayInterruptIfRunning */);

            }
            
            /*
             * Lookup the leader using its UUID.
             * 
             * Note: We do not use the token to find the leader. If the token is
             * invalid, then we will handle that once we are in the GatherTask.
             * 
             * Note: We do this early and pass it into the GatherTask. We can
             * not send back an RMI response unless we know the leader's proxy.
             */
            
            final UUID leaderId = req.getLeaderId();

            // Note: Will throw exception if our HAQuorumService is not running.
            final HAGlue leader = getQuorum().getClient().getService(leaderId);

            if (leader == null)
                throw new RuntimeException(
                        "Could not discover the quorum leader.");

            // Get our serviceId.
            final UUID serviceId = getServiceId();

            if (serviceId == null)
                throw new AssertionError();
            
            final Callable<IHANotifyReleaseTimeResponse> task = ((AbstractHATransactionService) AbstractJournal.this
                    .getLocalTransactionManager().getTransactionService())
                    .newGatherMinimumVisibleCommitTimeTask(leader,
                            serviceId, req);

            final FutureTask<IHANotifyReleaseTimeResponse> ft = new FutureTask<IHANotifyReleaseTimeResponse>(task);

            // Save reference to the gather Future.
            gatherFuture.set(ft);

            /*
             * Fire and forget. The Future is checked by prepare2Phase.
             * 
             * Note: This design pattern was used to due a DGC thread leak
             * issue. The gather protocol should be robust even through the
             * Future is not checked (or awaited) here.
             */
            getExecutorService().execute(ft);

            return;
            
        }

        @Override
        public IHANotifyReleaseTimeResponse notifyEarliestCommitTime(
                final IHANotifyReleaseTimeRequest req) throws IOException,
                InterruptedException, BrokenBarrierException {

            /*
             * Note: Pass through [req] without checks. We need to get this
             * message to the CyclicBarrier regardless of whether it is
             * well-formed or valid.
             */
 
            return ((HATXSGlue) AbstractJournal.this
                    .getLocalTransactionManager().getTransactionService())
                    .notifyEarliestCommitTime(req);

        }
        
        /**
         * This exposes the clock used to assign transaction identifiers and
         * commit times. It is being exposed to support certain kinds of
         * overrides for unit tests.
         * <p>
         * Note: This method is NOT exposed to RMI. However, it can still be
         * overridden by the unit tests.
         * 
         * @return The next timestamp from that clock.
         */
        public long nextTimestamp() {

            try {

                return AbstractJournal.this.getLocalTransactionManager()
                        .getTransactionService().nextTimestamp();
                
            } catch (IOException ex) {
                
                /*
                 * Note: This is a local method call. IOException will not be
                 * thrown.
                 */
                
                throw new RuntimeException(ex);
                
            }
     
        }
        
        /*
         * IService
         */
        
        @Override
        public UUID getServiceUUID() throws IOException {

            return getServiceId();
            
        }

        @SuppressWarnings("rawtypes")
        @Override
        public Class getServiceIface() throws IOException {
            
            return HAGlue.class;
            
        }

        @Override
        public String getHostname() throws IOException {

            return AbstractStatisticsCollector.fullyQualifiedHostName;
            
        }

        @Override
        public String getServiceName() throws IOException {

            // TODO Configurable service name?
            return getServiceIface().getName() + "@" + getHostname() + "#"
                    + hashCode();
            
        }

        @Override
        public void destroy() throws RemoteException {

            AbstractJournal.this.destroy();
            
        }

        @Override
        public <T> Future<T> submit(final IIndexManagerCallable<T> callable,
                final boolean asyncFuture) throws IOException {

            callable.setIndexManager(getIndexManager());

            final Future<T> ft = getIndexManager().getExecutorService().submit(
                    callable);

            return getProxy(ft, asyncFuture);

        }

	};

    /**
     * Remove all commit records between the two provided keys.
     * <p>
     * This is called from the RWStore when it checks for deferredFrees against
     * the CommitRecordIndex where the CommitRecords reference the deleteBlocks
     * that have been deferred.
     * <p>
     * Once processed the records for the effected range muct be removed as they
     * reference invalid states.
     * 
     * @param fromKey
     * @param toKey
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/440
     * @see IHistoryManager#checkDeferredFrees(AbstractJournal)
     */
    public int removeCommitRecordEntries(final byte[] fromKey,
            final byte[] toKey) {

        // Use the LIVE indeex!
        final CommitRecordIndex cri = _commitRecordIndex;

        @SuppressWarnings("unchecked")
        final ITupleIterator<CommitRecordIndex.Entry> commitRecords = cri
                .rangeIterator(fromKey, toKey, 0/* capacity */,
                        IRangeQuery.DEFAULT | IRangeQuery.CURSOR, null/* filter */);

        int removed = 0;
        
        while (commitRecords.hasNext()) {

            final ITuple<CommitRecordIndex.Entry> t = commitRecords.next();
            
            // Delete the associated ICommitRecord.
            delete(t.getObject().addr);
            
            // Remove the entry for the commit record from the commit record
            // index.
            commitRecords.remove();
            
            removed++;
            
        }
        
        return removed;
        
    }

    public interface ISnapshotEntry {
    	long getAddress();
    	byte[] getData();
    }
    
    public interface ISnapshotData {
    	void put(long addr, byte[] data);
    	
    	Iterator<ISnapshotEntry> entries();
    }
    
    static public class SnapshotData implements ISnapshotData {
    	
    	final TreeMap<Long, byte[]> m_map = new TreeMap<Long, byte[]>();

		@Override
		public void put(long addr, byte[] data) {
			m_map.put(addr, data);
		}

		@Override
		public Iterator<ISnapshotEntry> entries() {
			final Iterator<Map.Entry<Long, byte[]>> entries = m_map.entrySet().iterator();
			
			return new Iterator<ISnapshotEntry>() {

				@Override
				public boolean hasNext() {
					return entries.hasNext();
				}

				@Override
				public ISnapshotEntry next() {
					final Map.Entry<Long, byte[]> entry = entries.next();
					return new ISnapshotEntry() {

						@Override
						public long getAddress() {
							return entry.getKey();
						}

						@Override
						public byte[] getData() {
							return entry.getValue();
						}
						
					};
				}

				@Override
				public void remove() {
					entries.remove();
				}
				
			};
		}   	
    }

    @Override
	public IAllocationContext newAllocationContext(final boolean isolated) {
		if (_bufferStrategy instanceof RWStrategy) {
			return ((RWStrategy) _bufferStrategy).newAllocationContext(isolated);
		} else {
			return null;
		}
    }
}
