/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.log4j.Logger;

import com.bigdata.BigdataStatics;
import com.bigdata.LRUNexus;
import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.ICheckpointProtocol;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.ReadOnlyIndex;
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
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.ha.RunState;
import com.bigdata.ha.msg.HAReadResponse;
import com.bigdata.ha.msg.HARootBlockRequest;
import com.bigdata.ha.msg.HARootBlockResponse;
import com.bigdata.ha.msg.HAWriteSetStateResponse;
import com.bigdata.ha.msg.IHA2PhaseAbortMessage;
import com.bigdata.ha.msg.IHA2PhaseCommitMessage;
import com.bigdata.ha.msg.IHA2PhasePrepareMessage;
import com.bigdata.ha.msg.IHADigestRequest;
import com.bigdata.ha.msg.IHADigestResponse;
import com.bigdata.ha.msg.IHAGlobalWriteLockRequest;
import com.bigdata.ha.msg.IHALogDigestRequest;
import com.bigdata.ha.msg.IHALogDigestResponse;
import com.bigdata.ha.msg.IHALogRequest;
import com.bigdata.ha.msg.IHALogRootBlocksRequest;
import com.bigdata.ha.msg.IHALogRootBlocksResponse;
import com.bigdata.ha.msg.IHAReadRequest;
import com.bigdata.ha.msg.IHAReadResponse;
import com.bigdata.ha.msg.IHARebuildRequest;
import com.bigdata.ha.msg.IHARootBlockRequest;
import com.bigdata.ha.msg.IHARootBlockResponse;
import com.bigdata.ha.msg.IHASendStoreResponse;
import com.bigdata.ha.msg.IHASnapshotDigestRequest;
import com.bigdata.ha.msg.IHASnapshotDigestResponse;
import com.bigdata.ha.msg.IHASnapshotRequest;
import com.bigdata.ha.msg.IHASnapshotResponse;
import com.bigdata.ha.msg.IHASyncRequest;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.ha.msg.IHAWriteSetStateRequest;
import com.bigdata.ha.msg.IHAWriteSetStateResponse;
import com.bigdata.htree.HTree;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.IDataRecord;
import com.bigdata.io.IDataRecordAccess;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.Name2Addr.Entry;
import com.bigdata.journal.jini.ha.HAJournal;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.JournalMetadata;
import com.bigdata.quorum.AsynchronousQuorumCloseException;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumActor;
import com.bigdata.quorum.QuorumException;
import com.bigdata.quorum.QuorumMember;
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
import com.bigdata.rwstore.sector.MemStrategy;
import com.bigdata.rwstore.sector.MemoryManager;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.stream.Stream;
import com.bigdata.util.ChecksumUtility;
import com.bigdata.util.NT;

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
 * @version $Id$
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
	 * Used to cache the most recent {@link ICommitRecord} -- discarded on
	 * {@link #abort()}; set by {@link #commitNow(long)}.
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

	/**
	 * Return the root block view associated with the commitRecord for the
	 * provided commit time.  This requires accessing the next commit record
	 * since the previous root block is stored with each record.
	 * 
	 * @param commitTime
	 *            A commit time.
	 * 
	 * @return The root block view -or- <code>null</code> if there is no commit
	 *         record for that commitTime.
	 * 
	 */
	public IRootBlockView getRootBlock(final long commitTime) {

		final ICommitRecord commitRecord = getCommitRecordIndex().findNext(commitTime);

		if (commitRecord == null) {
			return null;
		}

		final long rootBlockAddr = commitRecord.getRootAddr(PREV_ROOTBLOCK);
		
		if (rootBlockAddr == 0) {
			return null;
		} else {
			ByteBuffer bb = read(rootBlockAddr);
			
			return new RootBlockView(true /* rb0 - WTH */, bb, checker);
		}

	}
	
	/**
	 * 
	 * @param startTime from which to begin iteration
	 * 
	 * @return an iterator over the committed root blocks
	 */
	public Iterator<IRootBlockView> getRootBlocks(final long startTime) {
		return new Iterator<IRootBlockView>() {
			ICommitRecord commitRecord = getCommitRecordIndex().findNext(startTime);

			public boolean hasNext() {
				return commitRecord != null;
			}
			
			public IRootBlockView next() {
				final long rootBlockAddr = commitRecord.getRootAddr(PREV_ROOTBLOCK);
				
				commitRecord = getCommitRecordIndex().findNext(commitRecord.getTimestamp());
				
				if (rootBlockAddr == 0) {
					return null;
				} else {
					ByteBuffer bb = read(rootBlockAddr);
					
					return new RootBlockView(true /* rb0 - WTH */, bb, checker);
				}
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};
	}

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
                            Integer.MAX_VALUE/* maxSectors */, properties));

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
			if (_bufferStrategy instanceof IRWStrategy) {

			    final int checkpointRecordSize = getByteCount(_commitRecordIndex
                        .getCheckpoint().getCheckpointAddr());

			    ((IRWStrategy) _bufferStrategy).registerExternalCache(
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
				public void sample() {
					final AbstractJournal jnl = ref.get();
					if (jnl != null) {
                        final File file = jnl.getFile();
                        if (file != null)
                            setValue(file.toString());
                    }
				}
			});
			// counters.addCounter("file", new OneShotInstrument<String>(""
			// + jnl.getFile()));

			counters.addCounter("createTime", new Instrument<Long>() {
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
				public void sample() {
					final AbstractJournal jnl = ref.get();
					if (jnl != null) {
						setValue(jnl.historicalIndexCache.size());
					}
				}
			});

            counters.addCounter("indexCacheSize", new Instrument<Integer>() {
                public void sample() {
                    final AbstractJournal jnl = ref.get();
                    if (jnl != null) {
                        setValue(jnl.indexCache.size());
                    }
                }
            });

            counters.addCounter("liveIndexCacheSize", new Instrument<Integer>() {
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

			counters.attach(jnl._bufferStrategy.getCounters());

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

	final public File getFile() {

		final IBufferStrategy tmp = getBufferStrategy();

		if (tmp == null)
			return null;

		return tmp.getFile();

    }

    /**
     * The HA log directory.
     * 
     * @see HAJournal.Options#HA_LOG_DIR
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
    public File getHALogDir() {
	    
	    throw new UnsupportedOperationException();

	}

    /**
     * The HA timeout in milliseconds for a 2-phase prepare.
     * 
     * @see HAJournal.Options#HA_PREPARE_TIMEOUT
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
    public long getHAPrepareTimeout() {
        
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

		if (LRUNexus.INSTANCE != null) {

			try {

				LRUNexus.INSTANCE.deleteCache(getUUID());

			} catch (Throwable t) {

				log.error(t, t);

			}

		}

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
	public void deleteResources() {

		if (isOpen())
			throw new IllegalStateException();

		if (log.isInfoEnabled())
			log.info("");

		final IBufferStrategy bufferStrategy = getBufferStrategy();

		if (bufferStrategy != null) {

			bufferStrategy.deleteResources();

			if (LRUNexus.INSTANCE != null) {

				try {

					LRUNexus.INSTANCE.deleteCache(getUUID());

				} catch (Throwable t) {

					log.error(t, t);

				}

			}

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
	synchronized public void close() {

		// Note: per contract for close().
		if (!isOpen())
			throw new IllegalStateException();

		if (log.isInfoEnabled())
			log.info("");

		shutdownNow();

	}

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

	final public UUID getUUID() {

		return journalMetadata.get().getUUID();

	}

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
	public boolean isOpen() {

		return _bufferStrategy != null && _bufferStrategy.isOpen();

	}

    /**
     * Return <code>true</code> if the journal was opened in a read-only mode or
     * if {@link #closeForWrites(long)} was used to seal the journal against
     * further writes.
     */
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

        if (token != Quorum.NO_QUORUM) {

            // Quorum exists, are we the leader for that token?
            final boolean isLeader = quorum.getClient().isLeader(token);

            // read-only unless this is the leader.
            return !isLeader;

        }

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

    }

	public boolean isStable() {

	    return _bufferStrategy.isStable();

	}

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

    /**
     * Return <code>true</code> if the journal is configured for high
     * availability.
     * 
     * @see QuorumManager#isHighlyAvailable()
     */
	public boolean isHighlyAvailable() {

        return quorum == null ? false : quorum.isHighlyAvailable();

	}

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

		int ncommitters = 0;

		final long[] rootAddrs = new long[_committers.length];

		for (int i = 0; i < _committers.length; i++) {

		    final ICommitter committer = _committers[i];
		    
			if (committer == null)
				continue;

			final long addr = committer.handleCommit(commitTime);

			rootAddrs[i] = addr;

			ncommitters++;

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

		final WriteLock lock = _fieldReadWriteLock.writeLock();

		lock.lock();

		try {

			if (log.isInfoEnabled())
				log.info("start");

			if (_bufferStrategy == null) {

				// Nothing to do.
				return;

			}

			txLog.info("ABORT");

			if (LRUNexus.INSTANCE != null) {

				/*
				 * Discard the LRU for this store. It may contain writes which
				 * have been discarded. The same addresses may be reissued by
				 * the WORM store after an abort, which could lead to incorrect
				 * reads from a dirty cache.
				 * 
				 * FIXME An optimization would essentially isolate the writes on
				 * the cache per BTree or between commits. At the commit point,
				 * the written records would be migrated into the "committed"
				 * cache for the store. The caller would read on the uncommitted
				 * cache, which would read through to the "committed" cache.
				 * This would prevent incorrect reads without requiring us to
				 * throw away valid records in the cache. This could be a
				 * significant performance gain if aborts are common on a
				 * machine with a lot of RAM.
				 */

				LRUNexus.getCache(this).clear();

			}

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
			 * Discard hard references to any indices. The Name2Addr reference
			 * will also be discarded below. This should be sufficient to ensure
			 * that any index requested by the methods on the AbstractJournal
			 * will be re-read from disk using the commit record which we
			 * re-load below. This is necessary in order to discard any
			 * checkpoints that may have been written on indices since the last
			 * commit.
			 * 
			 * FIXME Verify this is not required. Historical index references
			 * should not be discarded on abort as they remain valid. Discarding
			 * them admits the possibility of a non-canonicalizing cache for the
			 * historical indices since an existing historical index reference
			 * will continue to be held but a new copy of the index will be
			 * loaded on the next request if we clear the cache here.
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

			// setup new committers, e.g., by reloading from their last root
			// addr.
			setupCommitters();

			if (log.isInfoEnabled())
				log.info("done");

		} finally {

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

    @Override
	public long commit() {

		final ILocalTransactionManager transactionManager = getLocalTransactionManager();

		/*
		 * Get timestamp that will be assigned to this commit (RMI if the
		 * journal is part of a distributed federation).
		 */
		final long commitTime = transactionManager.nextTimestamp();

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

		transactionManager.notifyCommit(commitTime);

		return commitTime;

	}

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
	protected long commitNow(final long commitTime) {

		final WriteLock lock = _fieldReadWriteLock.writeLock();

		lock.lock();

		try {

			assertOpen();

			final long beginNanos = System.nanoTime();

			// #of bytes on the journal as of the previous commit point.
			final long byteCountBefore = _rootBlock.getNextOffset();

			if (log.isInfoEnabled())
				log.info("commitTime=" + commitTime);

			assertCommitTimeAdvances(commitTime);

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
			final long[] rootAddrs = notifyCommitters(commitTime);

			/*
			 * See if anything has been written on the store since the last
			 * commit.
			 */
			if (!_bufferStrategy.requiresCommit(_rootBlock)) {

				/*
				 * No data was written onto the store so the commit can not
				 * achieve any useful purpose.
				 */

				if (log.isInfoEnabled())
					log.info("Nothing to commit");

				return 0L;
			}
			
            /*
             * Explicitly call the RootBlockCommitter
             */
            rootAddrs[PREV_ROOTBLOCK] = this.m_rootBlockCommitter
                    .handleCommit(commitTime);

			/*
			 * Write the commit record onto the store.
			 * 
			 * @todo Modify to log the current root block and set the address of
			 * that root block in the commitRecord. This will be of use solely
			 * in disaster recovery scenarios where your root blocks are toast,
			 * but good root blocks can be found elsewhere in the file.
			 */

			final IRootBlockView old = _rootBlock;

			final long newCommitCounter = old.getCommitCounter() + 1;
			
            final ICommitRecord commitRecord = new CommitRecord(commitTime,
                    newCommitCounter, rootAddrs);

            final long commitRecordAddr = write(ByteBuffer
                    .wrap(CommitRecordSerializer.INSTANCE
                            .serialize(commitRecord)));

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
                ((IHistoryManager) _bufferStrategy).checkDeferredFrees(this);
            }

            /*
             * Add the commit record to an index so that we can recover
             * historical states efficiently.
             */
			_commitRecordIndex.add(commitRecordAddr, commitRecord);
			
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
            final long commitRecordIndexAddr = _commitRecordIndex
                    .writeCheckpoint();

            if (quorum != null) {
                /*
                 * Verify that the last negotiated quorum is still valid.
                 */
                quorum.assertLeader(quorumToken);
            }

            /*
             * Call commit on buffer strategy prior to retrieving root block,
             * required for RWStore since the metaBits allocations are not made
             * until commit, leading to invalid addresses for recent store
             * allocations.
             * 
             * Note: This will flush the write cache. For HA, that ensures that
             * the write set has been replicated to the followers.
             * 
             * Note: After this, we do not write anything on the backing store
             * other than the root block. The rest of this code is dedicated to
             * creating a properly formed root block. For a non-HA deployment,
             * we just lay down the root block. For an HA deployment, we do a
             * 2-phase commit.
             * 
             * Note: In HA, the followers lay down the replicated writes
             * synchronously. Thus, they are guaranteed to be on local storage
             * by the time the leader finishes WriteCacheService.flush(). This
             * does not create much latency because the WriteCacheService drains
             * the dirtyList in a seperate thread.
             */
			_bufferStrategy.commit();

			/*
			 *  next offset at which user data would be written.
			 *  Calculated, after commit!
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
			 * Prepare the new root block.
			 */
			final IRootBlockView newRootBlock;
			{

				/*
				 * Update the firstCommitTime the first time a transaction
				 * commits and the lastCommitTime each time a transaction
				 * commits (these are commit timestamps of isolated or
				 * unisolated transactions).
				 */

                final long firstCommitTime = (old.getFirstCommitTime() == 0L ? commitTime
                        : old.getFirstCommitTime());

                final long priorCommitTime = old.getLastCommitTime();

				if (priorCommitTime != 0L) {

					/*
					 * This is a local sanity check to make sure that the commit
					 * timestamps are strictly increasing. An error will be
					 * reported if the commit time for the current (un)isolated
					 * transaction is not strictly greater than the last commit
					 * time on the store as read back from the current root
					 * block.
					 */

                    assertPriorCommitTimeAdvances(commitTime, priorCommitTime);

				}

				final long lastCommitTime = commitTime;
				final long metaStartAddr = _bufferStrategy.getMetaStartAddr();
				final long metaBitsAddr = _bufferStrategy.getMetaBitsAddr();

				// Create the new root block.
                newRootBlock = new RootBlockView(!old.isRootBlock0(), old
                        .getOffsetBits(), nextOffset, firstCommitTime,
                        lastCommitTime, newCommitCounter, commitRecordAddr,
                        commitRecordIndexAddr, old.getUUID(), //
                        blockSequence, quorumToken,//
                        metaStartAddr, metaBitsAddr, old.getStoreType(),
                        old.getCreateTime(), old.getCloseTime(), 
                        old.getVersion(), checker);

			}

            if (quorum == null) {
                
                /*
                 * Non-HA mode.
                 */

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
                if (doubleSync) {

                    _bufferStrategy.force(false/* metadata */);

                }

                // write the root block on to the backing store.
                _bufferStrategy.writeRootBlock(newRootBlock, forceOnCommit);

                // set the new root block.
                _rootBlock = newRootBlock;

                // reload the commit record from the new root block.
                _commitRecord = _getCommitRecord();

                if (txLog.isInfoEnabled())
                    txLog.info("COMMIT: commitTime=" + commitTime);

            } else {
                
                /*
                 * HA mode.
                 * 
                 * Note: We need to make an atomic decision here regarding
                 * whether a service is joined with the met quorum or not. This
                 * information will be propagated through the HA 2-phase prepare
                 * message so services will know how they must intepret the
                 * 2-phase prepare(), commit(), and abort() requests. The atomic
                 * decision is necessary in order to enforce a consistent role
                 * on a services that is resynchronizing and which might vote to
                 * join the quorum and enter the quorum asynchronously with
                 * respect to this decision point.
                 * 
                 * TODO If necessary, we could also explicitly provide the zk
                 * version metadata for the znode that is the parent of the
                 * joined services. However, we would need an expanded interface
                 * to get that metadata from zookeeper out of the Quorum..
                 */
                
                // Local HA service implementation (non-Remote).
                final QuorumService<HAGlue> quorumService = quorum.getClient();

                // The services joined with the met quorum, in their join order.
                final UUID[] joinedServiceIds = getQuorum().getJoined();
                
                // The services in the write pipeline (in any order).
                final Set<UUID> nonJoinedPipelineServiceIds = new LinkedHashSet<UUID>(
                        Arrays.asList(getQuorum().getPipeline()));
                
                // Remove all services that are joined from this collection.
                for(UUID joinedServiceId : joinedServiceIds) {

                    nonJoinedPipelineServiceIds.remove(joinedServiceId);
                    
                }

                boolean didVoteYes = false;
                try {

                    // issue prepare request.
                    final int nyes = quorumService.prepare2Phase(//
                            joinedServiceIds,//
                            nonJoinedPipelineServiceIds,//
//                            !old.isRootBlock0(),//
                            newRootBlock,//
                            quorumService.getPrepareTimeout(), // timeout
                            TimeUnit.MILLISECONDS//
                            );

                    final boolean willCommit = quorum.isQuorum(nyes);
                    
                    if (haLog.isInfoEnabled())
                        haLog.info("Will " + (willCommit ? "" : "not ")
                                + "commit: " + nyes + " out of "
                                + quorum.replicationFactor()
                                + " services voted yes.");

                    if (willCommit) {

                        didVoteYes = true;

                        quorumService.commit2Phase(
                                joinedServiceIds,//
                                nonJoinedPipelineServiceIds, quorumToken,
                                commitTime);

                    } else {

                        quorumService.abort2Phase(quorumToken);

                    }

                } catch (Throwable e) {
                    if (didVoteYes) {
                        /*
                         * The quorum voted to commit, but something went wrong.
                         * 
                         * FIXME RESYNC : At this point the quorum is probably
                         * inconsistent in terms of their root blocks. Rather
                         * than attempting to send an abort() message to the
                         * quorum, we probably should force the leader to yield
                         * its role at which point the quorum will attempt to
                         * elect a new master and resynchronize.
                         */
                        if (quorumService != null) {
                            try {
                                quorumService.abort2Phase(quorumToken);
                            } catch (Throwable t) {
                                log.warn(t, t);
                            }
                        }
                    } else {
                        /*
                         * This exception was thrown during the abort handling
                         * logic. Note that we already attempting an 2-phase
                         * abort since the quorum did not vote "yes".
                         * 
                         * TODO We should probably force a quorum break since
                         * there is clearly something wrong with the lines of
                         * communication among the nodes.
                         */
                    }
                    throw new RuntimeException(e);
                }

            }

			final long elapsedNanos = System.nanoTime() - beginNanos;

			if (BigdataStatics.debug || log.isInfoEnabled()) {
				final String msg = "commit: commitTime=" + commitTime + ", latency="
						+ TimeUnit.NANOSECONDS.toMillis(elapsedNanos) + ", nextOffset=" + nextOffset + ", byteCount="
						+ (nextOffset - byteCountBefore);
				if (BigdataStatics.debug)
					System.err.println(msg);
				else if (log.isInfoEnabled())
					log.info(msg);
				if (BigdataStatics.debug && LRUNexus.INSTANCE != null) {
					System.err.println(LRUNexus.INSTANCE.toString());
				}
			}

			return commitTime;

		} finally {

			lock.unlock();

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
     * @param commitTime
     *            The proposed commit time.
     * 
     * @throws IllegalArgumentException
     *             if the <i>commitTime</i> is LTE the value reported by
     *             {@link IRootBlockView#getLastCommitTime()}.
     */
    protected void assertPriorCommitTimeAdvances(final long currentCommitTime,
            final long priorCommitTime) {

        if (currentCommitTime <= priorCommitTime) {

            throw new RuntimeException("Time goes backwards: commitTime="
                    + currentCommitTime + ", but lastCommitTime="
                    + priorCommitTime + " on the current root block");

        }

    }

	public void force(final boolean metadata) {

		assertOpen();

		_bufferStrategy.force(metadata);

	}

	public long size() {

		return _bufferStrategy.size();

	}

    public ByteBuffer read(final long addr) {
            assertOpen();

        assertCanRead();
            
        return _bufferStrategy.read(addr);
            
	}
    
    public long write(final ByteBuffer data) {

        assertCanWrite();

        return _bufferStrategy.write(data);
	
    }

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
    public void delete(final long addr) {

        assertCanWrite();

        _bufferStrategy.delete(addr);

    }

    public void delete(final long addr, final IAllocationContext context) {

        assertCanWrite();

        if(_bufferStrategy instanceof IRWStrategy) {
        
            ((IRWStrategy) _bufferStrategy).delete(addr, context);
            
        } else {
            
            _bufferStrategy.delete(addr);
            
        }

    }
    
    public void detachContext(final IAllocationContext context) {
        
        assertCanWrite();

        if(_bufferStrategy instanceof IRWStrategy) {

            ((IRWStrategy) _bufferStrategy).detachContext(context);
            
        }
    	
    }

    public void abortContext(final IAllocationContext context) {
        
        assertCanWrite();

        if(_bufferStrategy instanceof IRWStrategy) {

            ((IRWStrategy) _bufferStrategy).abortContext(context);
            
        }
        
    }

    public void registerContext(final IAllocationContext context) {
        
        assertCanWrite();

        if(_bufferStrategy instanceof IRWStrategy) {

            ((IRWStrategy) _bufferStrategy).registerContext(context);
            
        }
        
    }
    
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
        public long handleCommit(final long commitTime) {

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
	public IIndex getReadOnlyCommitRecordIndex() {

	    final ReadLock lock = _fieldReadWriteLock.readLock();

        lock.lock();

        try {

            assertOpen();
            
            final CommitRecordIndex commitRecordIndex = getCommitRecordIndex(_rootBlock
                    .getCommitRecordIndexAddr());

            return new ReadOnlyIndex(commitRecordIndex);

        } finally {

            lock.unlock();

        }
 
	}
	
	/**
	 * Return the current state of the index that resolves timestamps to
	 * {@link ICommitRecord}s.
	 * <p>
	 * Note: The returned object is NOT safe for concurrent operations and is
	 * NOT systematically protected by the use of synchronized blocks within
	 * this class.
	 * 
	 * @return The {@link CommitRecordIndex}.
	 * 
	 * @todo If you need access to this object in an outside class consider
	 *       using {@link #getRootBlockView()},
	 *       {@link IRootBlockView#getCommitRecordIndexAddr()}, and
	 *       {@link #getCommitRecord(long)} to obtain a distinct instance
	 *       suitable for read-only access.
	 */
	protected CommitRecordIndex getCommitRecordIndex() {

		final ReadLock lock = _fieldReadWriteLock.readLock();

		lock.lock();

		try {

			assertOpen();

			final CommitRecordIndex commitRecordIndex = _commitRecordIndex;

			if (commitRecordIndex == null)
				throw new AssertionError();

			return commitRecordIndex;

		} finally {

			lock.unlock();

		}

	}

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
		    
			return getCommitRecordIndex(addr);

		} catch (RuntimeException ex) {

			/*
			 * Log the root block for post-mortem.
			 */
			log.fatal("Could not read the commit record index:\n" + _rootBlock, ex);

			throw ex;

		}

	}

    /**
     * Create or re-load the index that resolves timestamps to
     * {@link ICommitRecord}s.
     * <p>
     * Note: The returned object is NOT cached. When addr is non-{@link #NULL},
     * each invocation will return a distinct {@link CommitRecordIndex} object.
     * This behavior is partly for historical reasons but it does serve to
     * protect the live {@link CommitRecordIndex} from outside access. This is
     * important since access to the live {@link CommitRecordIndex} is NOT
     * systematically protected by <code>synchronized</code> within this class.
     * 
     * @param addr
     *            The root address of the index -or- 0L if the index has not
     *            been created yet.
     * 
     * @return The {@link CommitRecordIndex} for that address or a new index if
     *         0L was specified as the address.
     * 
     * @see #_commitRecordIndex
     * 
     *      TODO We could modify getCommitRecordIndex() to accept
     *      readOnly:boolean and let it load/create a read-only view rather than
     *      wrapping it with a ReadOnlyIndex.
     */
	protected CommitRecordIndex getCommitRecordIndex(final long addr) {

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
			if (isReadOnly()) {

				ndx = CommitRecordIndex.createTransient();

			} else {

				ndx = CommitRecordIndex.create(this);

			}

		} else {

			/*
			 * Reload the mutable btree from its root address.
			 */

			ndx = (CommitRecordIndex) BTree.load(this, addr, false/* readOnly */);

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

	    if (this._bufferStrategy instanceof IRWStrategy) {

	        final long lastReleaseTime = ((IRWStrategy) _bufferStrategy)
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
     *      TODO Reconcile API tension with {@link IIndex} and
     *      {@link ICheckpointProtocol}, however this method is overridden by
     *      {@link Journal} and is also implemented by
     *      {@link IBigdataFederation}. The central remaining tensions are
     *      {@link FusedView} and the local/remote aspect. {@link FusedView}
     *      could probably be "fixed" by extending {@link AbstractBTree} rather
     *      than having an inner delegate for the mutable view. The local/remote
     *      issue is more complex.
     */
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
    public ICheckpointProtocol register(final String name,
            final IndexMetadata metadata) {

        final ICheckpointProtocol ndx = Checkpoint.create(this, metadata);

        _register(name, ndx);

        return ndx;

    }

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
     * Drops the named index. The index will no longer participate in atomic
     * commits and will not be visible to new transactions.  Storage will be
     * reclaimed IFF the backing store support that functionality.
     */
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

            final long readsOnCommitTime = getLocalTransactionManager().getTx(
                    timestamp).getReadsOnCommitTime();

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
    final public BTree getIndex(final String name) {

        return (BTree) getUnisolatedIndex(name);
        
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
    final public HTree getHTree(final String name) {
        
        return (HTree) getUnisolatedIndex(name);
        
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
    final public Stream getStream(final String name) {
        
        return (Stream) getUnisolatedIndex(name);
        
    }

    /**
     * Return the mutable view of the named persistence capable data structure
     * (aka the "live" or {@link ITx#UNISOLATED} view).
     * 
     * @return The mutable view of the persistence capable data structure.
     */
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

	final public long getOffset(long addr) {
		return _bufferStrategy.getOffset(addr);
	}

	final public long getPhysicalAddress(long addr) {
		return _bufferStrategy.getAddressManager().getPhysicalAddress(addr);
	}

	final public int getByteCount(long addr) {
		return _bufferStrategy.getByteCount(addr);
	}

	final public long toAddr(int nbytes, long offset) {
		return _bufferStrategy.toAddr(nbytes, offset);
	}

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

    protected void setQuorumToken(final long newValue) {

        /*
         * The token is [volatile]. Save it's state on entry. Figure out if this
         * is a quorum meet or a quorum break.
         */
        
        final long oldValue = quorumToken;

        if (haLog.isInfoEnabled())
            haLog.info("oldValue=" + oldValue + ", newToken=" + newValue);

        if (oldValue == newValue) {
         
            // No change.
            return;
            
        }
        
        final boolean didBreak;
        final boolean didMeet;

        if (newValue == Quorum.NO_QUORUM && oldValue != Quorum.NO_QUORUM) {

            /*
             * Quorum break.
             * 
             * Immediately invalidate the token. Do not wait for a lock.
             */
            
            this.quorumToken = newValue;

            didBreak = true;
            didMeet = false;

        } else if (newValue != Quorum.NO_QUORUM && oldValue == Quorum.NO_QUORUM) {

            /*
             * Quorum meet.
             * 
             * We must wait for the lock to update the token.
             */
            
            didBreak = false;
            didMeet = true;

        } else {

            /*
             * Excluded middle. If there was no change, then we returned
             * immediately up above. If there is a change, then it must be
             * either a quorum break or a quorum meet, which were identified in
             * the if-then-else above.
             */

            throw new AssertionError();
            
        }

        // This quorum member.
        final QuorumService<HAGlue> localService = quorum.getClient();

        /*
         * Both a meet and a break require an exclusive write lock.
         */
        final boolean isLeader;
        final boolean isFollower;
        final long localCommitCounter;

        final WriteLock lock = _fieldReadWriteLock.writeLock();
        
        lock.lock();

        try {

            if (didBreak) {

                /*
                 * We also need to discard any active read/write tx since there
                 * is no longer a quorum and a read/write tx was running on the
                 * old leader.
                 * 
                 * We do not need to discard read-only tx since the committed
                 * state should remain valid even when a quorum is lost.
                 * 
                 * FIXME HA : QUORUM TX INTEGRATION (discard running read/write tx).
                 */
                
                // local abort (no quorum, so we can do 2-phase abort).
//                _abort();
//                getLocalTransactionManager().
                
                /*
                 * Note: We can not re-cast our vote until our last vote is
                 * widthdrawn. That is currently done by QuorumWatcherBase. So,
                 * we have to wait until we observe that to cast a new vote.
                 */

                localCommitCounter = -1;
                isLeader = isFollower = false;
                
                haReadyToken = Quorum.NO_QUORUM; // volatile write.

                haReadyCondition.signalAll(); // signal ALL.
                
            } else if (didMeet) {

                final long tmp;
                
                quorumToken = newValue;

                boolean installedRBs = false;

                localCommitCounter = _rootBlock.getCommitCounter();
                
                if (localService.isFollower(quorumToken)) {

                    isLeader = false;
                    isFollower = true;

                    if (localCommitCounter == 0L) {
                        
                        /*
                         * Take the root blocks from the quorum leader and use
                         * them.
                         */

                        // Remote interface for the quorum leader.
                        final HAGlue leader = localService
                                .getLeader(quorumToken);

                        log.info("Fetching root block from leader.");
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

                } else if (localService.isLeader(quorumToken)) {

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

                if (!installedRBs) {

                    /*
                     * If we install the RBs, the a local abort was already
                     * done. Otherwise we need to do one now (this covers the
                     * case when setQuorumToken() is called on the leader as
                     * well as cases where the service is either not a follower
                     * or is a follower, but the leader is not at
                     * commitCounter==0L, etc.
                     */
                    
                    doLocalAbort();

                }

                this.haReadyToken = tmp; // volatile write.

                haReadyCondition.signalAll(); // signal ALL.
                
            } else {

                throw new AssertionError();
                
            }
            
        } finally {

            lock.unlock();
            
        }
        
        if (isLeader || isFollower) {

            localService.didMeet(newValue, localCommitCounter, isLeader);

        }
        
    }
    private final Condition haReadyCondition = _fieldReadWriteLock.writeLock().newCondition();
    private volatile long haReadyToken = Quorum.NO_QUORUM;
    
    /**
     * Await the service being ready to partitipate in an HA quorum. The
     * preconditions include:
     * <ol>
     * <li>receiving notice of the quorum token via
     * {@link #setQuorumToken(long)}</li>
     * <li>The service is joined with the met quorum for that token</li>
     * <li>If the service is a follower and it's local root blocks were at
     * <code>commitCounter:=0</code>, then the root blocks from the leader have
     * been installed on the follower.</li>
     * <ol>
     * 
     * @return the quorum token for which the service became HA ready.
     */
    final public long awaitHAReady() throws InterruptedException,
            AsynchronousQuorumCloseException, QuorumException {
        final WriteLock lock = _fieldReadWriteLock.writeLock();
        lock.lock();
        try {
            long t = Quorum.NO_QUORUM;
            while (((t = haReadyToken) != Quorum.NO_QUORUM)
                    && getQuorum().getClient() != null) {
                haReadyCondition.await();
            }
            final QuorumService<?> client = getQuorum().getClient();
            if (client == null)
                throw new AsynchronousQuorumCloseException();
           if (!client.isJoinedMember(t)) {
                throw new QuorumException();
            }
            return t;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Await the service being ready to partitipate in an HA quorum. The
     * preconditions include:
     * <ol>
     * <li>receiving notice of the quorum token via
     * {@link #setQuorumToken(long)}</li>
     * <li>The service is joined with the met quorum for that token</li>
     * <li>If the service is a follower and it's local root blocks were at
     * <code>commitCounter:=0</code>, then the root blocks from the leader have
     * been installed on the follower.</li>
     * <ol>
     * 
     * @param timeout
     *            The timeout to await this condition.
     * @param units
     *            The units for that timeout.
     *            
     * @return the quorum token for which the service became HA ready.
     */
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
     * Local commit protocol (HA).
     */
    protected void doLocalAbort() {

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

            // The timestamp for this commit point.
            final long commitTime = rootBlock.getLastCommitTime();

            // write the root block on to the backing store.
            _bufferStrategy.writeRootBlock(rootBlock, forceOnCommit);

            // set the new root block.
            _rootBlock = rootBlock;

            final boolean leader = localService == null ? false : localService
                    .isLeader(rootBlock.getQuorumToken());

            if (!leader) {

                /*
                 * Ensure allocators are synced after commit. This is only done
                 * for the followers. The leader has been updating the in-memory
                 * allocators as it lays down the writes. The followers have not
                 * be updating the allocators.
                 */

                if (haLog.isInfoEnabled() && localService != null)
                    haLog.info("Reset from root block: serviceUUID="
                            + localService.getServiceId());

                ((IHABufferStrategy) _bufferStrategy)
                        .resetFromHARootBlock(rootBlock);

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
     * The {@link Quorum} for this service -or- <code>null</code> if the service
     * is not running with a quorum.
     */
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

        @Override
        final public UUID getServiceId() {

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
        public long awaitHAReady(final long timeout, final TimeUnit units)
                throws AsynchronousQuorumCloseException, InterruptedException,
                TimeoutException {

            return AbstractJournal.this.awaitHAReady(timeout, units);

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

        @Override
        public Future<Void> globalWriteLock(final IHAGlobalWriteLockRequest req)
                throws IOException, TimeoutException, InterruptedException {

            throw new UnsupportedOperationException();

        }

        @Override
        public Future<IHASnapshotResponse> takeSnapshot(
                final IHASnapshotRequest req) throws IOException {

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

			// Save off a reference to the prepare request.
			prepareRequest.set(prepareMessage);
			
            // Clear vote (assume NO unless proven otherwise).
            vote.set(false);

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

                ft = new FutureTaskMon<Boolean>(new VoteNoTask());

            } else {

                /*
                 * A task to flush and sync if the service is joined with the
                 * met quorum.
                 */

                ft = new FutureTaskMon<Boolean>(new Prepare2PhaseTask(
                        prepareMessage) {
                });

            }

			if(isLeader) {

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
        private class VoteNoTask implements Callable<Boolean>{

            public Boolean call() throws Exception {

                // Vote NO.
                vote.set(false);
                
                return vote.get();

            }

        } // class VoteNoTask

        /**
         * Task prepares for a 2-phase commit (syncs to the disk) and votes YES
         * iff if is able to prepare successfully.
         */
        private class Prepare2PhaseTask implements Callable<Boolean> {

            private final IHA2PhasePrepareMessage prepareMessage;

            public Prepare2PhaseTask(final IHA2PhasePrepareMessage prepareMessage) {

                if (prepareMessage == null)
                    throw new IllegalArgumentException();
                
                this.prepareMessage = prepareMessage;
                
            }
            
            public Boolean call() throws Exception {

                final IRootBlockView rootBlock = prepareMessage.getRootBlock();

                if (haLog.isInfoEnabled())
                    haLog.info("preparedRequest=" + rootBlock);

                if (rootBlock == null)
                    throw new IllegalStateException();

                if (!rootBlock.getUUID().equals(
                        AbstractJournal.this._rootBlock.getUUID())) {

                    /*
                     * The root block has a different UUID. We can not accept this
                     * condition.
                     */

                    throw new IllegalStateException();

                }

                if (rootBlock.getLastCommitTime() <= AbstractJournal.this._rootBlock
                        .getLastCommitTime()) {

                    /*
                     * The root block has a commit time that is LTE the most recent
                     * commit on this Journal. We can not accept this condition.
                     */

                    throw new IllegalStateException();

                }

                if (rootBlock.getCommitCounter() <= AbstractJournal.this._rootBlock
                        .getCommitCounter()) {

                    /*
                     * The root block has a commit counter that is LTE the most
                     * recent commit counter on this Journal. We can not accept this
                     * condition.
                     */

                    throw new IllegalStateException();
                    
                }

                // the quorum token from the leader is in the root block.
                final long prepareToken = rootBlock.getQuorumToken();

                quorum.assertQuorum(prepareToken);

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

                // Vote YES.
                vote.set(true);

                return vote.get();
                
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

            public void run() {

                _fieldReadWriteLock.writeLock().lock();

                try {

                    final long commitTime = commitMessage.getCommitTime();
                    
                    if (haLog.isInfoEnabled())
                        haLog.info("commitTime=" + commitTime + ", vote="
                                + vote);

                    if (!vote.get()) {
                        
                        /*
                         * This service voted NO. It will not participate in
                         * the commit.
                         */

                        return;
                    
                    }
                    
                    final IHA2PhasePrepareMessage prepareMessage = prepareRequest.get();

                    if (prepareMessage == null)
                        throw new IllegalStateException();

                    final IRootBlockView rootBlock = prepareMessage
                            .getRootBlock();

                    if (rootBlock == null)
                        throw new IllegalStateException();

                    if (rootBlock.getLastCommitTime() != commitTime) {
                        /*
                         * The commit time does not agree with the root
                         * block from the prepare message.
                         */
                        throw new IllegalStateException();
                    }

                    // verify that the qourum has not changed.
                    quorum.assertQuorum(rootBlock.getQuorumToken());

                    final QuorumService<HAGlue> localService = quorum
                            .getClient();

                    if (prepareMessage.isJoinedService()) {

                        /*
                         * Only the services that are joined go through the
                         * commit protocol.
                         */

                        AbstractJournal.this.doLocalCommit(localService,
                                rootBlock);

                    } // if(isJoinedService)

                    try {

                        /*
                         * Write the root block on the HA log file, closing
                         * out that file.
                         */
                        
                        localService.logRootBlock(
                                prepareMessage.isJoinedService(), rootBlock);

                    } catch (IOException e) {
                        /*
                         * We have already committed.
                         * 
                         * This HA log file will be unusable if we have to
                         * replay the write set to resynchronize some other
                         * service. However, it is still possible to obtain
                         * the HA log file from some other service in the
                         * qourum. If all else fails, the hypothetical
                         * service can be rebuilt from scratch.
                         */
                        haLog.error("UNABLE TO SEAL HA LOG FILE WITH ROOT BLOCK: "
                                + getServiceId()
                                + ", rootBlock="
                                + rootBlock);
                    }

                    /*
                     * The HA log files are purged on each node any time the
                     * quorum is fully met and goes through a commit point.
                     * Leaving only the current open log file.
                     */

                    localService.purgeHALogs(rootBlock.getQuorumToken());

                } catch(Throwable t) {
                    
                    haLog.error("ERROR IN 2-PHASE COMMIT: " + t
                            + ", rootBlock=" + prepareRequest.get(), t);

                    quorum.getActor().serviceLeave();

                    throw new RuntimeException(t);
                    
                } finally {

                    // Discard the prepare request.
                    prepareRequest.set(null/* discard */);

                    // Discard the vote.
                    vote.set(false);

                    _fieldReadWriteLock.writeLock().unlock();

                }

            }

        }

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
        public Future<IHAReadResponse> readFromDisk(
                final IHAReadRequest msg) {

            final long token = msg.getQuorumToken();
//            final UUID storeId = msg.getStoreUUID();
            final long addr = msg.getAddr();
            
            if (haLog.isInfoEnabled())
                haLog.info("token=" + token + ", addr=" + addr);

            final FutureTask<IHAReadResponse> ft = new FutureTask<IHAReadResponse>(
                    new Callable<IHAReadResponse>() {
				
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
                final IHAWriteMessage msg) throws IOException {

            if (haLog.isDebugEnabled())
                haLog.debug("req=" + req + ", msg=" + msg);

            final Future<Void> ft = quorum.getClient().receiveAndReplicate(req,
                    msg);

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

            return new HARootBlockResponse(
                    AbstractJournal.this.getRootBlockView());

        }

        @Override
        public Future<Void> bounceZookeeperConnection() {
            final FutureTask<Void> ft = new FutureTaskMon<Void>(new Runnable() {
                public void run() {
                    // NOP (not implemented at this layer).
                }
            }, null);
            ft.run();
            return getProxy(ft);
        }

        @Override
        public Future<Void> enterErrorState() {
            final FutureTask<Void> ft = new FutureTaskMon<Void>(new Runnable() {
                public void run() {
                    // NOP (not implemented at this layer).
                }
            }, null);
            ft.run();
            return getProxy(ft);
        }

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

        /*
         * ITransactionService.
         * 
         * Note: API is mostly implemented by Journal/HAJournal.
         */
        
        @Override
        public long newTx(long timestamp) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long commit(long tx) throws ValidationError, IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void abort(long tx) throws IOException {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public void notifyCommit(long commitTime) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLastCommitTime() throws IOException {
            
            return AbstractJournal.this.getLastCommitTime();
            
        }

        @Override
        public long getReleaseTime() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long nextTimestamp() throws IOException {
            throw new UnsupportedOperationException();
        }

        /*
         * IService
         */
        
        @Override
        public UUID getServiceUUID() throws IOException {

            return getServiceId();
            
        }

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

	};

    /**
     * Remove all commit records between the two provided keys.
     * 
     * This is called from the RWStore when it checks for deferredFrees against
     * the CommitRecordIndex where the CommitRecords reference the deleteBlocks
     * that have been deferred.
     * 
     * Once processed the records for the effected range muct be removed as they
     * reference invalid states.
     * 
     * @param fromKey
     * @param toKey
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/440
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
}
