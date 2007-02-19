/**

 The Notice below must appear in each file of the Source Code of any
 copy you distribute of the Licensed Product.  Contributors to any
 Modifications may add their own copyright notices to identify their
 own contributions.

 License:

 The contents of this file are subject to the CognitiveWeb Open Source
 License Version 1.1 (the License).  You may not copy or use this file,
 in either source code or executable form, except in compliance with
 the License.  You may obtain a copy of the License from

 http://www.CognitiveWeb.org/legal/license/

 Software distributed under the License is distributed on an AS IS
 basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
 the License for the specific language governing rights and limitations
 under the License.

 Copyrights:

 Portions created by or assigned to CognitiveWeb are Copyright
 (c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
 information for CognitiveWeb is available at

 http://www.CognitiveWeb.org

 Portions Copyright (c) 2002-2003 Bryan Thompson.

 Acknowledgements:

 Special thanks to the developers of the Jabber Open Source License 1.0
 (JOSL), from which this License was derived.  This License contains
 terms that differ from JOSL.

 Special thanks to the CognitiveWeb Open Source Contributors for their
 suggestions and support of the Cognitive Web.

 Modifications:

 */
/*
 * Created on Oct 8, 2006
 */

package com.bigdata.journal;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.objndx.BTree;
import com.bigdata.objndx.BTreeMetadata;
import com.bigdata.objndx.IIndex;
import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.Bytes;

/**
 * <p>
 * An append-only persistence capable data structure supporting atomic commit,
 * scalable named indices, and transactions. Writes are logically appended to
 * the journal to minimize disk head movement.
 * </p>
 * 
 * <p>
 * The journal provides for fast migration of committed data to a read-optimized
 * database. Data may be migrated as soon as a transaction commits and only the
 * most recent state for any datum need be migrated. Note that the criteria for
 * migration typically are met before the slots occupied by those objects may be
 * released. This is because a concurrent transaction must be permitted to read
 * from the committed state of the database at the time that the transaction was
 * begun.
 * </p>
 * <p>
 * The journal is a ring buffer divised of slots. The slot size and initial
 * extent are specified when the journal is provisioned. Objects are written
 * onto free slots. Slots are released once no active transaction can read from
 * that slots. In this way, the journal buffers consistent histories.
 * </p>
 * <p>
 * The journal maintains two indices:
 * <ol>
 * <li>An object index from int32 object identifier to a slot allocation; and
 * </li>
 * <li>An allocation index from slot to a slot status record.</li>
 * </ol>
 * These data structures are b+trees. The nodes of the btrees are stored in the
 * journal. These data structures are fully isolated. The roots of the indices
 * are choosen based on the transaction begin time. Changes result in a copy on
 * write. Writes percolate up the index nodes to the root node.
 * </p>
 * <p>
 * Commit processing. The journal also maintains two root blocks. Commit updates
 * the root blocks using the Challis algorithm. (The root blocks are updated
 * using an alternating pattern and timestamps are recorded at the head and tail
 * of each root block to detect partial writes.) When the journal is backed by a
 * disk file, the file is flushed to disk on commit.
 * </p>
 * <p>
 * A journal may be used without a database file as an object database. The
 * design is very efficient for absorbing writes, but read operations are not
 * optimized. Overall performance will degrade as the journal size increases
 * beyond the limits of physical memory and as the object index depth increases.
 * </p>
 * <p>
 * A journal and a database file form a logical segment in the bigdata
 * distributed database architecture. In bigdata, the segment size is generally
 * limited to a few hundred megabytes. At this scale, the journal and database
 * may both be wired into memory for the fastest performance. If memory
 * constraits are tighted, these files may be memory-mapped or used as a
 * disk-base data structures. A single journal may buffer for multiple copies of
 * the same database segment or journals may be chained for redundancy.
 * </p>
 * <p>
 * Very large objects should be handled by specially provisioned database files
 * using large pages and a "never overwrite" strategy.
 * </p>
 * <p>
 * Note: This class is NOT thread-safe. Instances of this class MUST use a
 * single-threaded context. That context is the single-threaded journal server
 * API. The journal server may be either embedded (in which case objects are
 * migrated to the server using FIFO queues) or networked (in which case the
 * journal server exposes a non-blocking service with a single thread for reads,
 * writes and deletes on the journal). (However, note transaction processing MAY
 * be concurrent since the write set of a transaction is written on a
 * {@link TemporaryStore}.)
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Priority items are:
 * <ol>
 * <li> Transaction isolation.</li>
 * <li> Scale out database (automatic re-partitioning of indices).</li>
 * <li> Distributed database protocols.</li>
 * <li> Segment server (mixture of journal server and read-optimized database
 * server).</li>
 * <li> Testing of an "embedded database" using both a journal only and a
 * journal + read-optimized database design. This can be tested up to the GPO
 * layer.</li>
 * <li>Support primary key (clustered) indices in GPO/PO layer.</li>
 * <li>Implement backward validation and state-based conflict resolution with
 * custom merge rules for RDFS, persistent objects, generic objects, and primary
 * key indices, and secondary indexes.</li>
 * <li> Architecture using queues from GOM to journal/database segment server
 * supporting both embedded and remote scenarios.</li>
 * <li>Expand on latency and decision criteria for notifying clients when pages
 * or objects of interest have been modified by another transaction that has
 * committed (or in the case of distributed workers on a single transaction, its
 * peers).</li>
 * </ol>
 * 
 * FIXME The notion of a committed state needs to be captured by a persistent
 * structure in the journal until (a) there are no longer any active
 * transactions that can read from that committed state; and (b) the slots
 * allocated to that committed state have been released on the journal. Those
 * commit states need to be locatable on the journal, suggesting a record
 * written by PREPARE and finalized by COMMIT.
 * 
 * @todo Work out protocol for shutdown with the single-threaded journal server.
 * 
 * @todo Normal transaction operations need to be interleaved with operations to
 *       migrate committed data to the read-optimized database; with operations
 *       to logically delete data versions (and their slots) on the journal once
 *       those version are no longer readable by any active transaction; and
 *       with operations to compact the journal (extending can occur during
 *       normal transaction operations). One approach is to implement
 *       thread-checking using thread local variables or the ability to assign
 *       the journal to a thread and then hand off the journal to the thread for
 *       the activity that needs to be run, returning control to the normal
 *       transaction thread on (or shortly after) interrupt or when the
 *       operation is finished.
 * 
 * @todo There is a dependency in a distributed database architecture on
 *       transaction begin time. A very long running transaction could force the
 *       journal to hold onto historical states. If a decision is made to
 *       discard those states and the transaction begins to read from the
 *       journal then the transaction must be rolled back. This should be worked
 *       out with the resource deallocation for old journals and segments.
 * 
 * @todo Define distributed transaction protocol.
 * 
 * @todo Define distributed protocol for robust startup, operation, and
 *       failover.
 * 
 * @todo Divide into API layers. The network facing layer uses a single threaded
 *       nio interface to receive writes into direct buffers. Writes on the
 *       journal are also single-threaded. Buffers of rows are queued by one
 *       thread and then written through to the journal by another. Isolation
 *       occurs in a layer above raw journal writes that maintains the object
 *       and allocation indices. Rows can be written onto slots using simple
 *       math since both the row size and the slot size are known. Objects must
 *       be deserialized during state-based validation if a conflict is
 *       detected.
 * 
 * FIXME Write tests for writable and read-only fully isolated transactions,
 * including the non-duplication of per-tx data structures (e.g., you always get
 * the same object back when you ask for an isolated named index).
 * 
 * FIXME Write test suites for the {@link TransactionServer}.
 * 
 * @todo I need to revisit the assumptions for very large objects in the face of
 *       the recent / planned redesign. I expect that using an index with a key
 *       formed as [URI][chuck#] would work just fine. Chunks would then be
 *       limited to 32k or so. Writes on such indices should probably be
 *       directed to a journal using a disk-only mode.
 */
public class Journal implements IJournal {

    /**
     * Logger.
     */
    public static final Logger log = Logger.getLogger(Journal.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final public boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final public boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * The index of the root address containing the address of the persistent
     * {@link Name2Addr} mapping names to {@link BTree}s registered for the
     * store.
     */
    public static transient final int ROOT_NAME2ADDR = 0;

    /**
     * A clone of the properties used to initialize the {@link Journal}.
     */
    final protected Properties properties;

    /**
     * The implementation logic for the current {@link BufferMode}.
     */
    final IBufferStrategy _bufferStrategy;

    /**
     * The service used to generate commit timestamps.
     * 
     * @todo paramterize using {@link Options} so that we can resolve a
     *       low-latency service for use with a distributed database commit
     *       protocol.
     */
    protected final ITimestampService timestampFactory = LocalTimestampService.INSTANCE;

    /**
     * The current root block. This is updated each time a new root block is
     * written.
     */
    private IRootBlockView _rootBlock;

    /**
     * The registered committers for each slot in the root block.
     */
    private ICommitter[] _committers = new ICommitter[ICommitRecord.MAX_ROOT_ADDRS];

    /**
     * Used to cache the most recent {@link ICommitRecord} -- discarded on
     * {@link #abort()}.
     */
    private ICommitRecord _commitRecord;

    /**
     * BTree mapping index names to the last metadata record committed for the
     * named index. The keys are index names (unicode strings). The values are
     * the last known {@link Addr address} of the named btree.
     */
    private Name2Addr name2Addr;

    /**
     * BTree mapping commit timestamps to the address of the corresponding
     * {@link ICommitRecord}. The keys are timestamps (long integers). The
     * values are the {@link Addr address} of the {@link ICommitRecord} with
     * that commit timestamp.
     */
    private CommitRecordIndex _commitRecordIndex;

    /**
     * Option controls whether the journal forces application data to disk
     * before updating the root blocks.
     */
    private final boolean doubleSync;

    /**
     * Option controls how the journal behaves during a commit.
     */
    private final ForceEnum forceOnCommit;

    /**
     * Option set by the test suites causes the file backing the journal to be
     * deleted when the journal is closed.
     */
    private final boolean deleteOnClose;

    /**
     * The maximum extent before a {@link #commit()} will {@link #overflow()}.
     * In practice, overflow tries to trigger before this point in order to
     * avoid extending the journal.
     * 
     * @see Options#MAXIMUM_EXTENT
     */
    private final long maximumExtent;

    /**
     * A hash map containing all active transactions. A transaction that is
     * preparing will be in this collection until it has either successfully
     * prepared or aborted.
     */
    final Map<Long, ITx> activeTx = new HashMap<Long, ITx>();

    /**
     * A hash map containing all transactions that have prepared but not yet
     * either committed or aborted.
     */
    final Map<Long, ITx> preparedTx = new HashMap<Long, ITx>();

    /**
     * Create or open a journal.
     * 
     * @param properties
     *            The properties as defined by {@link Options}.
     * 
     * @throws RuntimeException
     *             If there is a problem when creating, opening, or reading from
     *             the journal file.
     * 
     * @see Options
     * 
     * @todo Write tests that verify (a) that read-only mode does not permit
     *       writes; (b) that read-only mode is not supported for a transient
     *       buffer (since the buffer does not pre-exist by definition); (c)
     *       that read-only mode reports an error if the file does not
     *       pre-exist; and (d) that you can not write on a read-only journal.
     */
    public Journal(Properties properties) {

        int segmentId;
        long initialExtent = Options.DEFAULT_INITIAL_EXTENT;
        long maximumExtent = Options.DEFAULT_MAXIMUM_EXTENT;
        boolean useDirectBuffers = Options.DEFAULT_USE_DIRECT_BUFFERS;
        boolean create = Options.DEFAULT_CREATE;
        boolean readOnly = Options.DEFAULT_READ_ONLY;
        boolean deleteOnClose = Options.DEFAULT_DELETE_ON_CLOSE;
        ForceEnum forceWrites = Options.DEFAULT_FORCE_WRITES;
        ForceEnum forceOnCommit = Options.DEFAULT_FORCE_ON_COMMIT;
        boolean doubleSync = Options.DEFAULT_DOUBLE_SYNC;

        String val;

        if (properties == null)
            throw new IllegalArgumentException();

        this.properties = properties = (Properties) properties.clone();

        /*
         * "bufferMode" mode. Note that very large journals MUST use the
         * disk-based mode.
         */

        val = properties.getProperty(Options.BUFFER_MODE);

        if (val == null)
            val = BufferMode.Direct.toString();

        BufferMode bufferMode = BufferMode.parse(val);

        /*
         * "useDirectBuffers"
         */

        val = properties.getProperty(Options.USE_DIRECT_BUFFERS);

        if (val != null) {

            useDirectBuffers = Boolean.parseBoolean(val);

        }

        /*
         * "segment".
         */

        val = properties.getProperty(Options.SEGMENT);

        if (val == null) {

            if (bufferMode == BufferMode.Transient) {

                val = "0";

            } else {

                throw new RuntimeException("Required property: '"
                        + Options.SEGMENT + "'");

            }

        }

        segmentId = Integer.parseInt(val);

        /*
         * "initialExtent"
         */

        val = properties.getProperty(Options.INITIAL_EXTENT);

        if (val != null) {

            initialExtent = Long.parseLong(val);

            if (initialExtent < Bytes.megabyte) {

                throw new RuntimeException("The '" + Options.INITIAL_EXTENT
                        + "' must be at least one megabyte(" + Bytes.megabyte
                        + ")");

            }

        }

        /*
         * "maximumExtent"
         */

        val = properties.getProperty(Options.MAXIMUM_EXTENT);

        if (val != null) {

            maximumExtent = Long.parseLong(val);

            if (maximumExtent < initialExtent) {

                throw new RuntimeException("The '" + Options.MAXIMUM_EXTENT
                        + "' is less than the initial extent.");

            }

        }

        this.maximumExtent = maximumExtent;

        /*
         * "readOnly"
         */

        val = properties.getProperty(Options.READ_ONLY);

        if (val != null) {

            readOnly = Boolean.parseBoolean(val);

        }

        /*
         * "forceWrites"
         */

        val = properties.getProperty(Options.FORCE_WRITES);

        if (val != null) {

            forceWrites = ForceEnum.parse(val);

        }

        /*
         * "forceOnCommit"
         */

        val = properties.getProperty(Options.FORCE_ON_COMMIT);

        if (val != null) {

            forceOnCommit = ForceEnum.parse(val);

        }

        this.forceOnCommit = forceOnCommit;

        /*
         * "doubleSync"
         */

        val = properties.getProperty(Options.DOUBLE_SYNC);

        if (val != null) {

            doubleSync = Boolean.parseBoolean(val);

        }

        this.doubleSync = doubleSync;

        /*
         * "deleteOnClose"
         */

        val = properties.getProperty(Options.DELETE_ON_CLOSE);

        if (val != null) {

            deleteOnClose = Boolean.parseBoolean(val);

        }

        this.deleteOnClose = deleteOnClose;

        /*
         * Create the appropriate IBufferStrategy object.
         */

        switch (bufferMode) {

        case Transient: {

            /*
             * Setup the buffer strategy.
             */

            if (readOnly) {

                throw new RuntimeException(
                        "readOnly not supported for transient journals.");

            }

            _bufferStrategy = new TransientBufferStrategy(initialExtent,
                    maximumExtent, useDirectBuffers);

            /*
             * setup the root blocks.
             */
            final int nextOffset = 0;
            final long commitTimestamp = 0L;
            final long firstTxId = 0L;
            final long lastTxId = 0L;
            final long commitCounter = 0L;
            final long commitRecordAddr = 0L;
            final long commitRecordIndexAddr = 0L;
            IRootBlockView rootBlock0 = new RootBlockView(true, segmentId,
                    nextOffset, firstTxId, lastTxId, commitTimestamp,
                    commitCounter, commitRecordAddr, commitRecordIndexAddr);
            IRootBlockView rootBlock1 = new RootBlockView(false, segmentId,
                    nextOffset, firstTxId, lastTxId, commitTimestamp,
                    commitCounter, commitRecordAddr, commitRecordIndexAddr);
            _bufferStrategy.writeRootBlock(rootBlock0, ForceEnum.No);
            _bufferStrategy.writeRootBlock(rootBlock1, ForceEnum.No);

            this._rootBlock = rootBlock1;

            break;

        }

        case Direct: {

            /*
             * "file"
             */

            val = properties.getProperty(Options.FILE);

            if (val == null) {

                throw new RuntimeException("Required property: '"
                        + Options.FILE + "'");

            }

            File file = new File(val);

            /*
             * Setup the buffer strategy.
             */

            FileMetadata fileMetadata = new FileMetadata(segmentId, file,
                    BufferMode.Direct, useDirectBuffers, initialExtent, create,
                    readOnly, forceWrites);

            _bufferStrategy = new DirectBufferStrategy(maximumExtent,
                    fileMetadata);

            this._rootBlock = fileMetadata.rootBlock;

            break;

        }

        case Mapped: {

            /*
             * "file"
             */

            val = properties.getProperty(Options.FILE);

            if (val == null) {

                throw new RuntimeException("Required property: '"
                        + Options.FILE + "'");

            }

            File file = new File(val);

            /*
             * Setup the buffer strategy.
             */

            FileMetadata fileMetadata = new FileMetadata(segmentId, file,
                    BufferMode.Mapped, useDirectBuffers, initialExtent, create,
                    readOnly, forceWrites);

            _bufferStrategy = new MappedBufferStrategy(maximumExtent,
                    fileMetadata);

            this._rootBlock = fileMetadata.rootBlock;

            break;

        }

        case Disk: {

            /*
             * "file"
             */

            val = properties.getProperty(Options.FILE);

            if (val == null) {

                throw new RuntimeException("Required property: '"
                        + Options.FILE + "'");

            }

            File file = new File(val);

            /*
             * Setup the buffer strategy.
             */

            FileMetadata fileMetadata = new FileMetadata(segmentId, file,
                    BufferMode.Disk, useDirectBuffers, initialExtent, create,
                    readOnly, forceWrites);

            _bufferStrategy = new DiskOnlyStrategy(maximumExtent, fileMetadata);

            this._rootBlock = fileMetadata.rootBlock;

            break;

        }

        default:

            throw new AssertionError();

        }

        /*
         * Create or re-load the index of commit records.
         */
        this._commitRecordIndex = getCommitRecordIndex(this._rootBlock
                .getCommitRecordIndexAddr());

        /*
         * Give the store a chance to set any committers that it defines.
         */
        setupCommitters();

    }

    final public Properties getProperties() {

        return (Properties) properties.clone();

    }

    /**
     * The delegate that implements the {@link BufferMode}.
     * <p>
     * Note: this method MUST NOT check to see whether the journal is open since
     * we need to use it if we want to invoke
     * {@link IBufferStrategy#deleteFile()} and we can only invoke that method
     * once the journal is closed.
     */
    final public IBufferStrategy getBufferStrategy() {

        return _bufferStrategy;

    }

    /**
     * Shutdown the journal politely.
     * 
     * @exception IllegalStateException
     *                if there are active transactions.
     * @exception IllegalStateException
     *                if there are prepared transactions.
     * 
     * @todo Workout protocol for shutdown of the journal, including forced
     *       shutdown when there are active or prepar(ed|ing) transactions,
     *       timeouts on transactions during shutdown, notification of abort for
     *       transactions that do not complete in a timely manner, and
     *       survivability of prepared transactions across restart. Reconcile
     *       the semantics of this method with those declared by the raw store
     *       interface, probably by declaring a variant that accepts parameters
     *       specifying how to handle the shutdown (immediate vs wait).
     */
    public void shutdown() {

        assertOpen();

        final int nactive = activeTx.size();

        if (nactive > 0) {

            throw new IllegalStateException("There are " + nactive
                    + " active transactions");

        }

        final int nprepare = preparedTx.size();

        if (nprepare > 0) {

            throw new IllegalStateException("There are " + nprepare
                    + " prepared transactions.");

        }

        // close immediately.
        close();

    }

    /**
     * Close immediately.
     */
    public void close() {

        assertOpen();

        _bufferStrategy.close();

        if (deleteOnClose) {

            /*
             * This option is used by the test suite and MUST NOT be used with
             * live data.
             */

            _bufferStrategy.deleteFile();

        }

    }

    private void assertOpen() {

        if (!_bufferStrategy.isOpen()) {

            throw new IllegalStateException();

        }

    }

    public boolean isOpen() {

        return _bufferStrategy.isOpen();

    }

    public boolean isStable() {

        return _bufferStrategy.isStable();

    }

    /**
     * Return a read-only view of the current root block.
     * 
     * @return The current root block.
     */
    final public IRootBlockView getRootBlockView() {

        return _rootBlock;

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
     *            The slot in the root block where the {@link Addr address} of
     *            the {@link ICommitter} will be recorded.
     * 
     * @param committer
     *            The commiter.
     */
    final public void setCommitter(int rootSlot, ICommitter committer) {

        assertOpen();

        _committers[rootSlot] = committer;

    }

    /**
     * Notify all registered committers and collect their reported root
     * addresses in an array.
     * 
     * @return The array of collected root addresses for the registered
     *         committers.
     */
    final private long[] notifyCommitters() {

        int ncommitters = 0;

        long[] rootAddrs = new long[_committers.length];

        for (int i = 0; i < _committers.length; i++) {

            if (_committers[i] == null)
                continue;

            rootAddrs[i] = _committers[i].handleCommit();

            ncommitters++;

        }

        return rootAddrs;

    }

    /**
     * Invoked iff a transaction fails after it has begun writing data onto the
     * global state from its isolated state. Once the transaction has begun this
     * process it has modified the global (unisolated) state and the next commit
     * will make those changes restart-safe. While this processing is not begun
     * unless the commit SHOULD succeed, errors can nevertheless occur.
     * Therefore, if the transaction fails its writes on the unisolated state
     * must be discarded. Since the isolatable data structures (btrees) use a
     * copy-on-write policy, writing new data never overwrites old data so
     * nothing has been lost.
     * <p>
     * We can not simply reload the last root block since concurrent
     * transactions may write non-restart safe data onto the store (transactions
     * may use btrees to isolate changes, and those btrees will write on the
     * store). Reloading the root block would discarding all writes, including
     * those occurring in isolation in concurrent transactions.
     * <p>
     * Instead, what we do is discard the unisolated objects, reloading them
     * from the current root addresses on demand. This correctly discards any
     * writes on those unisolated objects while NOT resetting the nextOffset at
     * which writes will occur on the store and NOT causing persistence capable
     * objects (btrees) used for isolated by concurrent transactions to lose
     * their write sets.
     */
    public void abort() {

        // clear the root addresses - the will be reloaded.
        _commitRecord = null;

        // clear the array of committers.
        _committers = new ICommitter[_committers.length];

        /*
         * Re-load the commit record index from the address in the current root
         * block.
         * 
         * Note: This may not be strictly necessary since the only time we write
         * on this index is a single record during each commit. So, it should be
         * valid to simply catch an error during a commit and discard this index
         * forcing its reload. However, doing this here is definately safer.
         */
        _commitRecordIndex = getCommitRecordIndex(_rootBlock
                .getCommitRecordIndexAddr());

        // discard any hard references that might be cached.
        discardCommitters();

        // setup new committers, e.g., by reloading from their last root addr.
        setupCommitters();

    }

    /**
     * An atomic commit is performed by directing each registered
     * {@link ICommitter} to flush its state onto the store using
     * {@link ICommitter#handleCommit()}. The {@link Addr address} returned by
     * that method is the address from which the {@link ICommitter} may be
     * reloaded (and its previous address if its state has not changed). That
     * address is saved in the slot of the root block under which that committer
     * was {@link #registerCommitter(int, ICommitter) registered}. We then
     * force the data to stable store, update the root block, and force the root
     * block and the file metadata to stable store.
     */
    public long commit() {

        return commit(null);

    }

    /**
     * Handle the {@link #commit()} and integrations with transaction support so
     * that we can update the first and last transaction identifiers on the root
     * block as necessary.
     * 
     * @param tx
     *            The transaction that is committing or <code>null</code> if
     *            the commit is not transactional.
     * 
     * @return The timestamp assigned to the commit record -or- 0L if there were
     *         no data to commit.
     */
    protected long commit(Tx tx) {

        assertOpen();

        final long commitTimestamp = (tx == null ? timestampFactory
                .nextTimestamp() : tx.getCommitTimestamp());

        /*
         * First, run each of the committers accumulating the updated root
         * addresses in an array. In general, these are btrees and they may have
         * dirty nodes or leaves that needs to be evicted onto the store. The
         * first time through, any newly created btrees will have dirty empty
         * roots (the btree code does not optimize away an empty root at this
         * time). However, subsequent commits without intervening data written
         * on the store should not cause any committers to update their root
         * address.
         */
        final long[] rootAddrs = notifyCommitters();

        /*
         * See if anything has been written on the store since the last commit.
         */
        if (_bufferStrategy.getNextOffset() == _rootBlock.getNextOffset()) {

            /*
             * No data was written onto the store so the commit can not achieve
             * any useful purpose.
             */

            return 0L;

        }

        /*
         * Write the commit record onto the store.
         */

        final ICommitRecord commitRecord = new CommitRecord(commitTimestamp,
                rootAddrs);

        final long commitRecordAddr = write(ByteBuffer
                .wrap(CommitRecordSerializer.INSTANCE.serialize(commitRecord)));

        /*
         * Add the comment record to an index so that we can recover historical
         * states efficiently.
         */
        _commitRecordIndex.add(commitRecordAddr, commitRecord);

        /*
         * Flush the commit record index to the store and stash the address of
         * its metadata record in the root block.
         * 
         * Note: The address of the root of the CommitRecordIndex itself needs
         * to go right into the root block. We are unable to place it into the
         * commit record since we need to serialize the commit record, get its
         * address, and add the entry to the CommitRecordIndex before we can
         * flush the CommitRecordIndex to the store.
         */
        final long commitRecordIndexAddr = _commitRecordIndex.write();

        /*
         * Force application data to stable storage _before_ we update the root
         * blocks. This option guarentees that the application data is stable on
         * the disk before the atomic commit. Some operating systems and/or file
         * systems may otherwise choose an ordered write with the consequence
         * that the root blocks are laid down on the disk before the application
         * data and a hard failure could result in the loss of application data
         * addressed by the new root blocks (data loss on restart).
         * 
         * Note: We do not force the file metadata to disk
         */
        if (doubleSync) {

            _bufferStrategy.force(false);

        }

        /*
         * update the root block.
         */
        {

            final IRootBlockView old = _rootBlock;

            /*
             * Update the firstTxId the first time a transaction commits and the
             * lastTxId each time a transaction commits.
             * 
             * Note: These are commit time timestamps.
             */

            long firstTxId = old.getFirstTxCommitTime();

            long lastTxId = old.getLastTxCommitTime();

            if (tx != null && !tx.isReadOnly()) {

                if (firstTxId == 0L) {

                    assert lastTxId == 0L;

                    firstTxId = lastTxId = tx.getCommitTimestamp();

                } else {

                    lastTxId = tx.getCommitTimestamp();

                }

            }

            // Create the new root block.
            IRootBlockView newRootBlock = new RootBlockView(
                    !old.isRootBlock0(), old.getSegmentId(), _bufferStrategy
                            .getNextOffset(), firstTxId, lastTxId,
                    commitTimestamp, old.getCommitCounter() + 1,
                    commitRecordAddr, commitRecordIndexAddr);

            _bufferStrategy.writeRootBlock(newRootBlock, forceOnCommit);

            _rootBlock = newRootBlock;

            _commitRecord = commitRecord;

        }

        /*
         * Look for overflow condition.
         */
        {

            final int nextOffset = _bufferStrategy.getNextOffset();

            if (nextOffset > .9 * maximumExtent) {

                overflow();

            }

        }

        return commitTimestamp;

    }

    /**
     * Note: This implementation does not handle overflow of the journal. The
     * journal capacity will simply be extended until the available resources
     * are exhausted.
     */
    public void overflow() {

        // NOP.

    }

    public void force(boolean metadata) {

        assertOpen();

        _bufferStrategy.force(metadata);

    }

    public long write(ByteBuffer data) {

        assertOpen();

        return _bufferStrategy.write(data);

    }

    public ByteBuffer read(long addr, ByteBuffer dst) {

        assertOpen();

        return _bufferStrategy.read(addr, dst);

    }

    final public long getRootAddr(int index) {

        if (_commitRecord == null) {

            return getCommitRecord().getRootAddr(index);

        } else {

            return _commitRecord.getRootAddr(index);

        }

    }

    /**
     * Returns a read-only view of the root {@link Addr addresses}. The caller
     * may modify the returned array without changing the effective state of
     * those addresses as used by the store.
     * 
     * @return The root {@link Addr addresses}.
     */
    public ICommitRecord getCommitRecord() {

        if (_commitRecord == null) {

            long commitRecordAddr = _rootBlock.getCommitRecordAddr();

            if (commitRecordAddr == 0L) {

                _commitRecord = new CommitRecord(0L);

            } else {

                _commitRecord = CommitRecordSerializer.INSTANCE
                        .deserialize(_bufferStrategy.read(commitRecordAddr,
                                null));

            }

        }

        return _commitRecord;

    }

    /**
     * The default implementation discards the btree mapping names to named
     * btrees.
     * <p>
     * Subclasses MAY extend this method to discard their own committers but
     * MUST NOT override it completely.
     */
    public void discardCommitters() {

        // discard.
        name2Addr = null;

    }

    /**
     * The basic implementation sets up the btree that is responsible for
     * resolving named btrees.
     * <p>
     * Subclasses may extend this method to setup their own committers but MUST
     * NOT override it completely.
     */
    public void setupCommitters() {

        setupName2AddrBTree(getRootAddr(ROOT_NAME2ADDR));

    }

    /*
     * named indices.
     */

    /**
     * Setup the btree that resolved named btrees.
     * 
     * @param addr
     *            The root address of the btree -or- 0L iff the btree has not
     *            been defined yet.
     */
    private void setupName2AddrBTree(long addr) {

        assert name2Addr == null;

        if (addr == 0L) {

            /*
             * The btree has either never been created or if it had been created
             * then the store was never committed and the btree had since been
             * discarded. In any case we create a new btree now.
             */

            // create btree mapping names to addresses.
            name2Addr = new Name2Addr(this);

        } else {

            /*
             * Reload the btree from its root address.
             */

            name2Addr = (Name2Addr) BTreeMetadata.load(this, addr);

        }

        // register for commit notices.
        setCommitter(ROOT_NAME2ADDR, name2Addr);

    }

    /**
     * Create or re-load the index that resolves timestamps to
     * {@link ICommitRecord}s.
     * 
     * @param addr
     *            The root address of the index -or- 0L if the index has not
     *            been created yet.
     * 
     * @return The {@link CommitRecordIndex} for that address or a new index if
     *         0L was specified as the address.
     * 
     * @see #_commitRecordIndex, which hold the current commit record index.
     */
    private CommitRecordIndex getCommitRecordIndex(long addr) {

        CommitRecordIndex ndx;

        if (addr == 0L) {

            /*
             * The btree has either never been created or if it had been created
             * then the store was never committed and the btree had since been
             * discarded. In any case we create a new btree now.
             */

            // create btree mapping names to addresses.
            ndx = new CommitRecordIndex(this);

        } else {

            /*
             * Reload the btree from its root address.
             */

            ndx = (CommitRecordIndex) BTreeMetadata.load(this, addr);

        }

        return ndx;

    }

    /**
     * Note: You MUST {@link #commit()} before the registered index will be
     * either restart-safe or visible to new transactions.
     */
    public IIndex registerIndex(String name, IIndex btree) {

        if (getIndex(name) != null) {

            throw new IllegalStateException("Index already registered: name="
                    + name);

        }

        // add to the persistent name map.
        name2Addr.add(name, btree);

        return btree;

    }

    public void dropIndex(String name) {

        name2Addr.dropIndex(name);

    }

    /**
     * Return the named index (unisolated). Writes on the returned index will be
     * made restart-safe with the next {@link #commit()} regardless of the
     * success or failure of a transaction. Transactional writes must use the
     * same named method on the {@link Tx} in order to obtain an isolated
     * version of the named btree.
     */
    public IIndex getIndex(String name) {

        if (name == null)
            throw new IllegalArgumentException();

        return name2Addr.get(name);

    }

    /**
     * Return the {@link ICommitRecord} for the most recent committed state
     * whose commit timestamp is less than or equal to <i>timestamp</i>. This
     * is used by a {@link Tx transaction} to locate the committed state that is
     * the basis for its operations.
     * 
     * @param timestamp
     *            Typically, the timestamp assigned to a transaction.
     * 
     * @return The {@link ICommitRecord} for the most recent committed state
     *         whose commit timestamp is less than or equal to <i>timestamp</i>
     *         -or- <code>null</code> iff there are no {@link ICommitRecord}s
     *         that satisify the probe.
     * 
     * @todo this implies that timestamps for all commits are generated by a
     *       global timestamp service.
     */
    public ICommitRecord getCommitRecord(long timestamp) {

        return _commitRecordIndex.find(timestamp);

    }

    /**
     * Returns a read-only named index loaded from the given root block. Writes
     * on the index will NOT be made persistent and the index will NOT
     * participate in commits.
     */
    public IIndex getIndex(String name, ICommitRecord commitRecord) {

        /*
         * Note: since this is always a request for historical read-only data,
         * this method MUST NOT register a committer and the returned btree MUST
         * NOT participate in the commit protocol.
         * 
         * @todo cache these results in a weak value cache.
         */

        return ((Name2Addr) BTreeMetadata.load(this, commitRecord
                .getRootAddr(ROOT_NAME2ADDR))).get(name);

    }

    /*
     * transaction support.
     */

    /**
     * Create a new fully-isolated read-write transaction.
     * 
     * @see #newTx(boolean), to which this method delegates its implementation.
     */
    public long newTx() {

        return newTx(false);

    }

    /**
     * Create a new fully-isolated transaction.
     * 
     * @param readOnly
     *            When true, the transaction will reject writes.
     * 
     * @todo This method supports transactions in a non-distributed database in
     *       which there is a centralized {@link Journal} that handles all
     *       concurrency control. There needs to be a {@link TransactionServer}
     *       that starts transactions. The {@link JournalServer} should summon a
     *       transaction object them into being on a {@link Journal} iff
     *       operations isolated by that transaction are required on that
     *       {@link Journal}.
     */
    public long newTx(boolean readOnly) {

        return new Tx(this, timestampFactory.nextTimestamp(), readOnly)
                .getStartTimestamp();

    }

    /**
     * Create a new read-committed transaction.
     * 
     * @return A transaction that will reject writes. Any committed data will be
     *         visible to indices isolated by this transaction.
     * 
     * @todo implement read-committed transaction support.
     * 
     * @see #newTx(boolean)
     */
    public long newReadCommittedTx() {

        throw new UnsupportedOperationException();

    }

    /**
     * Return the named index as isolated by the transaction.
     * 
     * @param name
     *            The index name.
     * @param ts
     *            The start time of the transaction.
     * 
     * @return The isolated index.
     * 
     * @exception IllegalArgumentException
     *                if <i>name</i> is <code>null</code>
     * 
     * @exception IllegalStateException
     *                if there is no active transaction with that timestamp.
     */
    public IIndex getIndex(String name, long ts) {
        
        if(name == null) throw new IllegalArgumentException();
        
        ITx tx = activeTx.get(ts);
        
        if(tx==null) throw new IllegalStateException();
        
        return tx.getIndex(name);
        
    }
    
    public void abort(long ts) {

        ITx tx = getTx(ts);
        
        if (tx == null)
            throw new IllegalArgumentException("No such tx: " + ts);
        
        tx.abort();
        
    }

    /**
     * Commit the transaction on the journal.
     * 
     * @param ts The transaction start time.
     * 
     * @return The commit timestamp assigned to the transaction. 
     */
    public long commit(long ts) {

        ITx tx = getTx(ts);
        
        if (tx == null)
            throw new IllegalArgumentException("No such tx: " + ts);
        
        tx.prepare();

        return tx.commit();
        
    }
    
    /**
     * Notify the journal that a new transaction is being activated (starting on
     * the journal).
     * 
     * @param tx
     *            The transaction.
     * 
     * @throws IllegalStateException
     */
    protected void activateTx(ITx tx) throws IllegalStateException {

        Long timestamp = tx.getStartTimestamp();

        if (activeTx.containsKey(timestamp))
            throw new IllegalStateException("Already active: tx=" + tx);

        if (preparedTx.containsKey(timestamp))
            throw new IllegalStateException("Already prepared: tx=" + tx);

        activeTx.put(timestamp, tx);

    }

    /**
     * Notify the journal that a transaction has prepared (and hence is no
     * longer active).
     * 
     * @param tx
     *            The transaction
     * 
     * @throws IllegalStateException
     */
    protected void prepared(ITx tx) throws IllegalStateException {

        Long id = tx.getStartTimestamp();

        ITx tx2 = activeTx.remove(id);

        if (tx2 == null)
            throw new IllegalStateException("Not active: tx=" + tx);

        assert tx == tx2;

        if (preparedTx.containsKey(id))
            throw new IllegalStateException("Already preparing: tx=" + tx);

        preparedTx.put(id, tx);

    }

    /**
     * Notify the journal that a transaction is completed (either aborted or
     * committed).
     * 
     * @param tx
     *            The transaction.
     * 
     * @throws IllegalStateException
     */
    protected void completedTx(ITx tx) throws IllegalStateException {

        assert tx != null;
        assert tx.isComplete();

        Long id = tx.getStartTimestamp();

        ITx txActive = activeTx.remove(id);

        ITx txPrepared = preparedTx.remove(id);

        if (txActive == null && txPrepared == null) {

            throw new IllegalStateException(
                    "Neither active nor being prepared: tx=" + tx);

        }

    }

    /**
     * Lookup an active or prepared transaction (exact match).
     * 
     * @param startTimestamp
     *            The start timestamp for the transaction.
     * 
     * @return The transaction with that start time or <code>null</code> if
     *         the start time is not mapped to either an active or prepared
     *         transaction.
     */
    public ITx getTx(long startTimestamp) {

        ITx tx = activeTx.get(startTimestamp);

        if (tx == null) {

            tx = preparedTx.get(startTimestamp);

        }

        return tx;

    }
    
}
