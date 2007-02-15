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
 * writes and deletes on the journal).
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Priority items are:
 * <ol>
 * <li> Transaction isolation.</li>
 * <li> Distributed database protocols.</li>
 * <li> Segment server (mixture of journal server and read-optimized database
 * server).</li>
 * <li> Testing of an "embedded database" using both a journal only and a
 * journal + read-optimized database design. This can be tested up to the GPO
 * layer.</li>
 * <li>Support primary key (clustered) indices in GPO/PO layer.</li>
 * <li>Implement backward validation and state-based conflict resolution with
 * custom merge rules for persistent objects, generic objects, and primary key
 * indices, and secondary indexes.</li>
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
 *       journal then the transaction must be rolled back.
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
 * @todo Flushing to disk on commit could be optional, e.g., if there are
 *       redundent journals then this is not required.
 * 
 * @todo Add feature to return the transient journal buffer so that we can do
 *       interesting things with it, including write it to disk with the
 *       appropriate file header and root blocks so that it becomes restartable.
 * 
 * FIXME Implement a read only transaction and write test suites for its
 * semantics, including both the inability to write (or delete) on the
 * transaction, the handling of pre-existing versions, and the non-duplication
 * of per-tx data structures.
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
     * A clone of the properties used to initialize the {@link Journal}.
     */
    final protected Properties properties;

    /**
     * The implementation logic for the current {@link BufferMode}.
     */
    final IBufferStrategy _bufferStrategy;

    /**
     * The current root block. This is updated each time a new root block is
     * written.
     */
    private IRootBlockView _rootBlock;

    /**
     * The registered committers for each slot in the root block.
     */
    private ICommitter[] _committers = new ICommitter[RootBlockView.MAX_ROOT_ADDRS];

    /**
     * The index of the root slot whose value is the address of the persistent
     * {@link Name2Addr} mapping names to {@link BTree}s registered for the
     * store.
     */
    public static transient final int ROOT_NAME2ADDR = 0;

    /**
     * BTree mapping index names to the last metadata record committed for the
     * named index. The keys are index names (unicode strings). The values are
     * the last known {@link Addr address} of the named btree.
     */
    private Name2Addr name2Addr;

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
            final long firstTxId = 0L;
            final long lastTxId = 0L;
            final long commitCounter = 0L;
            final long[] rootIds = new long[RootBlockView.MAX_ROOT_ADDRS];
            IRootBlockView rootBlock0 = new RootBlockView(true, segmentId,
                    nextOffset, firstTxId, lastTxId, commitCounter, rootIds);
            IRootBlockView rootBlock1 = new RootBlockView(false, segmentId,
                    nextOffset, firstTxId, lastTxId, commitCounter, rootIds);
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
         * Give the store a chance to set any committers that it defines.
         */
        setupCommitters();

    }

    final public Properties getProperties() {
        
        return (Properties)properties.clone();
        
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

        // clear the array of committers.
        _committers = new ICommitter[_committers.length];

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
    public void commit() {

        commit(null);

    }
    
    /**
     * Handle the {@link #commit()} and integrations with transaction support so
     * that we can update the first and last transaction identifiers on the root
     * block as necessary.
     * 
     * @param tx
     *            The transaction that is committing or <code>null</code> if
     *            the commit is not transactional.
     */
    void commit(Tx tx) {

        assertOpen();
        
        /*
         * First, run each of the committers. In general, these are btrees and
         * they may have dirty nodes or leaves that needs to be evicted onto the
         * store.
         */
        int ncommitters = 0;

        long[] rootAddrs = new long[_committers.length];

        for (int i = 0; i < _committers.length; i++) {

            if (_committers[i] == null)
                continue;

            rootAddrs[i] = _committers[i].handleCommit();

            ncommitters++;

        }

        /*
         * See if anything has been written on the store since the last commit.
         */
        if (_bufferStrategy.getNextOffset() == _rootBlock.getNextOffset()) {

            /*
             * No data was written onto the store so the commit can not achieve
             * any useful purpose.
             */

            return;

        }

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
             */
            
            long firstTxId = old.getFirstTxId();
            
            long lastTxId = old.getLastTxId();
            
            if (tx != null) {

                if (firstTxId == 0L) {

                    assert lastTxId == 0L;

                    firstTxId = lastTxId = tx.getId();

                } else if (lastTxId == 0L) {

                    lastTxId = tx.getId();

                }
                
            }
            
            // Create the new root block.
            IRootBlockView newRootBlock = new RootBlockView(
                    !old.isRootBlock0(), old.getSegmentId(), _bufferStrategy
                            .getNextOffset(), firstTxId, lastTxId, old
                            .getCommitCounter() + 1, rootAddrs);

            _bufferStrategy.writeRootBlock(newRootBlock, forceOnCommit);

            _rootBlock = newRootBlock;

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

    final public long getAddr(int rootSlot) {

        return _rootBlock.getRootAddr(rootSlot);

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

        setupName2AddrBTree();

    }

    /*
     * named indices.
     */
    
    /**
     * Setup the btree that resolved named btrees.
     */
    private void setupName2AddrBTree() {

        assert name2Addr == null;
        
        // the root address of the btree.
        long addr = getAddr(ROOT_NAME2ADDR);

        if (addr == 0L) {

            /*
             * The btree has either never been created or if it had been created
             * then the store was never committed and the btree had since been
             * discarded.  In any case we create a new btree now.
             */

            // create btree mapping names to addresses.
            name2Addr = new Name2Addr(this);

        } else {

            /*
             * Reload the btree from its root address.
             */

            name2Addr = new Name2Addr(this, BTreeMetadata.read(this, addr));

        }

        // register for commit notices.
        setCommitter(ROOT_NAME2ADDR, name2Addr);

    }
    
    public IIndex registerIndex(String name, IIndex btree) {

        if( getIndex(name) != null ) {
            
            throw new IllegalStateException("BTree already registered: name="+name);
            
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

        if(name==null) throw new IllegalArgumentException();
        
        return name2Addr.get(name);

    }

    /*
     * transaction support.
     */
    
    /**
     * Notify the journal that a new transaction is being activated (starting on
     * the journal).
     * 
     * @param tx
     *            The transaction.
     * 
     * @throws IllegalStateException
     * 
     * FIXME Detect transaction identifiers that go backwards? For example tx0
     * starts on one segment A while tx1 starts on segment B. Tx0 later starts
     * on segment B. From the perspective of segment B, tx0 begins after tx1.
     * This does not look like a problem unless there is an intevening commit,
     * at which point tx0 and tx1 will have starting contexts that differ by the
     * write set of the commit.<br>
     * What exactly is the impact when transactions start out of sequence? Do we
     * need to negotiated a distributed start time among all segments on which
     * the transaction starts? That would be a potential source of latency and
     * other kinds of pain. Look at how this is handled in the literature. One
     * way to handle it is essentially to declare the intention of the
     * transaction and pre-notify segments that will be written. This requires
     * some means of figuring out that intention and is probably relevant (and
     * solvable) only for very large row or key scans.
     * 
     * @todo What exactly is the impact when transactions end out of sequence? I
     *       presume that this is absolutely Ok.
     */
    void activateTx(ITx tx) throws IllegalStateException {

        Long id = tx.getId();

        if (activeTx.containsKey(id))
            throw new IllegalStateException("Already active: tx=" + tx);

        if (preparedTx.containsKey(id))
            throw new IllegalStateException("Already prepared: tx=" + tx);

        activeTx.put(id, tx);

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
    void prepared(ITx tx) throws IllegalStateException {

        Long id = tx.getId();

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
     * 
     * @todo Keep around complete transaction identifiers as a sanity check for
     *       repeated identifiers? This is definately not something to do for a
     *       deployed system since it will cause a memory leak.
     */
    void completedTx(ITx tx) throws IllegalStateException {

        assert tx != null;
        assert tx.isComplete();
        
        Long id = tx.getId();

        ITx txActive = activeTx.remove(id);

        ITx txPrepared = preparedTx.remove(id);

        if (txActive == null && txPrepared == null) {

            throw new IllegalStateException(
                    "Neither active nor being prepared: tx=" + tx);

        }

    }

    /**
     * Lookup an active or prepared transaction.
     * 
     * @param txId
     *            The transaction identifier.
     * 
     * @return The identified transaction or <code>null</code> if the
     *         transaction identifier is not mapped to either an active or
     *         prepared transaction.
     */
    public ITx getTx(long txId) {

        ITx tx = activeTx.get(txId);

        if (tx == null) {

            tx = preparedTx.get(txId);

        }

        return null;

    }

    // /**
    // * <p>
    // * Deallocate slots for versions having a transaction timestamp less than
    // or
    // * equal to <i>timestamp</i> that have since been overwritten (or deleted)
    // * by a committed transaction having a timestamp greater than
    // <i>timestamp</i>.
    // * </p>
    // * <p>
    // * The criteria for deallocating historical versions is that (a) there is
    // a
    // * more recent version; and (b) there is no ACTIVE (vs PENDING or
    // COMPLETED)
    // * transaction which could read from that historical version. The journal
    // * does NOT locally have enough information to decide when it can swept
    // * historical versions written by a given transaction. This notice MUST
    // come
    // * from a transaction service which has global knowledge of which
    // * transactions have PREPARED or ABORTED and can generate notices when all
    // * transactions before a given timestamp have been PREPARED or ABORTED.
    // For
    // * example, a long running transaction can cause notice to be delayed for
    // * many short lived transactions that have since completed. Once the long
    // * running transaction completes, the transaction server can compute the
    // * largest timestamp value below which there are no active transactions
    // and
    // * generate a single notice with that timestamp.
    // * </p>
    // *
    // * @param timestamp
    // * The timestamp.
    // *
    // * @todo This operation MUST be extremely efficient.
    // *
    // * @todo This method is exposed suposing a transaction service that will
    // * deliver notice when the operation should be conducted based on
    // * total knowledge of the state of all transactions running against
    // * the distributed database. As such, it may have to scan the journal
    // * to locate the commit record for transactions satisifying the
    // * timestamp criteria.
    // */
    // void gcTx( long timestamp ) {

    // // * <p>
    // // * Note: Migration to the read-optimized database is NOT a
    // pre-condition for
    // // * deallocation of historical versions - rather it enables us to remove
    // the
    // // * <em>current</em> committed version from the journal.
    // // * </p>

    // /*
    // * FIXME Implement garbage collection of overwritten and unreachable
    // * versions. Without migration to a read-optimized database, GC by
    // * itself is NOT sufficient to allow us to deallocate versions that have
    // * NOT been overwritten and hence is NOT sufficient to allow us to
    // * discard historical transactions in their entirety.
    // *
    // * Given a transaction Tn that overwrites one or more pre-existing
    // * versions, the criteria for deallocation of the overwritten versions
    // * are:
    // *
    // * (A) Tn commits, hence its intention has been made persistent; and
    // *
    // * (B) There are no active transactions remaining that started from a
    // * committed state before the commit state resulting from Tn, hence the
    // * versions overwritten by Tn are not visible to any active transaction.
    // * Any new transaction will read through the committed state produced by
    // * Tn and will perceive the new versions rather than the overwritten
    // * versions.
    // *
    // * Therefore, once Tn commits (assuming it has overwritten at least one
    // * pre-existing version), we can add each concurrent transaction Ti that
    // * is still active when Tn commits to a set of transactions that must
    // * either validate or abort before we may GC(Tn). Since Tn has committed
    // * it is not possible for new transactions to be created that would have
    // * to be included in this set since any new transaction would start from
    // * the committed state of Tn or its successors in the serialization
    // * order. As transactions validate or abort they are removed from
    // * GC(Tn). When this set is empty, we garbage collect the pre-existing
    // * versions that were overwritten by Tn.
    // *
    // * The sets GC(T) must be restart safe. Changes to the set can only
    // * occur when a transaction commits or aborts. However, even the abort
    // * of a transaction MUST be noticable on restart.
    // *
    // * A summary may be used that is the highest transaction timestamp for
    // * which Tn must wait before running GC(Tn). That can be written once
    // *
    // *
    // * Note that multiple transactions may have committed, so we may find
    // * that Tn has successors in the commit/serialization order that also
    // * meet the above criteria. All such committed transactions may be
    // * processed at once, but they MUST be processed in their commit order.
    // *
    // * Once those pre-conditions have been met the following algorithm is
    // * applied to GC the historical versions that were overwritten by Tn:
    // *
    // * 1. For each write by Ti where n < i <= m that overwrote a historical
    // * version, deallocate the slots for that historical version. This is
    // * valid since there are no active transactions that can read from that
    // * historical state. The processing order for Ti probably does not
    // * matter, but in practice there may be a reason to choose the
    // * serialization or reverse serialization order
    // *
    // * ---- this is getting closed but is not yet correct ----
    // *
    // * All versions written by a given transaction have the timestamp of
    // * that transaction.
    // *
    // * The committed transactions are linked by their commit records into a
    // * reverse serialization sequence.
    // *
    // * Each committed transaction has an object index that is accessible
    // * from its commit record. The entries in this index are versions that
    // * were written (or deleted) by that transaction. This index reads
    // * through into the object index for the committed state of the journal
    // * from which the transaction was minted.
    // *
    // * We could maintain in the entry information about the historical
    // * version that was overwritten. For example, its firstSlot or a run
    // * length encoding of the slots allocated to the historical version.
    // *
    // * We could maintain an index for all overwritten versions from
    // * [timestamp + dataId] to [slotId] (or a run-length encoding of the
    // * slots on which the version was written). Given a timestamp, we would
    // * then do a key scan from the start of the index for all entries whose
    // * timestamp was less than or equal to the given timestamp. For each
    // * such entry, we would deallocate the version and delete the entry from
    // * the index.
    // *
    // * tx0 : begin tx0 : write id0 (v0) tx0 : commit journal : deallocate <=
    // * tx0 (NOP since no overwritten versions)
    // *
    // * tx1 : begin tx2 : begin tx1 : write id0 (v1) tx1 : commit journal :
    // * deallocate <= tx1 (MUST NOT BE GENERATED since dependencies exist :
    // * tx1 and tx0 both depend on the committed state of tx0 -- sounds like
    // * lock style dependencies for deallocation !) tx2 : commit journal :
    // * deallocate <= tx2
    // *
    // * index:: [ tx0 : id0 ] : v0 [ tx1 : id1 ] : v1
    // *
    // * keyscan <= tx2
    // */

    // }

    // /**
    // * The transaction identifier of the last transaction begun on this
    // journal.
    // * In order to avoid extra IO this value survives restart IFF there is an
    // * intervening commit by any active transaction. This value is used to
    // * reject transactions whose identifier arrives out of sequence at the
    // * journal.
    // *
    // * @return The transaction identifier or <code>-1</code> if no
    // * transactions have begun on the journal (or if no transactions
    // * have ever committed and no transaction has begun since restart).
    // */
    // public long getLastBegunTx() {
    //        
    // return lastBegunTx;
    //        
    // }
    // private long lastBegunTx = -1;

    // /**
    // * Return the first commit record whose transaction timestamp is less than
    // * or equal to the given timestamp.
    // *
    // * @param timestamp
    // * The timestamp.
    // *
    // * @param exact
    // * When true, only an exact match is accepted.
    // *
    // * @return The commit record.
    // *
    // * @todo Define ICommitRecord.
    // *
    // * @todo A transaction may not begin if a transaction with a later
    // timestamp
    // * has already committed. Use this method to verify that. Since the
    // * commit records are cached, this probably needs to be moved back
    // * into the Journal.
    // *
    // * @todo Read the commit records into memory on startup.
    // *
    // * @todo The commit records are chained together by a "pointer" to the
    // prior
    // * commit record together with the timestamp of that record. The root
    // * block contains the timestamp of the earliest commit record that is
    // * still on the journal. Traversal of the prior pointers stops when
    // * the referenced record has a timestamp before the earliest versions
    // * still on hand.
    // */
    // protected Object getCommitRecord(long timestamp, boolean exact) {
    // 
    // /*
    // * @todo Search backwards from the current {@link IRootBlockView}.
    // */
    // throw new UnsupportedOperationException();
    //        
    // }
    //
    // /**
    // * FIXME Write commit record, including: the transaction identifier, the
    // * location of the object index and the slot allocation index, the
    // location
    // * of a run length encoded representation of slots allocated by this
    // * transaction, and the identifier and location of the prior transaction
    // * serialized on this journal.
    // */
    // protected void writeCommitRecord(IStore tx) {
    //        
    // }

}
