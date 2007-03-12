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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakValueCache;
import com.bigdata.isolation.ReadOnlyIsolatedIndex;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.ReadCommittedTx.ReadCommittedIndex;
import com.bigdata.objndx.BTree;
import com.bigdata.objndx.IIndex;
import com.bigdata.objndx.IndexSegment;
import com.bigdata.objndx.ReadOnlyIndex;
import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.scaleup.MasterJournal;
import com.bigdata.scaleup.SlaveJournal;
import com.bigdata.scaleup.MasterJournal.Options;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * <p>
 * The {@link Journal} is an append-only persistence capable data structure
 * supporting atomic commit, named indices, and transactions. Writes are
 * logically appended to the journal to minimize disk head movement.
 * </p>
 * <p>
 * Commit processing. The journal maintains two root blocks. Commit updates the
 * root blocks using the Challis algorithm. (The root blocks are updated using
 * an alternating pattern and timestamps are recorded at the head and tail of
 * each root block to detect partial writes.) When the journal is backed by a
 * disk file, the data
 * {@link Options#FORCE_ON_COMMIT optionally flushed to disk on commit}. If
 * desired, the writes may be flushed before the root blocks are updated to
 * ensure that the writes are not reordered - see {@link Options#DOUBLE_SYNC}.
 * </p>
 * <p>
 * Note: This class does NOT provide a thread-safe implementation of the
 * {@link IRawStore} API. Writes on the store MUST be serialized. Transaction
 * that write on the store are automatically serialized by {@link #commit(long)}.
 * </p>
 * <p>
 * Note: transaction processing MAY occur be concurrent since the write set of a
 * each transaction is written on a distinct {@link TemporaryRawStore}.
 * However, each transaction is NOT thread-safe and MUST NOT be executed by more
 * than one concurrent thread. Typically, a thread pool of some fixed size is
 * created and transctions are assigned to threads when they start on the
 * journal. If and as necessary, the {@link TemporaryRawStore} will spill from
 * memory onto disk allowing scalable transactions (up to 2G per transactions).
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Priority items are:
 * <ol>
 * <li> Concurrent load for RDFS w/o rollback.</li>
 * <li> Group commit for higher transaction throughput.<br>
 * Note: For short transactions, TPS is basically constant for a given
 * combination of the buffer mode and whether or not commits are forced to disk.
 * This means that the #of clients is not a strong influence on performance. The
 * big wins are Transient and Force := No since neither conditions synchs to
 * disk. This suggests that the big win for TPS throughput is going to be group
 * commit. </li>
 * <li> Scale-up database (automatic re-partitioning of indices and processing
 * of deletion markers).</li>
 * <li> AIO for the Direct and Disk modes.</li>
 * <li> GOM integration, including: support for primary key (clustered) indices;
 * using queues from GOM to journal/database segment server supporting both
 * embedded and remote scenarios; and using state-based conflict resolution to
 * obtain high concurrency for generic objects, link set metadata, and indices.</li>
 * <li> Scale-out database, including:
 * <ul>
 * <li> Data server (mixture of journal server and read-optimized database
 * server).</li>
 * <li> Transaction service (low-latency with failover).</li>
 * <li> Metadata index services (one per named index with failover).</li>
 * <li> Resource reclaimation. </li>
 * <li> Job scheduler to map functional programs across the data (possible
 * Hadoop integration point).</li>
 * </ul>
 * </ol>
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
 * @todo I need to revisit the assumptions for very large objects in the face of
 *       the recent / planned redesign. I expect that using an index with a key
 *       formed as [URI][chuck#] would work just fine. Chunks would then be
 *       limited to 32k or so. Writes on such indices should probably be
 *       directed to a journal using a disk-only mode.
 * 
 * @todo Checksums and/or record compression are currently handled on a per-{@link BTree}
 *       or other persistence capable data structure basis. It is nice to be
 *       able to choose for which indices and when ( {@link Journal} vs
 *       {@link IndexSegment}) to apply these algorithms. However, it might be
 *       nice to factor their application out a bit into a layered api - as long
 *       as the right layering is correctly re-established on load of the
 *       persistence data structure.
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
     * The directory that should be used for temporary files.
     */
    final public File tmpDir;
    
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
        boolean createTempFile = Options.DEFAULT_CREATE_TEMP_FILE;
        boolean isEmptyFile = false;
        boolean deleteOnClose = Options.DEFAULT_DELETE_ON_CLOSE;
        boolean deleteOnExit = Options.DEFAULT_DELETE_ON_EXIT;
        boolean readOnly = Options.DEFAULT_READ_ONLY;
        ForceEnum forceWrites = Options.DEFAULT_FORCE_WRITES;
        ForceEnum forceOnCommit = Options.DEFAULT_FORCE_ON_COMMIT;
        boolean doubleSync = Options.DEFAULT_DOUBLE_SYNC;

        String val;

        if (properties == null)
            throw new IllegalArgumentException();

        this.properties = properties = (Properties) properties.clone();
//        this.properties = properties;

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
         * "createTempFile"
         */

        val = properties.getProperty(Options.CREATE_TEMP_FILE);

        if (val != null) {

            createTempFile = Boolean.parseBoolean(val);

            if(createTempFile) {
                
                create = false;
            
                isEmptyFile = true;
                
            }
         
        }

        // "tmp.dir"
        val = properties.getProperty(Options.TMP_DIR);
        
        tmpDir = val == null ? new File(System.getProperty("java.io.tmpdir"))
                : new File(val); 

        if (!tmpDir.exists()) {
            
            if (!tmpDir.mkdirs()) {

                throw new RuntimeException("Could not create directory: "
                        + tmpDir.getAbsolutePath());
                
            }
            
        }

        if(!tmpDir.isDirectory()) {
            
            throw new RuntimeException("Not a directory: "
                    + tmpDir.getAbsolutePath());
            
        }
            
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
         * "deleteOnExit"
         */

        val = properties.getProperty(Options.DELETE_ON_EXIT);

        if (val != null) {

            deleteOnExit = Boolean.parseBoolean(val);

        }
        
        /*
         * "file"
         */

        File file;
        
        if(bufferMode==BufferMode.Transient) {
            
            file = null;
            
        } else {
            
            val = properties.getProperty(Options.FILE);

            if(createTempFile && val != null) {
                
                throw new RuntimeException("Can not use option '"
                        + Options.CREATE_TEMP_FILE + "' with option '"
                        + Options.FILE + "'");
                
            }

            if( createTempFile ) {
                
                try {

                    val = File.createTempFile("bigdata-" + bufferMode + "-",
                            ".jnl", tmpDir).toString();
                    
                } catch(IOException ex) {
                    
                    throw new RuntimeException(ex);
                    
                }
                
            }
            
            if (val == null) {

                throw new RuntimeException("Required property: '"
                        + Options.FILE + "'");

            }
            
            file = new File(val);

        }

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
             * Setup the buffer strategy.
             */

            FileMetadata fileMetadata = new FileMetadata(segmentId, file,
                    BufferMode.Direct, useDirectBuffers, initialExtent,
                    maximumExtent, create, isEmptyFile, deleteOnExit,
                    readOnly, forceWrites);

            _bufferStrategy = new DirectBufferStrategy(maximumExtent,
                    fileMetadata);

            this._rootBlock = fileMetadata.rootBlock;

            break;

        }

        case Mapped: {

            /*
             * Setup the buffer strategy.
             */

            FileMetadata fileMetadata = new FileMetadata(segmentId, file,
                    BufferMode.Mapped, useDirectBuffers, initialExtent,
                    maximumExtent, create, isEmptyFile, deleteOnExit,
                    readOnly, forceWrites);

            _bufferStrategy = new MappedBufferStrategy(maximumExtent,
                    fileMetadata);

            this._rootBlock = fileMetadata.rootBlock;

            break;

        }

        case Disk: {

            /*
             * Setup the buffer strategy.
             */

            FileMetadata fileMetadata = new FileMetadata(segmentId, file,
                    BufferMode.Disk, useDirectBuffers, initialExtent,
                    maximumExtent, create, isEmptyFile, deleteOnExit,
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
     * Shutdown the journal politely. Active transactions and transactions
     * pending commit will run to completion, but no new transactions will be
     * accepted.
     */
    public void shutdown() {

        assertOpen();

        final long begin = System.currentTimeMillis();
        
        log.warn("#active="+activeTx.size()+", shutting down...");
        
        /*
         * allow all pending tasks to complete, but no new tasks will be
         * accepted.
         */
        commitService.shutdown();

        // close immediately.
        close();
        
        final long elapsed = System.currentTimeMillis() - begin;
        
        log.warn("Journal is shutdown: elapsed="+elapsed);

    }

    public File getFile() {
        
        return _bufferStrategy.getFile();
        
    }
    
    public void close() {

        assertOpen();

        // force the commit thread to quit immediately.
        commitService.shutdownNow();
        
        _bufferStrategy.close();

        if (deleteOnClose) {

            /*
             * This option is used by the test suite and MUST NOT be used with
             * live data.
             */

            _bufferStrategy.deleteFile();

        }

    }

    public void closeAndDelete() {
        
        close();
        
        if (!deleteOnClose) {
            
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

    public boolean isFullyBuffered() {
        
        return _bufferStrategy.isFullyBuffered();
        
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

        // clear the root addresses - they will be reloaded.
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
     * address is saved in the {@link ICommitRecord} under the index for which
     * that committer was {@link #registerCommitter(int, ICommitter) registered}.
     * We then force the data to stable store, update the root block, and force
     * the root block and the file metadata to stable store.
     */
    public long commit() {

        return commitNow(nextTimestamp());

    }

    /**
     * Handles the {@link #commit()} and integrations with transaction support
     * so that we can update the first and last transaction identifiers on the
     * root block as necessary.
     * 
     * @param tx
     *            The transaction that is committing or <code>null</code> if
     *            the commit is not transactional.
     * 
     * @return The timestamp assigned to the commit record -or- 0L if there were
     *         no data to commit.
     */
    protected long commitNow(long commitTime) {

        assertOpen();

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

        final IRootBlockView old = _rootBlock;

        final long newCommitCounter = old.getCommitCounter() + 1;

        final ICommitRecord commitRecord = new CommitRecord(commitTime,
                newCommitCounter, rootAddrs);

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

            /*
             * Update the firstTxId the first time a transaction commits and the
             * lastTxId each time a transaction commits.
             * 
             * Note: These are commit time timestamps.
             */

            final long firstCommitTime = (old.getFirstCommitTime() == 0L ? commitTime
                    : old.getFirstCommitTime());

            final long lastCommitTime = commitTime;


            // Create the new root block.
            IRootBlockView newRootBlock = new RootBlockView(
                    !old.isRootBlock0(), old.getSegmentId(), _bufferStrategy
                            .getNextOffset(), firstCommitTime, lastCommitTime,
                    commitTime, newCommitCounter,
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

        return commitTime;

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

    public long size() {
        
        return _bufferStrategy.size();
        
    }
    
    public long write(ByteBuffer data) {

        assertOpen();

        return _bufferStrategy.write(data);

    }

    public ByteBuffer read(long addr) {

        assertOpen();

        return _bufferStrategy.read(addr);

    }

    final public long getRootAddr(int index) {

        assertOpen();

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

                _commitRecord = new CommitRecord();

            } else {

                _commitRecord = CommitRecordSerializer.INSTANCE
                        .deserialize(_bufferStrategy.read(commitRecordAddr));

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

            name2Addr = (Name2Addr) BTree.load(this, addr);

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

            ndx = (CommitRecordIndex) BTree.load(this, addr);

        }

        return ndx;

    }

    /**
     * Registers an {@link UnisolatedBTree} that will support transactional
     * isolation.
     * <p>
     * Note: You MUST {@link #commit()} before the registered index will be
     * either restart-safe or visible to new transactions.
     */
    public IIndex registerIndex(String name) {
        
        return registerIndex( name, new UnisolatedBTree(this));
        
    }
    
    /**
     * Note: You MUST {@link #commit()} before the registered index will be
     * either restart-safe or visible to new transactions.
     */
    public IIndex registerIndex(String name, IIndex btree) {

        assertOpen();

        if (getIndex(name) != null) {

            throw new IllegalStateException("Index already registered: name="
                    + name);

        }

        // add to the persistent name map.
        name2Addr.add(name, btree);

        return btree;

    }

    public void dropIndex(String name) {

        assertOpen();
        
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

        assertOpen();

        if (name == null)
            throw new IllegalArgumentException();

        return name2Addr.get(name);

    }

    /**
     * @todo the {@link CommitRecordIndex} is a possible source of thread
     *       contention since transactions need to use this code path in order
     *       to locate named indices but the {@link #commitService} can also
     *       write on this index. I have tried some different approaches to
     *       handling this.
     */
    public ICommitRecord getCommitRecord(long commitTime) {

        assertOpen();

        return _commitRecordIndex.find(commitTime);

    }

    /**
     * Returns a read-only named index loaded from the given root block. This
     * method imposes a canonicalizing mapping and contracts that there will be
     * at most one instance of the historical index at a time. This contract is
     * used to facilitate buffer management. Writes on the index will NOT be
     * made persistent and the index will NOT participate in commits.
     * <p>
     * Note: since this is always a request for historical read-only data, this
     * method MUST NOT register a committer and the returned btree MUST NOT
     * participate in the commit protocol.
     * <p>
     * Note: The caller MUST take care not to permit writes since they could be
     * visible to other users of the same read-only index. This is typically
     * accomplished by wrapping the returned object in class that will throw an
     * exception for writes such as {@link ReadOnlyIndex},
     * {@link ReadOnlyIsolatedIndex}, or {@link ReadCommittedIndex}.
     */
    protected IIndex getIndex(String name, ICommitRecord commitRecord) {

        assertOpen();

        if(name == null) throw new IllegalArgumentException();

        if(commitRecord == null) throw new IllegalArgumentException();
        
        /*
         * The address of an historical Name2Addr mapping used to resolve named
         * indices for the historical state associated with this commit record.
         */
        final long metaAddr = commitRecord.getRootAddr(ROOT_NAME2ADDR);

        /*
         * Resolve the address of the historical Name2Addr object using the
         * canonicalizing object cache. This prevents multiple historical
         * Name2Addr objects springing into existance for the same commit
         * record.
         */
        Name2Addr name2Addr = (Name2Addr)getIndex(metaAddr);
        
        /*
         * The address at which the named index was written for that historical
         * state.
         */
        final long indexAddr = name2Addr.getAddr(name);
        
        // No such index by name for that historical state.
        if(indexAddr==0L) return null;
        
        /*
         * Resolve the named index using the object cache to impose a
         * canonicalizing mapping on the historical named indices based on the
         * address on which it was written in the store.
         */
        return getIndex(indexAddr);

    }
    
    /**
     * A cache that is used by the {@link Journal} to provide a canonicalizing
     * mapping from an {@link Addr address} to the instance of a read-only
     * historical object loaded from that {@link Addr address}.
     * <p>
     * Note: the "live" version of an object MUST NOT be placed into this cache
     * since its state will continue to evolve with additional writes while the
     * cache is intended to provide a canonicalizing mapping to only the
     * historical states of the object. This means that objects such as indices
     * and the {@link Name2Addr} index MUST NOT be inserted into the cache if
     * the are being read from the store for "live" use. For this reason
     * {@link Name2Addr} uses its own caching mechanisms.
     * 
     * @todo discard cache on abort? that should not be necessary. even through
     *       it can contain objects whose addresses were not made restart safe
     *       those addresses should not be accessible to the application and
     *       hence the objects should never be looked up and will be evicted in
     *       due time from the cache. (this does rely on the fact that the store
     *       never reuses an address.)
     * 
     * FIXME This is the place to solve the resource (RAM) burden for indices is
     * Name2Addr. Currently, indices are never closed once opened which is a
     * resource leak. We need to close them out eventually based on LRU plus
     * timeout plus NOT IN USE. The way to approach this is a weak reference
     * cache combined with an LRU or hard reference queue that tracks reference
     * counters (just like the BTree hard reference cache for leaves). Eviction
     * events lead to closing an index iff the reference counter is zero.
     * Touches keep recently used indices from closing even though they may have
     * a zero reference count.
     * 
     * @todo the {@link MasterJournal} needs to do similar things with
     *       {@link IndexSegment}.
     * 
     * @todo review the metadata index lookup in the {@link SlaveJournal}. This
     *       is a somewhat different case since we only need to work with the
     *       current metadata index as along as we make sure not to reclaim
     *       resources (journals and index segments) until there are no more
     *       transactions which can read from them.
     * 
     * @todo support metering of index resources and timeout based shutdown of
     *       indices. note that the "live" {@link Name2Addr} has its own cache
     *       for the unisolated indices and that metering needs to pay attention
     *       to the indices in that cache as well. Also, those indices can be
     *       shutdown as long as they are not dirty (pending a commit).
     */
    final private WeakValueCache<Long, ICommitter> objectCache = new WeakValueCache<Long, ICommitter>(
            new LRUCache<Long, ICommitter>(20));
    
    /**
     * A canonicalizing mapping for index objects.
     * 
     * @param addr
     *            The {@link Addr address} of the index object.
     *            
     * @return The index object.
     */
    final protected IIndex getIndex(long addr) {
        
        synchronized (objectCache) {

            IIndex obj = (IIndex) objectCache.get(addr);

            if (obj == null) {
                
                obj = BTree.load(this,addr);
                
            }
            
            objectCache.put(addr, (ICommitter)obj, false/*dirty*/);
    
            return obj;

        }
        
    }

//    /**
//     * Insert or touch an object in the object cache.
//     * 
//     * @param addr
//     *            The {@link Addr address} of the object in the store.
//     * @param obj
//     *            The object.
//     * 
//     * @see #getIndex(long), which provides a canonicalizing mapping for index
//     *      objects using the object cache.
//     */
//    final protected void touch(long addr,Object obj) {
//        
//        synchronized(objectCache) {
//            
//            objectCache.put(addr, (ICommitter)obj, false/*dirty*/);
//            
//        }
//        
//    }

    /*
     * ITransactionManager and friends.
     * 
     * @todo refactor into an ITransactionManager service. provide an
     * implementation that supports only a single Journal resource and an
     * implementation that supports a scale up/out architecture. the journal
     * should resolve the service using JINI. the timestamp service should
     * probably be co-located with the transaction service.
     */

    /**
     * The service used to generate commit timestamps.
     * 
     * @todo parameterize using {@link Options} so that we can resolve a
     *       low-latency service for use with a distributed database commit
     *       protocol.
     */
    protected final ITimestampService timestampFactory = LocalTimestampService.INSTANCE;

    /**
     * A hash map containing all active transactions. A transaction that is
     * preparing will be in this collection until it has either successfully
     * prepared or aborted.
     */
    final Map<Long, ITx> activeTx = new ConcurrentHashMap<Long, ITx>();

    /**
     * A hash map containing all transactions that have prepared but not yet
     * either committed or aborted.
     * 
     * @todo A transaction will be in this map only while it is actively
     *       committing, so this is always a "map" of one and could be replaced
     *       by a scalar reference (except that we may allow concurrent prepare
     *       and commit of read-only transactions).
     */
    final Map<Long, ITx> preparedTx = new ConcurrentHashMap<Long, ITx>();

    /**
     * A thread that imposes serializability on transactions. A writable
     * transaction that attempts to {@link #commit()} is added as a
     * {@link TxCommitTask} and queued for execution by this thread. When its turn
     * comes, it will validate its write set and commit iff validation succeeds.
     */
    final ExecutorService commitService = Executors
            .newSingleThreadExecutor(DaemonThreadFactory
                    .defaultThreadFactory());
    
    public long nextTimestamp() {
        
        return timestampFactory.nextTimestamp();
        
    }

    public long newTx() {

        return newTx(IsolationEnum.ReadWrite);

    }

    public long newTx(IsolationEnum level) {

        final long startTime = nextTimestamp();

        switch (level) {

        case ReadCommitted:
            new ReadCommittedTx(this, startTime);
            break;
        
        case ReadOnly:
            new Tx(this, startTime, true);
            break;
        
        case ReadWrite:
            new Tx(this, startTime, false);
            break;

        default:
            throw new AssertionError("Unknown isolation level: " + level);
        }

        return startTime;
        
    }

    public void abort(long ts) {

        ITx tx = getTx(ts);
        
        if (tx == null)
            throw new IllegalArgumentException("No such tx: " + ts);
        
        if(tx.isReadOnly()) {
         
            // abort is synchronous.
            tx.abort();
            
        } else {

            // queue the abort request.
            commitService.submit(new TxAbortTask(tx));
            
        }
        
    }

    /**
     * @todo Implement group commit. large transactions do not get placed into
     *       commit groups. Group commit collects up to N 'small' transactions
     *       with at most M milliseconds of latency and perform a single atomic
     *       commit for that group of transactions, forcing the data to disk and
     *       then returning control to the clients. Note that we need to collect
     *       transactions once they have completed but not yet validated. Any
     *       transaction in the group that fails validation must be aborted.
     *       Validation needs to be against then current state of the database,
     *       so the process is serialized (validate+commit) with transactions
     *       that fail validation being placed onto an abort queue. A single
     *       unisolated commit is then performed for the group, so there is one
     *       SYNC per commit.
     */
    public long commit(long ts) {

        ITx tx = getTx(ts);
        
        if (tx == null)
            throw new IllegalArgumentException("No such tx: " + ts);

        /*
         * A read-only transaction can commit immediately since validation and
         * commit are basically NOPs.
         * 
         * @todo We could also detect transactions with empty write sets (no
         * touched indices) and shortcut the prepare/commit for those
         * transactions as well. The easy way to do this is to rangeCount each
         * index isolated by the transaction.
         */

        if(tx.isReadOnly()) {
        
            // read-only transactions do not get a commit time.
            tx.prepare(0L);

            return tx.commit();
            
        }
        
        try {

            long commitTime = commitService.submit(new TxCommitTask(tx)).get();
            
            if(DEBUG) {
                
                log.debug("committed: startTime="+tx.getStartTimestamp()+", commitTime="+commitTime);
                
            }
            
            return commitTime;
            
        } catch(InterruptedException ex) {
            
            // interrupted, perhaps during shutdown.
            throw new RuntimeException(ex);
            
        } catch(ExecutionException ex) {
            
            Throwable cause = ex.getCause();
            
            if(cause instanceof ValidationError) {
                
                throw (ValidationError) cause;
                
            }

            // this is an unexpected error.
            throw new RuntimeException(cause);
            
        }
        
    }
    
    /**
     * Task validates and commits a transaction when it is run by the
     * {@link Journal#commitService}.
     * <p>
     * Note: The commit protocol does not permit unisolated writes on the
     * journal once a transaction begins to prepare until the transaction has
     * either committed or aborted (if such writes were allowed then we would
     * have to re-validate any prepared transactions in order to enforce
     * serializability).
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class TxCommitTask implements Callable<Long> {
        
        private final ITx tx;
        
        public TxCommitTask(ITx tx) {
            
            assert tx != null;
            
            this.tx = tx;
            
        }

        public Long call() throws Exception {
            
            /*
             * The commit time is assigned when we prepare the transaction.
             * 
             * @todo resolve this against a service in a manner that will
             * support a distributed database commit protocol.
             */
            final long commitTime = ((Tx)tx).journal.nextTimestamp();
            
            tx.prepare(commitTime);
            
            return tx.commit();
            
        }
        
    }
    
    /**
     * Task aborts a transaction when it is run by the
     * {@link Journal#commitService}. This is used to serialize some abort
     * processing which would otherwise be concurrent with commit processing.
     * {@link Journal#abort()} makes some assumptions that it is a
     * single-threaded environment and those assumptions would be violated
     * without serialization aborts and commits on the same queue.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class TxAbortTask implements Callable<Long> {
        
        private final ITx tx;
        
        public TxAbortTask(ITx tx) {
            
            assert tx != null;
            
            this.tx = tx;
            
        }

        public Long call() throws Exception {
            
            tx.abort();
            
            return 0L;
           
        }
        
    }

    /**
     * Notify the journal that a new transaction is being activated (starting on
     * the journal).
     * 
     * @param tx
     *            The transaction.
     * 
     * @throws IllegalStateException
     * 
     * @todo test for transactions that have already been completed? that would
     *       represent a protocol error in the transaction manager service.
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
     * @param startTime
     *            The start timestamp for the transaction.
     * 
     * @return The transaction with that start time or <code>null</code> if
     *         the start time is not mapped to either an active or prepared
     *         transaction.
     */
    public ITx getTx(long startTime) {

        ITx tx = activeTx.get(startTime);

        if (tx == null) {

            tx = preparedTx.get(startTime);

        }

        return tx;

    }

    public IIndex getIndex(String name, long ts) {
        
        if(name == null) throw new IllegalArgumentException();
        
        ITx tx = activeTx.get(ts);
        
        if(tx==null) throw new IllegalStateException();
        
        return tx.getIndex(name);
        
    }
    
}
