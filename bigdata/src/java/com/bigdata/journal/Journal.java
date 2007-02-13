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
 * <li> Segment server (mixture of journal server and read-optimized database
 * server).</li>
 * <li> Persistence capable data structures for the object index (basically, a
 * btree) and the allocation indices. The allocation index is less clear, but a
 * BitSet will do until I settle on something better - one of the tricks no
 * matter how you approach it is getting closure on the self-referential issue
 * with slots and a slot allocation index; maybe the per-tx data structure is
 * just transient will the committed data structure is persistent?</li>
 * <li> Transaction isolation.</li>
 * <li> Commit protocol, including plan for PREPARE in support of distributed
 * transactions.</li>
 * <li> Migration of data to a read-optimized database (have to implement the
 * read-optimized database as well). Explore use of page at a time compression
 * of rows. Compression makes it possible to fit more data on a logical page.
 * Overflow of rows on a logical page forces use of continuation pages.</li>
 * <li> Testing of an "embedded database" using both a journal only and a
 * journal + read-optimized database design. This can be tested up to the GPO
 * layer.</li>
 * <li>Support primary key (clustered) indices in GPO/PO layer.</li>
 * <li>Implement forward validation and state-based conflict resolution with
 * custom merge rules for persistent objects, generic objects, and primary key
 * indices, and secondary indexes.</li>
 * <li> Architecture using queues from GOM to journal/database segment server
 * supporting both embedded and remote scenarios.</li>
 * <li> Distributed database protocols.</li>
 * </ol>
 * 
 * FIXME Document the sources of latency and stages of identifer resolution for
 * bigdata
 * <ul>
 * <li>Transaction identifiers must be generated by a central source, which is
 * a source of latency.</li>
 * <li>A client must resolve each new segment identifer a service on a host. If
 * the segment is moved, the client must be notified or rediscover the location
 * of the segment (service and host) (multiple services on a host to let us
 * access all RAM using Java)</li>
 * <li>A segment identifier is resolved to a journal and database extent by a
 * service using an in-memory hash table.</li>
 * <li>An int32 identifier must be resolved against the journal first, if there
 * is a journal for that segment. If the journal is fully buffered, then this
 * will have low latency. Otherwise, we need a mixture of per-transaction hash
 * table (fast hits on data versions written within that transaction), an object
 * index with a high branch factor (fast access times to historical versions on
 * the journal) and perhaps a bloom filter in front of the persistent global
 * object index (weeds out most misses and transactional isolation has already
 * been handled). </li>
 * <li>A miss on the journal requires a read on the database segment. The
 * database uses a direct mapping of the int32 identifier to a logical page. The
 * logical page is a real page. The row on the page is found using a slot map.
 * If the logical page overflows, then some rows will be moved onto continuation
 * page(s). This means ONE(1) disk access (without cache hits) in the common
 * case, but overflow can result in chasing a page chain. If overflow proves
 * common, then we could list the continuation pages directly so that there was
 * never more than TWO (2) disk accesses to retrieve a page. We always read a
 * page at a time on the database and send back the entire page to the client.
 * We need to keep a list of pages that we have sent back recently so that we
 * can avoid re-sending pages that the client will already have. E.g., the
 * client promises to hold the last 10 pages from any given segment for at least
 * N seconds. If the client did not hold onto the page, then they can send
 * another request with an "Oops, I dropped that one" flag set.</li>
 * <li>Resolving a key for a clustered index requires (a) resolving the
 * clustered index using its int64 identifier per above; and (b) traversing the
 * index. Traversing the index means resolving a whole bunch of int64
 * identifiers. <em>Ideally</em> those identifiers have great locality and we
 * can do lots of in process hits on a segment by sending the find() request to
 * the segment containing the index root.</li>
 * <li>Transactions are distributed. A PREPARE must be sent to all segments on
 * which the client has written, all segments must validate, etc., and all
 * responses must be returned to the client. When all segments have responded,
 * the client can commit. (A transaction that does not write can send abort
 * notices - it still needs to wait for an acknowledgement in order to have a
 * robust notice of the abort. Transactions probably need to send heartbeats so
 * that they can be presumptively killed if they stop working.) </li>
 * <li> Replicating segments should be part of how they are written and failover
 * to an existing replication is very cheap. However, splitting a segment could
 * be relatively expensive.</li>
 * </ul>
 * 
 * FIXME Expand on latency and decision criteria for notifying clients when
 * pages or objects of interest have been modified by another transaction that
 * has committed (or in the case of distributed workers on a single transaction,
 * its peers).
 * 
 * FIXME Expand on the expected cost of finding a key or key range in a
 * clustered index, and on growth, splitting, migration, merging, and rebuilding
 * of key ranges for a clustered index as part of the above. It is absolutely
 * essential that the total cost of clustered index operations remains efficient
 * in the distributed database, that clustered indices can be readily (in terms
 * of transaction isolation) and efficiently (in terms of cost) split into and
 * old and a new segment (this may presume binary copy, which we don't really
 * support!). The tool that we have to work with is the journal, which can
 * absorb writes while we split the segment. However it MAY be MUCH easier to
 * manage this when the native segment model is a key range rather than int64
 * OIDs.
 * 
 * FIXME The disk writes are not being verified for the "direct" mode since we
 * are not testing restart and the journal is reading from an in-memory image.
 * 
 * FIXME Stress tests that cause the journal to wrap several times. Also tests
 * of extension, compaction, and truncation of the journal. A test that causes
 * wrapping needs to mix writes and deletes or the journal will run out of space
 * (running out of space should trigger extension). Compaction may require re-
 * copy of committed transactions and MUST NOT cause fragmentation. Compaction
 * makes sense when large swathes of the journal have been released, e.g., due
 * to data migration or overwrite. However, note that overwrite tends to cause
 * spotty release of slots rather than releasing entire ranges written by some
 * transaction.
 * 
 * FIXME The notion of a committed state needs to be captured by a persistent
 * structure in the journal until (a) there are no longer any active
 * transactions that can read from that committed state; and (b) the slots
 * allocated to that committed state have been released on the journal. Those
 * commit states need to be locatable on the journal, suggesting a record
 * written by PREPARE and finalized by COMMIT.
 * 
 * @todo Do we need to explicitly zero the allocated buffers? Are they already
 *       zeroed? How much do we actually need to write on them before the rest
 *       of the contents do not matter, e.g., just the root blocks?
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
 * @todo cache objects? They are materialized only for state-based validation.
 *       If possible, validation should occur in its own layer so that the basic
 *       store can handle IO using either byte[] or streams (allocated from a
 *       high concurrency queue to minimize stream creation overhead).
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
 * FIXME Write a test that performs an index split. This is one of the remaining
 * tricky issues and I want to work it through in more depth. Binary copy of the
 * index rows will not work since they refer to one another by int64 id, and
 * those ids are linked to the segment. It is acceptable for the split to be
 * moderately costly as long as it has low / no apparent latency to bigdata
 * clients. For example, we could do a split based on a binary copy and an
 * temporary (mutual) id mapping for the old and new segment and then fixup the
 * rows in the background. Another alterative is an index specific segment
 * structure that models a keyrange as sorted data on disk, i.e., the rows are
 * arranged in sorted order. As far as I can tell, these become the same thing
 * when a row is an index node is a sufficiently large page.
 * 
 * FIXME I've been considering the problem of a distributed index more. A
 * mutiplexed journal holding segments for one or more clustered indices
 * (containing anything from database rows to generic objects to terms to
 * triples) would provide fast write absorption and good read rates without a
 * secondary read-optimized segment. If this is combined with pipelined journals
 * for per-index segment redundency (a given segment is replicated on N=3+
 * hosts) then we can have both failover and load-balancing for read-intensive
 * segments.
 * 
 * Clustered indices support (re-)distribution and redundency since their rows
 * are their essential data while the index simply provides efficient access.
 * Therefore the same objects may be written onto multiple replications of an
 * index range hosted by different journals. The object data will remain
 * invariant, but the entailed index data will be different for each journal on
 * which the object is written.
 * 
 * This requires a variety of metadata for each index segment. If a segment is
 * defined by an index identifier and a separator key, then the metadata model
 * needs to identify the master journal for a given segment (writes are directed
 * to this journal) and the secondary journals for the segment (the master
 * writes through to the secondary journals using a pipeline). Likewise, the
 * journal must be able to identify the root for any given committed state of
 * each index range written on that journal.
 * 
 * We need to track write load and read load per index range in the journal, per
 * journal, per IO channel, per disk, per host, and per network segment. Write
 * load can be reduced by splitting a segment, by using a host with faster
 * resources, or by reducing other utilization of the host. The latter would
 * include the case of preferring reads from a secondary journal for that index
 * segment. It is an extremely cheap action to offload readers to a secondary
 * service. Likewise, failover of the master for an index range is also
 * inexpensive since the data are already in place on several secondaries,
 * 
 * A new replication of an index range may be built up gradually, e.g., by
 * moving a leaf at a time and piplining only the sub-range of the index range
 * that has been already mirrored. For simplicity, the new copy of the index
 * range would not be visible in the index metadata until a full copy of the
 * index range was life. Registration of the copy with the metadata index is
 * then trivial. Until that time, the metadata index would only know that a copy
 * was being developed. If one of the existing replicas is not heavily loaded
 * then it can handle the creation of the new copy.
 * 
 * The nut to crack is that if the journal exceeds the capacity to readily
 * buffer directly, then we are faced with the prospect of having the index
 * nodes scattered over the disk with poor locality. This makes it a relatively
 * expensive operation to clone part of an index. In contrast, if we can fully
 * buffer a journal during this operation then we face one large sequential read
 * followed by sustained (network or disk) IO (depending on where we are
 * extracting the index range).
 * 
 * We need an efficient means to allocate storage within the journal extent.
 * This is linked to the issue of deallocation of inaccessible historical
 * versions and load balancing. If load balancing is handled by rollover to an
 * existing replication of an index range, then load balancing is a very fast
 * action.
 * 
 * One alternative is to freeze the journal every ~200M and start a new one (or
 * alternatively to reuse the same journal segment by logically releasing slots
 * as historical data versions are no longer accessible to readers). With this
 * approach the journal could be treated as a WORM store and allocation would be
 * optimized (next free N bytes, so we don't even need to use slots). In this
 * model we would use small index (order 64) nodes in the journal (since it is
 * fully buffered IO is not a problem and this minimizes the cost of inserts and
 * deletions). Either once the journal is frozen or periodically during its life
 * we would evict an index to a perfect index segment on disk (write the leaves
 * out in a linear sequence, complete with prior-next references, and build up a
 * perfect index over those leaves - the branching factor here can be much
 * higher in order to reduce the tree depth and optimize IO). The perfect index
 * would have to have "delete" entries to mark deleted keys. Periodically we
 * would merge evicted index segments. The goal is to balance write absorbtion
 * and concurrency control within memory and using pure sequantial IO against
 * 100:1 or better data on disk with random seeks for reading any given leaf.
 * The index nodes would be written densely after the leaves such that it would
 * be possible to fully buffer the index nodes with a single sequential IO. A
 * key range scan would likewise be a sequential IO since we would know the
 * start and end leaf for the key range directly from the index. A read would
 * first resolve against the current journal, then the prior journal iff one is
 * in the process of be paritioned out into individual perfect index segments,
 * and then against those index segments in order. A "point" read would
 * therefore be biased to be more efficient for "recently" written data, but
 * reads otherwise must present a merged view of the data in each remaining
 * historical perfect index that covers at least part of the key range.
 * Periodically the index segments could be fully merged, at which point we
 * would no longer have any delete entries in a given perfect index segment.
 * 
 * At one extreme there will be one journal per disk and the journal will use
 * the entire disk partition. In a pure write scenario the disk would perform
 * only sequential IO. However, applications will also need to read data from
 * disk. Read and write buffers need to be split. Write buffers are used to
 * defer IOs until large sequential IOs may be realized. Read buffers are used
 * for pre-fetching when the data on disk is much larger than the available RAM
 * and the expectation of reuse is low while the expectation of completing a
 * sequential scan is high. Direct buffering may be used for hot-spots but
 * requires a means to bound the locality of the buffered segment, e.g., by not
 * multiplexing an index segment that will be directly buffered and providing a
 * sufficient resource abundence for high performance.
 * 
 * @todo I need to revisit the assumptions for very large objects in the face of
 *       the recent / planned redesign. I expect that using an index with a key
 *       formed as [URI][chuck#] would work just fine. Chunks would then be
 *       limited to 32k or so.
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

    // private final IConflictResolver conflictResolver;
    //    
    // /**
    // * The delegate that handles write-write conflict resolution during
    // backward
    // * validation. The conflict resolver is expected to make a best attempt
    // * using data type specific rules to reconcile the state for two versions
    // of
    // * the same persistent identifier. If the conflict can not be resolved,
    // then
    // * validation will fail. State-based conflict resolution when combined
    // with
    // * validation (aka optimistic locking) is capable of validating the
    // greatest
    // * number of interleavings of transactions (aka serialization orders).
    // *
    // * @return The conflict resolver to be applied during validation or
    // * <code>null</code> iff no conflict resolution will be performed.
    // */
    // public IConflictResolver getConflictResolver() {
    //        
    // return conflictResolver;
    //        
    // }

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
     * The maximum extent before a {@link #commit()} will trigger an
     * {@link #overflow()} event (overflow tries to trigger before this
     * point in order to avoid extending the journal).
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
        assert tx.isCommitted();
        
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
//
//    /**
//     * The transaction identifier of the most recently committed (last)
//     * transaction that was committed on the journal regardless of whether or
//     * not the data for that transaction remains on the journal.
//     * 
//     * @return The transaction identifier or <code>0L</code> IFF there no
//     *         transactions have ever committed on the journal.
//     */
//    public long getLastCommittedTx() {
//
//        return _rootBlock.getLastTxId();
//
//    }
//
//    /**
//     * The transaction identifier of the first (earliest) transaction that was
//     * committed on the journal and whose data versions have not since been
//     * deleted from the journal.
//     * 
//     * @return The transaction identifier or <code>0L</code> IFF there are no
//     *         committed transactions on the journal.
//     */
//    public long getFirstTxOnJournal() {
//
//        return _rootBlock.getFirstTxId();
//
//    }

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

        // Class conflictResolverClass = null;
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

        // /*
        // * "conflictResolver"
        // */
        //
        // val = properties.getProperty(Options.CONFLICT_RESOLVER);
        //        
        // if( val != null ) {
        //
        // try {
        //
        // conflictResolverClass = getClass().getClassLoader().loadClass(val);
        //
        // if (!IConflictResolver.class
        // .isAssignableFrom(conflictResolverClass)) {
        //
        // throw new RuntimeException(
        // "Conflict resolver does not implement: "
        // + IConflictResolver.class
        // + ", name=" + val);
        //
        // }
        //
        // } catch (ClassNotFoundException ex) {
        //
        // throw new RuntimeException(
        // "Could not load conflict resolver class: name=" + val
        // + ", " + ex, ex);
        //                
        // }
        //
        // /*
        // * Note: initialization of the conflict resolver is delayed until
        // * the journal is fully initialized.
        // */
        //            
        // }

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

        // /*
        // * Initialize the conflict resolver.
        // */
        //        
        // if( conflictResolverClass != null ) {
        //
        // try {
        //
        // Constructor ctor = conflictResolverClass
        // .getConstructor(new Class[] { Journal.class });
        //
        // this.conflictResolver = (IConflictResolver) ctor
        // .newInstance(new Object[] { this });
        //                
        // }
        //
        // catch (Exception ex) {
        //
        // throw new RuntimeException("Conflict resolver: " + ex, ex);
        //
        // }
        //            
        // } else {
        //            
        // /*
        // * The journal will not attempt to resolve write-write conflicts.
        // */
        //            
        // this.conflictResolver = null;
        //            
        // }

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

    /*
     * commit processing support.
     */

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
    final protected void _discardCommitters() {

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
             * FIXME update the firstTxId the first time a transaction commits
             * and the lastTxId each time a transaction commits. This needs to
             * be coordinated with the Tx class or the maps that maintain
             * metadata about transactions on the Journal.
             */
            IRootBlockView newRootBlock = new RootBlockView(
                    !old.isRootBlock0(), old.getSegmentId(), _bufferStrategy
                            .getNextOffset(), old.getFirstTxId(), old
                            .getLastTxId(), old.getCommitCounter() + 1,
                    rootAddrs);

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

}
