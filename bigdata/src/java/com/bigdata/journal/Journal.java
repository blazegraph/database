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
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * <p>
 * An object journal for very fast write absorbtion. The journal supports
 * concurrent fully isolated transactions and is designed to absorb writes
 * destined for a read-optimized database file. Writes are logically appended to
 * the journal to minimize disk head movement.
 * </p>
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
 * Commit processing. The journal also maintains two root blocks. When the
 * journal is wired into memory, incremental writes are absorbed by the journal
 * into a direct buffer and written through to disk. Writes are flushed to disk
 * on commit. Commit processing also updates the root blocks using the Challis
 * algorithm. (The root blocks are updated using an alternating pattern and
 * timestamps are recorded at the head and tail of each root block to detect
 * partial writes.)
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
 * FIXME Expand on latency and decision criteria for int64 assignments.
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
 * of transaction isolation) and efficient (in terms of cost) split into and old
 * and a new segment (this may presume binary copy, which we don't really
 * support!). The tool that we have to work with is the journal, which can
 * absorb writes while we split the segment. However it MAY be MUCH easier to
 * manage this when the native segment model is a key range rather than int64
 * OIDs.
 * 
 * FIXME Explore use of bloom filters in front of segments. How does this
 * interact with transaction isolation?
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
 * FIXME Migration of data to the read-optimized database means that the current
 * committed version is on the database. However, subsequent migration of
 * another version of the same data item can require the re-introduction of a
 * mapping into the object index IFF there are active transactions that can read
 * the now historical data version. This suggests that migration probably should
 * not remove the entry from the object index, but rather flag that the current
 * version is on the database. That flag can be cleared when the version is
 * replaced. This also suggests efficient lookup of prior versions is required.
 * 
 * FIXME The notion of a committed state needs to be captured by a persistent
 * structure in the journal until (a) there are no longer any active
 * transactions that can read from that committed state; and (b) the slots
 * allocated to that committed state have been released on the journal. Those
 * commit states need to be locatable on the journal, suggesting a record
 * written by PREPARE and finalized by COMMIT.
 * 
 * FIXME When is the most efficient time to migrate committed state to the read-
 * optimized database? How does this trade off system resources and latency?
 * 
 * FIXME I need to work through the conditional deallocation of slots using a
 * persistence capable slot allocation before putting more effort into a
 * persistence capable object index. I can do that by working through making the
 * store restart safe, even if the object index is a full serialization of a
 * hash table rather than incremental updates of a persistent btree. The commit
 * time will suck, but it will provide the design for conditional deallocation
 * and restart.
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
 * @todo cache index and allocation nodes regardless of the strategy since those
 *       are materialized objects.
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
 * @todo Make decision about whether or not id == 0 is allowed and whether or
 *       not ids are 32bit clean. The main purpose for the journal is part of
 *       bigdata and in that role it needs to absorb any within segment
 *       identifier that is legal for bigdata. If bigdata uses 32bit ids, then
 *       the journal must as well. However, I think that it is quite unlikely
 *       that bigdata will use a 32-bit within segment identifier with an int16
 *       page and int16 slot since the goal is to keep the #of pages in a
 *       segment down (this is true even for large (blob) segments since they
 *       just use large pages). When the journal is used as part of an embedded
 *       database, id == 0 is typically treated as null and disallowed. Thse
 *       only part of the code that depends on this in any way may be the object
 *       index which has a test for a "null" key value and needs to know the
 *       minimum and maximum key values. Other than that the constraint is just
 *       imposed by checks on read(), write() and delete() on the Journal and Tx
 *       classes.
 * 
 * FIXME Implement a read only transaction and write test suites for its
 * semantics, including both the inability to write (or delete) on the
 * transaction, the handling of pre-existing versions, and the non-duplication
 * of per-tx data structures.
 * 
 * FIXME Write test suites for the {@link TransactionServer} - this will prove
 * out the GC design.
 * 
 * FIXME Write a migration thread that just moves the data into another journal
 * or a jdbm instance. The point is to prove out the migration design in code
 * and with tests.
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
 * FIXME Consider a journal that combines log information destined for multiple
 * segments on the same host. With one journal per segment, we have sequential
 * access per journal but the head must seek from one journal to the next. With
 * one journal for N segments, there is less head seek time. This probably does
 * NOT make the basic concurrency control logic any more complex, but it
 * probably does make it more complex to move a segment from one host to
 * another. That can be handled by differentiating the host on which the data
 * resides on disk from the host serving requests for a segment.
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
 */

public class Journal implements IRawStore, IStore {

    final SlotMath slotMath;
    
    public SlotMath getSlotMath() {
        
        return slotMath;
        
    }
    
    /**
     * The implementation logic for the current {@link BufferMode}.
     * 
     * @todo Support dynamically changing the buffer mode or just require that
     *       the journal be closed and opened under a new buffer mode? The
     *       latter is much simpler, but the operation can potentially be
     *       optimized when the data is already in memory. The most interesting
     *       cases are promoting from a direct buffer strategy to a disk-only
     *       strategy or from a transient strategy to a persistent strategy. It
     *       is also interesting to back down from a memory mapped strategy (or
     *       even a disk-only strategy) to one using a buffer.
     */
    final IBufferStrategy _bufferStrategy;

    /**
     * The delegate that implements the {@link BufferMode}.-
     */
    public IBufferStrategy getBufferStrategy() {
        
        return _bufferStrategy;
        
    }

    private final IConflictResolver conflictResolver;
    
    /**
     * The delegate that handles write-write conflict resolution during backward
     * validation. The conflict resolver is expected to make a best attempt
     * using data type specific rules to reconcile the state for two versions of
     * the same persistent identifier. If the conflict can not be resolved, then
     * validation will fail. State-based conflict resolution when combined with
     * validation (aka optimistic locking) is capable of validating the greatest
     * number of interleavings of transactions (aka serialization orders).
     * 
     * @return The conflict resolver to be applied during validation or
     *         <code>null</code> iff no conflict resolution will be performed.
     */
    public IConflictResolver getConflictResolver() {
        
        return conflictResolver;
        
    }
    
    /**
     * The object index.
     * 
     * @todo Change to use the {@link IObjectIndex} interface.
     */
    final SimpleObjectIndex objectIndex;
    
    /**
     * The slot allocation index.
     */
    final ISlotAllocationIndex allocationIndex;

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
     * A hash map containing all active transactions. A transaction that is
     * preparing will be in this collection until it has either successfully
     * prepared or aborted.
     */
    final Map<Long,ITx> activeTx = new HashMap<Long,ITx>();

    /**
     * A hash map containing all transactions that have prepared but not yet
     * either committed or aborted.
     */
    final Map<Long,ITx> preparedTx = new HashMap<Long,ITx>();
    
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
    void activateTx( ITx tx ) throws IllegalStateException {
        
        Long id = tx.getId();
        
        if( activeTx.containsKey( id ) ) throw new IllegalStateException("Already active: tx="+tx);
        
        if( preparedTx.containsKey(id)) throw new IllegalStateException("Already prepared: tx="+tx);

        activeTx.put(id,tx);
        
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
    void prepared( ITx tx ) throws IllegalStateException {
        
        Long id = tx.getId();
        
        IStore tx2 = activeTx.remove(id);
        
        if( tx2 == null ) throw new IllegalStateException("Not active: tx="+tx);
        
        assert tx == tx2;
        
        if( preparedTx.containsKey(id)) throw new IllegalStateException("Already preparing: tx="+tx);
        
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
    void completedTx( ITx tx ) throws IllegalStateException {
        
        Long id = tx.getId();
        
        IStore txActive = activeTx.remove(id);
        
        IStore txPrepared = preparedTx.remove(id);
        
        if( txActive == null && txPrepared == null ) {
            
            throw new IllegalStateException("Neither active nor being prepared: tx="+tx);
            
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
        
        if( tx == null ) {
            
            tx = preparedTx.get(txId);
            
        }

        return null;
        
    }
    
    /**
     * <p>
     * Deallocate slots for versions having a transaction timestamp less than or
     * equal to <i>timestamp</i> that have since been overwritten (or deleted)
     * by a committed transaction having a timestamp greater than <i>timestamp</i>.
     * </p>
     * <p>
     * The criteria for deallocating historical versions is that (a) there is a
     * more recent version; and (b) there is no ACTIVE (vs PENDING or COMPLETED)
     * transaction which could read from that historical version. The journal
     * does NOT locally have enough information to decide when it can swept
     * historical versions written by a given transaction. This notice MUST come
     * from a transaction service which has global knowledge of which
     * transactions have PREPARED or ABORTED and can generate notices when all
     * transactions before a given timestamp have been PREPARED or ABORTED. For
     * example, a long running transaction can cause notice to be delayed for
     * many short lived transactions that have since completed. Once the long
     * running transaction completes, the transaction server can compute the
     * largest timestamp value below which there are no active transactions and
     * generate a single notice with that timestamp.
     * </p>
     * 
     * @param timestamp
     *            The timestamp.
     * 
     * @todo This operation MUST be extremely efficient.
     * 
     * @todo This method is exposed suposing a transaction service that will
     *       deliver notice when the operation should be conducted based on
     *       total knowledge of the state of all transactions running against
     *       the distributed database. As such, it may have to scan the journal
     *       to locate the commit record for transactions satisifying the
     *       timestamp criteria.
     */
    void gcTx( long timestamp ) {

//        * <p>
//        * Note: Migration to the read-optimized database is NOT a pre-condition for
//        * deallocation of historical versions - rather it enables us to remove the
//        * <em>current</em> committed version from the journal.
//        * </p>

        /*
         * FIXME Implement garbage collection of overwritten and unreachable
         * versions. Without migration to a read-optimized database, GC by
         * itself is NOT sufficient to allow us to deallocate versions that have
         * NOT been overwritten and hence is NOT sufficient to allow us to
         * discard historical transactions in their entirety.
         * 
         * Given a transaction Tn that overwrites one or more pre-existing
         * versions, the criteria for deallocation of the overwritten versions
         * are:
         * 
         * (A) Tn commits, hence its intention has been made persistent; and
         * 
         * (B) There are no active transactions remaining that started from a
         * committed state before the commit state resulting from Tn, hence the
         * versions overwritten by Tn are not visible to any active transaction.
         * Any new transaction will read through the committed state produced by
         * Tn and will perceive the new versions rather than the overwritten
         * versions.
         * 
         * Therefore, once Tn commits (assuming it has overwritten at least one
         * pre-existing version), we can add each concurrent transaction Ti that
         * is still active when Tn commits to a set of transactions that must
         * either validate or abort before we may GC(Tn). Since Tn has committed
         * it is not possible for new transactions to be created that would have
         * to be included in this set since any new transaction would start from
         * the committed state of Tn or its successors in the serialization
         * order. As transactions validate or abort they are removed from
         * GC(Tn). When this set is empty, we garbage collect the pre-existing
         * versions that were overwritten by Tn.
         * 
         * The sets GC(T) must be restart safe. Changes to the set can only
         * occur when a transaction commits or aborts.  However, even the abort
         * of a transaction MUST be noticable on restart.
         * 
         * A summary may be used that is the highest transaction timestamp for
         * which Tn must wait before running GC(Tn).  That can be written once
         * 
         * 
         * Note that multiple transactions may have committed, so we may find
         * that Tn has successors in the commit/serialization order that also
         * meet the above criteria. All such committed transactions may be
         * processed at once, but they MUST be processed in their commit order.
         * 
         * Once those pre-conditions have been met the following algorithm is
         * applied to GC the historical versions that were overwritten by Tn:
         * 
         * 1. For each write by Ti where n < i <= m that overwrote a historical
         * version, deallocate the slots for that historical version. This is
         * valid since there are no active transactions that can read from that
         * historical state. The processing order for Ti probably does not
         * matter, but in practice there may be a reason to choose the
         * serialization or reverse serialization order
         * 
         * ---- this is getting closed but is not yet correct ----
         * 
         * All versions written by a given transaction have the timestamp of
         * that transaction.
         * 
         * The committed transactions are linked by their commit records into a
         * reverse serialization sequence.
         * 
         * Each committed transaction has an object index that is accessible
         * from its commit record. The entries in this index are versions that
         * were written (or deleted) by that transaction. This index reads
         * through into the object index for the committed state of the journal
         * from which the transaction was minted.
         * 
         * We could maintain in the entry information about the historical
         * version that was overwritten. For example, its firstSlot or a run
         * length encoding of the slots allocated to the historical version.
         * 
         * We could maintain an index for all overwritten versions from
         * [timestamp + dataId] to [slotId] (or a run-length encoding of the
         * slots on which the version was written). Given a timestamp, we would
         * then do a key scan from the start of the index for all entries whose
         * timestamp was less than or equal to the given timestamp. For each
         * such entry, we would deallocate the version and delete the entry from
         * the index.
         * 
         * tx0 : begin tx0 : write id0 (v0) tx0 : commit journal : deallocate <=
         * tx0 (NOP since no overwritten versions)
         * 
         * tx1 : begin tx2 : begin tx1 : write id0 (v1) tx1 : commit journal :
         * deallocate <= tx1 (MUST NOT BE GENERATED since dependencies exist :
         * tx1 and tx0 both depend on the committed state of tx0 -- sounds like
         * lock style dependencies for deallocation !) tx2 : commit journal :
         * deallocate <= tx2
         * 
         * index:: [ tx0 : id0 ] : v0 [ tx1 : id1 ] : v1
         * 
         * keyscan <= tx2
         */
        
    }
    
//    /**
//     * The transaction identifier of the last transaction begun on this journal.
//     * In order to avoid extra IO this value survives restart IFF there is an
//     * intervening commit by any active transaction. This value is used to
//     * reject transactions whose identifier arrives out of sequence at the
//     * journal.
//     * 
//     * @return The transaction identifier or <code>-1</code> if no
//     *         transactions have begun on the journal (or if no transactions
//     *         have ever committed and no transaction has begun since restart).
//     */
//    public long getLastBegunTx() {
//        
//        return lastBegunTx;
//        
//    }
//    private long lastBegunTx = -1;
    
    /**
     * The transaction identifier of the most recently committed (last)
     * transaction that was committed on the journal regardless of whether or
     * not the data for that transaction remains on the journal.
     * 
     * @return The transaction identifier or <code>-1</code> IFF there no
     *         transactions have ever committed on the journal.
     */
    public long getLastCommittedTx() {
        
        return lastCommittedTx;
        
    }
    private long lastCommittedTx = -1;
    
    /**
     * The transaction identifier of the first (earliest) transaction that was
     * committed on the journal and whose data versions have not since been
     * deleted from the journal.
     * 
     * @return The transaction identifier or <code>-1</code> IFF there are no
     *         committed transactions on the journal.
     */
    public long getFirstTxOnJournal() {
     
        return firstTxOnJournal;
        
    }
    private long firstTxOnJournal = -1;
    
    /**
     * Create or open a journal.
     * 
     * @param properties The properties as defined by {@link Options}.
     * 
     * @throws IOException
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
     * 
     * @todo Caller should start migration thread.
     * 
     * @todo Caller should provide network interface and distributed commit
     *       support.
     */
    
    public Journal(Properties properties) throws IOException {
        
        long segment;
        int slotSize;
        long initialExtent = Options.DEFAULT_INITIAL_EXTENT;
        int objectIndexSize = Options.DEFAULT_OBJECT_INDEX_SIZE;
        boolean create = Options.DEFAULT_CREATE;
        boolean readOnly = Options.DEFAULT_READ_ONLY;
        boolean deleteOnClose = Options.DEFAULT_DELETE_ON_CLOSE;
        ForceEnum forceWrites = Options.DEFAULT_FORCE_WRITES;
        ForceEnum forceOnCommit = Options.DEFAULT_FORCE_ON_COMMIT;
        
        Class conflictResolverClass = null;
        String val;

        if( properties == null ) throw new IllegalArgumentException();

        /*
         * "bufferMode" mode. Note that very large journals MUST use the
         * disk-based mode.
         */

        val = properties.getProperty(Options.BUFFER_MODE);

        if (val == null)
            val = BufferMode.Direct.toString();

        BufferMode bufferMode = BufferMode.parse(val);

        /*
         * "segment".
         */
        
        val = properties.getProperty(Options.SEGMENT);
        
        if( val == null ) {
            
            if( bufferMode == BufferMode.Transient) {
            
            val = "0";
            
            } else {
            
                throw new RuntimeException("Required property: '"+Options.SEGMENT+"'");
                
            }
            
        }

        segment = Long.parseLong(val);
        
        /*
         * "slotSize"
         */

        val = properties.getProperty(Options.SLOT_SIZE);

        if (val == null) {
         
            val = "128";
            
        }

        slotSize = Integer.parseInt(val);

        if (slotSize < Options.MIN_SLOT_SIZE ) {

            throw new RuntimeException("'" + Options.SLOT_SIZE
                    + "' must be at least " + Options.MIN_SLOT_SIZE
                    + ", but found " + slotSize);

        }

        /*
         * Helper object for slot-based computations.
         */
        
        this.slotMath = new SlotMath(slotSize);

        /*
         * "initialExtent"
         */

        val = properties.getProperty(Options.INITIAL_EXTENT);

        if (val != null) {

            initialExtent = Long.parseLong(val);

            if( initialExtent < Bytes.megabyte ) {
                
                throw new RuntimeException(
                        "The '" + Options.INITIAL_EXTENT
                        + "' must be at least one megabyte(" + Bytes.megabyte
                        + ")");
                
            }
            
        }
        
        /*
         * "readOnly"
         */

        val = properties.getProperty(Options.READ_ONLY);
        
        if( val != null ) {

            readOnly = Boolean.parseBoolean(val);
            
        }

        /*
         * "forceWrites"
         */

        val = properties.getProperty(Options.FORCE_WRITES);
        
        if( val != null ) {

            forceWrites = ForceEnum.parse(val);
            
        }

        /*
         * "forceOnCommit"
         */

        val = properties.getProperty(Options.FORCE_ON_COMMIT);
        
        if( val != null ) {

            forceOnCommit = ForceEnum.parse(val);
            
        }

        this.forceOnCommit = forceOnCommit;
        
        /*
         * "objectIndexSize"
         */

        val = properties.getProperty(Options.OBJECT_INDEX_SIZE);
        
        if( val != null ) {

            objectIndexSize = Integer.parseInt(val);

            if (objectIndexSize < Options.MIN_OBJECT_INDEX_SIZE
                    || objectIndexSize > Options.MAX_OBJECT_INDEX_SIZE) {

                throw new RuntimeException("'"
                        + Options.DEFAULT_OBJECT_INDEX_SIZE + "' must be in ["
                        + Options.MIN_OBJECT_INDEX_SIZE + ":"
                        + Options.MAX_OBJECT_INDEX_SIZE + "], but found "
                        + objectIndexSize);
                
            }
            
        }

        /*
         * "deleteOnClose"
         */

        val = properties.getProperty(Options.DELETE_ON_CLOSE);
        
        if( val != null ) {

            deleteOnClose = Boolean.parseBoolean(val);
            
        }
        
        this.deleteOnClose = deleteOnClose;

        /*
         * "conflictResolver"
         */

        val = properties.getProperty(Options.CONFLICT_RESOLVER);
        
        if( val != null ) {

            try {

                conflictResolverClass = getClass().getClassLoader().loadClass(val);

                if (!IConflictResolver.class
                        .isAssignableFrom(conflictResolverClass)) {

                    throw new RuntimeException(
                            "Conflict resolver does not implement: "
                                    + IConflictResolver.class
                                    + ", name=" + val);

                }

            } catch (ClassNotFoundException ex) {

                throw new RuntimeException(
                        "Could not load conflict resolver class: name=" + val
                                + ", " + ex, ex);
                
            }

            /*
             * Note: initialization of the conflict resolver is delayed until
             * the journal is fully initialized.
             */
            
        }

        /*
         * Create the appropriate IBufferStrategy object.
         */
        
        switch (bufferMode) {
        
        case Transient: {

            /*
             * Setup the buffer strategy.
             */
            
            if( readOnly ) {
            
                throw new RuntimeException(
                        "readOnly not supported for transient journals.");
            
            }

            _bufferStrategy = new TransientBufferStrategy(slotMath,
                    initialExtent);

            // FIXME Refactor code for bootstrapping the root block and use it
            // here also.
            this._rootBlock = null;
            
            break;
            
        }
        
        case Direct: {

            /*
             * "file"
             */

            val = properties.getProperty(Options.FILE);

            if (val == null) {
             
                throw new RuntimeException("Required property: '"+Options.FILE+"'");
                
            }

            File file = new File(val);

            /*
             * Setup the buffer strategy.
             */

            FileMetadata fileMetadata = new FileMetadata(segment, file,
                    BufferMode.Direct, initialExtent, slotSize,
                    objectIndexSize, create, readOnly, forceWrites);

            _bufferStrategy = new DirectBufferStrategy(fileMetadata, slotMath);

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

            FileMetadata fileMetadata = new FileMetadata(segment, file,
                    BufferMode.Mapped, initialExtent, slotSize,
                    objectIndexSize, create, readOnly, forceWrites);

            _bufferStrategy = new MappedBufferStrategy(fileMetadata, slotMath);

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

            FileMetadata fileMetadata = new FileMetadata(segment, file,
                    BufferMode.Disk, initialExtent, slotSize, objectIndexSize,
                    create, readOnly, forceWrites);
            
            _bufferStrategy = new DiskOnlyStrategy(fileMetadata, slotMath);

            this._rootBlock = fileMetadata.rootBlock;
            
            break;
        
        }
        
        default:
            
            throw new AssertionError();
        
        }
        
        /*
         * An index of the free and used slots in the journal.
         * 
         * FIXME For any of the file-backed modes we need to read the slot
         * allocation index and the object index from the file.
         * 
         * FIXME This needs to be refactored to be a persistent data structure.
         * 
         * FIXME We need to set _nextSlot based on the persistent slot
         * allocation index chain.
         */
        allocationIndex = new SimpleSlotAllocationIndex(slotMath,
                _bufferStrategy.getSlotLimit());

        /*
         * FIXME Change this to use the persistence capable object index.
         */
        objectIndex = new SimpleObjectIndex(allocationIndex);
        
        /*
         * Initialize the conflict resolver.
         */
        
        if( conflictResolverClass != null ) {

            try {

                Constructor ctor = conflictResolverClass
                        .getConstructor(new Class[] { Journal.class });

                this.conflictResolver = (IConflictResolver) ctor
                        .newInstance(new Object[] { this });
                
            }

            catch (Exception ex) {

                throw new RuntimeException("Conflict resolver: " + ex, ex);

            }
            
        } else {
            
            /*
             * The journal will not attempt to resolve write-write conflicts.
             */
            
            this.conflictResolver = null;
            
        }

//        /*
//         * FIXME This is NOT restart safe. We need to store the state on the
//         * journal and cache the location of the state in the root block.
//         */
//
//        _extSer = ExtensibleSerializer.createInstance(this);
        
    }
    
    /**
     * Shutdown the journal.
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
     *       survivability of prepared transactions across restart.
     */
    public void close() {
        
        assertOpen();

        final int nactive = activeTx.size();
        
        if( nactive > 0 ) {
            
            throw new IllegalStateException("There are "+nactive+" active transactions");
            
        }
        
        final int nprepare = preparedTx.size();
        
        if( nprepare > 0 ) {
            
            throw new IllegalStateException("There are "+nprepare+" prepared transactions.");
            
        }
        
        _bufferStrategy.close();
        
        if( deleteOnClose ) {
            
            /*
             * This option is used by the test suite and MUST NOT be used with
             * live data.
             */
            
            _bufferStrategy.deleteFile();
            
        }
        
    }
    
    private void assertOpen() {
        
        if( ! _bufferStrategy.isOpen() ) {

            throw new IllegalStateException();

        }
        
    }
    
    public boolean isOpen() {
        
        return _bufferStrategy.isOpen();
        
    }
    
    /**
     * <p>
     * Extend the journal by at least this many bytes. The actual number of
     * bytes for the extension is choosen based on a growth policy for the
     * journal.
     * </p>
     * <p>
     * Note: This method is invoked automatically when the journal is out of
     * space during a write.
     * </p>
     * 
     * @param minBytes
     *            The minimum #of bytes to extend the journal.
     * 
     * FIXME Implement the ability to extend the journal. The
     * {@link #_bufferStrategy} and {@link #allocationIndex} are both effected
     * by this operation. If the new extent would exceed the limits of the
     * current strategy, then this operation requires changing to a
     * {@link BufferMode#Disk} buffer strategy.
     * 
     * FIXME Implement the ability to transparently change the buffer mode. For
     * example, the journal comes only using a disk-only mode (paged, low memory
     * burden) but access starts to heat up. We need to be able to convert
     * access to direct without forcing restart of the journal. There could be a
     * similar conversion to a special "transient" mode in which redundent
     * servers provide restart safety for even high performance. Likewise, we
     * should be able to reduce the resource consumption of the journal at any
     * time by converting from direct to disk-only.
     */
    public void extend( int minBytes ) {
        
        throw new UnsupportedOperationException();
        
    }
    
    /**
     * Insert the data into the store and associate it with the persistent
     * identifier from which the data can be retrieved (non-transactional).
     */
    public void write(int id,ByteBuffer data) {

        if( id <= 0 ) throw new IllegalArgumentException();
        
        assertOpen();

        /*
         * Write the data onto the journal and obtain the slots onto which the
         * data was written.
         */
        ISlotAllocation slots = write(data);

        /*
         * Update the object index so that the current data version is mapped
         * onto the slots on which the data was just written.
         */

        objectIndex.put(id, slots);

    }
    
    /**
     * Write the data on the journal. This method is not isolated and does not
     * update the object index. It operates directly on the slots in the journal
     * and returns a {@link ISlotAllocation} that may be used to recover the
     * data.
     * 
     * @param data
     *            The data. The bytes from the current position to the limit
     *            (exclusive) will be written onto the journal. The position
     *            will be updated as a side effect. An attempt will be made to
     *            write the data onto a contiguous run of slots.
     * 
     * @return A {@link ISlotAllocation} representing the slots on which the
     *         data was written. This may be used to read the data back from the
     *         journal.
     * 
     * @todo Since we try hard to use contiguous slot runs, that has
     *       implications for the best way to encode the {@link ISlotAllocation}.
     *       If we require contiguous runs, then all we need is the firstSlot
     *       and the size (in bytes) of the allocation (4 bytes tops).
     * 
     * FIXME WRITE and READ with contiguous slot allocations should bulk get /
     * put all the data at once rather than performing one operation per slot.
     */
    public ISlotAllocation write(ByteBuffer data) {
        
        assertOpen();

        if( data == null ) {
            
            throw new IllegalArgumentException("data is null");
            
        }
        
        // #of bytes of data that fit in a single slot. @todo move onto the journal.
        final int slotSize = slotMath.slotSize;

        // #of bytes to be written.
        int remaining = data.remaining();
        
        /*
         * Verify that there is some data to be written. While nothing really
         * prevents writing empty records, this is a useful test for "gotcha's"
         * when the caller has not setup the buffer correctly for the write.
         */
        if( remaining == 0 ) {
            
            throw new IllegalArgumentException(
                    "No bytes remaining in data buffer.");
            
        }
        
        // Total size of the data in bytes.
        final int nbytes = remaining;

        /*
         * Obtain an allocation of slots sufficient to write this many bytes
         * onto the journal.
         */
        ISlotAllocation slots = allocationIndex.alloc( nbytes );
        
        if( slots == null ) {
            
            extend( nbytes );
            
            slots = allocationIndex.alloc(nbytes);
            
            if( slots == null ) {
                
                /*
                 * There should always be enough space after an extension.
                 */
                
                throw new AssertionError();
                
            }
            
        }

        // current position on source.
        int pos = data.position();
        
        // starting position -- used to test post-conditions.
        final int startingPosition = pos;

        /*
         * Write data onto the allocated slots.
         */
        for( int slot=slots.firstSlot(); slot != -1; slot=slots.nextSlot() ) {

            assert(allocationIndex.isAllocated(slot));

            // #of bytes to write onto this slot.
            final int thisCopy = remaining > slotSize ? slotSize : remaining; 
            
            // Set limit on data to be written on the slot.
            data.limit( pos + thisCopy );

            // write data on the slot.
            _bufferStrategy.writeSlot(slot, data);

            // Update #of bytes remaining in data.
            remaining -= thisCopy;

            // Update the position.
            pos += thisCopy;

            // post-condition tests.
            assert pos == data.position();
            assert data.position() == data.limit();
                        
        }

        assert pos == startingPosition + nbytes;
        assert data.position() == pos;
        assert data.limit() == pos;

        // The slots on which the data were written.
        return slots;
        
    }
    
    /**
     * <p>
     * Read the data from the store (non-transactional).
     * </p>
     */
    public ByteBuffer read(int id, ByteBuffer dst ) {

        if( id <= 0 ) throw new IllegalArgumentException();

        assertOpen();
        
        ISlotAllocation slots = objectIndex.get(id);
        
        if( slots == null ) return null;
        
        return read( slots, dst );
        
    }
    
    /**
     * Reads data from the slot allocation in sequence, assembling the result in
     * a buffer. This method is not isolated.
     * 
     * @param slots
     *            The slots whose data will be read.
     * @param dst
     *            The destination buffer (optional). When specified, the data
     *            will be appended starting at the current position. If there is
     *            not enough room in the buffer then a new buffer will be
     *            allocated and used for the read operation. In either case, the
     *            position will be advanced as a side effect and the limit will
     *            equal the final position.
     * 
     * @return The data read from those slots. A new buffer will be allocated if
     *         <i>dst</i> is <code>null</code> -or- if the data will not fit
     *         in the provided buffer.
     * 
     * FIXME WRITE and READ with contiguous slot allocations should bulk get /
     * put all the data at once rather than performing one operation per slot.
     * The code for this partly exists as readSlice() and writeSlice() on
     * {@link BasicBufferStrategy}. Refactor those methods into the
     * {@link IBufferStrategy} interface and deprecate the readSlot() and
     * writeSlot() methods (which could provide alternative implementations of
     * the slots methods based on the read() and write() methods in the Journal
     * class if we allowed non-contiguous allocations).
     * 
     * FIXME {@link SimpleSlotAllocationIndex} is WAY TO SLOW.
     */
    public ByteBuffer read(ISlotAllocation slots, ByteBuffer dst) {

        assertOpen();

        // #of bytes in a slot.
        final int slotSize = slotMath.slotSize;
        
        // The #of bytes to be read.
        final int nbytes = slots.getByteCount();
        
        /*
         * The starting position on the destination buffer.
         */
        final int startingPosition;
        
        /*
         * Verify that the destination buffer exists and has sufficient
         * remaining capacity.
         */
        if (dst == null || dst.remaining() < nbytes) {

//            if( _bufferStrategy instanceof BasicBufferStrategy ) {
//                
//                /*
//                 * For direct buffered modes (including "transient", "direct",
//                 * and "mapped"), we just return a read-only slice of the
//                 * buffer.
//                 * 
//                 * Note: This relies on the assumption that slot allocations are
//                 * contiguous.
//                 */
//                
//                return ((BasicBufferStrategy)_bufferStrategy).getSlice(slots);
//                
//            } else {

                /*
                 * Allocate a destination buffer to size.
                 * 
                 * @todo Allocate from a pool?
                 */
            
                dst = ByteBuffer.allocate(nbytes);
            
//            }
            
            startingPosition = 0;
            
        } else {
            
            startingPosition = dst.position();
            
        }
        
        // Set the limit on the copy operation.
        dst.limit( startingPosition + nbytes );

        /*
         * Read the data into the buffer.
         */

        int slotsRead = 0;
        
        int remaining = nbytes;
        
        int pos = startingPosition;
        
        for( int slot=slots.firstSlot(); slot != -1; slot = slots.nextSlot() ) {

            // Verify that this slot has been allocated.
            assert( allocationIndex.isAllocated(slot) );

            /*
             * In this operation we copy no more than the remaining bytes and no
             * more than the data available in the slot.
             */
            final int thisCopy = remaining > slotSize ? slotSize : remaining; 

            dst.limit(pos + thisCopy);
            
            // Append the data into the buffer.
            _bufferStrategy.readSlot(slot, dst);

            pos += thisCopy;
            
            remaining -= thisCopy;
            
            slotsRead++;
            
        }
        
        if (dst.limit() - startingPosition != nbytes) {

            throw new AssertionError("Expected to read " + nbytes + " bytes in "
                    + slots.getSlotCount() + " slots, but read=" + dst.limit()
                    + " bytes over " + slotsRead + " slots");
            
        }

        dst.position( startingPosition );
        
        return dst;
        
    }

    /**
     * Delete the data from the store (non-transactional).
     * 
     * @param id
     *            The int32 within-segment persistent identifier.
     * 
     * @exception DataDeletedException
     *                if the persistent identifier is already deleted.
     * 
     * @todo This notion of deleting probably interfers with the ability to
     *       latter reuse the same persistent identifier for new data. Explore
     *       the scope of such interference and its impact on the read-optimized
     *       database. Can we get into a situation with persistent identifier
     *       exhaustion for some logical page? {@link Tx#delete(int)} does
     *       basically the same thing.
     */
    public void delete(int id) {

        if( id <= 0 ) throw new IllegalArgumentException();

        assertOpen();

        // No isolation.
        objectIndex.delete(id );
            
    }

    /**
     * Immediately deallocates the slot allocation (no isolation).
     * 
     * @param slots
     *            The slot allocation.
     */
    public void delete(ISlotAllocation slots) {

        allocationIndex.clear(slots);
        
    }
    
    /**
     * Writes the new data on the journal and deallocates the old data. This
     * low-level method is NOT isolated and SHOULD NOT be used for general
     * purpose applications. The change is NOT restart safe unless (a) the new
     * allocation is reachable from the root block; and (b) you write a new root
     * block that can access the new allocation.
     * 
     * @param oldSlots
     *            The old slots (required). The caller MUST take care that this
     *            is in fact the slots on which the current version of the data
     *            is written.
     * @param data
     *            The new version of the data (required).
     * 
     * @return The slots on which the new version of the data is written.
     * 
     * @todo The slots deallocated by this method need to be with held from
     *       reallocation until the next commit or there is the danger that the
     *       contents of those slots may have changed while the slots are not
     *       reachable if the restart occurs before the next root block is
     *       written. This might be automagically handled by multi-state values
     *       for the slot allocation index entries (deallocated, vs allocated,
     *       vs allocated+commited, vs conditionally-deallocated).
     * 
     * @deprecated Is this method required?
     */
    public ISlotAllocation update(ISlotAllocation oldSlots,ByteBuffer data) {
        
        ISlotAllocation newSlots = write(data);
        
        allocationIndex.clear(oldSlots);
        
        return newSlots;
        
    }

//    /*
//     * Low-level interface for non-isolated reads, writes of objects (vs
//     * ByteBuffer's) using the extensible serialization API.
//     */
//    
//    /**
//     * Write an object on the store (no isolation). This method does NOT use the
//     * object index. The caller MUST explicitly manage the store. Normally this
//     * consists of ensuring that the object is recoverable from metadata in a
//     * root block or a commit record. For example, the {@link IObjectIndex} uses
//     * this method to store its nodes. The root node of the current object index
//     * is stored in the commit record.
//     * 
//     * @param obj
//     *            The object to be inserted into the store (required). The
//     *            object will be serialized using the extSer package.
//     * 
//     * @return The <em>journal local</em> persistent identifier assigned to
//     *         the object. This is simply a compact encoding of the
//     *         {@link ISlotAllocation} on which the object is written.
//     */
//    public long _insertObject(Object obj) {
//
//        if( obj == null ) throw new IllegalArgumentException();
//
//        try {
//
//            return write(
//                ByteBuffer.wrap(getExtensibleSerializer()
//                .serialize(0, obj))).toLong();
//            
//        }
//        
//        catch(IOException ex ) {
//
//            throw new RuntimeException(ex);
//            
//        }
//        
//    }
//    
//    /**
//     * Fetch an object from the store (no isolation). This operation is
//     * unchecked. As long as the identifier decodes into a
//     * {@link ISlotAllocation} the contents of that slot allocation will be read
//     * into a buffer and an attempt will be made to deserialize the buffer into
//     * an object using the extSer package.
//     * 
//     * @param id
//     *            The <em>journal local</em> persistent identifier for the
//     *            object.
//     * 
//     * @return The object.
//     */
//    public Object _readObject( long id ) {
//    
//        ByteBuffer tmp = read( slotMath.toSlots(id), null );
//
//        try {
//
//            return getExtensibleSerializer().deserialize(id, tmp.array() );
//            
//        }
//        
//        catch(IOException ex ) {
//
//            throw new RuntimeException(ex);
//            
//        }
//
//    }
//    
//    /**
//     * Update an object in the store (no isolation). The object will serialized
//     * using the extSer package and the resulting byte[] will be written onto a
//     * new {@link ISlotAllocation}. That allocation will be converted into a
//     * long integer by {@link SlotMath#toLong(ISlotAllocation)} and the
//     * resulting long integer value is returned to the caller.
//     * 
//     * @param id
//     *            The <em>journal local</em> persistent identifier for the
//     *            current version of the object. The slots allocated to the
//     *            object will be deallocated, releasing the space on the store.
//     *            (This deallocation operation is unchecked. As long as the
//     *            identifier decodes into a {@link ISlotAllocation}, the slot
//     *            allocation will be deallocated.)
//     * 
//     * @param obj
//     *            The object to be written (required).
//     * 
//     * @return The <em>new journal local</em> persistent identifier for that
//     *         object. The new identifier is NOT the same as the old identifier.
//     *         The old identifier MUST now be considered to be invalid. Further
//     *         reads on the old identifier are STRONGLY DISCOURAGED but
//     *         nevertheless unchecked.
//     */
//    public long _updateObject(long id, Object obj) {
//
//        if( obj == null ) throw new IllegalArgumentException();
//        
//        try {
//
//            return update(
//                    slotMath.toSlots(id),
//                    ByteBuffer
//                            .wrap(getExtensibleSerializer().serialize(0, obj)))
//                    .toLong();
//
//        }
//
//        catch (IOException ex) {
//
//            throw new RuntimeException(ex);
//
//        }
//
//    }
//
//    /**
//     * Delete an object from the store (no isolation). The slots allocated to
//     * the object will be deallocated, releasing the space on the store. (This
//     * deallocation operation is unchecked. As long as the identifier decodes
//     * into a {@link ISlotAllocation}, the slot allocation will be
//     * deallocated.)
//     * 
//     * @param id
//     *            The <em>journal local</em> persistent identifier of the
//     *            object.
//     */
//    public void _deleteObject( long id )
//    {
//
//        delete( slotMath.toSlots(id) );
//        
//    }

    /*
     * commit processing support.
     */
    
    /**
     * Return the first commit record whose transaction timestamp is less than
     * or equal to the given timestamp.
     * 
     * @param timestamp
     *            The timestamp.
     * 
     * @param exact
     *            When true, only an exact match is accepted.
     * 
     * @return The commit record.
     * 
     * @todo Define ICommitRecord.
     * 
     * @todo A transaction may not begin if a transaction with a later timestamp
     *       has already committed. Use this method to verify that. Since the
     *       commit records are cached, this probably needs to be moved back
     *       into the Journal.
     * 
     * @todo Read the commit records into memory on startup.
     * 
     * @todo The commit records are chained together by a "pointer" to the prior
     *       commit record together with the timestamp of that record. The root
     *       block contains the timestamp of the earliest commit record that is
     *       still on the journal. Traversal of the prior pointers stops when
     *       the referenced record has a timestamp before the earliest versions
     *       still on hand.
     */
    protected Object getCommitRecord(long timestamp, boolean exact) {
 
        /*
         * @todo Search backwards from the current {@link IRootBlockView}.
         */
        throw new UnsupportedOperationException();
        
    }

    /**
     * FIXME Write commit record, including: the transaction identifier, the
     * location of the object index and the slot allocation index, the location
     * of a run length encoded representation of slots allocated by this
     * transaction, and the identifier and location of the prior transaction
     * serialized on this journal.
     */
    protected void writeCommitRecord(IStore tx) {
        
    }

    /**
     * Return a read-only view of the current root block.
     * 
     * @return The current root block.
     * 
     * @todo Currently [null] for the transient buffer mode.
     */
    public IRootBlockView getRootBlockView() {

        return _rootBlock;
        
    }
    IRootBlockView _rootBlock;

    /**
     * FIXME Write a new root block. This provides atomic commit for a
     * transaction. After this method has been invoked the intention of the
     * transaction is persistent the journal. This is achieved by updating the
     * root of the object index, which is stored as part of the root block.
     * 
     * @todo Where is the divide between PREPARE and COMMIT? What do we do with
     *       PREPARE on restart?
     */
    public void writeRootBlock() {
        
        final IRootBlockView old = getRootBlockView();
        
        // @todo currently null for the transient buffer mode.
        if( old != null ) {

            // FIXME Need to update the slot allocation chain root.
            // FIXME Need to update the object index root.
            //
            // Those data are probably from the transaction.  Does it make sense
            // to do this without isolation?
            //
            // @todo A clone constructor with some overrides might make a lot of
            // sense here, especially as we add metadata to the root block.
            
            /*
             * @todo The notional use case for named roots is storing things
             * such as the root of the named object map or the state of an
             * extser instance for an embedded database. In order to do that,
             * the array needs to be made visible one layer up so that the root
             * ids may be modified before they are set on the new root block.
             */ 
            int[] rootIds = old.getRootIds();
            
            IRootBlockView newRootBlock = new RootBlockView(
                    !old.isRootBlock0(), old.getSegmentId(), old.getSlotSize(),
                    old.getSlotLimit(), old.getObjectIndexSize(), old
                            .getSlotIndexChainHead(), old.getObjectIndexRoot(),
                    old.getCommitCounter() + 1, rootIds);

            _bufferStrategy.writeRootBlock(newRootBlock, forceOnCommit);
            
        }
        
    }

    /**
     * @todo This is a notional non-transactional commit. It just writes a new
     *       root block.
     */
    public void commit() {
        
        writeRootBlock();
        
    }
    
//    /**
//     * FIXME Make this more flexible in terms of a service vs a static instance
//     * (for journal only to support the object index) vs true extensibility (for
//     * an embedded database).
//     * 
//     * @return
//     */
//    public ExtensibleSerializer getExtensibleSerializer() {
//        
//        return _extSer;
//        
//    }
//    
//    // FIXME This is NOT restart safe :-)
//    final private ExtensibleSerializer _extSer;

}
