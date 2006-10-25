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
 * <li> Transaction isolation. (Also, will there be a "non-transactional mode"?)
 * The API in {@link Journal} should probably be modified to use "Tx" objects
 * rather than just long ids for transactions.</li>
 * <li> Commit protocol, including plan for PREPARE in support of distributed
 * transactions.</li>
 * <li> Migration of data to a read-optimized database (have to implement the
 * read-optimized database as well).</li>
 * <li>Support primary key indices in GPO/PO layer.</li>
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
 * FIXME Does the Journal need to have a concept of "named root objects?" How is
 * that concept supported by bigdata? Per segment? Overall? Both?
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
 * to data migration or overwrite.  However, note that overwrite tends to cause
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
 * @todo Refactor btree support for journals.
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
 */

public class Journal {

    final static long DEFAULT_INITIAL_EXTENT = 10 * Bytes.megabyte;
    
    final SlotMath slotMath;
    
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
     * The delegate that implements the {@link BufferMode}.
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
    final SimpleObjectIndex objectIndex = new SimpleObjectIndex();
    
    /**
     * Indicates the last slot in a chain of slots representing a data version
     * (-1).
     * 
     * @see SlotMath#headerSize
     * 
     * @deprecated The use of slot headers is being phased out.
     */
    public static final int LAST_SLOT_MARKER = -1;
    
    /**
     * The index of the first slot that MUST NOT be addressed ([0:slotLimit-1]
     * is the valid range of slot indices).
     * 
     * @todo Is this field required on this class?
     */
    private final int slotLimit;
    
    /**
     * The slot allocation index.
     */
    final ISlotAllocationIndex allocationIndex;

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
    final Map<Long,Tx> activeTx = new HashMap<Long,Tx>();

    /**
     * A hash map containing all transactions that have prepared but not yet
     * either committed or aborted.
     */
    final Map<Long,Tx> preparedTx = new HashMap<Long,Tx>();
    
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
    void activateTx( Tx tx ) throws IllegalStateException {
        
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
    void prepared( Tx tx ) throws IllegalStateException {
        
        Long id = tx.getId();
        
        Tx tx2 = activeTx.remove(id);
        
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
    void completedTx( Tx tx ) throws IllegalStateException {
        
        Long id = tx.getId();
        
        Tx txActive = activeTx.remove(id);
        
        Tx txPrepared = preparedTx.remove(id);
        
        if( txActive == null && txPrepared == null ) {
            
            throw new IllegalStateException("Neither active nor being prepared: tx="+tx);
            
        }
        
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
     * Asserts that the slot index is in the legal range for the journal
     * <code>[0:slotLimit)</code>
     * 
     * @param slot The slot index.
     */
    
    void assertSlot( int slot ) {
        
        if( slot>=0 && slot<slotLimit ) return;
        
        throw new AssertionError("slot=" + slot + " is not in [0:"
                + slotLimit + ")");
        
    }
    
    /**
     * Create or open a journal.
     * 
     * @param properties
     *            <dl>
     *            <dt>file</dt>
     *            <dd>The name of the file. If the file not found and "create"
     *            is true, then a new journal will be created.</dd>
     *            <dt>segment</dt>
     *            <dd>The unique segment identifier (required unless this is a
     *            transient journal). Segment identifiers are assigned by a
     *            bigdata federation. When using the journal as part of an
     *            embedded database you may safely assign an arbitrary segment
     *            identifier.</dd>
     *            <dt>slotSize</dt>
     *            <dd>The slot size in bytes.</dd>
     *            <dt>initialExtent</dt>
     *            <dd>The initial extent of the journal (bytes). The initial
     *            file size is computed by subtracting off the space required by
     *            the root blocks and dividing by the slot size.</dd>
     *            <dt>create</dt>
     *            <dd>When [create == true] and the named file is not found, a
     *            new journal will be created.</dd>
     *            <dt>bufferMode</dt>
     *            <dd>Either "transient", "direct", "mapped", or "disk". See
     *            {@link BufferMode} for more information about each mode.</dd>
     *            <dt>forceWrites</dt>
     *            <dd>When true, the journal file is opened in a mode where
     *            writes are written through to disk immediately. The use of
     *            this option is not recommended as it imposes strong
     *            performance penalties and does NOT provide any additional data
     *            safety (it is here mainly for performance testing).</dd>
     *            <dt>conflictResolver</dt>
     *            <dd>The name of a class that implements the
     *            {@link IConflictResolver} interface (optional). The class MUST
     *            define a public constructor with the signature
     *            <code><i>class</i>( Journal journal )</code>. There is NO
     *            default. State-based resolution of write-write conflicts is
     *            enabled iff a conflict resolution class is declared with this
     *            parameter.</dd>
     *            <dt>deleteOnClose</dt>
     *            <dd>This optional boolean option causes the journal file to
     *            be deleted when the journal is closed (default <em>false</em>).
     *            This option is used by the test suites to keep down the disk
     *            burden of the tests and MUST NOT be used with live data.</dd>
     *            </dl>
     * 
     * @throws IOException
     *             If there is a problem when creating, opening, or reading from
     *             the journal file.
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
        long initialExtent = DEFAULT_INITIAL_EXTENT;
        boolean readOnly = false;
        boolean forceWrites = false;
        boolean deleteOnClose = false;
        Class conflictResolverClass = null;
        String val;

        if( properties == null ) throw new IllegalArgumentException();

        /*
         * "bufferMode" mode. Note that very large journals MUST use the
         * disk-based mode.
         */

        val = properties.getProperty("bufferMode");

        if (val == null)
            val = BufferMode.Direct.toString();

        BufferMode bufferMode = BufferMode.parse(val);

        /*
         * "segment".
         */
        
        val = properties.getProperty("segment");
        
        if( val == null ) {
            
            if( bufferMode == BufferMode.Transient) {
            
            val = "0";
            
            } else {
            
                throw new RuntimeException("Required property: 'segment'");
                
            }
            
        }

        segment = Long.parseLong(val);
        
        /*
         * "slotSize"
         */

        val = properties.getProperty("slotSize");

        if (val == null) {
         
            val = "128";
            
        }

        slotSize = Integer.parseInt(val);

        final int MIN_SLOT_DATA = 32;
        final int minSlotSize = ( SlotMath.HEADER_SIZE + MIN_SLOT_DATA );
        
        if (slotSize < minSlotSize ) {

            throw new RuntimeException("slotSize must be at least "
                    + minSlotSize + " : " + slotSize);

        }

        /*
         * Helper object for slot-based computations.
         */
        
        this.slotMath = new SlotMath(slotSize);

        /*
         * "initialExtent"
         */

        val = properties.getProperty("initialExtent");

        if (val != null) {

            initialExtent = Long.parseLong(val);

            if( initialExtent <= Bytes.megabyte ) {
                
                throw new RuntimeException(
                        "The initialExtent must be at least one megabyte("
                                + Bytes.megabyte + ")");
                
            }
            
        }
        
        /*
         * "readOnly"
         */

        val = properties.getProperty("readOnly");
        
        if( val != null ) {

            readOnly = Boolean.parseBoolean(val);
            
        }

        /*
         * "forceWrites"
         */

        val = properties.getProperty("forceWrites");
        
        if( val != null ) {

            forceWrites = Boolean.parseBoolean(val);
            
        }

        /*
         * "deleteOnClose"
         */

        val = properties.getProperty("deleteOnClose");
        
        if( val != null ) {

            deleteOnClose = Boolean.parseBoolean(val);
            
        }
        
        this.deleteOnClose = deleteOnClose;

        /*
         * "conflictResolver"
         */

        val = properties.getProperty("conflictResolver");
        
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

            break;
            
        }
        
        case Direct: {

            /*
             * "file"
             */

            val = properties.getProperty("file");

            if (val == null) {
                throw new RuntimeException("Required property: 'file'");
            }

            File file = new File(val);

            /*
             * Setup the buffer strategy.
             */

            _bufferStrategy = new DirectBufferStrategy(new FileMetadata(segment,file,
                    BufferMode.Direct, initialExtent, slotSize, readOnly, forceWrites), slotMath);

            break;
        
        }

        case Mapped: {
            
            /*
             * "file"
             */

            val = properties.getProperty("file");

            if (val == null) {
                throw new RuntimeException("Required property: 'file'");
            }

            File file = new File(val);

            /*
             * Setup the buffer strategy.
             */

            _bufferStrategy = new MappedBufferStrategy(new FileMetadata(segment,file,
                    BufferMode.Mapped, initialExtent, slotSize, readOnly, forceWrites), slotMath);

            break;
            
        }
        
        case Disk: {
            /*
             * "file"
             */

            val = properties.getProperty("file");

            if (val == null) {
                throw new RuntimeException("Required property: 'file'");
            }

            File file = new File(val);

            /*
             * Setup the buffer strategy.
             */

            _bufferStrategy = new DiskOnlyStrategy(new FileMetadata(segment,file,
                    BufferMode.Disk, initialExtent, slotSize, readOnly, forceWrites), slotMath);

            break;
        
        }
        
        default:
            
            throw new AssertionError();
        
        }

        this.slotLimit = _bufferStrategy.getSlotLimit();
        
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
         * Initialize the conflict resolver.
         */
        
        if( conflictResolverClass != null ) {

            try {

                Constructor ctor = conflictResolverClass
                        .getConstructor(new Class[] { Journal.class });

                this.conflictResolver = (IConflictResolver) ctor
                        .newInstance(new Object[] { this });
                
            }
            catch(Exception ex ) {
                
                throw new RuntimeException("Conflict resolver: "+ex, ex);
                
            }
            
        } else {
            
            /*
             * The journal will not attempt to resolve write-write conflicts.
             */
            
            this.conflictResolver = null;
            
        }
        
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
     */
    public void extend( int minBytes ) {
        
        throw new UnsupportedOperationException();
        
    }
    
    /**
     * Insert the data into the store and associate it with the persistent
     * identifier from which the data can be retrieved. If there is an entry for
     * that persistent identifier within the transaction scope, then its data is
     * logically overwritten. The written version of the data will not be
     * visible outside of this transaction until the transaction is committed.
     * 
     * @param tx
     *            The transaction.
     * @param id
     *            The int32 within-segment persistent identifier.
     * @param data
     *            The data to be written. The bytes from
     *            {@link ByteBuffer#position()} to {@link ByteBuffer#limit()}
     *            will be written. The position will be advanced to the limit.
     * 
     * @exception DataDeletedException
     *                if the persistent identifier is deleted.
     * 
     * @exception IllegalArgumentException
     *                if data is null.
     * @exception IllegalArgumentException
     *                if data has no remaining bytes (this can happen if you
     *                forget to set the position and limit before calling this
     *                method).
     */

    public void write(Tx tx,int id,ByteBuffer data) {
        
        assertOpen();

        /*
         * Write the data onto the journal and obtain the slots onto which the
         * data was written.
         */
        ISlotAllocation slots = write( data );
        
        /*
         * Update the object index so that the current data version is mapped
         * onto the slots on which the data was just written.
         */

        if( tx != null ) {
            
            // transactional isolation.
            tx.getObjectIndex().mapIdToSlots(id, slots, allocationIndex);
            
        } else {
            
            // no isolation.
            objectIndex.mapIdToSlots(id, slots, allocationIndex);
            
        }

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

            assertSlot( slot );

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
     * Read the data from the store.
     * </p>
     * <p>
     * This resolves the data by looking up the entry in the object index. If an
     * entry is found, it records the first slot onto which a data version was
     * last written. If more than one slot was required to store the data, then
     * the slots are chained together using their headers. All such slots are
     * read into a buffer, which is then returned to the caller.
     * </p>
     * <p>
     * Note: You can use this method to read objects into a block buffer using
     * this method. When the next object will not fit into the buffer, it is
     * read anyway and time to handle the full buffer.
     * </p>
     * 
     * @param tx
     *            The transaction scope for the read request.
     * @param id
     *            The int32 within-segment persistent identifier.
     * @param dst
     *            When non-null and having sufficient bytes remaining, the data
     *            version will be read into this buffer. If null or if the
     *            buffer does not have sufficient bytes remaining, then a new
     *            (non-direct) buffer will be allocated that is right-sized for
     *            the data version, the data will be read into that buffer, and
     *            the buffer will be returned to the caller.
     * 
     * @return The data. The position will always be zero if a new buffer was
     *         allocated. Otherwise, the position will be invariant across this
     *         method. The limit - position will be the #of bytes read into the
     *         buffer, starting at the position. A <code>null</code> return
     *         indicates that the object was not found in the journal, in which
     *         case the application MUST attempt to resolve the object against
     *         the database (i.e., the object MAY have been migrated onto the
     *         database and the version logically deleted on the journal).
     * 
     * @exception IllegalArgumentException
     *                if the transaction identifier is bad.
     *                
     * @exception DataDeletedException
     *                if the current version of the identifier data has been
     *                deleted within the scope visible to the transaction. The
     *                caller MUST NOT read through to the database if the data
     *                were deleted.
     * 
     * @todo Document that tx==null implies NO isolation and modify / partition
     *       the test suite to handle both unisolated and isolated testing. The
     *       semantics on read are that you can read anything that was visible
     *       as of the last committed state (but nothing written in any still
     *       active transaction) - and unlike a transactionally isolated read,
     *       each read operation is reads against the then-current last
     *       committed state.<br>
     *       The semantics on write is that the object data version is replaced.
     *       Unless the 'native transaction counter' is positive, a commit will
     *       take place immediately.<br>
     *       The semantics on delete are just like on write (delete is just a
     *       special case of write, even though it generally gets optimized
     *       quite a bit and triggers various cleaning behaviors).
     * 
     * @see #read(ISlotAllocation, ByteBuffer)
     */
    public ByteBuffer read(Tx tx, int id, ByteBuffer dst ) {

        assertOpen();

        ISlotAllocation slots = (tx == null ? objectIndex.getSlots(id)
              : tx.getObjectIndex().getSlots(id));
        
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

            /* Allocate a destination buffer to size.
             * 
             * @todo Allocate from a pool?
             */
            dst = ByteBuffer.allocate(nbytes);

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

            assertSlot(slot);
            
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
     * Delete the data from the store.
     * 
     * @param tx
     *            The transaction.
     * @param id
     *            The int32 within-segment persistent identifier.
     * 
     * @exception IllegalArgumentException
     *                if the transaction identifier is bad.
     * @exception DataDeletedException
     *                if the persistent identifier is already deleted.
     * 
     * @todo This notion of deleting probably interfers with the ability to
     *       latter reuse the same persistent identifier for new data. Explore
     *       the scope of such interference and its impact on the read-optimized
     *       database. Can we get into a situation with persistent identifier
     *       exhaustion for some logical page?
     */
    public void delete(Tx tx, int id) {

        assertOpen();

        if( tx == null ) {
            
            // No isolation.
            objectIndex.delete(id, allocationIndex );
            
        } else {
            
            // Transactional isolation.
            tx.getObjectIndex().delete(id, allocationIndex );
            
        }

    }
        
}
