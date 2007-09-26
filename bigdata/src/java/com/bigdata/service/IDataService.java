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
package com.bigdata.service;

import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IKeyBuffer;
import com.bigdata.btree.IValueBuffer;
import com.bigdata.btree.BytesUtil.UnsignedByteArrayComparator;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.ITransactionManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.ITxCommitProtocol;
import com.bigdata.scaleup.JournalMetadata;

/**
 * Data service interface.
 * <p>
 * The data service exposes the methods on this interface to the client and the
 * {@link ITxCommitProtocol} methods to the {@link ITransactionManager} service.
 * <p>
 * The data service exposes both isolated (transactional) and unisolated batch
 * operations on scalable named btrees. Transactions are identified by their
 * start time. BTrees are identified by name. The btree batch API provides for
 * existence testing, lookup, insert, removal, and an extensible mutation
 * operation. Other operations exposed by this interface include: remote
 * procedure execution, key range traversal, and mapping of an operator over a
 * key range.
 * <p>
 * Unisolated processing is broken down into idempotent operation (reads) and
 * mutation operations (insert, remove, the extensible batch operator, and
 * remote procedure execution).
 * <p>
 * Unisolated writes are serialized and ACID. If an unisolated write succeeds,
 * then it will commit immediately. If the unisolated write fails, the partial
 * write on the journal will be discarded.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo add support for triggers. unisolated triggers must be asynchronous if
 *       they will take actions with high latency (such as writing on a
 *       different index partition, which could be remote). Low latency actions
 *       might include emitting asynchronous messages. transactional triggers
 *       can have more flexibility since they are under less of a latency
 *       constraint.
 * 
 * @todo add protocol / service version information to this interface and
 *       provide for life switch-over from service version to service version so
 *       that you can update or rollback the installed service versions with
 *       100% uptime.
 */
public interface IDataService extends IRemoteTxCommitProtocol {

    /**
     * A constant that may be used as the transaction identifier when the
     * operation is <em>unisolated</em> (non-transactional).  The value of
     * this constant is ZERO (0L).
     */
    public static final long UNISOLATED = ITx.UNISOLATED;
    
    /**
     * A constant that may be used as the partition identifier when the target
     * index is not partitioned (-1).
     */
    public static final int UNPARTITIONED = -1;
    
    /**
     * Flag specifies that keys in the key range will be returned. When not
     * given, the keys will NOT be included in the {@link ResultSet} sent to the
     * client.
     */
    public static final int KEYS = 1 << 0;

    /**
     * Flag specifies that values in the key range will be returned. When not
     * given, the values will NOT be included in the {@link ResultSet} sent to
     * the client.
     */
    public static final int VALS = 1 << 1;
    
    /**
     * A description of the journal currently backing the data service.
     * 
     * @throws IOException
     */
    public JournalMetadata getJournalMetadata() throws IOException;
    
    /**
     * Register a named mutable B+Tree on the {@link DataService} (unisolated).
     * The keys will be variable length unsigned byte[]s. The values will be
     * variable length byte[]s. The B+Tree will support version counters and
     * delete markers (it will be compatible with the use of transactions for
     * concurrency control).
     * 
     * @param name
     *            The name that can be used to recover the index. In order to
     *            create a partition of an index you must form the name of the
     *            index partition using
     *            {@link DataService#getIndexPartitionName(String, int)} (this
     *            operation is generally performed by the
     *            {@link IMetadataService} which manages scale-out indices).
     * 
     * @param indexUUID
     *            The UUID that identifies the index. When the mutable B+Tree is
     *            part of a scale-out index, then you MUST provide the indexUUID
     *            for that scale-out index. Otherwise this MUST be a random
     *            UUID, e.g., using {@link UUID#randomUUID()}.
     * 
     * @param className
     *            The name of the implementation class for the index (must
     *            extend {@link BTree}). This MUST be formed using
     *            {@link Class#getName()}. Normally this is
     *            {@link UnisolatedBTree} for an unpartitioned index and
     *            {@link UnisolatedBTreePartition} for a partitioned index.
     * 
     * @param config
     *            A serializable object containing configuration state that will
     *            be used to initialize the index. For a partitioned index, this
     *            will include the partition metadata for that index partition.
     * 
     * @return The object that would be returned by {@link #getIndex(String)}.
     * 
     * @todo exception if index exists? or modify to validate consistent decl
     *       and exception iff not consistent.
     * 
     * @todo provide configuration options {whether the index supports isolation
     *       or not ({@link BTree} vs {@link UnisolatedBTree}), the branching
     *       factor for the index, and the value serializer. For a client server
     *       divide I think that we can always go with an
     *       {@link UnisolatedBTree}. We should pass in the UUID so that this
     *       can be used by the {@link MetadataService} to create mutable btrees
     *       to absorb writes when one or more partitions of a scale out index
     *       are mapped onto the {@link DataService}.
     */
    public void registerIndex(String name, UUID uuid, String className,
            Object config) throws IOException, InterruptedException,
            ExecutionException;

    /**
     * Return the unique index identifier for the named index (synchronous,
     * unisolated).
     * 
     * @param name
     *            The index name.
     *            
     * @return The index UUID -or- <code>null</code> if the index is not
     *         registered on this {@link IDataService}.
     *         
     * @throws IOException
     */
    public UUID getIndexUUID(String name) throws IOException;

    /**
     * Drops the named index (unisolated).
     * 
     * @param name
     *            The index name. In order to drop a partition of an index you
     *            must form the name of the index partition using
     *            {@link DataService#getIndexPartitionName(String, int)} (this
     *            operation is generally performed by the
     *            {@link IMetadataService} which manages scale-out indices).
     * 
     * @exception IllegalArgumentException
     *                if <i>name</i> does not identify a registered index.
     */
    public void dropIndex(String name) throws IOException,
            InterruptedException, ExecutionException;

//    /**
//     * Maps a <em>new</em> index partition onto the data service. This method
//     * must be invoked before a client will be permitted to access the key range
//     * for the index partition on the data service.
//     * 
//     * @param name
//     *            The name of the scale-out index.
//     * @param pmd
//     *            The partition metadata.
//     * 
//     * @see #unmapPartition(String, int)
//     * 
//     * @todo When a new partition is created or destroyed that is a sibling of
//     *       this partition then the data service needs to be notified so that
//     *       it will update the partition definition.
//     */
//    public void mapPartition(String name, PartitionMetadataWithSeparatorKeys pmd)
//            throws IOException, InterruptedException, ExecutionException;
//    
//    /**
//     * Unmaps an index partition from the data service. This method is invoked
//     * under two circumstances: (1) the index partition is being deleted
//     * following a join of two index partitions; and (2) the index partition is
//     * being shed by the data service.
//     * 
//     * @param name
//     *            The index name.
//     * @param partitionId
//     *            The partition identifier (must be zero for an unpartitioned
//     *            index).
//     * 
//     * @todo Review state machine for unmapping an index partition for (a) an
//     *       index partition join; and (b) shedding an index partition (it must
//     *       be picked up by a different data service).
//     */
//    public void unmapPartition(String name, int partitionId)
//            throws IOException, InterruptedException, ExecutionException;
    
    /**
     * <p>
     * Used by the client to submit a batch operation on a named B+Tree
     * (synchronous).
     * </p>
     * <p>
     * Unisolated operations SHOULD be used to achieve "auto-commit" semantics.
     * Fully isolated transactions are useful IFF multiple operations must be
     * composed into a ACID unit.
     * </p>
     * <p>
     * While unisolated batch operations on a single data service are ACID,
     * clients are required to locate all index partitions for the logical
     * operation and distribute their operation across the distinct data service
     * instances holding the affected index partitions. In practice, this means
     * that contract for ACID unisolated operations is limited to operations
     * where the data is located on a single data service instance. For ACID
     * operations that cross multiple data service instances the client MUST use
     * a fully isolated transaction. While read-committed transactions impose
     * low system overhead, clients interested in the higher possible total
     * throughput SHOULD choose unisolated read operations in preference to a
     * read-committed transaction.
     * </p>
     * <p>
     * This method is thread-safe. It will block for each operation. It should
     * be invoked within a pool request handler threads servicing a network
     * interface and thereby decoupling data service operations from client
     * requests. When using as part of an embedded database, the client
     * operations MUST be buffered by a thread pool with a FIFO policy so that
     * client requests may be decoupled from data service operations.
     * </p>
     * 
     * @param tx
     *            The transaction identifier -or- zero (0L) IFF the operation is
     *            NOT isolated by a transaction.
     * @param name
     *            The index name (required).
     * @param partitionId
     *            The partition identifier (must be -1 for an unpartitioned
     *            index).
     * @param ntuples
     *            The #of items in the batch operation.
     * @param keys
     *            The keys for the batch operation (must be in ascending order
     *            when the keys are interpreted as unsigned byte[]s, e.g., using
     *            an {@link UnsignedByteArrayComparator}).
     * 
     * @exception IOException
     *                if there was a problem with the RPC.
     * @exception InterruptedException
     *                if the operation was interrupted (typically by
     *                {@link #shutdownNow()}.
     * @exception ExecutionException
     *                If the operation caused an error. See
     *                {@link ExecutionException#getCause()} for the underlying
     *                error.
     * 
     * @todo modify to use {@link IKeyBuffer} and {@link IValueBuffer} so that
     *       we can get good compression for the wire. Note that on the wire
     *       compression could be different from in the index compression and
     *       could take advantage of application knowledge.
     *       
     * @todo javadoc update.
     * 
     * @todo support extension operations (read or mutable).
     */
    public byte[][] batchInsert(long tx, String name, int partitionId,
            int ntuples, byte[][] keys, byte[][] values, boolean returnOldValues)
            throws InterruptedException, ExecutionException, IOException;

    public boolean[] batchContains(long tx, String name, int partitionId,
            int ntuples, byte[][] keys) throws InterruptedException,
            ExecutionException, IOException;

    public byte[][] batchLookup(long tx, String name, int partitionId,
            int ntuples, byte[][] keys) throws InterruptedException,
            ExecutionException, IOException;

    public byte[][] batchRemove(long tx, String name, int partitionId,
            int ntuples, byte[][] keys, boolean returnOldValues)
            throws InterruptedException, ExecutionException, IOException;

    /**
     * <p>
     * Streaming traversal of keys and/or values in a given key range.
     * </p>
     * <p>
     * In order to visit all keys in a range, clients are expected to issue
     * repeated calls in which the <i>fromKey</i> is incremented to the
     * successor of the last key visited until either an empty {@link ResultSet}
     * is returned or the {@link ResultSet#isLast()} flag is set, indicating
     * that all keys up to (but not including) the <i>startKey</i> have been
     * visited.
     * </p>
     * <p>
     * Note: Both the {@link ClientIndexView} (scale-out indicex) and
     * {@link RangeQuery} (unpartitioned indices) provide iterators that
     * encapsulate this method.
     * </p>
     * 
     * @param tx
     *            The transaction identifier -or- zero (0L) IFF the operation is
     *            NOT isolated by a transaction.
     * @param name
     *            The index name (required).
     * @param partitionId
     *            The partition identifier (must be -1 for an unpartitioned
     *            index).
     * @param fromKey
     *            The starting key for the scan.
     * @param toKey
     *            The first key that will not be visited.
     * @param capacity
     *            The maximum #of key-values to return.
     * @param flags
     *            One or more flags formed by bitwise OR of zero or more of the
     *            constants {@link #KEYS} and {@link #VALS}.
     * 
     * @exception InterruptedException
     *                if the operation was interrupted (typically by
     *                {@link #shutdownNow()}.
     * @exception ExecutionException
     *                If the operation caused an error. See
     *                {@link ExecutionException#getCause()} for the underlying
     *                error.
     * 
     * @todo The capacity must be rounded up if necessary in order to all values
     *       selected for a single row of a sparse row store.
     * 
     * @todo provide for optional filter.
     */
    public ResultSet rangeQuery(long tx, String name, int partitionId,
            byte[] fromKey, byte[] toKey, int capacity, int flags)
            throws InterruptedException, ExecutionException, IOException;
    
    /**
     * <p>
     * Range count of entries in a key range for the named index on this
     * {@link DataService}.
     * </p>
     * 
     * @param tx
     *            The transaction identifier -or- zero (0L) IFF the operation is
     *            NOT isolated by a transaction.
     * @param name
     *            The index name (required).
     * @param partitionId
     *            The partition identifier (must be -1 for an unpartitioned
     *            index).
     * @param fromKey
     *            The starting key for the scan.
     * @param toKey
     *            The first key that will not be visited.
     * 
     * @return The upper bound estimate of the #of key-value pairs in the key
     *         range of the partition(s) of the named index found on this
     *         {@link DataService}.
     * 
     * @exception InterruptedException
     *                if the operation was interrupted (typically by
     *                {@link #shutdownNow()}.
     * @exception ExecutionException
     *                If the operation caused an error. See
     *                {@link ExecutionException#getCause()} for the underlying
     *                error.
     */
    public int rangeCount(long tx, String name, int partitionId,
            byte[] fromKey, byte[] toKey) throws InterruptedException,
            ExecutionException, IOException;
        
    /**
     * <p>
     * Submit a procedure.
     * </p>
     * <p>
     * Unisolated operations SHOULD be used to achieve "auto-commit" semantics.
     * Fully isolated transactions are useful IFF multiple operations must be
     * composed into a ACID unit.
     * </p>
     * <p>
     * While unisolated batch operations on a single data service are ACID,
     * clients are required to locate all index partitions for the logical
     * operation and distribute their operation across the distinct data service
     * instances holding the affected index partitions. In practice, this means
     * that contract for ACID unisolated operations is limited to operations
     * where the data is located on a single data service instance. For ACID
     * operations that cross multiple data service instances the client MUST use
     * a fully isolated transaction. While read-committed transactions impose
     * low system overhead, clients interested in the higher possible total
     * throughput SHOULD choose unisolated read operations in preference to a
     * read-committed transaction.
     * </p>
     * 
     * @param tx
     *            The transaction identifier -or- zero (0L) IFF the operation is
     *            NOT isolated by a transaction.
     * @param name
     *            The name of the scale-out index.
     * @param partitionId
     *            The partition identifier (must be -1 for an unpartitioned
     *            index).
     * @param proc
     *            The procedure to be executed. This MUST be downloadable code
     *            since it will be executed on the {@link DataService}.
     * 
     * @return The result, which is entirely defined by the procedure
     *         implementation and which MAY be null. In general, this MUST be
     *         {@link Serializable} since it may have to pass across a network
     *         interface.
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public Object submit(long tx, String name, int partitionId, IProcedure proc) throws InterruptedException,
            ExecutionException, IOException;

}
