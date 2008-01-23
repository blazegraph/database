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
package com.bigdata.service;

import java.io.IOException;
import java.io.Serializable;
import java.rmi.Remote;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.IEntryFilter;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IIndexProcedure;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.BytesUtil.UnsignedByteArrayComparator;
import com.bigdata.journal.ITransactionManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.IsolationEnum;
import com.bigdata.scaleup.IPartitionMetadata;
import com.bigdata.scaleup.JournalMetadata;
import com.bigdata.sparse.SparseRowStore;

/**
 * <p>
 * The data service interface provides remote access to named indices, provides
 * for both unisolated and isolated operations on those indices, and exposes the
 * {@link IRemoteTxCommitProtocol} interface to the {@link ITransactionManager}
 * service for the coordination of distributed transactions. Clients normally
 * write to the {@link IIndex} interface. The {@link ClientIndexView} provides
 * an implementation of that interface supporting range partitioned scale-out
 * indices which transparently handles lookup of data services in the metadata
 * index and mapping of operations across the appropriate data services.
 * </p>
 * <p>
 * Indices are identified by name. Scale-out indices are broken into index
 * partitions, each of which is a named index hosted on a data service. The name
 * of an index partition is given by
 * {@link DataService#getIndexPartitionName(String, int)}. Clients are
 * <em>strongly</em> encouraged to use the {@link ClientIndexView} which
 * encapsulates lookup and distribution of operations on range partitioned
 * scale-out indices.
 * </p>
 * <p>
 * The data service exposes both isolated (transactional) and unisolated batch
 * operations on named indices. The isolation level is identified the start
 * time, also known as the transaction identifier or <i>tx</i>. The following
 * distinctions are available:
 * <dl>
 * 
 * <dt>Unisolated</dt>
 * 
 * <dd>
 * <p>
 * Unisolated operation specify {@link ITx#UNISOLATED} as their transaction
 * identifier. Unisolated operations are ACID, but their scope is limited to the
 * commit group on the data service where the operation is executed. Unisolated
 * operations correspond more or less to read-committed semantics except that
 * writes are immediately visible to other operations in the same commit group.
 * </p>
 * <p>
 * Unisolated operations that allow writes obtain an exclusive lock on the live
 * version of the named index for the duration of the operation. Unisolated
 * operations that are declared as read-only read from the last committed state
 * of the named index and therefore do not compete with read-write unisolated
 * operations. This allows unisolated read operations to achieve higher
 * concurrency. The effect is as if the unisolated read operation runs before
 * the unisolated writes in a given commit group since the impact of those
 * writes are not visible to unisolated readers until the next commit point.
 * </p>
 * <p>
 * Unisolated write operations MAY be used to achieve "auto-commit" semantics
 * when distributed transactions are not required. Fully isolated transactions
 * are useful when multiple operations must be composed into a ACID unit.
 * </p>
 * <p>
 * While unisolated operations on a single data service are ACID, clients
 * generally operate against scale-out indices having multiple index partitions
 * hosted on multiple data services. Therefore client MUST NOT assume that an
 * unisolated operation described by the client against a scale-out index will
 * be ACID when that operation is distributed across the various index
 * partitions relevant to the client's request. In practice, this means that
 * contract for ACID unisolated operations is limited to either: (a) operations
 * where the data is located on a single data service instance; or (b)
 * unisolated operations that are inherently designed to achieve a
 * <em>consistent</em> result. Sometimes it is sufficient to configure a
 * scale-out index such that index partitions never split some logical unit -
 * for example, the {schema + primaryKey} for a {@link SparseRowStore}, thereby
 * obtaining an ACID guarentee since operations on a logical row will always
 * occur within the same index partition.
 * </p>
 * </dd>
 * 
 * <dt>Historical read</dt>
 * 
 * <dd>Historical reads are indicated using <code>-tx</code>, where <i>tx</i>
 * is the commit point. A historical read is fully isolated but has very low
 * overhead and does NOT require the caller to open the transaction. The read
 * will have a consistent view of the data as of the most recent commit point
 * not greater than <i>tx</i>. Historical reads correspond to
 * {@link IsolationEnum#ReadOnly}. Unlike a distributed read-only transaction,
 * a historical read does NOT cause the resources required for a consistent view
 * as of that timestamp to be retained for the duration of the read.</dd>
 * 
 * <dt>Distributed transactions</dt>
 * 
 * <dd>Distributed transactions are coordinated using an
 * {@link ITransactionManager} service and incur more overhead than both
 * unisolated and historical read operations. Transactions are assigned a
 * timestamp (the transaction identifier) and an {@link IsolationEnum} when they
 * begin and must be explicitly closed by either an abort or a commit. Both the
 * {@link IsolationEnum#ReadOnly} and the {@link IsolationEnum#ReadWrite} modes
 * force the retention of resources required for a consistent view as of the
 * transaction start time until the transaction is closed.</dd>
 * </dl>
 * </p>
 * <p>
 * Implementations of this interface MUST be thread-safe. Methods declared by
 * this interface MUST block for each operation. Client operations SHOULD be
 * buffered by a thread pool with a FIFO policy so that client requests may be
 * decoupled from data service operations and clients may achieve greater
 * parallelism.
 * </p>
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
public interface IDataService extends IRemoteTxCommitProtocol, Remote {

    /**
     * A description of the journal currently backing the data service.
     * 
     * @throws IOException
     */
    public JournalMetadata getJournalMetadata() throws IOException;

    /**
     * Statistics describing the data service, including IO, indices, etc.
     * 
     * @throws IOException
     */
    public String getStatistics() throws IOException;
    
    /**
     * Register a named mutable index on the {@link DataService}.
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
     *            The UUID that identifies the index. When the named index
     *            corresponds to a partition of a scale-out index, then you MUST
     *            provide the indexUUID for that scale-out index. Otherwise this
     *            MUST be a random UUID, e.g., using {@link UUID#randomUUID()}.
     * 
     * @param ctor
     *            An object that will be sent to the data service and used to
     *            create the index to be registered on that data service.
     * 
     * @param pmd
     *            The partition metadata for the index (optional, required only
     *            when creating an instance of a partitioned index).
     * 
     * @return <code>true</code> iff the index was created. <code>false</code>
     *         means that the index was pre-existing, but the metadata specifics
     *         for the index MAY differ from those specified.
     * 
     * @todo exception if index exists? or modify to validate consistent decl
     *       and exception iff not consistent. right now it just silently
     *       succeeds if the index already exists.
     */
    public void registerIndex(String name, UUID uuid, IIndexConstructor ctor, IPartitionMetadata pmd)
            throws IOException, InterruptedException, ExecutionException;

    /**
     * Return the unique index identifier for the named index.
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
     * Return various statistics about the named index.
     * 
     * @param name
     *            The index name.
     *            
     * @return Statistics about the named index.
     * 
     * @throws IOException
     */
    public String getStatistics(String name) throws IOException;
    
    /**
     * <p>
     * Return <code>true</code> iff the named index exists and supports
     * transactional isolation.
     * </p>
     * <p>
     * Note:If you are inquiring about a scale-out index then you MUST provide
     * the name of an index partition NOT the name of the metadata index.
     * </p>
     * 
     * @param name
     *            The index name.
     * 
     * @exception IllegalArgumentException
     *                if <i>name</i> does not identify a registered index.
     * 
     * @throws IOException
     */
    public boolean isIsolatable(String name) throws IOException;
    
    /**
     * Drops the named index.
     * <p>
     * Note: In order to drop a partition of an index you must form the name of
     * the index partition using
     * {@link DataService#getIndexPartitionName(String, int)} (this operation is
     * generally performed by the {@link IMetadataService} which manages
     * scale-out indices).
     * 
     * @param name
     *            The index name.
     * 
     * @exception IllegalArgumentException
     *                if <i>name</i> does not identify a registered index.
     */
    public void dropIndex(String name) throws IOException,
            InterruptedException, ExecutionException;

    /**
     * <p>
     * Submit a batch operation to a named index.
     * </p>
     * 
     * @param tx
     *            The transaction identifier -or- {@link ITx#UNISOLATED} IFF the
     *            operation is NOT isolated by a transaction -or-
     *            <code> - tx </code> to read from the most recent commit point
     *            not later than the absolute value of <i>tx</i> (a fully
     *            isolated read-only transaction using a historical start time).
     * @param name
     *            The index name (required).
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
     */
    public byte[][] batchInsert(long tx, String name, int ntuples,
            byte[][] keys, byte[][] values, boolean returnOldValues)
            throws InterruptedException, ExecutionException, IOException;

    public boolean[] batchContains(long tx, String name, int ntuples,
            byte[][] keys) throws InterruptedException, ExecutionException,
            IOException;

    public byte[][] batchLookup(long tx, String name, int ntuples, byte[][] keys)
            throws InterruptedException, ExecutionException, IOException;

    public byte[][] batchRemove(long tx, String name, int ntuples,
            byte[][] keys, boolean returnOldValues)
            throws InterruptedException, ExecutionException, IOException;

    /**
     * <p>
     * Streaming traversal of keys and/or values in a key range.
     * </p>
     * <p>
     * Note: In order to visit all keys in a range, clients are expected to
     * issue repeated calls in which the <i>fromKey</i> is incremented to the
     * successor of the last key visited until either an empty {@link ResultSet}
     * is returned or the {@link ResultSet#isLast()} flag is set, indicating
     * that all keys up to (but not including) the <i>startKey</i> have been
     * visited. See {@link ClientIndexView} (scale-out indices) and
     * {@link RangeQueryIterator} (unpartitioned indices), both of which
     * encapsulate this method.
     * </p>
     * 
     * @param tx
     *            The transaction identifier -or- {@link ITx#UNISOLATED} IFF the
     *            operation is NOT isolated by a transaction -or-
     *            <code> - tx </code> to read from the most recent commit point
     *            not later than the absolute value of <i>tx</i> (a fully
     *            isolated read-only transaction using a historical start time).
     * @param name
     *            The index name (required).
     * @param fromKey
     *            The starting key for the scan (or <code>null</code> iff
     *            there is no lower bound).
     * @param toKey
     *            The first key that will not be visited (or <code>null</code>
     *            iff there is no upper bound).
     * @param capacity
     *            When non-zero, this is the maximum #of entries to process.
     * @param flags
     *            One or more flags formed by bitwise OR of zero or more of the
     *            constants defined by {@link IRangeQuery}.
     * @param filter
     *            An optional filter that may be used to filter the keys to be
     *            visited by the iterator.
     * 
     * @exception InterruptedException
     *                if the operation was interrupted (typically by
     *                {@link #shutdownNow()}.
     * @exception ExecutionException
     *                If the operation caused an error. See
     *                {@link ExecutionException#getCause()} for the underlying
     *                error.
     * 
     * @todo the implementation could use a server-side buffer holding the data
     *       and read on that buffer using a socket. this would reduce the
     *       latency owing to (de-)serialization of the result set. the server
     *       side buffer could be swept based on an expired lease model, in
     *       which case the client would have to re-issue the query from the
     *       last key read.
     */
    public ResultSet rangeIterator(long tx, String name, byte[] fromKey,
            byte[] toKey, int capacity, int flags, IEntryFilter filter)
            throws InterruptedException, ExecutionException, IOException;
    
    /**
     * <p>
     * Range count of entries in a key range for the named index on this
     * {@link DataService}.
     * </p>
     * <p>
     * Note: This method reports the upper bound estimate of the #of key-value
     * pairs in the key range of the the named index. The cost of computing this
     * estimate is comparable to two index lookup probes. The estimate is an
     * upper bound because deleted entries in the index that have not been
     * eradicated through a suitable compacting merge will be reported. An exact
     * count may be obtained using the
     * {@link #rangeIterator(long, String, byte[], byte[], int, int, IEntryFilter)}
     * by NOT requesting either the keys or the values.
     * </p>
     * 
     * @param tx
     *            The transaction identifier -or- {@link ITx#UNISOLATED} IFF the
     *            operation is NOT isolated by a transaction -or-
     *            <code> - tx </code> to read from the most recent commit point
     *            not later than the absolute value of <i>tx</i> (a fully
     *            isolated read-only transaction using a historical start time).
     * @param name
     *            The index name (required).
     * @param fromKey
     *            The starting key for the scan (or <code>null</code> iff
     *            there is no lower bound).
     * @param toKey
     *            The first key that will not be visited (or <code>null</code>
     *            iff there is no upper bound).
     * 
     * @return The upper bound estimate of the #of key-value pairs in the key
     *         range of the named index.
     * 
     * @exception InterruptedException
     *                if the operation was interrupted (typically by
     *                {@link #shutdownNow()}.
     * @exception ExecutionException
     *                If the operation caused an error. See
     *                {@link ExecutionException#getCause()} for the underlying
     *                error.
     */
    public int rangeCount(long tx, String name, byte[] fromKey, byte[] toKey)
            throws InterruptedException, ExecutionException, IOException;
        
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
     *            The transaction identifier -or- {@link ITx#UNISOLATED} IFF the
     *            operation is NOT isolated by a transaction -or-
     *            <code> - tx </code> to read from the most recent commit point
     *            not later than the absolute value of <i>tx</i> (a fully
     *            isolated read-only transaction using a historical start time).
     * @param name
     *            The name of the scale-out index.
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
    public Object submit(long tx, String name, IIndexProcedure proc)
            throws InterruptedException, ExecutionException, IOException;
 
}
