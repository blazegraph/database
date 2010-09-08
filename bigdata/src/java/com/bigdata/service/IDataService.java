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
import java.rmi.RemoteException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.bop.engine.IQueryPeer;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.filter.IFilterConstructor;
import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.journal.Options;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.rawstore.IBlock;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.resources.StaleLocatorException;
import com.bigdata.resources.StoreManager;
import com.bigdata.service.ndx.ClientIndexView;
import com.bigdata.service.ndx.DataServiceTupleIterator;
import com.bigdata.sparse.SparseRowStore;

/**
 * <p>
 * The data service interface provides remote access to named indices, provides
 * for both unisolated and isolated operations on those indices, and exposes the
 * {@link ITxCommitProtocol} interface to the {@link ITransactionManagerService}
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
 * The data service exposes both fully isolated read-write transactions,
 * read-only transactions, lightweight read-historical operations, and
 * unisolated operations on named indices. These choices are captured by the
 * timestamp associated with the operation. When it is a transaction, this is
 * also known as the transaction identifier or <i>tx</i>. The following
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
 * <dt>Light weight historical reads</dt>
 * 
 * <dd>Historical reads are indicated using <code>tx</code>, where <i>tx</i>
 * is a timestamp and is associated with the closest commit point LTE to the
 * timestamp. A historical read is fully isolated but has very low overhead and
 * does NOT require the caller to open the transaction. The read will have a
 * consistent view of the data as of the most recent commit point not greater
 * than <i>tx</i>. Unlike a distributed read-only transaction, a historical
 * read does NOT impose a distributed read lock. While the operation will have
 * access to the necessary resources on the local data service, it is possible
 * that resources for the same timestamp will be concurrently released on other
 * data services. If you need to map a read operation across the distributed
 * database, the you must use a read only transaction which will assert the
 * necessary read-lock.</dd>
 * 
 * <dt>Distributed transactions</dt>
 * 
 * <dd>Distributed transactions are coordinated using an
 * {@link ITransactionManagerService} service and incur more overhead than both
 * unisolated and historical read operations. Transactions are assigned a start
 * time (the transaction identifier) when they begin and must be explicitly
 * closed by either an abort or a commit. Both read-only and read-write
 * transactions assert read locks which force the retention of resources
 * required for a consistent view as of the transaction start time until the
 * transaction is closed.</dd>
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
 * <h2>Index Partitions: Split, Join, and Move</h2>
 * 
 * <p>
 * 
 * Scale-out indices are broken tranparently down into index partitions. When a
 * scale-out index is initially registered, one or more index partitions are
 * created and registered on one or more data services.
 * </p>
 * 
 * <p>
 * 
 * Note that each index partitions is just an {@link IIndex} registered under
 * the name assigned by {@link DataService#getIndexPartitionName(String, int)}
 * and whose {@link IndexMetadata#getPartitionMetadata()} returns a description
 * of the resources required to compose a view of that index partition from the
 * resources located on a {@link DataService}. The {@link IDataService} will
 * respond for that index partition IFF there is an index under that name
 * registered on the {@link IDataService} as of the <i>timestamp</i> associated
 * with the request. If the index is not registered then a
 * {@link NoSuchIndexException} will be thrown. If the index was registered and
 * has since been split, joined or moved then a {@link StaleLocatorException}
 * will be thrown (this will occur only for index partitions of scale-out
 * indices). <strong>All methods on this and derived interfaces which are
 * defined for an index name and timestamp MUST conform to these semantics.</strong>
 * 
 * </p>
 * 
 * <p>
 * 
 * As index partitions grow in size they may be <em>split</em> into 2 or more
 * index partitions covering the same key range as the original index partition.
 * When this happens a new index partition identifier is assigned by the
 * metadata service to each of the new index partitions and the old index
 * partition is retired in an atomic operation. A similar operation can
 * <em>move</em> an index partition to a different {@link IDataService} in
 * order to load balance a federation. Finally, when two index partitions shrink
 * in size, they maybe moved to the same {@link IDataService} and an atomic
 * <i>join</i> operation may re-combine them into a single index partition
 * spanning the same key range.
 * 
 * </p>
 * 
 * <p>
 * 
 * Split, join, and move operations all result in the old index partition being
 * dropped on the {@link IDataService}. Clients having a stale
 * {@link PartitionLocator} record will attempt to reach the now defunct index
 * partition after it has been dropped and will receive a
 * {@link StaleLocatorException}.
 * 
 * </p>
 * 
 * 
 * <h2>{@link StaleLocatorException}</h2>
 * 
 * <p>
 * 
 * {@link IDataService} clients MUST handle this exception by refreshing their
 * cached {@link PartitionLocator} for the key range associated with the index
 * partition which they wish to query and then re-issuing their request. By
 * following this simple rule the client will automatically handle index
 * partition splits, joins, and moves without error and in a manner which is
 * completely transparent to the application. Note that splits, joins, and moves
 * DO NOT alter the {@link PartitionLocator} for historical reads, only for
 * ongoing writes. This exception is generally (but not always) wrapped.
 * Applications typically DO NOT write directly to the {@link IDataService}
 * interface and therefore DO NOT need to worry about this. See
 * {@link ClientIndexView}, which automatically handles this exception.
 * 
 * </p>
 * 
 * <h2>{@link IOException}</h2>
 * 
 * <p>
 * 
 * All methods on this and derived interfaces can throw an {@link IOException}.
 * In all cases an <em>unwrapped</em> exception that is an instance of
 * {@link IOException} indicates an error in the Remote Method Invocation (RMI)
 * layer.
 * 
 * </p>
 * 
 * <h2>{@link ExecutionException} and {@link InterruptedException}</h2>
 * 
 * <p>
 * 
 * An <em>unwrapped</em> {@link ExecutionException} or
 * {@link InterruptedException} indicates a problem when running the request as
 * a task in the {@link IConcurrencyManager} on the {@link IDataService}. The
 * exception always wraps a root cause which may indicate the underlying
 * problem. Methods which do not declare these exceptions are not run under the
 * {@link IConcurrencyManager}.
 * 
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo add support for triggers? unisolated triggers must be asynchronous if
 *       they will take actions with high latency (such as writing on a
 *       different index partition, which could be remote). Low latency actions
 *       might include emitting asynchronous messages. transactional triggers
 *       can have more flexibility since they are under less of a latency
 *       constraint.
 */
public interface IDataService extends ITxCommitProtocol, IService, IRemoteExecutor {

    /**
     * Register a named mutable index on the {@link DataService}.
     * <p>
     * Note: In order to register an index partition the
     * {@link IndexMetadata#getPartitionMetadata() partition metadata} property
     * MUST be set. The {@link LocalPartitionMetadata#getResources() resources}
     * property will then be overriden when the index is actually registered so
     * as to reflect the {@link IResourceMetadata} description of the journal on
     * which the index actually resides.
     * 
     * @param name
     *            The name that can be used to recover the index. In order to
     *            create a partition of an index you must form the name of the
     *            index partition using
     *            {@link DataService#getIndexPartitionName(String, int)} (this
     *            operation is generally performed by the
     *            {@link IMetadataService} which manages scale-out indices).
     * 
     * @param metadata
     *            The metadata describing the index.
     *            <p>
     *            The {@link LocalPartitionMetadata#getResources()} property on
     *            the {@link IndexMetadata#getPartitionMetadata()} SHOULD NOT be
     *            set. The correct {@link IResourceMetadata}[] will be assigned
     *            when the index is registered on the {@link IDataService}.
     * 
     * @return <code>true</code> iff the index was created. <code>false</code>
     *         means that the index was pre-existing, but the metadata specifics
     *         for the index MAY differ from those specified.
     * 
     * @todo exception if index exists? or modify to validate consistent decl
     *       and exception iff not consistent. right now it just silently
     *       succeeds if the index already exists.
     */
    public void registerIndex(String name, IndexMetadata metadata)
            throws IOException, InterruptedException, ExecutionException;

    /**
     * Return the metadata for the named index.
     * 
     * @param name
     *            The index name.
     * @param timestamp
     *            A transaction identifier, {@link ITx#UNISOLATED} for the
     *            unisolated index view, {@link ITx#READ_COMMITTED}, or
     *            <code>timestamp</code> for a historical view no later than
     *            the specified timestamp.
     *            
     * @return The metadata for the named index.
     * 
     * @throws IOException
     */
    public IndexMetadata getIndexMetadata(String name, long timestamp)
            throws IOException, InterruptedException, ExecutionException;

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
     * Streaming traversal of keys and/or values in a key range.
     * </p>
     * <p>
     * Note: In order to visit all keys in a range, clients are expected to
     * issue repeated calls in which the <i>fromKey</i> is incremented to the
     * successor of the last key visited until either an empty {@link ResultSet}
     * is returned or the {@link ResultSet#isLast()} flag is set, indicating
     * that all keys up to (but not including) the <i>startKey</i> have been
     * visited. See {@link ClientIndexView} (scale-out indices) and
     * {@link DataServiceTupleIterator} (unpartitioned indices), both of which
     * encapsulate this method.
     * </p>
     * <p>
     * Note: If the iterator can be determined to be read-only and it is
     * submitted as {@link ITx#UNISOLATED} then it will be run as
     * {@link ITx#READ_COMMITTED} to improve concurrency.
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
     *            An optional object that may be used to layer additional
     *            semantics onto the iterator. The filter will be constructed on
     *            the server and in the execution context for the iterator, so
     *            it will execute directly against the index for the maximum
     *            efficiency.
     * 
     * @exception InterruptedException
     *                if the operation was interrupted.
     * @exception ExecutionException
     *                If the operation caused an error. See
     *                {@link ExecutionException#getCause()} for the underlying
     *                error.
     */
    public ResultSet rangeIterator(long tx, String name, byte[] fromKey,
            byte[] toKey, int capacity, int flags, IFilterConstructor filter)
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
     *            The transaction identifier, {@link ITx#UNISOLATED} for an ACID
     *            operation NOT isolated by a transaction,
     *            {@link ITx#READ_COMMITTED} for a read-committed operation not
     *            protected by a transaction (no global read lock), or any valid
     *            commit time for a read-historical operation not protected by a
     *            transaction (no global read lock).
     * @param name
     *            The name of the index partition.
     * @param proc
     *            The procedure to be executed.
     * 
     * @return The {@link Future} from which the outcome of the procedure may be
     *         obtained.
     * 
     * @throws RejectedExecutionException
     *             if the task can not be accepted for execution.
     * @throws IOException
     *             if there is an RMI problem.
     * 
     * @todo change API to <T> Future<T> submit(tx,name,IIndexProcedure<T>).
     *       Existing code will need to be recompiled after this API change.
     */
    public Future submit(long tx, String name, IIndexProcedure proc)
            throws IOException;

    /**
     * {@inheritDoc}
     * <p>
     * Note: This interface is specialized by the {@link IDataService} for tasks
     * which need to gain access to the {@link IDataService} in order to gain
     * local access to index partitions, etc. Such tasks declare the
     * {@link IDataServiceCallable}. For example, scale-out joins use
     * this mechanism.
     * 
     * @see IDataServiceCallable
     */
    public Future<? extends Object> submit(Callable<? extends Object> proc)
            throws RemoteException;

    /**
     * Read a low-level record from the described {@link IRawStore} described by
     * the {@link IResourceMetadata}.
     * 
     * @param resource
     *            The description of the resource containing that block.
     * @param addr
     *            The address of the block in that resource.
     * 
     * @return An object that may be used to read the block from the data
     *         service.
     * 
     * @throws IllegalArgumentException
     *             if the resource is <code>null</code>
     * @throws IllegalArgumentException
     *             if the addr is <code>0L</code>
     * @throws IllegalStateException
     *             if the resource is not available.
     * @throws IllegalArgumentException
     *             if the record identified by addr can not be read from the
     *             resource.
     * 
     * @todo This is a first try at adding support for reading low-level records
     *       from a journal or index segment in support of the
     *       {@link BigdataFileSystem}.
     *       <p>
     *       The API should provide a means to obtain a socket from which record
     *       data may be streamed. The client sends the resource identifier
     *       (UUID of the journal or index segment) and the address of the
     *       record and the data service sends the record data. This is designed
     *       for streaming reads of up to 64M or more (a record recorded on the
     *       store as identified by the address).
     */
    public IBlock readBlock(IResourceMetadata resource, long addr)
            throws IOException;

    /*
     * Methods in support of unit tests.
     * 
     * @todo could be moved to their own interface.
     */

    /**
     * Method sets a flag that will force overflow processing during the next
     * group commit and optionally forces a group commit <strong>(Note: This
     * method exists primarily for unit tests and benchmarking activities and
     * SHOULD NOT be used on a deployed federation as the overhead associated
     * with a compacting merge of each index partition can be significant).
     * </strong>
     * <p>
     * Normally there is no reason to invoke this method directly. Overflow
     * processing is triggered automatically on a bottom-up basis when the
     * extent of the live journal nears the {@link Options#MAXIMUM_EXTENT}.
     * 
     * @param immediate
     *            The purpose of this argument is to permit the caller to
     *            trigger an overflow event even though there are no writes
     *            being made against the data service. When <code>true</code>
     *            the method will write a token record on the live journal in
     *            order to provoke a group commit. In this case synchronous
     *            overflow processing will have occurred by the time the method
     *            returns. When <code>false</code> a flag is set and overflow
     *            processing will occur on the next commit.
     * @param compactingMerge
     *            The purpose of this flag is to permit the caller to indicate
     *            that a compacting merge should be performed for all indices on
     *            the data service (at least, all indices whose data are not
     *            simply copied onto the new journal) during the next
     *            synchronous overflow. Note that compacting merges of indices
     *            are performed automatically from time to time so this flag
     *            exists mainly for people who want to force a compacting merge
     *            for some reason.
     * 
     * @throws IOException
     * @throws InterruptedException
     *             may be thrown if <i>immediate</i> is <code>true</code>.
     * @throws ExecutionException
     *             may be thrown if <i>immediate</i> is <code>true</code>.
     */
    public void forceOverflow(boolean immediate, boolean compactingMerge)
            throws IOException, InterruptedException, ExecutionException;
    
    /**
     * This attempts to pause the service accepting {@link ITx#UNISOLATED}
     * writes and then purges any resources that are no longer required based on
     * the {@link StoreManager.Options#MIN_RELEASE_AGE}.
     * <p>
     * Note: Resources are normally purged during synchronous overflow handling.
     * However, asynchronous overflow handling can cause resources to no longer
     * be needed as new index partition views are defined. This method MAY be
     * used to trigger a release before the next overflow event.
     * 
     * @param timeout
     *            The timeout (in milliseconds) that the method will await the
     *            pause of the write service.
     * @param truncateJournal
     *            When <code>true</code>, the live journal will be truncated
     *            to its minimum extent (all writes will be preserved but there
     *            will be no free space left in the journal). This may be used
     *            to force the {@link DataService} to its minimum possible
     *            footprint for the configured history retention policy.
     * 
     * @return <code>true</code> if successful and <code>false</code> if the
     *         write service could not be paused after the specified timeout.
     * 
     * @param truncateJournal
     *            When <code>true</code> the live journal will be truncated
     *            such that no free space remains in the journal.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    public boolean purgeOldResources(long timeout, boolean truncateJournal)
            throws IOException, InterruptedException;
    
    /**
     * The #of asynchronous overflows that have taken place on this data service
     * (the counter is not restart safe).
     */
    public long getAsynchronousOverflowCounter() throws IOException;
    
    /**
     * Return <code>true</code> iff the data service is currently engaged in
     * overflow processing.
     */
    public boolean isOverflowActive() throws IOException;

    /**
     * Return the {@link IQueryPeer} running on this service.
     */
    public IQueryPeer getQueryEngine() throws IOException;
    
//    /**
//     * Shutdown the service immediately and destroy any persistent data
//     * associated with the service.
//     * 
//     * moved to {@link IService}?
//     */
//    public void destroy() throws IOException;
    
}
