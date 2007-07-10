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
 * Created on Mar 14, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.rmi.Remote;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BatchContains;
import com.bigdata.btree.BatchInsert;
import com.bigdata.btree.BatchLookup;
import com.bigdata.btree.BatchRemove;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IBatchBTree;
import com.bigdata.btree.IBatchOperation;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IIndexWithCounter;
import com.bigdata.btree.IKeyBuilder;
import com.bigdata.btree.ILinearList;
import com.bigdata.btree.IReadOnlyOperation;
import com.bigdata.btree.ISimpleBTree;
import com.bigdata.btree.ReadOnlyIndex;
import com.bigdata.isolation.IIsolatedIndex;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IAtomicStore;
import com.bigdata.journal.ICommitter;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.scaleup.JournalMetadata;
import com.bigdata.scaleup.ResourceState;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.sun.corba.se.impl.orbutil.closure.Future;

/**
 * An implementation of a network-capable {@link IDataService}. The service is
 * started using the {@link DataServer} class.
 * <p>
 * This implementation is thread-safe. It will block for each operation. It MUST
 * be invoked within a pool of request handler threads servicing a network
 * interface in order to decouple data service operations from client requests.
 * When using as part of an embedded database, the client operations MUST be
 * buffered by a thread pool with a FIFO policy so that client requests will be
 * decoupled from data service operations.
 * <p>
 * The {@link #txService} provides concurrency for transaction processing.
 * <p>
 * The {@link #readService} provides concurrency for unisolated reads from the
 * last committed state of the store (concurrent writes do not show up in this
 * view).
 * <p>
 * Unisolated writes are serialized using
 * {@link AbstractJournal#serialize(Callable)}. This ensures that each
 * unisolated write operation occurs within a single-theaded context and thereby
 * guarentees atomicity, consistency, and isolatation without requiring locking.
 * Unisolated writes either committing or aborting after each unisolated
 * operation using a restart-safe protocol. Successful unisolated write
 * operations are therefore ACID.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see DataServer, which is used to start this service.
 * 
 * @see NIODataService, which contains some old code that can be refactored for
 *      an NIO interface to the data service.
 * 
 * @todo Validation of partition indices during the PREPARE phase of a
 *       transaction should proceed in parallel.
 * 
 * @todo With the change to the submit method, it is now trivial to process
 *       writes on distinct indices mapped on the same data service
 *       concurrently. The can be done as follows: Modify the journal's
 *       IRawStore implementation to support concurrent writers (it is MROW now
 *       and will become MRMW). The change is limited to a very small section of
 *       code where the next address is computed. Commits are still
 *       single-threaded of course and will continue to run in the
 *       "writeService" which could be renamed "commitService". Create an
 *       executor thread (write service) per named index mapped onto a journal.
 *       There are a variety of strategies here. The easiest is one thread per
 *       named index, but that does not provide governance over the #of threads
 *       that can run directly. An alternative is a thread pool with a lock per
 *       named index such that new writes on an index block until the lock is
 *       released by the current writer. The size of the pool could be
 *       configured. In any case, index writes run concurrently on different
 *       indices with concurrent writes against the backing IRawStore and then
 *       index commits are serialized. This preserves consistency and might make
 *       group commit trivial by accepting all commit requests in the commit
 *       queue periodically, e.g., at intervals of no more than 100ms latency.
 *       <p>
 *       Note: The commit protocol for indices needs to be modified slightly. As
 *       it stands, all dirty indices are flushed when the journal commits. In
 *       order to permit concurrent writes on distinct indices on the same
 *       journal the protocol must be modified so that an index does not commit
 *       unless and until the unisolated operation has completed. Failure to
 *       respect this would result in partial unisolated writes being made
 *       restart safe. One way to approach this is to default indices to an
 *       "auto-commit" such that they commit whenever the journal commits.
 *       Auto-commit would then be turned off for unisolated writes and an
 *       explicit commit on the index would be required as a pre-condition for
 *       the index to participate in the journal commit. In practice, it may be
 *       that "auto-commit" is just a bad idea for named indices and that writes
 *       should be discarded unless an explicit commit is requested (the
 *       exception might be certain shared indices, which tend not to be named,
 *       such as the commit record index itself).
 * 
 * @todo break out a map/reduce service - at least at an interface level. There
 *       is a distinction between map/reduce where the inputs (outputs) are
 *       index partitions and map/reduce when the inputs (outputs) are files.
 *       The basic issue is that index reads (and to a much greater extent index
 *       writes) contend with other data service processes. Map file or index
 *       read operations may run with full concurrency, as may map file write
 *       and reduce file write operations. Reduce operations that complete with
 *       non-indexed outputs may also have full concurrency. Reduce operations
 *       that seek to build a scale-out index from the individual reduce inputs
 *       will conduct unisolated writes on index partitions, which would of
 *       necessity be distributed since the hash function does not produce a
 *       total ordering. (There may well be optimizations available for
 *       partitioned bulk index builds from the reduce operation that would then
 *       be migrated to the appropriate data service and merged into the current
 *       index view for the appropriate index partition).
 *       <p>
 *       So, the take away for map processes is that they always have full
 *       concurrency since they never write directly on indices. Index reads
 *       will be advantaged if they can occur in the data service on which the
 *       input index partition exists.
 *       <p>
 *       Likewise, the take away for reduce processes is that they are
 *       essentially clients rather than data service processes. As such, the
 *       reduce process should run fully concurrently with a data service for
 *       the machine on which the reduce inputs reside (its own execution
 *       thread, and perhaps its own top-level service), but index writes will
 *       be distributed across the network to various data services.
 * 
 * @todo consider that all getIndex() methods on the various tasks should be
 *       getIndexPartition() methods. The getIndexPartition() method could be
 *       implemented by a single static helper method. The IIndexPartition could
 *       cache the partition metadata and optional range check the keys for
 *       operations on that partition. Consider throwing
 *       {@link NoSuchIndexException} and {@link NoSuchIndexPartitionException}
 *       from getIndexPartition().
 * 
 * @todo The data service should redirect clients if an index partition has been
 *       moved (shed) while a client has a lease.
 * 
 * @todo make sure that all service methods that create a {@link Future} do a
 *       get() so that the will block until the serialized task actually runs.
 * 
 * @todo Note that "auto-commit" is provided for unisolated writes. This relies
 *       on two things. First, only the {@link UnisolatedBTree} recoverable from
 *       {@link AbstractJournal#getIndex(String)} is mutable - all other ways to
 *       recover the named index will return a read-only view of a historical
 *       committed state. Second, an explicit {@link IAtomicStore#commit()} must
 *       be performed to make the changes restart-safe. The commit can only be
 *       performed when no unisolated write is executing (even presuming that
 *       different mutable btrees can receive writes concurrently) since it will
 *       cause all dirty {@link ICommitter} to become restart-safe. Group commit
 *       is essential to high throughput when unisolated writes are relatively
 *       small.
 * 
 * @todo support group commit for unisolated writes. i may have to refactor some
 *       to get group commit to work for both transaction commits and unisolated
 *       writes. basically, the tasks on the
 *       {@link AbstractJournal#writeService} need to get aggregated (or each
 *       commit examines the length of the write queue (basically, is there
 *       another write in the queue or is this the last one), latency and data
 *       volumn since the last commit and makes a decision whether or not to
 *       commit at that time; if the commit is deferred, then it is placed onto
 *       a queue of operations that have not finished and for which we can not
 *       yet report "success" - even though additional unisolated writes must
 *       continue to run; we also need to make sure that a commit will occur at
 *       the first opportunity following the minimum latency -- even if no
 *       unisolated writes are scheduled (or a single client would hang waiting
 *       for a commit).
 * 
 * @todo add assertOpen() throughout
 * 
 * @todo declare interface for managing service shutdown()/shutdownNow()?
 * 
 * @todo implement NIODataService, RPCDataService(possible), EmbeddedDataService
 *       (uses queue to decouple operations), DataServiceClient (provides
 *       translation from {@link ISimpleBTree} to {@link IBatchBTree}, provides
 *       transparent partitioning of batch operations, handles handshaking and
 *       leases with the metadata index locator service; abstract IO for
 *       different client platforms (e.g., support PHP, C#). Bundle ICU4J with
 *       the client.
 * 
 * @todo JobScheduler service for map/reduce (or Hadoop integration).
 * 
 * @todo another data method will need to be defined to support GOM with
 *       pre-fetch. the easy way to do this is to get 50 objects to either side
 *       of the object having the supplied key. This is easy to compute using
 *       the {@link ILinearList} interface. I am not sure about unisolated
 *       operations for GOM.... Isolated operations are straight forward. The
 *       other twist is supporting scalable link sets, link set indices (not
 *       named, unless the identity of the object collecting the link set is
 *       part of the key), and non-OID indices (requires changes to
 *       generic-native).
 * 
 * @todo Have the {@link DataService} notify the transaction manager when a
 *       write is performed on that service so that all partitipating
 *       {@link DataService} instances will partitipate in a 2-/3-phase commit
 *       (and a simple commit can be used when the transaction write set is
 *       localized on a single dataservice instance). The message needs to be
 *       synchronous each time a new index partition is written on by the client
 *       so that the transaction manager can locate the primary
 *       {@link DataService} instance for the write when it needs to commit or
 *       abort the tx.
 * 
 * @todo narrow file access permissions so that we only require
 *       {read,write,create,delete} access to a temporary directory and a data
 *       directory.
 * 
 * @todo all of the interfaces implemented by this class need to extend
 *       {@link Remote} in order to be made visible on the proxy object exported
 *       by JERI.
 * 
 * @todo Write benchmark test to measure interhost transfer rates. Should be
 *       100Mbits/sec (~12M/sec) on a 100BaseT switched network. With full
 *       duplex in the network and the protocol, that rate should be
 *       bidirectional. Can that rate be sustained with a fully connected
 *       bi-directional transfer?
 * 
 * @todo We will use non-blocking I/O for the data transfer protocol in order to
 *       support an efficient pipelining of writes across data services. Review
 *       options to secure that protocol since we can not use JERI for that
 *       purpose. For example, using SSL or an SSH tunnel. For most purposes I
 *       expect bigdata to operate on a private network, but replicate across
 *       gateways is also a common use case. Do we have to handle it specially?
 * 
 * @todo Keep the "wire" format for the data and metadata services as clean as
 *       possible so that it will be possible for non-Java clients to talk to
 *       these services (assuming that they can talk to Jini...).
 */
abstract public class DataService implements IDataService,
        IWritePipeline, IResourceTransfer {

    IKeyBuilder keyBuilder;
    
    protected Journal journal;

    public static final transient Logger log = Logger
            .getLogger(DataService.class);
    
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

    protected volatile int nunisolated = 0;
    
    /**
     * Pool of threads for handling unisolated reads.
     */
    final protected ExecutorService readService;
    
    /**
     * Pool of threads for handling concurrent transactions.
     */
    final protected ExecutorService txService;
    
    /**
     * Thread printing out periodic service status information (counters).
     */
    final protected Thread statusService;

    /**
     * Options understood by the {@link DataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Options extends com.bigdata.journal.Options {
        
        /**
         * <code>readServicePoolSize</code> - The #of threads in the pool
         * handling concurrent unisolated read requests.
         * 
         * @see #DEFAULT_READ_SERVICE_POOL_SIZE
         */
        public static final String READ_SERVICE_POOL_SIZE = "readServicePoolSize";
        
        /**
         * The default #of threads in the read service thread pool.
         */
        public final static int DEFAULT_READ_SERVICE_POOL_SIZE = 20;

        /**
         * <code>txServicePoolSize</code> - The #of threads in the pool
         * handling concurrent transactions.
         * 
         * @see #DEFAULT_TX_SERVICE_POOL_SIZE
         */
        public static final String TX_SERVICE_POOL_SIZE = "txServicePoolSize";
        
        /**
         * The default #of threads in the transaction service thread pool.
         */
        public final static int DEFAULT_TX_SERVICE_POOL_SIZE = 100;
        
    }
    
    /**
     * 
     * @param properties
     */
    public DataService(Properties properties) {

        String val;
        
        final int txServicePoolSize;
        final int readServicePoolSize;
        
        /*
         * "readServicePoolSize"
         */

        val = properties.getProperty(Options.READ_SERVICE_POOL_SIZE);

        if (val != null) {

            readServicePoolSize = Integer.parseInt(val);

            if (readServicePoolSize < 1 ) {

                throw new RuntimeException("The '"
                        + Options.READ_SERVICE_POOL_SIZE
                        + "' must be at least one.");

            }

        } else readServicePoolSize = Options.DEFAULT_READ_SERVICE_POOL_SIZE;

        /*
         * "txServicePoolSize"
         */

        val = properties.getProperty(Options.TX_SERVICE_POOL_SIZE);

        if (val != null) {

            txServicePoolSize = Integer.parseInt(val);

            if (txServicePoolSize < 1 ) {

                throw new RuntimeException("The '"
                        + Options.TX_SERVICE_POOL_SIZE
                        + "' must be at least one.");

            }

        } else txServicePoolSize = Options.DEFAULT_TX_SERVICE_POOL_SIZE;

        /*
         * The journal's write service will be used to handle unisolated writes
         * and transaction commits.
         */
        journal = new Journal(properties);

        // setup thread pool for unisolated read operations.
        readService = Executors.newFixedThreadPool(readServicePoolSize,
                DaemonThreadFactory.defaultThreadFactory());

        // setup thread pool for concurrent transactions.
        txService = Executors.newFixedThreadPool(txServicePoolSize,
                DaemonThreadFactory.defaultThreadFactory());

        // thread for periodic status messages.
        statusService = new StatusThread();
        
        statusService.start();
        
    }

    /**
     * Polite shutdown does not accept new requests and will shutdown once
     * the existing requests have been processed.
     */
    public void shutdown() {
        
        readService.shutdown();
        
        txService.shutdown();
        
        statusService.interrupt();
        
        journal.shutdown();
        
    }
    
    /**
     * Shutdown attempts to abort in-progress requests and shutdown as soon
     * as possible.
     */
    public void shutdownNow() {

        readService.shutdownNow();
        
        txService.shutdownNow();

        statusService.interrupt();
        
        journal.close();
        
    }

    /**
     * Writes out periodic status information about the {@link DataService}.
     * 
     * @todo I am not convinced that the shutdown logic is correctly bringing
     *       down the status thread - what is a more robust way to write the
     *       status service, e.g., using a scheduled executor service?
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class StatusThread extends Thread {
        
        final public long millis = 500;
        
        public StatusThread() {
            
            setDaemon(true);
            
        }
        
        public void run() {

            while (true) {

                if(isInterrupted()) {
                    
                    System.err.println("Status thread was interrupted(1)");
                    
                    return;
                    
                }
                
                try {

                    Thread.sleep(millis);
                    
                    status();

                } catch (InterruptedException ex) {

                    System.err.println("Status thread was interrupted(2)");

                    status();

                    return;

                }

            }
            
        }
        
        /*
         * @todo Write out the queue depths, #of operations to date, etc.
         */
        public void status() {
            
            System.err.println("status: nunisolated="+nunisolated);
            
        }
        
    }
    
    /**
     * The unique identifier for this data service - this is used mainly for log
     * messages.
     * 
     * @return The unique data service identifier.
     */
    public abstract UUID getServiceUUID() throws IOException;
    
    /*
     * ITxCommitProtocol.
     */
    
    public long commit(long tx) throws IOException {
        
        // will place task on writeService and block iff necessary.
        return journal.commit(tx);
        
    }

    public void abort(long tx) throws IOException {

        // will place task on writeService iff read-write tx.
        journal.abort(tx);
        
    }

    /*
     * IDataService.
     */
    
    /**
     * Forms the name of the index corresponding to a partition of a named
     * scale-out index as <i>name</i>#<i>partitionId</i>.
     * 
     * @return The name of the index partition.
     */
    public static final String getIndexPartitionName(String name,
            int partitionId) {

        assert name != null;

        return name + "#" + partitionId;

    }
    
    private boolean isReadOnly(long startTime) {
        
        assert startTime != 0l;
        
        ITx tx = journal.getTx(startTime);
        
        if (tx == null) {

            throw new IllegalStateException("Unknown: tx=" + startTime);
            
        }
        
        return tx.isReadOnly();
        
    }

    /**
     * @todo if the journal overflows then the returned metadata can become
     *       stale (the journal in question will no longer be absorbing writes
     *       but it will continue to be used to absorb reads until the asyn
     *       overflow operation is complete, at which point the journal can be
     *       closed. the journal does not become "Dead" until it is no longer
     *       possible that a live transaction will want to read from a
     *       historical state found on that journal).
     */
    public JournalMetadata getJournalMetadata() throws IOException {
        
        return new JournalMetadata(journal,ResourceState.Live);
        
    }

    public void registerIndex(String name, UUID indexUUID, String className,
            Object config) throws IOException, InterruptedException,
            ExecutionException {

        journal.serialize(
                new RegisterIndexTask(name, indexUUID, className, config))
                .get();

    }
    
    public UUID getIndexUUID(String name) throws IOException {
        
        IIndex ndx = journal.getIndex(name);
        
        if(ndx == null) {
            
            return null;
            
        }
        
        return ndx.getIndexUUID();
        
    }

    public void dropIndex(String name) throws IOException,
            InterruptedException, ExecutionException {
        
        journal.serialize(new DropIndexTask(name)).get();
        
    }
    
//    public void mapPartition(String name, PartitionMetadataWithSeparatorKeys pmd)
//            throws IOException, InterruptedException, ExecutionException {
//
//        log.info(getServiceUUID() + ", name=" + name + ", partitionId="
//                + pmd.getPartitionId() + ", leftSeparatorKey="
//                + BytesUtil.toString(pmd.getLeftSeparatorKey())
//                + ", rightSeparatorKey="
//                + BytesUtil.toString(pmd.getRightSeparatorKey()));
//        
//        journal.serialize(new MapIndexPartitionTask(name,pmd)).get();
//    
//    }
//
//    /**
//     * @todo implement {@link #unmapPartition(String, int)}
//     */
//    public void unmapPartition(String name, int partitionId)
//            throws IOException, InterruptedException, ExecutionException {
//
//        journal.serialize(new UnmapIndexPartitionTask(name,partitionId)).get();
//        
//    }

    // FIXME modify to allow vals[] as null when index does not use values.
    public byte[][] batchInsert(long tx, String name, int partitionId, int ntuples,
            byte[][] keys, byte[][] vals, boolean returnOldValues)
            throws IOException, InterruptedException, ExecutionException {

        BatchInsert op = new BatchInsert(ntuples, keys, vals);

        batchOp(tx, name, partitionId, op);

        return returnOldValues ? (byte[][]) op.values : null;

    }

    public boolean[] batchContains(long tx, String name, int partitionId, int ntuples,
            byte[][] keys) throws IOException, InterruptedException, ExecutionException {

        BatchContains op = new BatchContains(ntuples, keys, new boolean[ntuples]);
        
        batchOp( tx, name, partitionId, op );

        return op.contains;
        
    }
    
    public byte[][] batchLookup(long tx, String name, int partitionId, int ntuples, byte[][] keys)
            throws IOException, InterruptedException, ExecutionException {

        BatchLookup op = new BatchLookup(ntuples,keys,new byte[ntuples][]);
        
        batchOp(tx, name, partitionId, op);
        
        return (byte[][])op.values;
        
    }
    
    public byte[][] batchRemove(long tx, String name, int partitionId, int ntuples,
            byte[][] keys, boolean returnOldValues)
            throws IOException, InterruptedException, ExecutionException {
        
        BatchRemove op = new BatchRemove(ntuples,keys,new byte[ntuples][]);
        
        batchOp(tx, name, partitionId, op);
        
        return returnOldValues ? (byte[][])op.values : null;
        
    }
    
    /**
     * Executes a batch operation on a named btree.
     * 
     * @param tx
     *            The transaction identifier -or- zero (0L) IFF the operation is
     *            NOT isolated by a transaction.
     * @param name
     *            The index name (required).
     * @param op
     *            The batch operation.
     * 
     * @exception InterruptedException
     *                if the operation was interrupted (typically by
     *                {@link #shutdownNow()}.
     * @exception ExecutionException
     *                If the operation caused an error. See
     *                {@link ExecutionException#getCause()} for the underlying
     *                error.
     */
    protected void batchOp(long tx, String name, int partitionId, IBatchOperation op)
            throws InterruptedException, ExecutionException {
        
        if( name == null ) throw new IllegalArgumentException();
        
        if( op == null ) throw new IllegalArgumentException();
        
        final boolean isolated = tx != 0L;
        
        final boolean readOnly = (op instanceof IReadOnlyOperation)
                || (isolated && isReadOnly(tx));
        
        if(isolated) {
            
            txService.submit(new TxBatchTask(tx,readOnly,name,partitionId,op)).get();
            
        } else if( readOnly ) {
            
            /*
             * Note: The IReadOnlyOperation is a promise that the procedure will
             * not write on an index (or anything on the store), but it is NOT a
             * guarentee.  The guarentee is provided by wrapping the index in a
             * read-only view.
             * 
             * @todo Write tests that demonstrate that writes are rejected if
             * the proc implements IReadOnlyOperation (same for read-only index
             * operations).
             */
            
            readService.submit(new UnisolatedReadBatchTask(name,partitionId,op)).get();
            
        } else {
            
            /*
             * Special case since incomplete writes MUST be discarded and
             * complete writes MUST be committed.
             */
            nunisolated++;
            if(DEBUG)log.debug("nunisolated(inc)="+nunisolated);
            journal.serialize(new UnisolatedBatchReadWriteTask(name,partitionId,op)).get();
            nunisolated--;
            if(DEBUG)log.debug("nunisolated(dec)="+nunisolated);
            
        }
        
    }
    
    public Object submit(long tx, String name, int partitionId, IProcedure proc) throws InterruptedException,
            ExecutionException {

        if( proc == null ) throw new IllegalArgumentException();
        
        final boolean isolated = tx != 0L;
        
        final boolean readOnly = proc instanceof IReadOnlyOperation;
        
        if(isolated) {
            
            return txService.submit(
                    new TxProcedureTask(tx, readOnly, name, partitionId, proc))
                    .get();
            
        } else if( readOnly ) {
            
            /*
             * Note: The IReadOnlyOperation is a promise that the procedure will
             * not write on an index (or anything on the store), but it is NOT a
             * guarentee.  The guarentee is provided by wrapping the index in a
             * read-only view.
             * 
             * @todo Write tests that demonstrate that writes are rejected if
             * the proc implements IReadOnlyOperation (same for read-only index
             * operations).
             */
            
            return readService.submit(
                    new UnisolatedReadProcedureTask(name, partitionId, proc))
                    .get();
            
        } else {
            
            /*
             * Special case since incomplete writes MUST be discarded and
             * complete writes MUST be committed.
             */
            return journal.serialize(
                    new UnisolatedReadWriteProcedureTask(name, partitionId,
                            proc)).get();
            
        }

    }

    public int rangeCount(long tx, String name, int partitionId,
            byte[] fromKey, byte[] toKey) throws InterruptedException,
            ExecutionException {

        if (name == null)
            throw new IllegalArgumentException();

        final RangeCountTask task = new RangeCountTask(tx, name, partitionId,
                fromKey, toKey);

        final boolean isolated = tx != 0L;

        if (isolated) {

            return (Integer) readService.submit(task).get();

        } else {
            
            /*
             * @todo do unisolated read operations need to get serialized?!?
             * check rangeQuery also.
             */
            return (Integer) journal.serialize(task).get();

        }

    }
    
    /**
     * 
     * @todo the iterator needs to be aware of the defintion of a "row" for the
     *       sparse row store so that we can respect the atomic guarentee for
     *       reads as well as writes.
     * 
     * @todo support filters. there are a variety of use cases from clients that
     *       are aware of version counters and delete markers to clients that
     *       encode a column name and datum or write time into the key to those
     *       that will filter based on inspection of the value associated with
     *       the key, e.g., only values having some attribute.
     * 
     * @todo if we allow the filter to cause mutations (e.g., deleting matching
     *       entries) then we have to examine the operation to determine whether
     *       or not we need to use the {@link #txService} or the
     *       {@link #readService}
     */
    public ResultSet rangeQuery(long tx, String name, int partitionId,
            byte[] fromKey, byte[] toKey, int capacity, int flags)
            throws InterruptedException, ExecutionException {

        if( name == null ) throw new IllegalArgumentException();
        
        final RangeQueryTask task = new RangeQueryTask(tx, name, partitionId,
                fromKey, toKey, capacity, flags);

        final boolean isolated = tx != 0L;
        
        if(isolated) {
            
            return (ResultSet) readService.submit(task).get();
            
        } else {
            
            return (ResultSet) journal.serialize(task).get();
            
        }
        
    }
    
//    /**
//     * @todo if unisolated or isolated at the read-commit level, then the
//     *       operation really needs to be broken down by partition or perhaps by
//     *       index segment leaf so that we do not have too much latency during a
//     *       read (this could be done for rangeQuery as well).
//     * 
//     * @todo if fully isolated, then there is no problem running map.
//     * 
//     * @todo The definition of a row is different if using a key formed from the
//     *       column name, application key, and timestamp.
//     * 
//     * @todo For at least GOM we need to deserialize rows from byte[]s, so we
//     *       need to have the (de-)serializer to the application level value on
//     *       hand.
//     */
//    public void map(long tx, String name, byte[] fromKey, byte[] toKey,
//            IMapOp op) throws InterruptedException, ExecutionException {
//
//            if( name == null ) throw new IllegalArgumentException();
//            
//            if (tx == 0L)
//                throw new UnsupportedOperationException(
//                        "Unisolated context not allowed");
//            
//            int flags = 0; // @todo set to deliver keys + values for map op.
//            
//            ResultSet result = (ResultSet) txService.submit(
//                new RangeQueryTask(tx, name, fromKey, toKey, flags)).get();
//
//            // @todo resolve the reducer service.
//            IReducer reducer = null;
//            
//            op.apply(result.itr, reducer);
//            
//    }

    protected abstract class AbstractIndexTask implements Callable<Object> {
    
        /**
         * The transaction identifier -or- 0L iff the operation is not isolated
         * by a transaction.
         */
        protected final long startTime;
        /**
         * True iff the operation is isolated by a transaction.
         */
        protected final boolean isolated;
        /**
         * True iff the operation is not permitted to write.
         */
        protected final boolean readOnly;
        /**
         * The name of the index.
         */
        protected final String name;
        /**
         * The transaction object iff the operation is isolated by a transaction
         * and otherwise <code>null</code>. 
         */
        protected final ITx tx;
        
        protected AbstractIndexTask(long startTime,boolean readOnly,String name) {

            if(name==null) throw new IllegalArgumentException();
            
            this.startTime = startTime;
            
            this.isolated = startTime != 0L;
            
            this.readOnly = readOnly;
            
            this.name = name;

            if(startTime != 0L) {
                
                /*
                 * Isolated read.
                 */
                
                tx = journal.getTx(startTime);
                
                if (tx == null) {

                    throw new IllegalStateException("Unknown tx");
                    
                }
                
                if (!tx.isActive()) {
                    
                    throw new IllegalStateException("Tx not active");
                    
                }
                
            } else {
                
                /*
                 * Unisolated read.
                 */

                tx = null;
                
            }

        }

        /**
         * Implement the task behavior here.
         * 
         * @return The object that will be returned by {@link #call()} iff the
         *         operation succeeds.
         * 
         * @throws Exception
         *             The exception that will be thrown by {@link #call()} iff
         *             the operation fails.
         */
        abstract protected Object doTask() throws Exception;

        /**
         * Delegates the task behavior to {@link #doTask()}.
         * <p>
         * For an unisolated operation, this method provides safe commit iff the
         * task succeeds and otherwise invokes abort() so that partial task
         * executions are properly discarded. When possible, the original
         * exception is re-thrown so that we do not encapsulate the cause unless
         * it would violate our throws clause.
         * <p>
         * Commit and abort are NOT invoked for an isolated operation regardless
         * of whether the operation succeeds or fails.  It is the responsibility
         * of the "client" to commit or abort a transaction as it sees fit.
         */
        final public Object call() throws Exception {

            if(isolated) {
                
                return doTask();
                
            }
            
            try {

                Object result = doTask();

                if(!readOnly) {
                    
                    // commit (synchronous, immediate).
                    journal.commit();
                    
                }

                return result;

            } catch (RuntimeException ex) {

                // abort and do not masquerade the exception.
                journal.abort();

                throw ex;
                
            } catch (Exception ex) {

                // abort and do not masquerade the exception.
                journal.abort();

                throw ex;

            } catch (Throwable t) {

                // abort and masquerade the Throwable.
                journal.abort();

                throw new RuntimeException(t);

            }

        }

    }
    
    /**
     * Abstract class for tasks on an index partition.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public abstract class AbstractIndexPartitionTask extends AbstractIndexTask {
        
        protected final int partitionId;
        protected final String indexName;
        private IIndexWithCounter index = null; // lazily set.
                
        public AbstractIndexPartitionTask(long startTime, boolean readOnly,
                String name, int partitionId) {
            
            super(startTime,readOnly,name);
            
            this.partitionId = partitionId;

            /*
             * Note: -1 indicates an unpartitioned index.
             */
            this.indexName = (partitionId == -1 ? name : getIndexPartitionName(
                    name, partitionId));
            
        }
        
        /**
         * Return an appropriate view of the index for the operation. When the
         * operation is isolated by a transaction, then the index will be
         * isolated by the transaction. When the operation is read-only, the
         * index will be read-only.
         * 
         * @return The index partition or <code>null</code> if the named index
         *         does not exist or the identifed index partition is not mapped
         *         onto the {@link DataService}.
         * 
         * @exception NoSuchIndexException
         *                if the named index does not exist at the time that the
         *                operation is executed.
         * 
         * @exception NoSuchIndexPartitionException
         *                if the named index is a partitioned index but the
         *                partition identifier specified in the operation does
         *                not correspond to a partition that is mapped onto this
         *                {@link DataService}.
         * 
         * @exception IllegalArgumentException
         *                if the partition identifier is non-zero for an
         *                unpartitioned index.
         */
        final public IIndexWithCounter getIndex() {
            
            if(index != null) {
                
                // Cached value.
                return index;
                
            }

            // the name of the index partition.
            final String name = this.indexName;
            
            final IIndexWithCounter tmp;
            
            if (isolated) {

                /*
                 * isolated operation.
                 */
                
                final IIsolatedIndex isolatedIndex = (IIsolatedIndex) tx
                        .getIndex(name);
                
                if (isolatedIndex == null) {

                    throw new NoSuchIndexPartitionException(name, partitionId);
                    
                }
                
                 tmp = (IIndexWithCounter) isolatedIndex;

            } else {

                /*
                 * unisolated operation.
                 */
                
                final BTree unisolatedIndex = (BTree) journal.getIndex(name);

                if(unisolatedIndex == null) {
                    
                    throw new NoSuchIndexPartitionException(name, partitionId);
                    
                }
                
                if(readOnly) {
                    
                    tmp = new ReadOnlyIndex( unisolatedIndex );
                    
                } else {
                    
                    tmp = unisolatedIndex;
                    
                }
                
            }

            index = tmp;
            
            return index;
            
        }

        /**
         * Verify that the key lies within the partition.
         * 
         * @param pmd
         *            The partition.
         * @param key
         *            The key.
         * 
         * @exception RuntimeException
         *                if the key does not lie within the partition.
         * 
         * @todo could be moved into {@link UnisolatedBTreePartition}
         */
        protected void checkPartition(PartitionMetadataWithSeparatorKeys pmd,
                byte[] key) {

            assert pmd != null;
            assert key != null;
            
            final byte[] leftSeparatorKey = pmd.getLeftSeparatorKey();
            
            final byte[] rightSeparatorKey = pmd.getRightSeparatorKey();
            
            if (BytesUtil.compareBytes(key, leftSeparatorKey ) < 0) {

                throw new RuntimeException("KeyBeforePartition");
                
            }
            
            if (rightSeparatorKey != null && BytesUtil.compareBytes(key, rightSeparatorKey) >= 0) {
                
                throw new RuntimeException("KeyAfterPartition");
                
            }
            
        }
        
    }
    
    /**
     * Abstract base class for index management tasks (these tasks are all
     * unisolated).
     * <p>
     * Note: Tasks that register and unregister indices MUST commit before
     * the change is visible via {@link IJournal#getIndex(String)}
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected abstract class AbstractUnisolatedIndexManagementTask extends AbstractIndexTask {

        protected AbstractUnisolatedIndexManagementTask(boolean readOnly, String name) {
        
            super(0L/*unisolated*/,readOnly, name );
            
        }

    }

    /**
     * Register a {@link UnisolatedBTree} under the given name and having the
     * given index UUID.
     */
    protected class RegisterIndexTask extends AbstractUnisolatedIndexManagementTask {

        protected final UUID indexUUID;
        protected final Class cls;
        protected final Object config;
        
        public RegisterIndexTask(String name, UUID indexUUID, String className,
                Object config) {

            super(false/*readOnly*/,name);
            
            if (indexUUID == null)
                throw new IllegalArgumentException();

            if( className == null)
                throw new IllegalArgumentException();
            
            final Class cls;
            
            try {
                // @todo review choice of class loader.
                cls = Class.forName(className);
                
            } catch(Exception ex) {
                
                throw new RuntimeException(ex);
                
            }
            
            if (!BTree.class.isAssignableFrom(cls)) {

                throw new IllegalArgumentException("Class does not extend: "
                        + BTree.class);
                
            }
            
            this.indexUUID = indexUUID;
            
            this.cls = cls;

            this.config = config;
            
        }
        
        /**
         * Registers the named index.
         * 
         * @return The newly created index.
         * 
         * @exception IndexExistsException
         *                if there is already an index registered with the same
         *                name and a different index UUID.
         */
        public Object doTask() throws Exception {

            IIndex ndx = journal.getIndex(name);
            
            if(ndx != null) {
                
                if(!ndx.getIndexUUID().equals(indexUUID)) {
                    
                    throw new IndexExistsException(name);
                    
                }

                return ndx;
                
            }

            if(cls.equals(UnisolatedBTree.class)) {

                ndx = journal.registerIndex(name, new UnisolatedBTree(journal,
                        indexUUID));

                log.info(getServiceUUID() + " - registered unpartitioned index: name=" + name
                        + ", indexUUID=" + indexUUID);
                
            } else if(cls.equals(UnisolatedBTreePartition.class)) {

                UnisolatedBTreePartition.Config tmp = (UnisolatedBTreePartition.Config) config;

                final int partitionId = tmp.pmd.getPartitionId();

                ndx = journal.registerIndex(name, new UnisolatedBTreePartition(
                        journal, indexUUID, tmp));

                log.info(getServiceUUID() + " - registered partitioned index: name=" + name
                        + ", partitionId=" + partitionId + ", indexUUID="
                        + indexUUID);
                
            } else {
                
                /*
                 * @todo Don't know how to configure this kind of btree.  The
                 * registration should be based on the configuration object.
                 */
                throw new UnsupportedOperationException();
                
            }

            assert ndx != null;
            
            assert journal.getIndex(name) != null;
            
            return ndx;
            
        }
        
    }

    protected class DropIndexTask extends AbstractUnisolatedIndexManagementTask {

        public DropIndexTask(String name) {

            super(false/*readOnly*/,name);

        }

        public Object doTask() throws Exception {

            journal.dropIndex(name);
            
            return null;
     
        }
        
    }
    
//    /**
//     * Maps a new index partition onto this data service.
//     * <p>
//     * Note: This method will initialize the partition-local counter to zero(0).
//     * When a partition fails over from one {@link DataService} to another it is
//     * already "mapped" in the persistent data on the journal and this method is
//     * NOT invoked. This method is only invoked when an index partition is being
//     * created for the first time and mapped onto a specific {@link DataService}.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    protected class MapIndexPartitionTask extends AbstractUnisolatedIndexManagementTask {
//        
//        private final PartitionMetadataWithSeparatorKeys pmd;
//        
//        public MapIndexPartitionTask(String name, PartitionMetadataWithSeparatorKeys pmd) {
//            
//            super(false,name);
//           
//            if (pmd == null)
//                throw new IllegalArgumentException();
//            
//            this.pmd = pmd;
//            
//        }
//        
//        public Object doTask() throws Exception {
//
//            PartitionedUnisolatedBTree ndx = (PartitionedUnisolatedBTree) journal
//                    .getIndex(name);
//            
//            if (ndx == null) {
//
//                throw new NoSuchIndexException(name);
//                
//            }
//
//            ndx.map(pmd);
//            
//            return null;
//            
//        }
//        
//    }
//    
//    /**
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    protected class UnmapIndexPartitionTask extends AbstractUnisolatedIndexManagementTask {
//        
//        private final int partitionId;
//        
//        public UnmapIndexPartitionTask(String name, int partitionId) {
//            
//            super(false,name);
//           
//            this.partitionId = partitionId;
//            
//        }
//        
//        public Object doTask() throws Exception {
//
//            PartitionedUnisolatedBTree ndx = (PartitionedUnisolatedBTree) journal
//                    .getIndex(name);
//            
//            if (ndx == null) {
//
//                throw new NoSuchIndexException(name);
//                
//            }
//
//            ndx.unmap(partitionId);
//            
//            return null;
//            
//        }
//        
//    }
    
    /**
     * Abstract class for tasks that execute batch api operations. There are
     * various concrete subclasses, each of which MUST be submitted to the
     * appropriate service for execution.
     * <p>
     * Note: While this does verify that the first/last key are inside of the
     * specified index partition, it does not verify that the keys are sorted -
     * this is the responsibility of the client. Therefore it is possible that
     * an incorrect client providing unsorted keys could execute an operation
     * that read or wrote data on the data service that lay outside of the
     * indicated partitionId.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected abstract class AbstractBatchTask extends AbstractIndexPartitionTask {
        
        private final IBatchOperation op;
        
        public AbstractBatchTask(long startTime, boolean readOnly, String name,
                int partitionId, IBatchOperation op) {

            super(startTime, readOnly, name, partitionId);
            
            if (op == null)
                throw new IllegalArgumentException();
            
            this.op = op;
            
        }
        
        final protected Object doTask() throws Exception {
        
            IIndexWithCounter ndx = getIndex();
            
            final int ntuples = op.getTupleCount();
            
            final byte[][] keys = op.getKeys();

            if(ndx instanceof UnisolatedBTreePartition) {

                /*
                 * If this is an index partition, then test the keys against the
                 * separator keys for the partition. All client keys must lie
                 * within the partition ( left <= key < right ).
                 */
                
                PartitionMetadataWithSeparatorKeys pmd = ((UnisolatedBTreePartition) ndx)
                        .getPartitionMetadata();
             
                checkPartition(pmd, keys[0]);
                
                checkPartition(pmd, keys[ntuples-1]);

            }
            
            if( op instanceof BatchContains ) {

                ndx.contains((BatchContains) op);
                
            } else if( op instanceof BatchLookup ) {

                ndx.lookup((BatchLookup) op);

            } else if( op instanceof BatchInsert ) {

                ndx.insert((BatchInsert) op);

            } else if( op instanceof BatchRemove ) {

                ndx.remove((BatchRemove) op);

            } else {

                /*
                 * Extension batch mutation operation.
                 * 
                 * @todo range check against the partitionId.
                 */ 
                op.apply(ndx);
                
            }
            
            return null;
            
        }
        
    }

    /**
     * Resolves the named index against the transaction in order to provide
     * appropriate isolation for reads, read-committed reads, or writes.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @see ITx
     * 
     * @todo In order to allow multiple clients to do work on the same
     *       transaction at once, we need a means to ensure that the same
     *       transaction is not assigned to more than one thread in the
     *       {@link DataService#txService}. In the absence of clients imposing
     *       a protocol among themselves for this purpose, we can simply
     *       maintain a mapping of transactions to threads. If a transaction is
     *       currently bound to a thread (its callable task is executing) then
     *       the current thread must wait. This protocol can be easily
     *       implemented using thread local variables.<br>
     *       Note: it is possible for this protocol to result in large numbers
     *       of worker threads blocking, but as long as each worker thread makes
     *       progress it should not be possible for the thread pool as a whole
     *       to block.
     */
    protected class TxBatchTask extends AbstractBatchTask {
        
        private final ITx tx;

        public TxBatchTask(long startTime, boolean readOnly, String name,
                int partitionId, IBatchOperation op) {
            
            super(startTime, readOnly, name, partitionId, op);
            
            assert startTime != 0L;
            
            tx = journal.getTx(startTime);
            
            if (tx == null) {

                throw new IllegalStateException("Unknown tx");
                
            }
            
            if (!tx.isActive()) {
                
                throw new IllegalStateException("Tx not active");
                
            }
            
        }

    }
 
    /**
     * Class used for unisolated operations.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    abstract protected class AbstractUnisolatedBatchTask extends AbstractBatchTask {

        public AbstractUnisolatedBatchTask(boolean readOnly, String name,
                int partitionId, IBatchOperation op) {
            
            super(0L/*unisolated*/,readOnly,name,partitionId, op);
            
        }
                
    }

    /**
     * Class used for unisolated <em>read</em> operations.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class UnisolatedReadBatchTask extends AbstractUnisolatedBatchTask {

        public UnisolatedReadBatchTask(String name, int partitionId, IBatchOperation op) {
            
            super(true/*readOnly*/,name,partitionId, op);
            
        }
        
    }

    /**
     * Class used for unisolated <em>write</em> operations. This class
     * performs the necessary handshaking with the journal to discard partial
     * writes in the event of an error during processing and to commit after a
     * successful write operation, thereby providing the ACID contract for an
     * unisolated write.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class UnisolatedBatchReadWriteTask extends AbstractUnisolatedBatchTask {

        public UnisolatedBatchReadWriteTask(String name, int partitionId, IBatchOperation op) {
            
            super(false/*readOnly*/,name,partitionId, op);
            
        }

    }

    /**
     * Task for running a rangeCount operation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class RangeCountTask extends AbstractIndexPartitionTask {

        private final byte[] fromKey;
        private final byte[] toKey;
        
        public RangeCountTask(long startTime, String name, int partitionId,
                byte[] fromKey, byte[] toKey) {
            
            super(startTime,true/*readOnly*/,name,partitionId);
            
            this.fromKey = fromKey;
            this.toKey = toKey;
            
        }

        public Object doTask() throws Exception {
            
            return new Integer(getIndex().rangeCount(fromKey, toKey));
            
        }
        
    }

    /**
     * Task for running a rangeQuery operation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class RangeQueryTask extends AbstractIndexPartitionTask {

        private final byte[] fromKey;
        private final byte[] toKey;
        private final int capacity;
        private final int flags;
        
        public RangeQueryTask(long startTime, String name, int partitionId,
                byte[] fromKey, byte[] toKey, int capacity, int flags) {

            super(startTime,true/*readOnly*/,name,partitionId);
            
            this.fromKey = fromKey;
            this.toKey = toKey;
            this.capacity = capacity;
            this.flags = flags;
            
        }
        
        public Object doTask() throws Exception {
            
            final boolean sendKeys = (flags & KEYS) != 0;
            
            final boolean sendVals = (flags & VALS) != 0;
            
            /*
             * setup iterator since we will visit keys and/or values in the key
             * range.
             */
            return new ResultSet(getIndex(), fromKey, toKey, capacity,
                    sendKeys, sendVals);
            
        }
        
    }

    /**
     * Abstract class for tasks that execute {@link IProcedure} operations.
     * There are various concrete subclasses, each of which MUST be submitted to
     * the appropriate service for execution.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected abstract class AbstractProcedureTask extends AbstractIndexPartitionTask {
        
        protected final IProcedure proc;
        
        public AbstractProcedureTask(long startTime, boolean readOnly,
                String name, int partitionId, IProcedure proc) {

            super(startTime, readOnly, name, partitionId);
            
            assert proc != null;
            
            this.proc = proc;
            
        }
        
        final public Object doTask() throws Exception {

            return proc.apply(getIndex());
                        
        }

    }

    /**
     * Resolves the named index against the transaction in order to provide
     * appropriate isolation for reads, read-committed reads, or writes.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @see ITx
     * 
     * @todo In order to allow multiple clients to do work on the same
     *       transaction at once, we need a means to ensure that the same
     *       transaction is not assigned to more than one thread in the
     *       {@link DataService#txService}. In the absence of clients imposing
     *       a protocol among themselves for this purpose, we can simply
     *       maintain a mapping of transactions to threads. If a transaction is
     *       currently bound to a thread (its callable task is executing) then
     *       the current thread must wait. This protocol can be easily
     *       implemented using thread local variables.<br>
     *       Note: it is possible for this protocol to result in large numbers
     *       of worker threads blocking, but as long as each worker thread makes
     *       progress it should not be possible for the thread pool as a whole
     *       to block.
     */
    protected class TxProcedureTask extends AbstractProcedureTask {
        
//        private final ITx tx;

        public TxProcedureTask(long startTime, boolean readOnly, String name,
                int partitionId, IProcedure proc) {
            
            super(startTime, readOnly, name, partitionId, proc);
            
//            assert startTime != 0L;
//            
//            tx = journal.getTx(startTime);
//            
//            if (tx == null) {
//
//                throw new IllegalStateException("Unknown tx");
//                
//            }
//            
//            if (!tx.isActive()) {
//                
//                throw new IllegalStateException("Tx not active");
//                
//            }
            
        }

    }
    
    /**
     * Abstract class for unisolated procedures.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class AbstractUnisolatedProcedureTask extends AbstractProcedureTask {

        public AbstractUnisolatedProcedureTask(boolean readOnly, String name,
                int partitionId, IProcedure proc) {
            
            super(0L/* unisolated */, readOnly, name, partitionId, proc);
            
        }

    }

    /**
     * Class used for unisolated <em>read</em> operations.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class UnisolatedReadProcedureTask extends AbstractUnisolatedProcedureTask {

        public UnisolatedReadProcedureTask(String name, int partitionId, IProcedure proc) {
            
            super(true/* readOnly */, name, partitionId, proc);
            
        }

    }

    /**
     * Class used for unisolated <em>write</em> operations. This class
     * performs the necessary handshaking with the journal to discard partial
     * writes in the event of an error during processing and to commit after a
     * successful write operation, thereby providing the ACID contract for an
     * unisolated write.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class UnisolatedReadWriteProcedureTask extends AbstractUnisolatedProcedureTask {

        public UnisolatedReadWriteProcedureTask(String name, int partitionId, IProcedure proc) {
            
            super(false/*readOnly*/,name,partitionId, proc);
            
        }
        
    }

    /**
     * @todo IResourceTransfer is not implemented.
     */
    public void sendResource(String filename, InetSocketAddress sink) {
        throw new UnsupportedOperationException();
    }

    /*
     * Exceptions.
     */
    
    public static class NoSuchIndexException extends RuntimeException {

        /**
         * 
         */
        private static final long serialVersionUID = 6124193775040326194L;

        /**
         * @param message The index name.
         */
        public NoSuchIndexException(String message) {
            super(message);
        }

    }
    
    public static class NoSuchIndexPartitionException extends NoSuchIndexException {

        /**
         * 
         */
        private static final long serialVersionUID = -4626663314737025524L;

        /**
         * @param name
         *            The index name.
         * @param partitionId
         *            The partition identifier.
         */
        public NoSuchIndexPartitionException(String name, int partitionId) {

            super(name+", partitionId="+partitionId);
            
        }

    }

    public static class IndexExistsException extends IllegalStateException {

        /**
         * 
         */
        private static final long serialVersionUID = -2151894480947760726L;

        /**
         * @param message The index name.
         */
        public IndexExistsException(String message) {
            super(message);
        }


    }
    
    public static class IndexPartitionExistsException extends IndexExistsException {

        /**
         * 
         */
        private static final long serialVersionUID = -7272309604448771916L;

        /**
         * @param name
         *            The index name.
         * @param partitionId
         *            The partition identifier.
         */
        public IndexPartitionExistsException(String name, int partitionId) {

            super(name+", partitionId="+partitionId);
            
        }

    }

}
