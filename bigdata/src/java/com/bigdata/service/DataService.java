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

import org.apache.log4j.Logger;

import com.bigdata.btree.BatchContains;
import com.bigdata.btree.BatchInsert;
import com.bigdata.btree.BatchLookup;
import com.bigdata.btree.BatchRemove;
import com.bigdata.btree.IBatchBTree;
import com.bigdata.btree.IBatchOp;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ILinearList;
import com.bigdata.btree.IReadOnlyBatchOp;
import com.bigdata.btree.ISimpleBTree;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IAtomicStore;
import com.bigdata.journal.ICommitter;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
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
 * The {@link #opService} provides concurrency for unisolated reads.
 * <p>
 * Unisolated writes serialized using
 * {@link AbstractJournal#serialize(Callable)}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see DataServer, which is used to start this service.
 * 
 * @see NIODataService, which contains some old code that can be refactored for
 *      an NIO interface to the data service.
 * 
 * @todo should the data service monitor key ranges so that it can readily
 *       reject requests when the index is registered but the key lies outside
 *       of the range of an index partition mapped onto the data service? This
 *       probably needs to happen in order for the data service to be able to
 *       redirect clients if it sheds an index partition while the client has a
 *       lease.
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

    protected Journal journal;

    public static final transient Logger log = Logger
            .getLogger(DataService.class);
    
    /**
     * Pool of threads for handling unisolated reads.
     */
    final protected ExecutorService readService;
    
    /**
     * Pool of threads for handling concurrent transactions.
     */
    final protected ExecutorService txService;

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
        
    }

    /**
     * Polite shutdown does not accept new requests and will shutdown once
     * the existing requests have been processed.
     */
    public void shutdown() {
        
        readService.shutdown();
        
        txService.shutdown();
        
        journal.shutdown();
        
    }
    
    /**
     * Shutdown attempts to abort in-progress requests and shutdown as soon
     * as possible.
     */
    public void shutdownNow() {

        readService.shutdownNow();
        
        txService.shutdownNow();
        
        journal.close();
        
    }

    /**
     * The unique identifier for this data service - this is used mainly for log
     * messages.
     * 
     * @return The unique data service identifier.
     */
    protected abstract UUID getDataServiceUUID();
    
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
    
    private boolean isReadOnly(long startTime) {
        
        assert startTime != 0l;
        
        ITx tx = journal.getTx(startTime);
        
        if (tx == null) {

            throw new IllegalStateException("Unknown: tx=" + startTime);
            
        }
        
        return tx.isReadOnly();
        
    }

    public void registerIndex(String name, UUID indexUUID) throws IOException,
            InterruptedException, ExecutionException {

        journal.serialize(new RegisterIndexTask(name, indexUUID)).get();

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
    
//    public byte[] lookup(long tx, String name, byte[] key) throws IOException,
//            InterruptedException, ExecutionException {
//
//        byte[][] vals = batchLookup(tx, name, 1, new byte[][]{key});
//
//        return vals[0];
//        
//    }
    
    public byte[][] batchInsert(long tx, String name, int ntuples,
            byte[][] keys, byte[][] vals, boolean returnOldValues)
            throws InterruptedException, ExecutionException {

        BatchInsert op = new BatchInsert(ntuples, keys, vals);

        batchOp(tx, name, op);

        return returnOldValues ? (byte[][]) op.values : null;

    }

    public boolean[] batchContains(long tx, String name, int ntuples,
            byte[][] keys) throws InterruptedException, ExecutionException {
        
        BatchContains op = new BatchContains(ntuples, keys, new boolean[ntuples]);
        
        batchOp( tx, name, op );

        return op.contains;
        
    }
    
    public byte[][] batchLookup(long tx, String name, int ntuples, byte[][] keys)
            throws InterruptedException, ExecutionException {
        
        BatchLookup op = new BatchLookup(ntuples,keys,new byte[ntuples][]);
        
        batchOp(tx, name, op);
        
        return (byte[][])op.values;
        
    }
    
    public byte[][] batchRemove(long tx, String name, int ntuples,
            byte[][] keys, boolean returnOldValues)
            throws InterruptedException, ExecutionException {
        
        BatchRemove op = new BatchRemove(ntuples,keys,new byte[ntuples][]);
        
        batchOp(tx, name, op);
        
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
     * 
     * @todo it is possible to have concurrent execution of batch operations for
     *       distinct indices. In order to support this, the write thread would
     *       have to become a pool of N worker threads fed from a queue of
     *       operations. Concurrent writers can execute as long as they are
     *       writing on different indices. (Concurrent readers can execute as
     *       long as they are reading from a historical commit time.)
     */
    protected void batchOp(long tx, String name, IBatchOp op)
            throws InterruptedException, ExecutionException {
        
        if( name == null ) throw new IllegalArgumentException();
        
        if( op == null ) throw new IllegalArgumentException();
        
        final boolean isolated = tx != 0L;
        
        final boolean readOnly = (op instanceof IReadOnlyBatchOp)
                || (isolated && isReadOnly(tx));
        
        if(isolated) {
            
            txService.submit(new TxBatchTask(tx,name,op)).get();
            
        } else if( readOnly ) {
            
            readService.submit(new UnisolatedReadBatchTask(name,op)).get();
            
        } else {
            
            /*
             * Special case since incomplete writes MUST be discarded and
             * complete writes MUST be committed.
             */
            journal.serialize(new UnisolatedBatchReadWriteTask(name,op)).get();
            
        }
        
    }
    
    public Object submit(long tx, IProcedure proc) throws InterruptedException,
            ExecutionException {

        if( proc == null ) throw new IllegalArgumentException();
        
        final boolean isolated = tx != 0L;
        
        final boolean readOnly = proc instanceof IReadOnlyProcedure;
        
        if(isolated) {
            
            return txService.submit(new TxProcedureTask(tx,proc)).get();
            
        } else if( readOnly ) {
            
            /*
             * FIXME The IReadOnlyInterface is a promise that the procedure will
             * not write on an index (or anything on the store), but it is NOT a
             * guarentee. Consider removing that interface and the option to run
             * unisolated as anything but "read/write" since a "read-only" that
             * in fact attempted to write could cause problems with the index
             * data structure. Alternatively, examine other means for running a
             * "read-only" unisolated store that enforces read-only semantics.
             * 
             * For example, since the IIndexStore defines only a single method,
             * getIndex(String), we could provide an implementation of that
             * method that always selected a historical committed state for the
             * index. This would make writes impossible since they would be
             * rejected by the index object itself.
             * 
             * Fix this and write tests that demonstrate that writes are
             * rejected if the proc implements IReadOnlyProcedure.
             */
            
            return readService.submit(new UnisolatedReadProcedureTask(proc)).get();
            
        } else {
            
            /*
             * Special case since incomplete writes MUST be discarded and
             * complete writes MUST be committed.
             */
            return journal.serialize(new UnisolatedReadWriteProcedureTask(proc)).get();
            
        }

    }

    public int rangeCount(long tx, String name, byte[] fromKey, byte[] toKey)
            throws InterruptedException, ExecutionException {

        if (name == null)
            throw new IllegalArgumentException();

        final RangeCountTask task = new RangeCountTask(tx, name, fromKey, toKey);

        final boolean isolated = tx != 0L;

        if (isolated) {

            return (Integer) readService.submit(task).get();

        } else {
            // @todo do unisolated read operations need to get serialized?!?  check rangeQuery also.
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
    public ResultSet rangeQuery(long tx, String name, byte[] fromKey,
            byte[] toKey, int capacity, int flags) throws InterruptedException, ExecutionException {

        if( name == null ) throw new IllegalArgumentException();
        
        final RangeQueryTask task = new RangeQueryTask(tx, name, fromKey,
                toKey, capacity, flags);

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

    /**
     * 
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected abstract class AbstractIndexManagementTask implements Callable<Object> {

        protected final String name;

        protected AbstractIndexManagementTask(String name) {
        
            if(name==null) throw new IllegalArgumentException();

            this.name = name;
        
        }

    }

    protected class RegisterIndexTask extends AbstractIndexManagementTask {

        protected final UUID indexUUID;
        
        public RegisterIndexTask(String name,UUID indexUUID) {

            super(name);
            
            if(indexUUID==null) throw new IllegalArgumentException();
            
            this.indexUUID = indexUUID;
            
        }
        
        public Object call() throws Exception {

            IIndex ndx = journal.getIndex(name);
            
            if(ndx != null) {
                
                if(!ndx.getIndexUUID().equals(indexUUID)) {
                    
                    throw new IllegalStateException(
                            "Index already registered with that name and a different indexUUID");
                    
                }

                return ndx;
                
            }
            
            ndx = journal.registerIndex(name, new UnisolatedBTree(journal,
                    indexUUID));

            journal.commit();
            // @todo log the dataServiceID with each log message for this class.
            log.info("registeredIndex: "+name+", indexUUID="+indexUUID);
            
            return ndx;
            
        }
        
    }
    
    protected class DropIndexTask extends AbstractIndexManagementTask {

        public DropIndexTask(String name) {
            
            super(name);
            
        }
        
        public Object call() throws Exception {

            journal.dropIndex(name);
            
            journal.commit();
            
            return null;
            
        }
        
    }
    
    /**
     * Abstract class for tasks that execute batch api operations. There are
     * various concrete subclasses, each of which MUST be submitted to the
     * appropriate service for execution.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected abstract class AbstractBatchTask implements Callable<Object> {
        
        private final String name;
        private final IBatchOp op;
        
        public AbstractBatchTask(String name, IBatchOp op) {

            this.name = name;
            this.op = op;
            
        }

        abstract IIndex getIndex(String name);
        
        public Object call() throws Exception {

            IIndex ndx = getIndex(name);
            
            if (ndx == null)
                throw new IllegalStateException("Index not registered: " + name);

            if( op instanceof BatchContains ) {
                
                ndx.contains((BatchContains) op);
                
            } else if( op instanceof BatchLookup ) {

                ndx.lookup((BatchLookup) op);

            } else if( op instanceof BatchInsert ) {

                ndx.insert((BatchInsert) op);

            } else if( op instanceof BatchRemove ) {

                ndx.remove((BatchRemove) op);

            } else {

                // Extension batch mutation operation. 
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

        public TxBatchTask(long startTime, String name, IBatchOp op) {
            
            super(name,op);
            
            assert startTime != 0L;
            
            tx = journal.getTx(startTime);
            
            if (tx == null) {

                throw new IllegalStateException("Unknown tx");
                
            }
            
            if (!tx.isActive()) {
                
                throw new IllegalStateException("Tx not active");
                
            }
            
        }

        public IIndex getIndex(String name) {

            return tx.getIndex(name);

        }
        
    }
 
    /**
     * Class used for unisolated <em>read</em> operations.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class UnisolatedReadBatchTask extends AbstractBatchTask {

        public UnisolatedReadBatchTask(String name, IBatchOp op) {
            
            super(name,op);
            
        }
        
        public IIndex getIndex(String name) {
            
            return journal.getIndex(name);

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
    protected class UnisolatedBatchReadWriteTask extends UnisolatedReadBatchTask {

        public UnisolatedBatchReadWriteTask(String name, IBatchOp op) {
            
            super(name,op);
            
        }

        protected void abort() {
            
            journal.abort();
            
        }
        
        public Long call() throws Exception {

            try {

                super.call();
                
                // commit (synchronous, immediate).
                return journal.commit();

            } catch(Throwable t) {
            
                abort();
                
                throw new RuntimeException(t);
                
            }
            
        }
        
    }

    protected class RangeCountTask implements Callable<Object> {

//        // startTime or 0L iff unisolated.
//        private final long startTime;
        private final String name;
        private final byte[] fromKey;
        private final byte[] toKey;
        
        private final ITx tx;

        public RangeCountTask(long startTime, String name, byte[] fromKey,
                byte[] toKey) {
            
//            this.startTime = startTime;
            
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
            
            this.name = name;
            this.fromKey = fromKey;
            this.toKey = toKey;
            
        }
        
        public IIndex getIndex(String name) {
            
            if(tx==null) {
             
                return journal.getIndex(name);
                
            } else {

                return tx.getIndex(name);
                
            }

        }

        public Object call() throws Exception {
            
            IIndex ndx = getIndex(name);
            
            if(ndx==null) {
                
                throw new IllegalStateException("No such index: "+name);
                
            }

            return new Integer(ndx.rangeCount(fromKey, toKey));
            
        }
        
    }

    protected class RangeQueryTask implements Callable<Object> {

//        // startTime or 0L iff unisolated.
//        private final long startTime;
        private final String name;
        private final byte[] fromKey;
        private final byte[] toKey;
        private final int capacity;
        private final int flags;
        
        private final ITx tx;

        public RangeQueryTask(long startTime, String name, byte[] fromKey,
                byte[] toKey, int capacity, int flags) {
            
//            this.startTime = startTime;
            
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
                
//                if( tx.getIsolationLevel() == IsolationEnum.ReadCommitted ) {
//                    
//                    throw new UnsupportedOperationException("Read-committed not supported");
//                    
//                }
                
            } else {
                
                /*
                 * Unisolated read.
                 */

                tx = null;
                
            }
            
            this.name = name;
            this.fromKey = fromKey;
            this.toKey = toKey;
            this.capacity = capacity;
            this.flags = flags;
            
        }
        
        public IIndex getIndex(String name) {
            
            if(tx==null) {
             
                return journal.getIndex(name);
                
            } else {

                return tx.getIndex(name);
                
            }

        }

        public Object call() throws Exception {
            
            IIndex ndx = getIndex(name);
            
            if(ndx==null) {
                
                throw new IllegalStateException("No such index: "+name);
                
            }

            final boolean sendKeys = (flags & KEYS) != 0;
            
            final boolean sendVals = (flags & VALS) != 0;
            
            /*
             * setup iterator since we will visit keys and/or values in the key
             * range.
             */
            return new ResultSet(ndx, fromKey, toKey, capacity, sendKeys,
                    sendVals);
            
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
    protected abstract class AbstractProcedureTask implements Callable<Object> {
        
        protected final IProcedure proc;
        
        public AbstractProcedureTask(IProcedure proc) {

            this.proc = proc;
            
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
        
        private final ITx tx;

        public TxProcedureTask(long startTime, IProcedure proc) {
            
            super(proc);
            
            assert startTime != 0L;
            
            tx = journal.getTx(startTime);
            
            if (tx == null) {

                throw new IllegalStateException("Unknown tx");
                
            }
            
            if (!tx.isActive()) {
                
                throw new IllegalStateException("Tx not active");
                
            }
            
        }

        public Object call() throws Exception {

            return proc.apply(tx.getStartTimestamp(),tx);
                        
        }

    }
 
    /**
     * Class used for unisolated <em>read</em> operations.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class UnisolatedReadProcedureTask extends AbstractProcedureTask {

        public UnisolatedReadProcedureTask(IProcedure proc) {
            
            super(proc);
            
        }

        public Object call() throws Exception {

            return proc.apply(0L,journal);

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
    protected class UnisolatedReadWriteProcedureTask extends UnisolatedReadProcedureTask {

        public UnisolatedReadWriteProcedureTask(IProcedure proc) {
            
            super(proc);
            
        }

        protected void abort() {
            
            journal.abort();
            
        }
        
        public Object call() throws Exception {

            try {

                Object result = super.call();
                
                // commit (synchronous, immediate).
                journal.commit();
                
                return result;

            } catch(Throwable t) {
            
                abort();
                
                throw new RuntimeException(t);
                
            }
            
        }
        
    }

    /**
     * @todo IResourceTransfer is not implemented.
     */
    public void sendResource(String filename, InetSocketAddress sink) {
        throw new UnsupportedOperationException();
    }

}
