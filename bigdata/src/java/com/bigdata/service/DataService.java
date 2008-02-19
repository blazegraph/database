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
/*
 * Created on Mar 14, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IEntryFilter;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IIndexProcedure;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.IReadOnlyOperation;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.ResultSet;
import com.bigdata.io.ByteBufferInputStream;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ConcurrentJournal;
import com.bigdata.journal.DropIndexTask;
import com.bigdata.journal.ITransactionManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.journal.ProcedureTask;
import com.bigdata.journal.RegisterIndexTask;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.JournalMetadata;
import com.bigdata.mdi.ResourceState;
import com.bigdata.rawstore.IBlock;

/**
 * An implementation of a network-capable {@link IDataService}. The service is
 * started using the {@link DataServer} class. Operations are submitted using
 * {@link ConcurrentJournal#submit(AbstractTask)} and will run with the
 * appropriate concurrency controls as imposed by that method.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see DataServer, which is used to start this service.
 * 
 * @todo The data service should redirect clients if an index partition has been
 *       moved (shed) while a client has a lease.
 * 
 * @todo Participate in 1-phase (local) and 2-/3- phrase (distributed) commits
 *       with an {@link ITransactionManager} service. The data service needs to
 *       notify the {@link ITransactionManager} each time an isolated writer
 *       touches a named index so that the transaction manager can build up the
 *       set of resources that must be locked during the validate/commit phrase.
 * 
 * @todo Write benchmark test to measure interhost transfer rates. Should be
 *       100Mbits/sec (~12M/sec) on a 100BaseT switched network. With full
 *       duplex in the network and the protocol, that rate should be
 *       bidirectional. Can that rate be sustained with a fully connected
 *       bi-directional transfer?
 * 
 * @todo RPC requests are currently made via RPC using JERI. While you can elect
 *       to use the TCP/NIO server via configuration options (see
 *       http://java.sun.com/products/jini/2.0.1/doc/api/net/jini/jeri/tcp/package-summary.html),
 *       there will still be a thread allocated per concurrent RPC and no
 *       throttling will be imposed by JERI.
 *       <p>
 *       The present design of the {@link IDataService} API requires that a
 *       server thread be dedicated to each request against that interface - in
 *       this way it exactly matches the RPC semantics supported by JERI. The
 *       underlying reason is that the RPC calls are all translated into
 *       {@link Future}s when the are submitted via
 *       {@link ConcurrentJournal#submit(AbstractTask)}. The
 *       {@link DataService} itself then invokes {@link Future#get()} in order
 *       to await the completion of the request and return the response (object
 *       or thrown exception).
 *       <p>
 *       A re-design based on an asynchronous response from the server could
 *       remove this requirement, thereby allowing a handful of server threads
 *       to handle a large volume of concurrent client requests. The design
 *       would use asynchronous callback to the client via JERI RPC calls to
 *       return results, indications that the operation was complete, or
 *       exception information. A single worker thread on the server could
 *       monitor the various futures and RPC clients when responses become
 *       available or on request timeout.
 *       <p>
 *       See {@link NIODataService}, which contains some old code that can be
 *       refactored for an NIO interface to the data service.
 *       <p>
 *       Another option to throttle requests is to use a blocking queue to
 *       throttle the #of tasks that are submitted to the data service. Latency
 *       should be imposed on threads submitting tasks as the queue grows in
 *       order to throttle clients. If the queue becomes full
 *       {@link RejectedExecutionException} will be thrown, and the client will
 *       have to handle that. In contrast, if the queue never blocks and never
 *       imposes latency on clients then it is possible to flood the data
 *       service with requests, even through they will be processed by no more
 *       than {@link ConcurrentJournal.Options#WRITE_SERVICE_MAXIMUM_POOL_SIZE}
 *       threads.
 * 
 * @todo Review JERI options to support secure RMI protocols. For example, using
 *       SSL or an SSH tunnel. For most purposes I expect bigdata to operate on
 *       a private network, but replicate across gateways is also a common use
 *       case. Do we have to handle it specially?
 */
abstract public class DataService implements IDataService,
        IWritePipeline, IResourceTransfer, IServiceShutdown {

    protected Journal journal;

    public static final Logger log = Logger.getLogger(DataService.class);

    /**
     * Options understood by the {@link DataService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends com.bigdata.journal.Options {
        
    }
    
    /**
     * 
     * @param properties
     */
    public DataService(Properties properties) {
        
        journal = new DataServiceJournal(properties);

    }

    /**
     * Polite shutdown does not accept new requests and will shutdown once
     * the existing requests have been processed.
     */
    public void shutdown() {
        
        journal.shutdown();
        
    }
    
    /**
     * Shutdown attempts to abort in-progress requests and shutdown as soon
     * as possible.
     */
    public void shutdownNow() {

        journal.shutdownNow();
        
    }

    /**
     * The unique identifier for this data service.
     * 
     * @return The unique data service identifier.
     */
    public abstract UUID getServiceUUID() throws IOException;
    
    /*
     * ITxCommitProtocol.
     */
    
    public long commit(long tx) throws IOException {
        
        setupLoggingContext();
        
        try {
        
            // will place task on writeService and block iff necessary.
            return journal.commit(tx);
        
        } finally {
            
            clearLoggingContext();
            
        }
        
    }

    public void abort(long tx) throws IOException {

        setupLoggingContext();

        try {

            // will place task on writeService iff read-write tx.
            journal.abort(tx);
            
        } finally {
            
            clearLoggingContext();
            
        }
        
    }

    /*
     * IDataService.
     */
    
    /**
     * Forms the name of the index corresponding to a partition of a named
     * scale-out index as <i>name</i>#<i>partitionId</i>.
     * <p>
     * Another advantage of this naming scheme is that index partitions are just
     * named indices and all of the mechanisms for operating on named indices
     * and for concurrency control for named indices apply automatically. Among
     * other things, this means that different tasks can write concurrently on
     * different partitions of the same named index on a given
     * {@link DataService}.
     * 
     * @return The name of the index partition.
     */
    public static final String getIndexPartitionName(String name,
            int partitionId) {

        if (name == null) {

            throw new IllegalArgumentException();
            
        }

        if (partitionId == -1) {

            // Not a partitioned index.
            return name;
            
        }
        
        return name + "#" + partitionId;

    }
    
    /**
     * Return true iff the value identifies a read-only transaction known to
     * this data service.
     * 
     * @param startTime
     *            The transaction identifier.
     * 
     * @return True iff that is a read-only transaction.
     * 
     * @exception IllegalStateException
     *                if the transaction identifier is not active.
     */
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

    public String getStatistics() throws IOException {
        
        StringBuilder sb = new StringBuilder();
        
        sb.append("dataService: uuid=" + getServiceUUID());

        sb.append("\n");

        sb.append(journal.getStatistics());
        
        return sb.toString();
        
    }

    /**
     * Sets up the {@link MDC} logging context. You should do this on every
     * client facing point of entry and then call {@link #clearLoggingContext()}
     * in a <code>finally</code> clause. You can extend this method to add
     * additional context.
     * <p>
     * This implementation add the "serviceUUID" parameter to the {@link MDC}.
     * The serviceUUID is, in general, assigned asynchronously by the service
     * registrar. Once the serviceUUID becomes available it will be added to the
     * {@link MDC}. This datum can be injected into log messages using
     * %X{serviceUUID} in your log4j pattern layout.
     */
    protected void setupLoggingContext() {

        try {
            
            // Note: This _is_ a local method call.
            
            UUID serviceUUID = getServiceUUID();
            
            // Will be null until assigned by the service registrar.
            
            if (serviceUUID == null) {

                return;
                
            }
            
            // Add to the logging context for the current thread.
            
            MDC.put("serviceUUID", serviceUUID.toString());

        } catch(Throwable t) {
            /*
             * Ignore.
             */
        }
        
    }

    /**
     * Clear the logging context.
     */
    protected void clearLoggingContext() {
        
        MDC.remove("serviceUUID");
        
    }

    public void registerIndex(String name, IndexMetadata metadata)
            throws IOException, InterruptedException, ExecutionException {

        setupLoggingContext();

        try {

            if (metadata == null)
                throw new IllegalArgumentException();

            journal.submit(new RegisterIndexTask(journal, name, metadata))
                    .get();
        
        } finally {
            
            clearLoggingContext();
            
        }

    }
    
    public void dropIndex(String name) throws IOException,
            InterruptedException, ExecutionException {

        setupLoggingContext();
        
        try {
        
            journal.submit(new DropIndexTask(journal, name)).get();

        } finally {
            
            clearLoggingContext();
            
        }

    }
   
    public IndexMetadata getIndexMetadata(String name) throws IOException {

        setupLoggingContext();
        
        try {

            final IIndex ndx = journal.getIndex(name);
            
            if(ndx == null) {
                
                return null;
                
            }
            
            return ((BTree)ndx).getIndexMetadata();
            
        } finally {
            
            clearLoggingContext();
            
        }
        
    }

    public String getStatistics(String name) throws IOException {

        setupLoggingContext();
        
        try {

            final IIndex ndx = journal.getIndex(name);
            
            if(ndx == null) {
                
                throw new NoSuchIndexException(name);
                
            }
            
            return ((BTree)ndx).getStatistics();
            
        } finally {
            
            clearLoggingContext();
            
        }
        
    }
    
    public Object submit(long tx, String name, IIndexProcedure proc)
            throws InterruptedException, ExecutionException {

        setupLoggingContext();

        try {
    
            final boolean readOnly = proc instanceof IReadOnlyOperation;
    
            // submit the procedure and await its completion.
    
            return journal.submit(
                    new ProcedureTask(journal, tx, readOnly, name, proc)).get();
        
        } finally {
            
            clearLoggingContext();
            
        }

    }

    public ResultSet rangeIterator(long tx, String name, byte[] fromKey,
            byte[] toKey, int capacity, int flags, IEntryFilter filter)
            throws InterruptedException, ExecutionException {

        setupLoggingContext();
        
        try {

            if( name == null ) throw new IllegalArgumentException();
            
            final RangeIteratorTask task = new RangeIteratorTask(journal, tx,
                    name, fromKey, toKey, capacity, flags, filter);
    
            // submit the task and wait for it to complete.
            return (ResultSet) journal.submit(task).get();
        
        } finally {
            
            clearLoggingContext();
            
        }
            
    }

    /**
     * Task for running a rangeIterator operation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class RangeIteratorTask extends AbstractTask {

        private final byte[] fromKey;
        private final byte[] toKey;
        private final int capacity;
        private final int flags;
        private final IEntryFilter filter;
        
        public RangeIteratorTask(ConcurrentJournal journal, long startTime,
                String name, byte[] fromKey, byte[] toKey, int capacity,
                int flags, IEntryFilter filter) {

            super(journal, startTime,
                    (flags & IRangeQuery.REMOVEALL) == 0/* readOnly */, name);

            this.fromKey = fromKey;
            this.toKey = toKey;
            this.capacity = capacity;
            this.flags = flags;
            this.filter = filter; // MAY be null.

        }

        public Object doTask() throws Exception {

            return new ResultSet(getIndex(getOnlyResource()), fromKey, toKey,
                    capacity, flags, filter);

        }
        
    }

    /**
     * @todo this operation should be able to abort an
     *       {@link IBlock#inputStream() read} that takes too long or if there
     *       is a need to delete the resource.
     */
    public IBlock readBlock(IResourceMetadata resource, final long addr) {
    
        if(resource.isJournal()) {
            
            // @todo support multiple journals (overflow).
            
            // @todo efficient (stream-based) read from the journal (IBlockStore API).
            
            return new IBlock() {

                public long getAddress() {
                    return addr;
                }

                // @todo reuse buffers
                public InputStream inputStream() {

                    ByteBuffer buf = journal.read(addr);
                    
                    return new ByteBufferInputStream(buf);
                    
                }

                public int length() {
                    
                    return journal.getByteCount(addr);
                    
                }
                
            };
            
        } else {
            
            /*
             * @todo read from index segments. make sure that we do not
             * double-open index segments. in fact, all we need to do for this
             * is obtain the index segment file store and we can read the block
             * directly from that (the index segment itself does not need to be
             * open).
             */
            
            throw new UnsupportedOperationException();
            
        }
        
    }
    
    /**
     * @todo IResourceTransfer is not implemented.
     */
    public void sendResource(String filename, InetSocketAddress sink) {
    
        throw new UnsupportedOperationException();
        
    }
    
    /**
     * 
     * FIXME Either this class or its outer class MUST discover the
     * {@link ITransactionManager} and delegate the methods on that interface to
     * that service.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class DataServiceJournal extends Journal {

        /**
         * @param properties
         */
        public DataServiceJournal(Properties properties) {
            
            super(properties);
            
        }
                
    }
    
}
