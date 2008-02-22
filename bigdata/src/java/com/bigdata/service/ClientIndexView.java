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
 * Created on Apr 22, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.BatchContains;
import com.bigdata.btree.BatchInsert;
import com.bigdata.btree.BatchLookup;
import com.bigdata.btree.BatchRemove;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.ICounter;
import com.bigdata.btree.IEntryFilter;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IIndexProcedure;
import com.bigdata.btree.ILinearList;
import com.bigdata.btree.IParallelizableIndexProcedure;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.IResultHandler;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.LongAggregator;
import com.bigdata.btree.RangeCountProcedure;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.AbstractKeyArrayIndexProcedure.ResultBitBuffer;
import com.bigdata.btree.AbstractKeyArrayIndexProcedure.ResultBuffer;
import com.bigdata.btree.IIndexProcedure.IIndexProcedureConstructor;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.IPartitionMetadata;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionMetadata;
import com.bigdata.mdi.PartitionMetadataWithSeparatorKeys;
import com.bigdata.mdi.MetadataIndex.MetadataIndexMetadata;

/**
 * A client-side view of an index.
 * <p>
 * 
 * @todo We should be able to transparently use either a hash mod N approach to
 *       distributed index partitions or a dynamic approach based on overflow.
 *       This could even be decided on a per-index basis. The different
 *       approaches would be hidden by appropriate implementations of this
 *       class.
 * 
 * @todo the client does not attempt to obtain a new data service proxy for a
 *       partition if the current proxy fails (no failover).
 * 
 * @todo the client does not notice deleted index partitions (which can arise
 *       from index partition joins). this case needs to be handled in any code
 *       that visits partitions using the entryIndex in the metadata index since
 *       some entries may be "deleted".
 * 
 * @todo It is a design goal (not yet obtained) that the client should interact
 *       with an interface rather than directly with {@link MetadataIndex} so
 *       that this code can look identical regardless of whether the metadata
 *       index is local (embedded) or remote. (We do in fact use the same code
 *       for both scenarios, but only because, at this time, the metadata index
 *       is fully cached in the remote case).
 * 
 * @todo note that it is possible (though uncommon) for an index partition split
 *       or join to occur during operations. Figure out how I want to handle
 *       that, and how I want to handle that with transactional isolation
 *       (presumably a read-only historical view of the metadata index would be
 *       used - in which case we need to pass the tx into the getPartition()
 *       method).
 * 
 * @todo cache leased information about index partitions of interest to the
 *       client. The cache will be a little tricky since we need to know when
 *       the client does not possess a partition definition. Index partitions
 *       are defined by the separator key - the first key that lies beyond that
 *       partition. the danger then is that a client will presume that any key
 *       before the first leased partition is part of that first partition. To
 *       guard against that the client needs to know both the separator key that
 *       represents the upper and lower bounds of each partition. If a lookup in
 *       the cache falls outside of any known partitions upper and lower bounds
 *       then it is a cache miss and we have to ask the metadata service for a
 *       lease on the partition. the cache itself is just a btree data structure
 *       with the proviso that some cache entries represent missing partition
 *       definitions (aka the lower bounds for known partitions where the left
 *       sibling partition is not known to the client).
 * 
 * @todo develop and offer policies for handling index partitions that are
 *       unavailable at the time of the request (continued operation during
 *       partial failure).
 * 
 * @todo support isolated views, share cached data service information between
 *       isolated and unisolated views.
 * 
 * @todo support failover metadata service discovery.
 * 
 * @todo Use a weak-ref cache with an LRU (or hard reference cache) to retain
 *       cached {@link IPartitionMetadata}. The client needs access by {
 *       indexName, key } to obtain a {@link ServiceID} for a
 *       {@link DataService} and then needs to translate the {@link ServiceID}
 *       to a data service using the {@link #dataServiceMap}.
 * 
 * @todo test the {@link ILinearList} API against a key-range partitioned index.
 * 
 * @todo test the {@link ILinearList} API against an index partition formed from
 *       more than one index resources.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ClientIndexView implements IIndex {

    protected static final transient Logger log = Logger
            .getLogger(ClientIndexView.class);
    

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    private final IBigdataFederation fed;

    public IBigdataFederation getFederation() {
        
        return fed;
        
    }
    
    /**
     * 
     */
    private static final String NON_BATCH_API = "Non-batch API";

    /**
     * This may be used to disable the non-batch API, which is quite convenient
     * for location code that needs to be re-written to use
     * {@link IIndexProcedure}s.
     * 
     * @todo make this a config option and also support this option on the
     *       bigdata clients.
     */
    private final boolean batchOnly = false;

    /**
     * The default capacity for the {@link #rangeIterator(byte[], byte[])}
     */
    final int capacity = 50000;

    private final long tx;

    /**
     * The transaction identifier -or- {@link ITx#UNISOLATED} iff the index view
     * is unisolated -or- <code>- timestamp</code> for a historical read of
     * the most recent committed state not later than <i>timestamp</i>.
     */
    public long getTx() {
        
        return tx;
        
    }
    
    private final String name;
    
    /**
     * The name of the scale-out index.
     */
    public String getName() {
        
        return name;
        
    }

    /**
     * The metadata for the {@link MetadataIndex} that manages the scale-out
     * index. The metadata template for the managed scale-out index is available
     * as a field on this object.
     */
    private final MetadataIndexMetadata metadataIndexMetadata;
    
    /**
     * Obtain the proxy for a metadata service. if this instance fails, then we
     * can always ask for a new instance for the same federation (failover).
     */
    final protected IMetadataService getMetadataService() {
        
        return fed.getMetadataService();
        
    }
    
    /**
     * @todo define an interface and use a read-only view of the metadata index.
     *       this approach is forward looking to when the metadata index is only
     *       partly materialized on the client.
     */
    final protected IMetadataIndex getMetadataIndex() {
        
        return fed.getMetadataIndex(name);
        
    }
    
    /**
     * Create a view on a scale-out index.
     * 
     * @param fed
     *            The federation containing the index.
     * @param tx
     *            The transaction identifier -or- {@link ITx#UNISOLATED} iff the
     *            index view is unisolated -or- <code>- timestamp</code> for a
     *            historical read of the most recent committed state not later
     *            than <i>timestamp</i>.
     * @param name
     *            The index name.
     */
    public ClientIndexView(IBigdataFederation fed, long tx, String name,
            MetadataIndexMetadata metadataIndexMetadata) {

        if (fed == null)
            throw new IllegalArgumentException();

        if (name == null)
            throw new IllegalArgumentException();
        
        this.fed = fed;

        this.tx = tx;
        
        this.name = name;
        
        this.metadataIndexMetadata = metadataIndexMetadata;
        
    }

    /**
     * Metadata for the {@link MetadataIndex} that manages the scale-out index
     * (cached).
     */
    public MetadataIndexMetadata getMetadataIndexMetadata() {
     
        return metadataIndexMetadata;
        
    }
    
    /**
     * The metadata for the managed scale-out index.
     */
    public IndexMetadata getIndexMetadata() {

        return getMetadataIndexMetadata().getManagedIndexMetadata();

    }

    /**
     * @todo report on both the metadata index and the individual index
     *       partitions. If we parallelize the index partition reporting then we
     *       need to use a thread-safe {@link StringBuffer} rather than a
     *       {@link StringBuilder}.
     */
    public String getStatistics() {

        StringBuilder sb = new StringBuilder();

        sb.append("scale-out index: name="+name);

        /*
         * Statistics for the metadata index.
         */
        try {
            
            String _name = MetadataService.getMetadataIndexName(name);
            
            sb.append("\n" + _name + " : "
                    + getMetadataService().getStatistics(_name));

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        /*
         * Statistics for the index partitions (at least those that are cached
         * by the client).
         */
        {
            
            final IMetadataIndex mdi = getMetadataIndex();
            
            final IEntryIterator itr = mdi.rangeIterator(null, null);
            
            while(itr.hasNext()) {
                
                final ITuple tuple = itr.next();
                
                final PartitionMetadata pmd = (PartitionMetadata) SerializerUtil.deserialize(tuple.getValueStream());
                
                final String _name = DataService.getIndexPartitionName(name, pmd.getPartitionId());
                
                sb.append("\npartition: " + _name);
                sb.append("\nresources: " + Arrays.toString(pmd.getResources()));
                sb.append("\ndataServices: " + Arrays.toString(pmd.getDataServices()));
                
                String _stats;
                try {
                    _stats = getDataService(pmd).getStatistics( _name );
                } catch (IOException e) {
                    _stats = "Could not obtain index partition statistics: "+e.toString();
                }
                
                sb.append( "\nindexStats: "+_stats);
                
            }
            
        }

        
        return sb.toString();
        
    }

    public boolean contains(byte[] key) {
        
        if (batchOnly)
            throw new RuntimeException(NON_BATCH_API);
        else
            log.warn(NON_BATCH_API);

        final BatchContains proc = new BatchContains(//
                1, // n,
                0, // offset
                new byte[][] { key } // keys
        );

        final boolean[] ret = ((ResultBitBuffer) submit(key, proc)).getResult();

        return ret[0];
        
    }
    
    /**
     * Counters are local to a specific index partition and are only available
     * to unisolated procedures.
     * 
     * @throws UnsupportedOperationException
     *             always
     */
    public ICounter getCounter() {
        
        throw new UnsupportedOperationException();
        
    }
    
    public byte[] insert(byte[] key, byte[] value) {

        if (batchOnly)
            throw new RuntimeException(NON_BATCH_API);
        else
            log.warn(NON_BATCH_API);

        final BatchInsert proc = new BatchInsert(//
                1, // n,
                0, // offset
                new byte[][] { key }, // keys
                new byte[][] { value }, // vals
                true // returnOldValues
        );

        final byte[][] ret = ((ResultBuffer)submit(key, proc)).getResult();

        return ret[0];

    }

    public byte[] lookup(byte[] key) {

        if (batchOnly)
            throw new RuntimeException(NON_BATCH_API);
        else
            log.warn(NON_BATCH_API);

        final BatchLookup proc = new BatchLookup(//
                1, // n,
                0, // offset
                new byte[][] { key } // keys
        );

        final byte[][] ret = ((ResultBuffer)submit(key, proc)).getResult();

        return ret[0];

    }

    public byte[] remove(byte[] key) {

        if (batchOnly)
            throw new RuntimeException(NON_BATCH_API);
        else
            log.warn(NON_BATCH_API);

        final BatchRemove proc = new BatchRemove(//
                1, // n,
                0, // offset
                new byte[][] { key }, // keys
                true // returnOldValues
        );

        final byte[][] ret = ((ResultBuffer)submit(key, proc)).getResult();

        return ret[0];

    }

    /*
     * All of these methods need to divide up the operation across index
     * partitions.
     */
    
    /**
     * Returns the sum of the range count for each index partition spanned by
     * the key range.
     */
    public long rangeCount(byte[] fromKey, byte[] toKey) {

        final LongAggregator handler = new LongAggregator();
        
        final RangeCountProcedure proc = new RangeCountProcedure(fromKey, toKey);

        submit(fromKey, toKey, proc, handler);

        return handler.getResult();
        
    }

    /**
     * An {@link IEntryIterator} that kinds the use of a series of
     * {@link ResultSet}s to cover all index partitions spanned by the key
     * range.
     */
    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey) {
        
        return rangeIterator(fromKey, toKey, capacity, IRangeQuery.KEYS
                | IRangeQuery.VALS/* flags */, null/*filter*/);
        
    }


    /**
     * Identifies the index partition(s) that are spanned by the key range query
     * and maps an iterator across each index partition. The iterator buffers
     * responses up to the specified capacity and a follow up iterator request
     * is automatically issued if the iterator has not exhausted the key range
     * on a given index partition. Once the iterator is exhausted on a given
     * index partition it is then applied to the next index partition spanned by
     * the key range.
     */
    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey,
            int capacity, int flags, IEntryFilter filter ) {

        if (capacity == 0) {

            capacity = this.capacity;

        }
        
        return new PartitionedRangeQueryIterator(this, tx, fromKey, toKey,
                capacity, flags, filter);
        
    }

    public Object submit(byte[] key, IIndexProcedure proc) {

        // The index partition for that key.
        final PartitionMetadata pmd = getPartition(key);

        // The data service for that index partition.
        final IDataService dataService = getDataService(pmd);

        /*
         * Submit procedure to that data service.
         */
        try {

            if (INFO) {

                log.info("Submitting " + proc.getClass() + " to partition"
                        + pmd + " on dataService=" + dataService);
                
            }

//            final String name = DataService.getIndexPartitionName(this.name,
//                    pmd.getPartitionId());

            // required to get the result back from the procedure.
            final IResultHandler resultHandler = new IdentityHandler();
            
            // setup proc to run on the thread pool in order to limit client parallelism
            final DataServiceProcedureTask task = new DataServiceProcedureTask(
                    new Split(pmd,0,0), dataService, proc, resultHandler );
            
            // submit procedure and await completion.
            getThreadPool().submit(task).get();
            
            // the singleton result.
            Object result = resultHandler.getResult();
            
//            Object result = dataService.submit(tx, name, proc);

            return result;

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }
    
    /**
     * Hands back the object visited for a single index partition.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    @SuppressWarnings("unused")
    private static class IdentityHandler implements IResultHandler<Object, Object> {

        int nvisited = 0;
        private Object ret;
        
        public void aggregate(Object result, Split split) {

            if (nvisited != 0) {
            
                /*
                 * You can not use this handler if the procedure is mapped over
                 * more than one split.
                 */
                
                throw new UnsupportedOperationException();

            }
            
            this.ret = result;
            
            nvisited++;
            
            
        }

        public Object getResult() {

            return ret;
            
        }
        
    }
    
    public void submit(byte[] fromKey, byte[] toKey,
            final IIndexProcedure proc, final IResultHandler resultHandler) {

        if (proc == null)
            throw new IllegalArgumentException();
        
        final int fromIndex, toIndex;
        {

            int a[] = getMetadataIndex().findIndices(fromKey, toKey);

            fromIndex = a[0];

            toIndex = a[1];
            
        }

        // #of index partitions on which the procedure will be run.
        final int nsplits = toIndex - fromIndex + 1;
        
        // true iff the procedure is known to be parallelizable.
        final boolean parallel = proc instanceof IParallelizableIndexProcedure;
        
        log.info("Procedure " + proc.getClass().getName()
                + " will run on " + nsplits + " index partitions in "
                + (parallel ? "parallel" : "sequence"));
        
        /*
         * Create an ordered list of the tasks to be executed.
         */
        
        final ArrayList<Callable<Void>> tasks = new ArrayList<Callable<Void>>(nsplits);
        
        for (int index = fromIndex; index <= toIndex; index++) {

            final IPartitionMetadata pmd = getPartitionAtIndex(index);

            assert pmd != null : "No partition metadata? name=" + name
                    + " @ index=" + index;
            
            final IDataService dataService = getDataService(pmd);
        
            assert dataService != null : "No data service? pmd="+pmd;
            
            final Split split = new Split(pmd, 0/* fromIndex */, 0/* toIndex */);
            
            tasks.add(new DataServiceProcedureTask(split, dataService,
                    proc, resultHandler));
            
        }

        if (parallel) {

            /*
             * Map procedure across the index partitions in parallel.
             */
            
            runParallel(tasks);
            
        } else {

            /*
             * Map procedure across the index partitions in sequence.
             */

            runSequence(tasks);
            
        }
        
    }

    /**
     * The thread pool exposed by {@link IBigdataClient#getThreadPool()}
     */
    protected ExecutorService getThreadPool() {

        return fed.getClient().getThreadPool();

    }
    
    /**
     * Maps a set of {@link DataServiceProcedureTask} tasks across the index
     * partitions in parallel.
     * 
     * @param tasks
     *            The tasks.
     * 
     * @todo add counters for the #of procedures run and the execution time for
     *       those procedures. add counters for the #of splits and the #of
     *       tuples in each split, as well as the total #of tuples.
     */
    protected void runParallel(ArrayList<Callable<Void>> tasks) {
        
        final ExecutorService service = getThreadPool();
        
        int nfailed = 0;
        
        try {

            final List<Future<Void>> futures = service.invokeAll(tasks);
            
            final Iterator<Future<Void>> itr = futures.iterator();
           
            int i = 0;
            
            while(itr.hasNext()) {
                
                final Future<Void> f = itr.next();
                
                try {
                    
                    f.get();
                    
                } catch (ExecutionException e) {
                    
                    DataServiceProcedureTask task = (DataServiceProcedureTask) tasks
                            .get(i);
                    
                    log.error("Execution failed: partition=" + task.pmd, e);
                    
                    nfailed++;
                    
                }
                
            }
            
        } catch (InterruptedException e) {

            throw new RuntimeException("Interrupted: "+e);

        }
        
        if (nfailed > 0) {
            
            throw new RuntimeException("Execution failed: ntasks="
                    + tasks.size() + ", nfailed=" + nfailed);
            
        }

    }

    /**
     * Maps a set of {@link DataServiceProcedureTask} tasks across the index
     * partitions in strict sequence. The tasks are run on the
     * {@link #getThreadPool()} so that sequential tasks never increase the
     * total burden placed by the client above the size of that thread pool.
     * 
     * @param tasks
     *            The tasks.
     * 
     * @todo add counters for the #of procedures run and the execution time for
     *       those procedures. add counters for the #of splits and the #of
     *       tuples in each split, as well as the total #of tuples.
     */
    protected void runSequence(List<Callable<Void>> tasks) {

        final ExecutorService service = getThreadPool();
        
        final Iterator<Callable<Void>> itr = tasks.iterator();

        while (itr.hasNext()) {

            final DataServiceProcedureTask task = (DataServiceProcedureTask) itr
                    .next();

            try {

                final Future<Void> f = service.submit(task);
                
                // await completion of the task.
                f.get();

            } catch (Exception e) {
        
                log.error("Execution failed: partition=" + task.pmd, e);

                throw new RuntimeException(e);

            }

        }

    }
    
    protected abstract class AbstractDataServiceTask<T> implements Callable<T> {

        protected final IPartitionMetadata pmd;
        protected final IDataService dataService;
        
        protected AbstractDataServiceTask(IPartitionMetadata pmd, IDataService dataService) {

            if (pmd == null)
                throw new IllegalArgumentException();
            
            if (dataService == null)
                throw new IllegalArgumentException();
            
            this.pmd = pmd;
            
            this.dataService = dataService;
            
        }

    }

    /**
     * Wraps an {@link IIndexProcedure} for submission to an
     * {@link ExecutorService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class DataServiceProcedureTask extends AbstractDataServiceTask<Void> {

        protected final Split split;
        protected final IIndexProcedure proc;
        protected final IResultHandler resultHandler;
        protected final String name;
        
        protected DataServiceProcedureTask(Split split,
                IDataService dataService, IIndexProcedure proc,
                IResultHandler resultHandler) {

            super(split.pmd, dataService);

            if (proc == null)
                throw new IllegalArgumentException();

//            if (resultHandler == null)
//                throw new IllegalArgumentException();

            this.split = split;
            
            this.proc = proc;

            this.resultHandler = resultHandler;

            final int partitionId = split.pmd.getPartitionId();
            
            // the name of the index partition.
            name = DataService.getIndexPartitionName(ClientIndexView.this.name,
                    partitionId);
            
        }

        @SuppressWarnings("unchecked")
        public Void call() throws Exception {
            
            Object result = dataService.submit(tx, name, proc);

            if (resultHandler != null) {

                resultHandler.aggregate(result, split);

            }

            return null;

        }

    }
    
    /**
     * The procedure will be transparently broken down and executed against each
     * index partitions spanned by its keys. If the <i>ctor</i> creates
     * instances of {@link IParallelizableIndexProcedure} then the procedure
     * will be mapped in parallel against the relevant index partitions.
     * 
     * @return The aggregated result of applying the procedure to the relevant
     *         index partitions.
     */
    public void submit(int n, byte[][] keys, byte[][] vals,
            IIndexProcedureConstructor ctor, IResultHandler aggregator) {

        if (ctor == null) {

            throw new IllegalArgumentException();
        
        }
        
//        if (aggregator == null) {
//
//            throw new IllegalArgumentException();
//            
//        }
        
        /*
         * Break down the data into a series of "splits", each of which will be
         * applied to a different index partition.
         */

        final List<Split> splits = splitKeys(n, keys);

        final int nsplits = splits.size();

        /*
         * Create the instances of the procedure for each split.
         */
        
        final ArrayList<Callable<Void>> tasks = new ArrayList<Callable<Void>>(
                nsplits);
        
        // assume true until proven otherwise.
        boolean parallel = true;
        {
         
            final Iterator<Split> itr = splits.iterator();

            while (itr.hasNext()) {

                final Split split = itr.next();

                final IDataService dataService = getDataService(split.pmd);

                final IIndexProcedure proc = ctor.newInstance(split.ntuples,
                        split.fromIndex, keys, vals);

                if (!(proc instanceof IParallelizableIndexProcedure)) {

                    parallel = false;

                }

                tasks.add(new DataServiceProcedureTask(split, dataService,
                        proc, aggregator));
                
            }
            
        }

        log.info("Procedures created by " + ctor.getClass().getName()
                + " will run on " + nsplits + " index partitions in "
                + (parallel ? "parallel" : "sequence"));
        
        if (parallel) {

            /*
             * Map procedure across the index partitions in parallel.
             */

            runParallel(tasks);

        } else {
            
            /*
             * sequential execution against of each split in turn.
             */

            runSequence(tasks);

        }

    }
    
    /**
     * Utility method to split a set of ordered keys into partitions based the
     * index partitions defined for a scale-out index.
     * <p>
     * Find the partition for the first key. Check the last key, if it is in the
     * same partition then then this is the simplest case and we can just send
     * the data along, perhaps breaking it down into smaller batches (note that
     * batch break points MUST respect the "row" identity for a sparse row
     * store, but we get that constraint by maintaining the index partition
     * boundaries in agreement with the split point constraints for the index).
     * <p>
     * Otherwise, perform a binary search on the remaining keys looking for the
     * index of the first key GTE the right separator key for that partition.
     * The batch for this partition is formed from all keys from the first key
     * for that partition up to but excluding the index position identified by
     * the binary search (if there is a match; if there is a miss, then the
     * binary search result needs to be converted into a key index and that will
     * be the last key for the current partition).
     * <p>
     * Examine the next key and repeat the process until all keys have been
     * allocated to index partitions.
     * 
     * @param ntuples
     *            The #of keys.
     * @param keys
     *            An array of keys. Each key is an interpreted as an unsigned
     *            byte[]. All keys must be non-null. The keys must be in sorted
     *            order.
     *            
     * @return The {@link Split}s that you can use to form requests based on
     *         the identified first/last key and partition identified by this
     *         process.
     * 
     * @see Arrays#sort(Object[], int, int, java.util.Comparator)
     * 
     * @see BytesUtil#compareBytes(byte[], byte[])
     */
    public List<Split> splitKeys(int ntuples, byte[][] keys) {
        
        if (ntuples <= 0)
            throw new IllegalArgumentException();
        
        List<Split> splits = new LinkedList<Split>();
        
        // start w/ the first key.
        int fromIndex = 0;

        while(fromIndex<ntuples) {
        
            // partition spanning that key.
            final PartitionMetadataWithSeparatorKeys pmd = getPartition(keys[fromIndex]);

            final byte[] rightSeparatorKey = pmd.getRightSeparatorKey();

            if (rightSeparatorKey == null) {

                /*
                 * The last index partition does not have an upper bound and
                 * will absorb any keys that order GTE to its left separator
                 * key.
                 */
                final int toIndex = ntuples;

                splits.add(new Split(pmd, fromIndex, toIndex));

                fromIndex = toIndex;

            } else {

                /*
                 * Otherwise this partition has an upper bound, so figure out
                 * the index of the last key that would go into this partition.
                 */
                int toIndex = BytesUtil.binarySearch(keys, fromIndex, ntuples
                        - fromIndex, rightSeparatorKey);

                if (toIndex < 0) {

                    toIndex = -toIndex - 1;

                }

                assert toIndex > fromIndex;

                splits.add(new Split(pmd, fromIndex, toIndex));

                fromIndex = toIndex;

            }

        }

        return splits;

    }

    /**
     * Return the metadata for the index partition which spans the given key.
     * 
     * @param key
     *            The key.
     *            
     * @return The index partition metadata.
     */
    public PartitionMetadataWithSeparatorKeys getPartition(byte[] key) {

        final IMetadataIndex mdi = fed.getMetadataIndex(name);

        final int index = mdi.findIndexOf(key);
        
        return getPartitionAtIndex(index);
        
//        final IPartitionMetadata pmd;
//        try {
//
//            /*
//             * The code from this point on is shared with getPartitionAtIndex() and
//             * also by some of the index partition tasks (CreatePartition for one).
//             */
//
//            if (index == -1)
//                return null;
//
//            /*
//             * The serialized index partition metadata record for the partition that
//             * spans the given key.
//             */
//            final byte[] val = mdi.valueAt(index);
//
//            /*
//             * The separator key that defines the left edge of that index partition
//             * (always defined).
//             */
//            final byte[] leftSeparatorKey = mdi.keyAt(index);
//
//            /*
//             * The separator key that defines the right edge of that index partition
//             * or [null] iff the index partition does not have a right sibling (a
//             * null has the semantics of no upper bound).
//             */
//            final byte[] rightSeparatorKey;
//
//            try {
//
//                rightSeparatorKey = mdi.keyAt(index + 1);
//
//            } catch (IndexOutOfBoundsException ex) {
//
//                rightSeparatorKey = null;
//
//            }
//            
//            pmd = (IPartitionMetadata) SerializerUtil.deserialize(val);
//
//            return new PartitionMetadataWithSeparatorKeys(leftSeparatorKey,
//                    pmd, rightSeparatorKey);
//
//        } catch (Exception ex) {
//
//            throw new RuntimeException(ex);
//
//        }

    }

    /**
     * @todo this is subject to concurrent modification of the metadata index
     *       would can cause the index to identify a different partition. client
     *       requests that use {@link #findIndexOfPartition(String, byte[])} and
     *       {@link #getPartitionAtIndex(String, int)} really need to refer to
     *       the same historical version of the metadata index (this effects
     *       range count and range iterator requests and to some extent batch
     *       operations that span multiple index partitions).
     */
    public PartitionMetadataWithSeparatorKeys getPartitionAtIndex(int index) {

        /*
         * The code from this point on is shared with getPartition()
         */

        if (index == -1)
            return null;

        final IMetadataIndex mdi = fed.getMetadataIndex(name);

        /*
         * The serialized index partition metadata record for the partition that
         * spans the given key.
         */
        final byte[] val = mdi.valueAt(index);

        /*
         * The separator key that defines the left edge of that index partition
         * (always defined).
         */
        final byte[] leftSeparatorKey = mdi.keyAt(index);

        /*
         * The separator key that defines the right edge of that index partition
         * or [null] iff the index partition does not have a right sibling (a
         * null has the semantics of no upper bound).
         */
        byte[] rightSeparatorKey;

        try {

            rightSeparatorKey = mdi.keyAt(index + 1);

        } catch (IndexOutOfBoundsException ex) {

            rightSeparatorKey = null;

        }

        final PartitionMetadata pmd = (PartitionMetadata) SerializerUtil
                .deserialize(val);

        return new PartitionMetadataWithSeparatorKeys(leftSeparatorKey, pmd,
                rightSeparatorKey);

    }

    /**
     * Resolve the data service to which the index partition is mapped.
     */
    public IDataService getDataService(IPartitionMetadata pmd) {

        final UUID [] dataServiceUUIDs = pmd.getDataServices();

        assert dataServiceUUIDs.length > 0: "No DataService UUIDs? : pmd="+pmd;

        final UUID serviceUUID = dataServiceUUIDs[0];

        assert serviceUUID != null : "DataService UUID is null? : pmd="+pmd;
        
        final IDataService dataService;

        try {

            dataService = fed.getClient().getDataService(serviceUUID);

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

        return dataService;

    }

    /**
     * This operation is not supported - the resource description of a scale-out
     * index would include all "live" resources in the corresponding
     * {@link MetadataIndex}.
     */
    public IResourceMetadata[] getResourceMetadata() {
        
        throw new UnsupportedOperationException();
        
    }

}
