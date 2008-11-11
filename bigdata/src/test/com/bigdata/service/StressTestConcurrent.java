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
 * Created on May 23, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.proc.BatchInsert.BatchInsertConstructor;
import com.bigdata.btree.proc.BatchRemove.BatchRemoveConstructor;
import com.bigdata.journal.BasicExperimentConditions;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.DiskOnlyStrategy;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.journal.ValidationError;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.WormAddressManager;
import com.bigdata.resources.DefaultSplitHandler;
import com.bigdata.service.DataService.Options;
import com.bigdata.test.ExperimentDriver;
import com.bigdata.test.ExperimentDriver.IComparisonTest;
import com.bigdata.test.ExperimentDriver.Result;
import com.bigdata.util.NV;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.concurrent.QueueStatisticsTask;

/**
 * Test suite for concurrent operations on a {@link DataService}. A federation
 * consisting of a {@link MetadataService} and a single {@link DataService} is
 * started. A client is created, connects to the federation, and registers an
 * index the federation. A pool of threads is created for that client and
 * populated with a number of operations. The threads then write and read
 * concurrently using unisolated operations on the data services. This test can
 * be used to observe the throughput and queue depth of arising from a variety
 * of data service and client configurations.
 * 
 * @todo I have observed better performance with the {@link BufferMode#Disk}
 *       when compared to {@link BufferMode#Direct}. I expect that this is due
 *       to resource starvation on a laptop class platform. The fully buffered
 *       mode should be faster if there are sufficent resources available.
 * 
 * @todo The primary metrics reported by the test are elapsed time and
 *       operations per second. Compute the through put in terms of bytes per
 *       second for writes. This is interesting since it allows us to compare
 *       the effect of batch size on writes. Add parameterization for read vs
 *       write vs remove so that we can test the effect of batch size for
 *       operation profiles based on each of those kinds of operations.
 * 
 * @todo get the comparison support working. Parameterize the
 *       {@link DataService} configuration from the test suite so that we can
 *       test Disk vs Direct, forceCommit=No vs default, and other properties
 *       that might have interesting effects. These things can be directly
 *       manipulated in the mean time by editing the DataServer0.properties
 *       file.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StressTestConcurrent extends
        AbstractEmbeddedFederationTestCase implements IComparisonTest {

    /**
     * 
     */
    public StressTestConcurrent() {
    }

    /**
     * @param arg0
     */
    public StressTestConcurrent(String arg0) {
        super(arg0);
    }
    
    public Properties getProperties() {
        
        Properties properties = new Properties(super.getProperties());
        
//        properties.setProperty(Options.DATA_DIR,getName());

        // Make sure this test uses disk so that it can trigger overflows.
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk
                .toString());

        /*
         * Note: if we make the initial and maximum extent small so that we
         * trigger overflow a lot then we introduce a lot of overhead.
         */
        properties.setProperty(Options.INITIAL_EXTENT, ""+1*Bytes.megabyte);
        properties.setProperty(Options.MAXIMUM_EXTENT, ""+1*Bytes.megabyte);

        // enable moves (overrides value set in the test setup for the super class)
        properties.setProperty(Options.MAXIMUM_MOVES_PER_TARGET,Options.DEFAULT_MAXIMUM_MOVES_PER_TARGET);

        // performance counter sampling interval.
        properties.setProperty(Options.PERFORMANCE_COUNTERS_SAMPLE_INTERVAL,"5000");
        
        // performance counter reporting interval (should be the same as the sampling interval).
        properties.setProperty(IBigdataClient.Options.REPORT_DELAY,"5000");

        // load balancer update delay
        properties.setProperty(LoadBalancerService.Options.UPDATE_DELAY,"10000");

        // Note: another way to disable moves is to restrict the test to a single data service.
//        properties.setProperty(com.bigdata.service.EmbeddedBigdataFederation.Options.NDATA_SERVICES,"1");

        // disable overflow processing
//        properties.setProperty(Options.OVERFLOW_ENABLED,"false");
        
//        properties.setProperty(Options.CREATE_TEMP_FILE,"true");
        
        return properties;
        
    }
    
    public void setUpComparisonTest(Properties properties) throws Exception {

        super.setUp();
        
    }

    public void tearDownComparisonTest() throws Exception {

        super.tearDown();

    }

    /**
     * Test of N concurrent operations.
     * 
     * @todo run a performance analysis generating a graph of response time by
     *       queue length. the queue length can be the #of parallel clients but
     *       be sure to set up the {@link ClientIndexView} so that it does not
     *       cap the concurrency or it will skew the results. also note that the
     *       maximum possible parallelism will be capped by the #of index
     *       partitions and (if indices are not being split) by the #of indices.
     * 
     * @todo declare a variety of tests (a) overflow disabled; (b) w/ ground
     *       truth; (c) overflow enabled; (d) with ground truth. these probably
     *       need to be each in their own subclass in order to get the setup
     *       correct since the properties need to be overriden. See
     *       {@link #doComparisonTest(Properties)}.
     * 
     * @throws Exception
     */
    public void test_stressTest1() throws Exception {

        int nclients = 40;
        long timeout = 20; // 20 or 40
        int ntrials = 1000; // 10000
        int keyLen = 4; // @todo not used right now.
        int nops = 100; // 100
        double insertRate = .8d;
        int nindices = 10;
        boolean testCorrectness = true;

        doConcurrentClientTest(client, nclients, timeout, ntrials, keyLen,
                nops, insertRate, nindices, testCorrectness );

//        log.info("dataService0\n" + dataService0.getStatistics());
//       
//        if (dataService1 != null) {
//            
//            log.info("dataService1\n" + dataService1.getStatistics());
//            
//        }
        
    }

    /**
     * A stress test with a pool of concurrent clients.
     * 
     * @param client
     *            The client.
     * 
     * @param timeout
     *            The #of seconds before the test will terminate (ignored if
     *            <i>testCorrectness := true</i>).
     * 
     * @param nclients
     *            The #of concurrent clients.
     * 
     * @param ntrials
     *            The #of batch (remote) operations to execute.
     * 
     * @param keyLen
     *            The length of the random unsigned byte[] keys used in the
     *            operations. The longer the keys the less likely it is that
     *            there will be a write-write conflict (that concurrent txs will
     *            write on the same key).
     * 
     * @param nops
     *            The #of rows in each operation.
     * 
     * @param insertRate
     *            The rate of insert operations (inserting <i>nops</i> tuples)
     *            in [0.0:1.0]. The balance of the operations will remove
     *            <i>nops</i> tuples.
     * 
     * @param nindices
     *            The #of different indices to which the operation will be
     *            applied. The tasks will be generated modulo <i>nindices</i>.
     *            When nindices is greater than one, there is increased
     *            likelyhood of tasks running concurrently before the first
     *            split. Regardless of the value of nindices, after a scale-out
     *            index has been split the liklelyhood of concurrent writers
     *            goes up significantly.
     * 
     * @param testCorrectness
     *            When <code>true</code>, ground truth will be maintained and
     *            verified against the post-condition of the index(s) under
     *            test. This option may be used to verify index partition
     *            split/join/move semantics and the correctness of
     *            {@link ClientIndexView} views. All operations on a ground
     *            truth index are serialized (all operations may be serialized
     *            if the ground truth indices are all backed by the same store)
     *            so this option can not be used when you are doing performance
     *            testing.
     * 
     * @todo Note: When <i>nindices</i> is high the setup time on this test is
     *       quite large since the indices are registered sequentially rather
     *       than using parallelism. Run the index registration tasks in a
     *       thread pool to cut down the test setup latency.
     * 
     * @todo factor out the operation to be run.
     * 
     * @todo factor out the setup for the federation so that we can test
     *       embedded or distributed (either one process, many processes, or
     *       many hosts). Setup of a distributed federation is more complex,
     *       whether on one host or many hosts, since it requires Jini
     *       configurations for each service. Finally, if the test index exists
     *       then it must be dropped.
     *       <p>
     *       In a distributed configuration, the clients can also be distributed
     *       which raises the complexity further. In all, we really need a means
     *       to setup a cluster as a bigdata federation based on a master
     *       configuration. E.g., something to generate the individual
     *       configuration files from a master description of the federation and
     *       something to deploy those files together with the necessary
     *       software onto the cluster. SCA probably addresses this issue.
     * 
     * @todo It would be especially nice to have this run against a cluster so
     *       that we could characterize throughput as a function of the #of
     *       machines, but that also requires a distributed client otherwise the
     *       client may become the bottleneck.
     * 
     * @todo introduce a ground truth index on a local (temp) store and verify
     *       the state of the scale-out index. access to the ground truth index
     *       would have to be synchronized, e.g., using a lock for mutation and
     *       for verification.
     * 
     * @todo parameterize for random deletes and writes and parameterize those
     *       operations so that they can be made likely to force a join or split
     *       of an index partition.
     */
    static public Result doConcurrentClientTest(final IBigdataClient client,
            final int nclients, final long timeout, final int ntrials,
            final int keyLen, final int nops, final double insertRate, final int nindices,
            boolean testCorrectness) throws InterruptedException, IOException {
        
        // The basename of the scale-out index(s) for the test.
        final String basename = "testIndex";

        // connect to the federation.
        final IBigdataFederation federation = client.connect();
        
        /*
         * Register the scale-out index(s).
         */
        assert nindices > 0;
        
        final IIndex[] index = new IIndex[nindices];
        final IIndex[] groundTruth = new IIndex[nindices];
        final IRawStore[] groundTruthStore = new IRawStore[nindices];
        final ReentrantLock[] lock = new ReentrantLock[nindices];
        
        for(int i=0; i<nindices; i++) {
            
            final String name = basename+i;
            final UUID indexUUID = UUID.randomUUID();
            final int entryCountPerSplit = 400;
            final double overCapacityMultiplier = 1.5;
            final int minimumEntryCountPerSplit = 100;
            {

                final IndexMetadata indexMetadata = new IndexMetadata(name,
                        indexUUID);

                // The threshold below which we will try to join index
                // partitions.
                ((DefaultSplitHandler) indexMetadata.getSplitHandler())
                        .setMinimumEntryCount(minimumEntryCountPerSplit);

                // The target #of index entries per partition.
                ((DefaultSplitHandler) indexMetadata.getSplitHandler())
                        .setEntryCountPerSplit(entryCountPerSplit);

                // Overcapacity multipler before an index partition will be
                // split.
                ((DefaultSplitHandler) indexMetadata.getSplitHandler())
                        .setOverCapacityMultiplier(overCapacityMultiplier);

                // must support delete markers
                indexMetadata.setDeleteMarkers(true);

                // register the scale-out index, creating a single index
                // partition.
                federation.registerIndex(indexMetadata);

                if(testCorrectness) {
                    
                    /*
                     * Setup a distinct backing store for the ground truth for
                     * each index and a lock to serialize access to that index.
                     * This allows concurrency if you start with more than one
                     * index or after an index has been split.
                     */
                    
                    groundTruthStore[i] = new TemporaryRawStore(
                            WormAddressManager.SCALE_UP_OFFSET_BITS);

                    IndexMetadata md = indexMetadata.clone();
                    
                    // turn off delete markers for the ground truth index.
                    md.setDeleteMarkers(false);
                    
                    groundTruth[i] = BTree.create(groundTruthStore[i], md);

                    lock[i] = new ReentrantLock();
                    
                }
                
            }
        
            index[i] = federation.getIndex(name, ITx.UNISOLATED);
        
        }
                
        final ThreadPoolExecutor executorService = (ThreadPoolExecutor) Executors
                .newFixedThreadPool(nclients, DaemonThreadFactory
                        .defaultThreadFactory());

        // will log the behavior of this queue.
        {        
        
            ScheduledExecutorService sampleService = Executors
                    .newSingleThreadScheduledExecutor(DaemonThreadFactory
                            .defaultThreadFactory());

            final long initialDelay = 0; // initial delay in ms.
            final long delay = 1000; // delay in ms.
            final TimeUnit unit = TimeUnit.MILLISECONDS;

            QueueStatisticsTask queueLengthTask = new QueueStatisticsTask(
                    "testExecutorService", executorService);

            sampleService.scheduleWithFixedDelay(queueLengthTask, initialDelay,
                    delay, unit);
            
        }
        
        Collection<Callable<Void>> tasks = new HashSet<Callable<Void>>(); 

        for(int i=0; i<ntrials; i++) {

            final int k = i % nindices;
            
            tasks.add(new Task(index[k], keyLen, nops, insertRate, groundTruth[k], lock[k]));
            
        }

        /*
         * Run the M transactions on N clients.
         */
        
        final long begin = System.currentTimeMillis();
        
        log.warn("Starting tasks on client");

        /*
         * Note: We have to wait for all tasks to complete in order to test
         * correctness since the ground truth data can otherwise differ from the
         * data successfully committed on the database.
         */
        final List<Future<Void>> results = executorService.invokeAll(tasks,
                testCorrectness ? Long.MAX_VALUE : timeout, TimeUnit.SECONDS);
        
        final long elapsed = System.currentTimeMillis() - begin;
        
        log.warn("Examining task results: elapsed="+elapsed);
        
        final Iterator<Future<Void>> itr = results.iterator();
        
        int nfailed = 0; // #of operations that failed
        int ncommitted = 0; // #of operations that committed.
        int nuncommitted = 0; // #of operations that did not complete in time.
        int ntimeout = 0;
        int ninterrupted = 0;
        LinkedList<Exception> failures = new LinkedList<Exception>();
        
        while(itr.hasNext()) {

            Future<Void> future = itr.next();
            
            if(future.isCancelled()) {
                
                nuncommitted++;
                
                continue;
                
            }

            try {

                // Don't wait
                future.get(0L,TimeUnit.MILLISECONDS);
                
                ncommitted++;
                
            } catch(ExecutionException ex ) {
                
                // Validation errors are allowed and counted as aborted txs.
                
                if(ex.getCause() instanceof ValidationError) {
                
                    nfailed++;
                    
                } else {
                
                    // Other kinds of exceptions are errors.

                    log.error("Not expecting: "+ex.getMessage());
                    
                    failures.add(ex);
                    
                }
                
            } catch (InterruptedException e) {

                ninterrupted++;
                
            } catch (TimeoutException e) {
                
                ntimeout++;
                
            }
            
        }
        
        /*
         * Note: This can cause exceptions to be thrown out of the write
         * executor service since the concurrency manager will have been
         * shutdown but asynchronous overflow processing is doubtless still
         * running some tasks.
         */
        executorService.shutdownNow();

        Result ret = new Result();

        // @todo the groupCommitCount is per data service so we can't easily report it here.
//        final long groupCommitCount = dataService.getConcurrencyManager().getWriteService().getGroupCommitCount();
        
        ret.put("ncommitted",""+ncommitted);
        ret.put("nfailed",""+nfailed);
        ret.put("nuncommitted", ""+nuncommitted);
        ret.put("ntimeout", ""+ntimeout);
        ret.put("ninterrupted", ""+ninterrupted);
        ret.put("elapsed(ms)", ""+elapsed);
        ret.put("operations/sec", ""+(ncommitted * 1000 / elapsed));
//        ret.put("groupCommitCount", ""+groupCommitCount);
//        ret.put("avgCommitGroupSize", ""+(ncommitted/groupCommitCount));
//        ret.put("#overflow", ""+((ResourceManager)dataService.getResourceManager()).getOverflowCount());
        ret.put("failures", ""+(failures.size()));

        System.err.println(ret.toString(true/*newline*/));

        if(!failures.isEmpty()) {
            
            System.err.println("failures:\n"+Arrays.toString(failures.toArray()));
            
            fail("There were "+failures.size()+" failed tasks for unexpected causes");
            
        }
        
        if(testCorrectness) {
            
//            /*
//             * Make sure that any asynchronous overflow processing has completed
//             * normally. 
//             */
//            {
//                
//                // all known data services.
//                final UUID[] dataServiceUUID = federation.getDataServiceUUIDs(0);
//                
//                for(int i=0; i<dataServiceUUID.length; i++) {
//                    
//                    // await async overflow processing ...
//                    
//                }
//                
//            }
            
            /*
             * For each index, verify its state against the corresponding ground
             * truth index.
             */
            
            for(int i=0; i<nindices; i++) {

                final String name = basename+i;

                final IIndex expected = groundTruth[i];
                
                System.err.println("Validating: "+name+" #groundTruthEntries="+groundTruth[i].rangeCount(null, null));
                
                /*
                 * Note: This uses an iterator based comparison so that we can
                 * compare a local index without delete markers and a key-range
                 * partitioned index with delete markers.
                 * 
                 * Note: test on the UNISOLATED as well as the READ_COMMITTED
                 * since this can turn up problems with consistent reads by the
                 * read committed operation.
                 */

                assertSameEntryIterator(expected, federation.getIndex(name, ITx.UNISOLATED));
                
                assertSameEntryIterator(expected, federation.getIndex(name, ITx.READ_COMMITTED));
                
                /*
                 * Release the ground truth index and the backing store.
                 */

                groundTruth[i] = null;
                
                groundTruthStore[i].destroy();
                
            }
            
            System.err.println("Validated "+nindices+" indices against ground truth.");
            
        }
        
        return ret;
       
    }
    
    /**
     * Run an unisolated operation.
     */
    public static class Task implements Callable<Void> {

        private final IIndex ndx;
//        private final int keyLen;
        private final int nops;
        private final double insertRate;
        private final IIndex groundTruth;
        private final ReentrantLock lock;
        
        /*
         * @todo This has a very large impact on the throughput. It directly
         * controls the maximum distance between keys in a batch operations.
         * In turn, that translates into the "sparsity" of the operation. A
         * small value (~10) can show 4x higher throughput than a value of
         * 1000. This is because the btree cache is more or less being
         * defeated as the spacing between the keys touched in any operation
         * grows.
         * 
         * The other effect of this parameter is to change the #of possible
         * keys in the index. A larger value allows more distinct keys to be
         * generated, which in turn increases the #of entries that are
         * permitted into the index.
         * 
         * incRange => operations per second (Disk, no sync on commit, laptop, 5.23.07).
         * 
         * 10 => 463
         * 
         * 100 => 222
         * 
         * 1000 => 132
         * 
         * 10000 => 114 
         * 
         * 100000 => 116 
         * 
         * @todo Tease apart the sparsity effect from the #of entries
         * effect, or at least report the #of entries and height of the
         * index at the end of the overall run.
         */
        final int incRange = 100;  
        
        int lastKey = 0;

        final Random r = new Random();

        final KeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_INT);
        
        final private byte[] nextKey() {

            final int key = lastKey + r.nextInt(incRange);
            
            byte[] data = keyBuilder.reset().append(key).getKey();
            
            lastKey = key;

            return data;

        }
        
        /**
         * @param ndx
         *            The index under test.
         * @param groundTruth
         *            Used for performing ground truth correctness tests when
         *            running against one or more data services with index
         *            partition split, move, and join enabled (optional). When
         *            specified this should be backed by a
         *            {@link TemporaryStore} or {@link TemporaryRawStore}. The
         *            caller is responsible for validating the index under test
         *            against the ground truth on completion of the test.
         * @param lock
         *            Used to coordinate operations on the groundTruth store.
         *            May be <code>null</code> if the groundTruth store is
         *            <code>null</code>.
         * 
         * @todo parameterize for operation type (insert, remove, read,
         *       contains). let the caller determine the profile of operations
         *       to be executed against the service.
         * 
         * @todo keyLen is ignored. It could be replaced by an increment value
         *       that would govern the distribution of the keys.
         */
        public Task(IIndex ndx, int keyLen, int nops, double insertRate, IIndex groundTruth, ReentrantLock lock) {

            this.ndx = ndx;
           
//            this.keyLen = keyLen;
            
            if (insertRate < 0d || insertRate > 1d)
                throw new IllegalArgumentException();
            
            this.insertRate = insertRate;
            
            this.nops = nops;
            
            this.groundTruth = groundTruth;
            
            this.lock = lock;
            
            if (groundTruth != null && lock == null) {
                
                throw new IllegalArgumentException();
                
            }
            
        }

        /**
         * Executes a random batch operation with keys presented in sorted
         * order.
         * <p>
         * Note: Batch operations with sorted keys have twice the performance of
         * the corresponding operation with unsorted keys due to improved
         * locality of the lookups performed on the index.
         * 
         * @return The commit time of the transaction.
         */
        public Void call() throws Exception {

            byte[][] keys = new byte[nops][];
            byte[][] vals = new byte[nops][];

            if (r.nextDouble() <= insertRate) {

                /*
                 * Insert
                 */

//                log.info("insert: nops=" + nops);

                for (int i = 0; i < nops; i++) {

                    keys[i] = nextKey();

                    vals[i] = new byte[5];

                    r.nextBytes(vals[i]);

                }

                /*
                 * Note: Lock is forcing the same serialization order on the
                 * test and ground truth index writes.
                 */
                lock.lock();

                try {

                    ndx.submit(0/* fromIndex */, nops/* toIndex */, keys, vals, //
                            BatchInsertConstructor.RETURN_NO_VALUES, //
                            null// handler
                            );

                    if (groundTruth != null) {

                        groundTruth.submit(0/* fromIndex */, nops/* toIndex */,
                                keys, vals, //
                                BatchInsertConstructor.RETURN_NO_VALUES, //
                                null// handler
                                );

                    }

                } finally {

                    lock.unlock();

                }

            } else {

                /*
                 * Remove.
                 */

//                log.info("remove: nops=" + nops);

                for (int i = 0; i < nops; i++) {

                    keys[i] = nextKey();

                }

                /*
                 * Note: Lock is forcing the same serialization order on the
                 * test and ground truth index writes.
                 */
                lock.lock();

                try {

                    ndx.submit(0/* fromIndex */, nops/* toIndex */, keys,
                            null/* vals */,//
                            BatchRemoveConstructor.RETURN_NO_VALUES,//
                            null// handler
                            );

                    if (groundTruth != null) {

                        groundTruth.submit(0/* fromIndex */, nops/* toIndex */,
                                keys, null/* vals */,//
                                BatchRemoveConstructor.RETURN_NO_VALUES,//
                                null// handler
                                );

                    }

                } finally {

                    lock.unlock();

                }

            }
            
            return null;
            
        }
        
    }

    /**
     * Runs a single instance of the test as configured in the code.
     * 
     * @todo try running the test out more than 30 seconds. Note that a larger
     *       journal maximum extent is required since the journal will otherwise
     *       overflow.
     * 
     * @todo compute the bytes/second rate (read/written) (its in the counters
     *       for the {@link DiskOnlyStrategy}).
     * 
     * @todo Try to make this a correctness test since there are lots of little
     *       ways in which things can go wrong. Note that the actual execution
     *       order is important....
     * 
     * @todo Test for correct aborts. E.g., seed some tasks with keys or values
     *       that are never allowed to enter the index - the presence of those
     *       data means that the operation will choose to abort rather than to
     *       continue. Since we have written the data on the index this will let
     *       us test that abort() correctly rolls back the index writes. If we
     *       observe those keys/values in an index then we know that either
     *       abort is not working correctly or concurrent operations are being
     *       executed on the _same_ named index.
     * 
     * @see ExperimentDriver, which parameterizes the use of this stress test.
     *      That information should be used to limit the #of transactions
     *      allowed to start at one time on the server and should guide a search
     *      for thinning down resource consumption, e.g., memory usage by
     *      btrees, the node serializer, etc.
     * 
     * @see GenerateExperiment, which may be used to generate a set of
     *      conditions to be run by the {@link ExperimentDriver}.
     */
    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();

//        properties.setProperty(Options.FORCE_ON_COMMIT, ForceEnum.No.toString());

        properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient.toString());

        // properties.setProperty(Options.BUFFER_MODE, BufferMode.Direct.toString());

        // properties.setProperty(Options.BUFFER_MODE, BufferMode.Mapped.toString());

         properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());

        properties.setProperty(Options.CREATE_TEMP_FILE, "true");

        properties.setProperty(TestOptions.TIMEOUT, "10");

        properties.setProperty(TestOptions.NCLIENTS, "10");

        properties.setProperty(TestOptions.NTRIALS, "10000");

        properties.setProperty(TestOptions.KEYLEN, "4");

        properties.setProperty(TestOptions.NOPS, "4");

        IComparisonTest test = new StressTestConcurrent();
        
        test.setUpComparisonTest(properties);
        
        try {

            test.doComparisonTest(properties);
        
        } finally {

            try {
                
                test.tearDownComparisonTest();
                
            } catch(Throwable t) {

                log.warn("Tear down problem: "+t, t);
                
            }
            
        }

    }

    /**
     * Additional properties understood by this test.
     */
    public static interface TestOptions extends Options {

        /**
         * The timeout for the test.
         */
        public static final String TIMEOUT = "timeout";

        /**
         * The #of concurrent clients to run.
         */
        public static final String NCLIENTS = "nclients";

        /**
         * The #of trials (aka transactions) to run.
         */
        public static final String NTRIALS = "ntrials";

        /**
         * The length of the keys used in the test. This directly impacts the
         * likelyhood of a write-write conflict. Shorter keys mean more
         * conflicts. However, note that conflicts are only possible when there
         * are at least two concurrent clients running.
         */
        public static final String KEYLEN = "keyLen";

        /**
         * The #of operations in each trial.
         */
        public static final String NOPS = "nops";
        
        /**
         * The rate of insert operations (inserting <i>nops</i> tuples) in
         * [0.0:1.0]. The balance of the operations will remove <i>nops</i>
         * tuples.
         */
        String INSERT_RATE = "insertRate";
        
        /**
         * The #of distinct scale-out indices that will be used during the run.
         * Each index may be split over time as the run progresses, eventually
         * yielding multiple index partitions.
         */
        public static final String NINDICES = "nindices";

        /**
         * When <code>true</code>, ground truth will be maintained and
         * verified against the post-condition of the index(s) under test.
         * <p>
         * Note: This option may be used to verify index partition
         * split/join/move semantics and the correctness of
         * {@link ClientIndexView} views.
         * <p>
         * Note: All operations on a ground truth index are serialized so this
         * option can not be used when you are doing performance testing.
         */
        final String TEST_CORRECTNESS = "testCorrectness";
        
    }

    /**
     * Setup and run a test.
     * 
     * @param properties
     *            There are no "optional" properties - you must make sure that
     *            each property has a defined value.
     */
    public Result doComparisonTest(Properties properties) throws Exception {

        final long timeout = Long.parseLong(properties
                .getProperty(TestOptions.TIMEOUT));

        final int nclients = Integer.parseInt(properties
                .getProperty(TestOptions.NCLIENTS));

        final int ntrials = Integer.parseInt(properties
                .getProperty(TestOptions.NTRIALS));

        final int keyLen = Integer.parseInt(properties
                .getProperty(TestOptions.KEYLEN));

        final int nops = Integer.parseInt(properties
                .getProperty(TestOptions.NOPS));

        final double insertRate = Integer.parseInt(properties
                .getProperty(TestOptions.INSERT_RATE));

        final int nindices = Integer.parseInt(properties
                .getProperty(TestOptions.NINDICES));

        final boolean testCorrectness = Boolean.parseBoolean(properties
                .getProperty(TestOptions.TEST_CORRECTNESS));

        Result result = doConcurrentClientTest(client, nclients, timeout,
                ntrials, keyLen, nops, insertRate, nindices, testCorrectness);

        return result;

    }

    /**
     * Experiment generation utility class.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class GenerateExperiment extends ExperimentDriver {

        /**
         * Generates an XML file that can be run by {@link ExperimentDriver}.
         * 
         * @param args
         */
        public static void main(String[] args) throws Exception {

            // this is the test to be run.
            String className = StressTestConcurrent.class.getName();

            Map<String, String> defaultProperties = new HashMap<String, String>();

            // force delete of the files on close of the journal under test.
            defaultProperties.put(Options.CREATE_TEMP_FILE, "true");

            // avoids journal overflow when running out to 60 seconds.
            defaultProperties.put(Options.MAXIMUM_EXTENT, "" + Bytes.megabyte32
                    * 400);

            /*
             * Set defaults for each condition.
             */

            defaultProperties.put(TestOptions.TIMEOUT, "30");

            defaultProperties.put(TestOptions.NTRIALS, "10000");

            // defaultProperties.put(TestOptions.NCLIENTS,"10");

            defaultProperties.put(TestOptions.KEYLEN, "4");

            defaultProperties.put(TestOptions.NOPS, "100");

            List<Condition> conditions = new ArrayList<Condition>();

            conditions.addAll(BasicExperimentConditions.getBasicConditions(
                    defaultProperties, new NV[] { new NV(TestOptions.NCLIENTS,
                            "1") }));

            conditions.addAll(BasicExperimentConditions.getBasicConditions(
                    defaultProperties, new NV[] { new NV(TestOptions.NCLIENTS,
                            "2") }));

            conditions.addAll(BasicExperimentConditions.getBasicConditions(
                    defaultProperties, new NV[] { new NV(TestOptions.NCLIENTS,
                            "10") }));

            conditions.addAll(BasicExperimentConditions.getBasicConditions(
                    defaultProperties, new NV[] { new NV(TestOptions.NCLIENTS,
                            "20") }));

            conditions.addAll(BasicExperimentConditions.getBasicConditions(
                    defaultProperties, new NV[] { new NV(TestOptions.NCLIENTS,
                            "100") }));

            conditions.addAll(BasicExperimentConditions.getBasicConditions(
                    defaultProperties, new NV[] { new NV(TestOptions.NCLIENTS,
                            "200") }));

            Experiment exp = new Experiment(className, defaultProperties,
                    conditions);

            // copy the output into a file and then you can run it later.
            System.err.println(exp.toXML());

        }

    }

}
