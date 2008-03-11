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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.BatchInsert.BatchInsertConstructor;
import com.bigdata.btree.BatchRemove.BatchRemoveConstructor;
import com.bigdata.journal.BasicExperimentConditions;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.ValidationError;
import com.bigdata.rawstore.Bytes;
import com.bigdata.resources.DefaultSplitHandler;
import com.bigdata.resources.ResourceManager.Options;
import com.bigdata.test.ExperimentDriver;
import com.bigdata.test.ExperimentDriver.IComparisonTest;
import com.bigdata.test.ExperimentDriver.Result;
import com.bigdata.util.concurrent.DaemonThreadFactory;

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
 *       mode should be faster if there are sufficent resources available (its
 *       main advantage occurs during overflow since that operation requires
 *       reads against partly random locations in the journal to build up the
 *       index segments). (The problem is also that we are not IO bound - the
 *       btree search and node/leaf serialization are the main bottlenecks.)
 * 
 * @todo The primary metrics reported by the test are elapsed time and
 *       operations per second. Compute the through put in terms of bytes per
 *       second for writes. This is interesting since it allows us to compare
 *       the effect of batch size on writes. Add parameterization for read vs
 *       write vs remove so that we can test the effect of batch size for
 *       operation profiles based on each of those kinds of operations.
 * 
 * @todo This test uses one index and one partition on one data service. Expand
 *       the test to test with multiple partitions on the same data service
 *       (concurrent writes on those partitions should be supported), with
 *       multiple indices (again, concurrent writes should be supported), and
 *       (possibly) with multiple data services.
 * 
 * @todo measure the time in the RPC calls, including marshalling and
 *       unmarshalling of the arguments, and use those measurements to guide
 *       performance tuning. This can be done by comparing an embedded data
 *       service (no RPC) with a data service connected by JINI (RPC).
 * 
 * @todo Test w/ group commit and various group sizes.
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
        AbstractEmbeddedBigdataFederationTestCase implements IComparisonTest {

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
    
//    /**
//     * Starts in {@link #setUp()}.
//     */
//    IBigdataClient client;
//    
//    /**
//     * 
//     */
//    IBigdataFederation federation;
    
    public Properties getProperties() {
        
        Properties properties = new Properties(super.getProperties());
        
//        properties.setProperty(Options.DATA_DIR,getName());

        // Make sure this test uses disk so that it can trigger overflows.
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk
                .toString());

        // make the initial and maximum extent small so that we trigger overflow a lot.
        properties.setProperty(Options.INITIAL_EXTENT, ""+1*Bytes.megabyte);
        properties.setProperty(Options.MAXIMUM_EXTENT, ""+1*Bytes.megabyte);

        // enable moves.
        properties.setProperty(Options.MAXIMUM_MOVES_PER_TARGET,Options.DEFAULT_MAXIMUM_MOVES_PER_TARGET);
        
//        properties.setProperty(Options.CREATE_TEMP_FILE,"true");
        
        return properties;
        
    }
    
//    /**
//     * Starts a {@link DataServer} ({@link #dataServer1}) and then a
//     * {@link MetadataServer} ({@link #metadataServer0}). Each runs in its own
//     * thread.
//     */
//    public void setUp() throws Exception {
//
//        super.setUp();
//        
//        log.info(getName());
//
//        client = new EmbeddedBigdataClient(getProperties());
//        
//        federation = client.connect();
//
//    }
//    
//    /**
//     * Destroy the test services.
//     */
//    public void tearDown() throws Exception {
//                
//        if(client!=null) {
//            
//            client.terminate();
//
//            client = null;
//            
//        }
//        
//        log.info(getName());
//
//        super.tearDown();
//        
//    }
    
    public void setUpComparisonTest(Properties properties) throws Exception {

        super.setUp();
        
//        log.info(getName());
//
//        client = new EmbeddedBigdataClient( properties );
//        
//        federation = client.connect();
        
    }
    
    public void tearDownComparisonTest() throws Exception {
        
//        if(client!=null) {
//            
//            client.terminate();
//
//            client = null;
//            
//        }
//        
//        log.info(getName());

        super.tearDown();
        
    }

    /**
     * Test of N concurrent operations against one {@link DataService}.
     * 
     * @throws Exception
     */
    public void test_stressTest1() throws Exception {

        DataService dataService = ((EmbeddedBigdataFederation)fed).getDataService(0);
        
        int nclients = 20;
        long timeout = 20;
        int ntrials = 1000;
        int keyLen = 4;
        int nops = 100;
        
        doConcurrentClientTest(client, dataService, nclients, timeout, ntrials,
                keyLen, nops);

    }

    /**
     * A stress test with a pool of concurrent clients.
     * 
     * @param client
     *            The client.
     * 
     * @param dataService
     *            The data service on which the initial index partition will be
     *            registered.
     * 
     * @param timeout
     *            The #of seconds before the test will terminate.
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
     * @todo parameterize the maximum journal size, random timing for forcing
     *       overflow, index split and join thresholds, etc.
     * 
     * @todo introduce a ground truth index on a local (temp) store and verify
     *       the state of the scale-out index. access to the ground truth index
     *       would have to be synchronized, e.g., using a lock for mutation and
     *       for verification.
     * 
     * @todo parameterize for random deletes and writes and parameterize those
     *       operations so that they can be made likely to force a join or split
     *       of an index partition.
     * 
     * @todo parameterize how likely we are to move index partitions around and
     *       the #of data services available to the federation. report on the
     *       #of builds, splits, joins, and moves and the load on each data
     *       service over time.
     */
    static public Result doConcurrentClientTest(IBigdataClient client,
            DataService dataService, int nclients, long timeout, int ntrials,
            int keyLen, int nops) throws InterruptedException, IOException {
        
        // name of the scale-out index for the test.
        final String name = "testIndex";

        // connect to the federation.
        final IBigdataFederation federation = client.connect();
        
        /*
         * Register the scale-out index.
         */
        
        final UUID indexUUID = UUID.randomUUID();
        final int entryCountPerSplit = 400;
        final double overCapacityMultiplier = 1.5;
        final int minimumEntryCountPerSplit = 100;
        {

            final IndexMetadata indexMetadata = new IndexMetadata(name,indexUUID);

            // The threshold below which we will try to join index partitions.
            ((DefaultSplitHandler)indexMetadata.getSplitHandler()).setMinimumEntryCount(minimumEntryCountPerSplit);
            
            // The target #of index entries per partition.
            ((DefaultSplitHandler)indexMetadata.getSplitHandler()).setEntryCountPerSplit(entryCountPerSplit);

            // Overcapacity multipler before an index partition will be split.
            ((DefaultSplitHandler)indexMetadata.getSplitHandler()).setOverCapacityMultiplier(overCapacityMultiplier);
            
            // must support delete markers
            indexMetadata.setDeleteMarkers(true);

            // register the scale-out index, creating a single index partition.
            federation.registerIndex(indexMetadata,dataService.getServiceUUID());
            
        }
        
        // request index view.
        IIndex ndx = federation.getIndex(name,ITx.UNISOLATED);
        
        assertEquals("indexUUID", indexUUID, ndx.getIndexMetadata()
                .getIndexUUID());
        
        ExecutorService executorService = Executors.newFixedThreadPool(
                nclients, DaemonThreadFactory.defaultThreadFactory());
        
        Collection<Callable<Void>> tasks = new HashSet<Callable<Void>>(); 

        for(int i=0; i<ntrials; i++) {
            
            tasks.add(new Task(ndx, keyLen, nops));
            
        }

        /*
         * Run the M transactions on N clients.
         */
        
        final long begin = System.currentTimeMillis();
        
        final List<Future<Void>> results = executorService.invokeAll(tasks, timeout, TimeUnit.SECONDS);
        
        final long elapsed = System.currentTimeMillis() - begin;
        
        System.err.println("Examining task results: elapsed="+elapsed);
        
        Iterator<Future<Void>> itr = results.iterator();
        
        int nfailed = 0; // #of operations that failed
        int ncommitted = 0; // #of operations that committed.
        int nuncommitted = 0; // #of operations that did not complete in time.
        int ntimeout = 0;
        int ninterrupted = 0;
        
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
                    
                    fail("Not expecting: "+ex, ex);
                    
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
        
//        String msg = "#clients=" + nclients + ", nops=" + nops + ", ntrials="
//                + ntrials + ", ncomitted=" + ncommitted + ", nfailed="
//                + nfailed + ", nuncommitted=" + nuncommitted + ", " + elapsed
//                + "ms, " + ncommitted * 1000 / elapsed
//                + " operations per second";

        Result ret = new Result();
        
//        // @todo these are conditions not results
//        ret.put("nops", ""+nops);
//        ret.put("ntx", ""+ntrials);
        
        ret.put("ncommitted",""+ncommitted);
        ret.put("nfailed",""+nfailed);
        ret.put("nuncommitted", ""+nuncommitted);
        ret.put("ntimeout", ""+ntimeout);
        ret.put("ninterrupted", ""+ninterrupted);
        ret.put("elapsed(ms)", ""+elapsed);
        ret.put("operations/sec", ""+(ncommitted * 1000 / elapsed));

        System.err.println(ret.toString(true/*newline*/));
        
        return ret;
       
    }
    
    /**
     * Run an unisolated operation.
     */
    public static class Task implements Callable<Void> {

        private final IIndex ndx;
//        private final int keyLen;
        private final int nops;
        
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
         * 
         * @todo parameterize for operation type (insert, remove, read,
         *       contains).  let the caller determine the profile of
         *       operations to be executed against the service.
         *       
         * @todo keyLen is ignored. It could be replaced by an increment value
         *       that would govern the distribution of the keys.
         */
        public Task(IIndex ndx,int keyLen, int nops) {

            this.ndx = ndx;
           
//            this.keyLen = keyLen;
            
            this.nops = nops;
            
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
            
            if (r.nextInt(100) > 10) {
                
                for (int i = 0; i < nops; i++) {

                    keys[i] = nextKey();
                    
                    vals[i] = new byte[5];

                    r.nextBytes(vals[i]);

                }
                
                ndx.submit(0/*fromIndex*/,nops/*toIndex*/, keys, vals, //
                        BatchInsertConstructor.RETURN_NO_VALUES, //
                        null// handler
                        );
                
            } else {

                for (int i = 0; i < nops; i++) {

                    keys[i] = nextKey();

                }
                
                ndx.submit(0/*fromIndex*/,nops/*toIndex*/, keys, null/*vals*/,//
                        BatchRemoveConstructor.RETURN_NO_VALUES,//
                        null// handler
                        );
                
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
     * @todo compute the bytes/second rate (read/written).
     * 
     * FIXME Parameterize so that test with more than one named index in use. If
     * we are only writing on a single named index then groupCommmit will only
     * defer syncs to disk since operations on the same index must be
     * serialized. If we are using more than one named index then those
     * operations can be parallelized and we should see additional performance
     * gains (for map/reduce this means that we are not going to find large
     * performance gains in group commit since writes on the same index will not
     * be parallelized - try using local map writes instead).
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

        DataService dataService = ((EmbeddedBigdataFederation) fed)
                .getDataService(0);

        Result result = doConcurrentClientTest(client, dataService, nclients,
                timeout, ntrials, keyLen, nops);

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
