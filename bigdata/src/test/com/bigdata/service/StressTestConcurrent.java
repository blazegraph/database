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
 * Created on May 23, 2007
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.bigdata.btree.BatchInsert;
import com.bigdata.btree.BatchRemove;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IBufferStrategy;
import com.bigdata.journal.ValidationError;
import com.bigdata.rawstore.Bytes;
import com.bigdata.service.EmbeddedBigdataFederation.Options;
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
 *       index segments).
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
 * @todo This test could be used to get group commit working.
 * 
 * @todo This test could be used to get concurrent writes on different indices
 *       working (also requires MRMW for the underlying {@link IBufferStrategy}s).
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
public class StressTestConcurrent extends AbstractServerTestCase { //implements IComparisonTest {

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
    
    public void setUpComparisonTest() throws Exception {

        setUp();
        
    }
    
    public void tearDownComparisonTest() throws Exception {
        
        tearDown();
        
    }

//    /**
//     * Starts in {@link #setUp()}.
//     */
//    MetadataServer metadataServer0;
//    /**
//     * Starts in {@link #setUp()}.
//     */
//    DataServer dataServer0;

    /**
     * Starts in {@link #setUp()}.
     */
    IBigdataClient client;
    
    /**
     * 
     */
    IBigdataFederation federation;
    
    public Properties getProperties() {
        
        Properties properties = new Properties(super.getProperties());
        
//        properties.setProperty(Options.DATA_DIR,getName());
        
        properties.setProperty(Options.CREATE_TEMP_FILE,"true");
        
        return properties;
        
    }
    
    /**
     * Starts a {@link DataServer} ({@link #dataServer1}) and then a
     * {@link MetadataServer} ({@link #metadataServer0}). Each runs in its own
     * thread.
     */
    public void setUp() throws Exception {

        super.setUp();
        
        log.info(getName());

        client = new EmbeddedBigdataClient(getProperties());
        
        federation = client.connect();

    }
    
    /**
     * Destroy the test services.
     */
    public void tearDown() throws Exception {
                
        if(client!=null) {
            
            client.terminate();

            client = null;
            
        }
        
        log.info(getName());

        super.tearDown();
        
    }
    
    /**
     * Test of N concurrent operations against one {@link DataService}.
     * 
     * @throws Exception
     */
    public void test_queueDepth() throws Exception {

        DataService dataService = ((EmbeddedBigdataFederation)federation).getDataService(0);
        
        doConcurrentClientTest(client, dataService, 20, 20, 10000, 3, 100);

    }

    /**
     * A stress test with a pool of concurrent clients.
     * 
     * @param ndx
     *            A remote scale-out index.
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
     * @todo can this also be a correctness test if we choose the
     *       read/write/delete operations carefully and maintain a ground truth
     *       index?
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
        UUID indexUUID = federation.registerIndex(name, new byte[][] {//
                new byte[] {} }, //
                new UUID[] {//
                dataService.getServiceUUID(), });

        // request index view.
        IIndex ndx = federation.getIndex(IBigdataFederation.UNISOLATED, name);
        
        assertEquals("indexUUID",indexUUID,ndx.getIndexUUID());
        
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
        
        List<Future<Void>> results = executorService.invokeAll(tasks, timeout, TimeUnit.SECONDS);

        final long elapsed = System.currentTimeMillis() - begin;
        
        Iterator<Future<Void>> itr = results.iterator();
        
        int nfailed = 0; // #of operations that failed
        int ncommitted = 0; // #of operations that committed.
        int nuncommitted = 0; // #of operations that did not complete in time.
        
        while(itr.hasNext()) {

            Future<Void> future = itr.next();
            
            if(future.isCancelled()) {
                
                nuncommitted++;
                
                continue;
                
            }

            try {

                future.get();
                
                ncommitted++;
                
            } catch(ExecutionException ex ) {
                
                // Validation errors are allowed and counted as aborted txs.
                
                if(ex.getCause() instanceof ValidationError) {
                
                    nfailed++;
                    
                } else {
                
                    // Other kinds of exceptions are errors.
                    
                    fail("Not expecting: "+ex, ex);
                    
                }
                
            }
            
        }
        
//        String msg = "#clients=" + nclients + ", nops=" + nops + ", ntrials="
//                + ntrials + ", ncomitted=" + ncommitted + ", nfailed="
//                + nfailed + ", nuncommitted=" + nuncommitted + ", " + elapsed
//                + "ms, " + ncommitted * 1000 / elapsed
//                + " operations per second";

        Result ret = new Result();
        
        // @todo these are conditions not results
        ret.put("nops", ""+nops);
        ret.put("ntx", ""+ntrials);
        
        ret.put("ncommitted",""+ncommitted);
        ret.put("nfailed",""+nfailed);
        ret.put("nuncommitted", ""+nuncommitted);
        ret.put("elapsed(ms)", ""+elapsed);
        ret.put("operations/sec", ""+(ncommitted * 1000 / elapsed));

        System.err.println(ret.toString());
        
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
                
                ndx.insert(new BatchInsert(nops,keys,vals));
                
            } else {

                for (int i = 0; i < nops; i++) {

                    keys[i] = nextKey();

                }
                
                ndx.remove(new BatchRemove(nops,keys,vals/* out */));
                
            }
            
            return null;
            
        }
        
    }
    
}
