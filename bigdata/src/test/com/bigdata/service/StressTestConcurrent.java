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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
 *       performance tuning.
 * 
 * @todo This test could be used to get group commit working.
 * 
 * @todo This test could be used to get concurrent writes on different indices
 *       working (also requires MRMW for the underlying {@link IBufferStrategy}s).
 * 
 * @todo get the comparison support working in main (it is commented out).
 *       Paramterize the {@link DataService} configuration from the test suite
 *       so that we can test Disk vs Direct, forceCommit=No vs default, and
 *       other properties that might have interesting effects. These things can
 *       be directly manipulated in the mean time by editing the
 *       DataServer0.properties file.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StressTestConcurrent extends AbstractServerTestCase {

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

    /**
     * Starts in {@link #setUp()}.
     */
    MetadataServer metadataServer0;
    /**
     * Starts in {@link #setUp()}.
     */
    DataServer dataServer0;
    /**
     * Starts in {@link #setUp()}.
     */
    BigdataClient client;
    
    /**
     * Starts a {@link DataServer} ({@link #dataServer1}) and then a
     * {@link MetadataServer} ({@link #metadataServer0}). Each runs in its own
     * thread.
     */
    public void setUp() throws Exception {

        log.info(getName());

//        final String groups = ".groups = new String[]{\"" + getName() + "\"}";
        
        /*
         * Start the metadata server.
         */
        metadataServer0 = new MetadataServer(
                new String[] { "src/resources/config/standalone/MetadataServer0.config"
//                        , AbstractServer.ADVERT_LABEL+groups
                        });
        
        new Thread() {

            public void run() {
                
                metadataServer0.run();
                
            }
            
        }.start();

        /*
         * Start up a data server after the metadata server so that we can make
         * sure that it is detected by the metadata server once it starts up.
         */
        dataServer0 = new DataServer(
                new String[] { "src/resources/config/standalone/DataServer0.config"
//                        , AbstractServer.ADVERT_LABEL+groups
                        });

        new Thread() {

            public void run() {
                
                dataServer0.run();
                
            }
            
        }.start();

        // Wait until all the services are up.
        getServiceID(metadataServer0);
        getServiceID(dataServer0);

        client = new BigdataClient(
                new String[] { "src/resources/config/standalone/Client.config"
//                        , BigdataClient.CLIENT_LABEL+groups
                        });

        // verify that the client has/can get the metadata service.
        assertNotNull("metadataService", client.getMetadataService());

    }
    
    /**
     * Destroy the test services.
     */
    public void tearDown() throws Exception {
        
        if(metadataServer0!=null) {

            metadataServer0.destroy();
        
            metadataServer0 = null;

        }

        if(dataServer0!=null) {

            dataServer0.destroy();
        
            dataServer0 = null;

        }
        
        if(client!=null) {

            client.terminate();

            client = null;
            
        }
        
        log.info(getName());
        
    }
    
    /**
     * Test concurrency of N concurrent operations against one
     * {@link DataService}.
     * 
     * @throws Exception
     */
    public void test_queueDepth() throws Exception {

        doConcurrentClientTest(client, dataServer0, 20, 20, 10000, 3, 100);

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
     */
    static public String doConcurrentClientTest(BigdataClient client,
            DataServer dataServer0, int nclients, long timeout, int ntrials,
            int keyLen, int nops) throws InterruptedException {
        
        // name of the scale-out index for the test.
        final String name = "testIndex";

        // connect to the federation.
        final BigdataFederation fed = (BigdataFederation) client.connect();
        
        /*
         * Register the scale-out index.
         */
        UUID indexUUID = fed.registerIndex(name, new byte[][] {//
                new byte[] {} }, //
                new UUID[] {//
                JiniUtil.serviceID2UUID(dataServer0.getServiceID()), });

        // request index view.
        IIndex ndx = fed.getIndex(IBigdataFederation.UNISOLATED, name);
        
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

        fed.disconnect();
        
        String msg = "#clients=" + nclients + ", nops=" + nops + ", ntrials="
                + ntrials + ", ncomitted=" + ncommitted + ", nfailed="
                + nfailed + ", nuncommitted=" + nuncommitted + ", " + elapsed
                + "ms, " + ncommitted * 1000 / elapsed
                + " operations per second";
        
        System.err.println(msg);
        
        return msg;
       
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

//    /**
//     * Runs a single instance of the test as configured in the code.
//     * 
//     * @todo try running the test out more than 30 seconds. Note that a larger
//     *       journal maximum extent is required since the journal will otherwise
//     *       overflow.
//     * 
//     * @todo compute the bytes/second rate on the journal for this test.
//     * 
//     * @todo test with more than one named index in use.
//     * 
//     * @todo try to make this a correctness test since there are lots of little
//     *       ways in which things can go wrong.
//     * 
//     * @todo the data service should use a thread pool to limit the #of started
//     *       transations.
//     * 
//     * @todo There may be a memory leak with concurrent transactions. I was able
//     *       to get rid of an {@link OutOfMemoryError} by setting the
//     *       {@link TemporaryRawStore#buf} to to null when the store was closed.
//     *       However, there is still going to be something that was causing
//     *       those transactions and their stores to be hanging around -- perhaps
//     *       the writeService in the Journal which might be holding onto
//     *       {@link Future}s?
//     * 
//     * @see ComparisonTestDriver, which parameterizes the use of this stress
//     *      test. That information should be used to limit the #of transactions
//     *      allowed to start at one time on the server and should guide a search
//     *      for thinning down resource consumption, e.g., memory usage by
//     *      btrees, the node serializer, and
//     */
//    public static void main(String[] args) throws Exception {
//
//        Properties properties = new Properties();
//
//        properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient.toString());
//        
//        properties.setProperty(Options.FORCE_ON_COMMIT,ForceEnum.No.toString());
//        
////        properties.setProperty(Options.BUFFER_MODE, BufferMode.Direct.toString());
//
////        properties.setProperty(Options.BUFFER_MODE, BufferMode.Mapped.toString());
//
////        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());
//
//        properties.setProperty(Options.CREATE_TEMP_FILE, "true");
//        
//        properties.setProperty(TestOptions.TIMEOUT,"10");
//
//        properties.setProperty(TestOptions.NCLIENTS,"100");
//
//        properties.setProperty(TestOptions.KEYLEN,"4");
//
//        properties.setProperty(TestOptions.NOPS,"4");
//        
//        new StressTestConcurrent().doComparisonTest(properties);
//
//    }
//    
//    /**
//     * Additional properties understood by this test.
//     */
//    public static class TestOptions extends Options {
//
//        /**
//         * The timeout for the test.
//         */
//        public static final String TIMEOUT = "timeout";
//        /**
//         * The #of concurrent clients to run.
//         */
//        public static final String NCLIENTS = "nclients";
//
//        /**
//         * The #of trials (aka transactions) to run.
//         */
//        public static final String NTRIALS = "ntrials";
//        /**
//         * The length of the keys used in the test. This directly impacts the
//         * likelyhood of a write-write conflict. Shorter keys mean more
//         * conflicts. However, note that conflicts are only possible when there
//         * are at least two concurrent clients running.
//         */
//        public static final String KEYLEN = "keyLen";
//        /**
//         * The #of operations in each trial.
//         */
//        public static final String NOPS = "nops";
//    
//    }
//
//    public String doComparisonTest(Properties properties) throws Exception {
//
//        String val;
//        
//        val = properties.getProperty(TestOptions.TIMEOUT);
//
//        final long timeout = (val ==null ? 30 : Long.parseLong(val));
//
//        val = properties.getProperty(TestOptions.NCLIENTS);
//
//        final int nclients = (val == null ? 10 : Integer.parseInt(val));
//
//        val = properties.getProperty(TestOptions.NTRIALS);
//
//        final int ntrials = (val == null ? 10000 : Integer.parseInt(val));
//
//        val = properties.getProperty(TestOptions.KEYLEN);
//
//        final int keyLen = (val == null ? 4 : Integer.parseInt(val));
//
//        val = properties.getProperty(TestOptions.NOPS);
//
//        final int nops = (val == null ? 100 : Integer.parseInt(val));
//
//        Journal journal = new Journal(properties);
//
//        String msg = doConcurrentClientTest(journal, timeout, nclients, ntrials,
//                keyLen, nops);
//
//        return msg;
//
//    }

}
