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
 * Created on Oct 9, 2007
 */

package com.bigdata.journal;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.bigdata.btree.IIndex;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.ConcurrentJournal.Options;
import com.bigdata.rawstore.Bytes;
import com.bigdata.test.ExperimentDriver;
import com.bigdata.test.ExperimentDriver.IComparisonTest;
import com.bigdata.test.ExperimentDriver.Result;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Stress tests for concurrent processing of operations on named unisolated indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StressTestConcurrentUnisolatedIndices extends ProxyTestCase implements IComparisonTest {

    public StressTestConcurrentUnisolatedIndices() {
    }

    public StressTestConcurrentUnisolatedIndices(String name) {

        super(name);
        
    }
    
    Journal journal;

    public void setUpComparisonTest(Properties properties) throws Exception {
        
        journal = new Journal(properties);
        
    }
    
    public void tearDownComparisonTest() throws Exception {
        
    }
    
    /**
     * A stress test with a small pool of concurrent clients.
     */
    public void test_concurrentClients() throws InterruptedException {

        Properties properties = getProperties();
        
        Journal journal = new Journal(properties);

//        if(journal.getBufferStrategy() instanceof MappedBufferStrategy) {
//            
//            /*
//             * @todo the mapped buffer strategy has become cpu bound w/o
//             * termination when used with concurrent clients - this needs to be
//             * looked into further.
//             */
//            
//            fail("Mapped buffer strategy may have problem with tx concurrency");
//            
//        }

        doConcurrentClientTest(journal, 5, 20, 100, 3, 100);
        
    }

    /**
     * A stress test with a pool of concurrent clients.
     * 
     * @param journal
     *            The database.
     * 
     * @param resource
     *            The name of the index on which the transactions will
     *            operation.
     * 
     * @param timeout
     *            The #of seconds before the test will terminate.
     * 
     * @param nclients
     *            The #of concurrent clients.
     * 
     * @param ntrials
     *            The #of transactions to execute.
     * 
     * @param keyLen
     *            The length of the random unsigned byte[] keys used in the
     *            operations. The longer the keys the less likely it is that
     *            there will be a write-write conflict (that concurrent txs will
     *            write on the same key).
     * 
     * @param nops
     *            The #of operations to be performed in each transaction.
     * 
     * @todo factor out the operation to be run as a test parameter?
     */
    static public Result doConcurrentClientTest(Journal journal,
            long timeout, int nclients, int ntrials, int keyLen, int nops)
            throws InterruptedException {
        
        final String name = "abc";
        
        { // Setup the named index and commit the journal.
            
            journal.registerIndex(name, new UnisolatedBTree(journal, UUID.randomUUID()));
            
            journal.commit();
            
        }
        
        ExecutorService executorService = Executors.newFixedThreadPool(
                nclients, DaemonThreadFactory.defaultThreadFactory());
        
        Collection<Callable<Object>> tasks = new HashSet<Callable<Object>>(); 

        ConcurrentHashMap<IIndex, Thread> btrees = new ConcurrentHashMap<IIndex, Thread>();
        
        for(int i=0; i<ntrials; i++) {
            
            tasks.add(new WriteTask(journal, name, keyLen, nops, btrees));
            
        }

        /*
         * Run the M transactions on N clients.
         */
        
        final long begin = System.currentTimeMillis();
        
        List<Future<Object>> results = executorService.invokeAll(tasks, timeout, TimeUnit.SECONDS);

        final long elapsed = System.currentTimeMillis() - begin;
        
        Iterator<Future<Object>> itr = results.iterator();
        
        int nfailed = 0; // #of transactions that failed validation (MUST BE zero if nclients==1).
        int naborted = 0; // #of transactions that choose to abort rather than commit.
        int ncommitted = 0; // #of transactions that successfully committed.
        int nuncommitted = 0; // #of transactions that did not complete in time.
        
        while(itr.hasNext()) {

            Future<Object> future = itr.next();
            
            if(future.isCancelled()) {
                
                nuncommitted++;
                
                continue;
                
            }

            try {

                future.get();
                
                ncommitted++;
                
            } catch(ExecutionException ex ) {
                
                // @todo allow retry for lock contention.
                // @todo allow retry if aborted due to an error in a commit group.
                // @todo examine conditions and handle of interrupts (InterruptedException).
                
                if(ex.getCause() instanceof SpuriousException) {
                
                    nfailed++;
                    
                } else {
                
                    // Other kinds of exceptions are errors.
                    
                    fail("Not expecting: "+ex, ex);
                    
                }
                
            }
            
        }

        journal.shutdown();
        
        journal.delete();
                
        /*
         * Compute bytes written per second.
         */
        
        long seconds = TimeUnit.SECONDS.convert(elapsed, TimeUnit.MILLISECONDS);
        
        long bytesWrittenPerSecond = journal.getRootBlockView().getNextOffset()
                / (seconds == 0 ? 1 : seconds);

        Result ret = new Result();
        
        // these are the results.
        ret.put("ncommitted",""+ncommitted);
        ret.put("nuncommitted", ""+nuncommitted);
        ret.put("elapsed(ms)", ""+elapsed);
        ret.put("bytesWrittenPerSec", ""+bytesWrittenPerSecond);
        ret.put("ops", ""+(ncommitted * 1000 / elapsed));

        System.err.println(ret.toString(true/*newline*/));
        
        return ret;
       
    }
    
    /**
     * A task that writes on a named unisolated index.
     * <p>
     * Note: defers creation of the tx until it begins to execute! This provides
     * a substantial resource savings and lets transactions begin execution
     * immediately.
     */
    public static class WriteTask implements Callable<Object> {

        private final Journal journal;
        private final String name;
        private final int keyLen;
        private final int nops;
        private final ConcurrentHashMap<IIndex, Thread> btrees;
        
        final Random r = new Random();
        
        public WriteTask(Journal journal,String name, int keyLen, int nops, ConcurrentHashMap<IIndex, Thread>btrees) {

            this.journal = journal;

            this.name = name;
            
            this.keyLen = keyLen;
            
            this.nops = nops;
            
            this.btrees = btrees;
            
        }

        /**
         * Executes random operation on a named unisolated index.
         * 
         * @return null
         */
        public Object call() throws Exception {

            final IIndex ndx = journal.getIndex(name);

            if (btrees.put(ndx, Thread.currentThread()) != null) {

                throw new AssertionError("Unisolated index already in use");

            }

            try {

                // Random write operations on the named index(s).

                for (int i = 0; i < nops; i++) {

                    byte[] key = new byte[keyLen];

                    r.nextBytes(key);

                    if (r.nextInt(100) > 10) {

                        byte[] val = new byte[5];

                        r.nextBytes(val);

                        ndx.insert(key, val);

                    } else {

                        ndx.remove(key);

                    }

                }

                /*
                 * the percentage of operation will throw a suprious error.
                 * 
                 * @todo make parameter to stress test.
                 */
                final double failureRate = 0.01d;

                if (r.nextDouble() > failureRate) {

                    throw new SpuriousException();

                }

                return null;
                
            } finally {

                btrees.remove(ndx);

            }
            
        }
        
    }
    
    /**
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class SpuriousException extends RuntimeException {

        /**
         * 
         */
        private static final long serialVersionUID = 5032559382234334218L;
        
    }

    /**
     * Runs a single instance of the test as configured in the code.
     * 
     * @todo try running the test out more than 30 seconds. Note that a larger
     *       journal maximum extent is required since the journal will otherwise
     *       overflow.
     * 
     * @todo add parameter for the #of named indices in use.
     * 
     * @todo provide separate parameters for the #of unisolated readers (from
     *       the last commit record) and unisolated writers (on the current
     *       index object).
     * 
     * @todo can we get to the point of being IO bound with lots of small write
     *       operations on lots of indices using Disk and a 2CPUs? 4CPUs? With
     *       larger write operations?  With fewer indices?  Is there a point at
     *       which we need to use a blocking queue for new operations and cause
     *       the clients to wait?
     * 
     * @todo vary the #of indices on which a given operation requires a lock.
     *       this could be done on a gaussian basis (most need one, some need 3
     *       or 4) or as (minLocks, maxLocks).
     * 
     * @todo Try to make this a correctness test since there are lots of little
     *       ways in which things can go wrong.
     * 
     * @see ExperimentDriver
     * @see GenerateExperiment
     */
    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();

        properties.setProperty(Options.FORCE_ON_COMMIT,ForceEnum.No.toString());
        
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient.toString());
        
//        properties.setProperty(Options.BUFFER_MODE, BufferMode.Direct.toString());

//        properties.setProperty(Options.BUFFER_MODE, BufferMode.Mapped.toString());

//        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());

        properties.setProperty(Options.CREATE_TEMP_FILE, "true");
        
        properties.setProperty(TestOptions.TIMEOUT,"60");

        properties.setProperty(TestOptions.NCLIENTS,"10");

        properties.setProperty(TestOptions.NTRIALS,"10000");

        properties.setProperty(TestOptions.KEYLEN,"4");

        properties.setProperty(TestOptions.NOPS,"4");
        
        IComparisonTest test = new StressTestConcurrentUnisolatedIndices();
        
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
    public static class TestOptions extends Options {

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

        final long timeout = Long.parseLong(properties.getProperty(TestOptions.TIMEOUT));

        final int nclients = Integer.parseInt(properties.getProperty(TestOptions.NCLIENTS));

        final int ntrials = Integer.parseInt(properties.getProperty(TestOptions.NTRIALS));

        final int keyLen = Integer.parseInt(properties.getProperty(TestOptions.KEYLEN));

        final int nops = Integer.parseInt(properties.getProperty(TestOptions.NOPS));

        Result result = doConcurrentClientTest(journal, timeout, nclients, ntrials,
                keyLen, nops);

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
            String className = StressTestConcurrentUnisolatedIndices.class.getName();
            
            Map<String,String> defaultProperties = new HashMap<String,String>();

            // force delete of the files on close of the journal under test.
            defaultProperties.put(Options.CREATE_TEMP_FILE,"true");

            // avoids journal overflow when running out to 60 seconds.
            defaultProperties.put(Options.MAXIMUM_EXTENT, ""+Bytes.megabyte32*400);

            /* 
             * Set defaults for each condition.
             */
            
            defaultProperties.put(TestOptions.TIMEOUT,"30");

            defaultProperties.put(TestOptions.NTRIALS,"10000");

//            defaultProperties.put(TestOptions.NCLIENTS,"10");

            defaultProperties.put(TestOptions.KEYLEN,"4");

            defaultProperties.put(TestOptions.NOPS,"100");

            List<Condition>conditions = new ArrayList<Condition>();

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
            
            Experiment exp = new Experiment(className,defaultProperties,conditions);

            // copy the output into a file and then you can run it later.
            System.err.println(exp.toXML());

        }
        
    }
    
}
