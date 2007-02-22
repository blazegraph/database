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
 * Created on Feb 18, 2007
 */

package com.bigdata.journal;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.ComparisonTestDriver.IComparisonTest;
import com.bigdata.objndx.IIndex;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Stress tests for concurrent transaction processing.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StressTestConcurrent extends ProxyTestCase implements IComparisonTest {

    public StressTestConcurrent() {
    }

    public StressTestConcurrent(String name) {
        super(name);
    }
    
    /**
     * A stress test with a small pool of concurrent clients.
     */
    public void test_concurrentClients() throws InterruptedException {

        Properties properties = getProperties();
        
        Journal journal = new Journal(properties);

        if(journal.getBufferStrategy() instanceof MappedBufferStrategy) {
            
            /*
             * @todo the mapped buffer strategy has become cpu bound w/o
             * termination when used with concurrent clients - this needs to be
             * looked into further.
             */
            
            fail("Mapped buffer strategy has problem with concurrency");
            
        }

        doConcurrentClientTest(journal, 5, 20, 100, 3, 100);
        
    }

    /**
     * A stress test with a pool of concurrent clients.
     * 
     * @param journal
     *            The database.
     * 
     * @param name
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
     * @todo can this also be a correctness test if we choose the
     *       read/write/delete operations carefully and maintain a ground truth
     *       index?
     */
    static public String doConcurrentClientTest(Journal journal,
            long timeout, int nclients, int ntrials, int keyLen, int nops)
            throws InterruptedException {
        
        final String name = "abc";
        
        { // Setup the named index and commit the journal.
            
            journal.registerIndex(name, new UnisolatedBTree(journal));
            
            journal.commit();
            
        }
        
        ExecutorService executorService = Executors.newFixedThreadPool(
                nclients, DaemonThreadFactory.defaultThreadFactory());
        
        Collection<Callable<Long>> tasks = new HashSet<Callable<Long>>(); 

        ConcurrentHashMap<IIndex, Long> btrees = new ConcurrentHashMap<IIndex, Long>();
        
        for(int i=0; i<ntrials; i++) {
            
            tasks.add(new Task(journal, name, keyLen, nops, btrees));
            
        }

        /*
         * Run the M transactions on N clients.
         */
        
        final long begin = System.currentTimeMillis();
        
        List<Future<Long>> results = executorService.invokeAll(tasks, timeout, TimeUnit.SECONDS);

        final long elapsed = System.currentTimeMillis() - begin;
        
        Iterator<Future<Long>> itr = results.iterator();
        
        int nfailed = 0; // #of transactions that failed validation (MUST BE zero if nclients==1).
        int naborted = 0; // #of transactions that choose to abort rather than commit.
        int ncommitted = 0; // #of transactions that successfully committed.
        int nuncommitted = 0; // #of transactions that did not complete in time.
        
        while(itr.hasNext()) {

            Future<Long> future = itr.next();
            
            if(future.isCancelled()) {
                
                nuncommitted++;
                
                continue;
                
            }

            try {

                if(future.get()==0L) naborted++;
                
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
        
        journal.closeAndDelete();
                
        String msg = "#clients="
                + nclients + ", nops=" + nops + ", ntx=" + ntrials + ", ncomitted="
                + ncommitted + ", naborted=" + naborted + ", nfailed=" + nfailed
                + ", nuncommitted=" + nuncommitted + ", " + elapsed + "ms, "
                + ncommitted * 1000 / elapsed + " tps";
        
        System.err.println(msg);
        
        return msg;
       
    }
    
    /**
     * Run a transaction.
     * <p>
     * Note: defers creation of the tx until it begins to execute! This provides
     * a substantial resource savings and lets transactions begin execution
     * immediately.
     */
    public static class Task implements Callable<Long> {

        private final Journal journal;
        private final String name;
        private final int keyLen;
        private final int nops;
        private final ConcurrentHashMap<IIndex, Long> btrees;
        
        final Random r = new Random();
        
        public Task(Journal journal,String name, int keyLen, int nops, ConcurrentHashMap<IIndex, Long>btrees) {

            this.journal = journal;

            this.name = name;
            
            this.keyLen = keyLen;
            
            this.nops = nops;
            
            this.btrees = btrees;
            
        }

        /**
         * Executes random operations in the transaction.
         * 
         * @return The commit time of the transaction.
         */
        public Long call() throws Exception {
            
            final long tx = journal.newTx();
            
            final IIndex ndx = journal.getIndex(name,tx);

            Long otx = btrees.put(ndx, tx);
            
            if(otx != null ) {
                
                throw new AssertionError("Index already in use by: "+otx);
                
            }
            
            // Random operations on the named index(s).
            
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
             * the percentage of transactions that will abort rather than
             * commit.
             * 
             * @todo make parameter to stress test.
             */
            final int abortPercentage = 10;
            
            if (r.nextInt(100) > abortPercentage) {

                // commit.
                
                final long commitTime = journal.commit(tx);

                return commitTime;

            } else {

                // abort.
                
                journal.abort(tx);

                return 0L;

            }
            
        }
        
    }

    /**
     * Runs a single instance of the test as configured in the code.
     * 
     * @todo try running the test out more than 30 seconds. Note that a larger
     *       journal maximum extent is required since the journal will otherwise
     *       overflow.
     * 
     * @todo compute the bytes/second rate on the journal for this test.
     * 
     * @todo test with more than one named index in use.
     * 
     * @todo try to make this a correctness test since there are lots of little
     *       ways in which things can go wrong.
     * 
     * @todo the data service should use a thread pool to limit the #of started
     *       transations.
     * 
     * @todo There may be a memory leak with concurrent transactions. I was able
     *       to get rid of an {@link OutOfMemoryError} by setting the
     *       {@link TemporaryRawStore#buf} to to null when the store was closed.
     *       However, there is still going to be something that was causing
     *       those transactions and their stores to be hanging around -- perhaps
     *       the commitService in the Journal which might be holding onto
     *       {@link Future}s?
     * 
     * @see ComparisonTestDriver, which parameterizes the use of this stress
     *      test. That information should be used to limit the #of transactions
     *      allowed to start at one time on the server and should guide a search
     *      for thinning down resource consumption, e.g., memory usage by
     *      btrees, the node serializer, and
     */
    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();

//        properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient.toString());
        
//        properties.setProperty(Options.FORCE_ON_COMMIT,ForceEnum.No.toString());
        
//        properties.setProperty(Options.BUFFER_MODE, BufferMode.Direct.toString());

//        properties.setProperty(Options.BUFFER_MODE, BufferMode.Mapped.toString());

//        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());

        properties.setProperty(Options.SEGMENT, "0");
        
        File file = File.createTempFile("bigdata", ".jnl");
        
        file.deleteOnExit();
        
        if(!file.delete()) fail("Could not remove temp file before test");
        
        properties.setProperty(Options.FILE, file.toString());

        new StressTestConcurrent().doComparisonTest(properties);

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

    public String doComparisonTest(Properties properties) throws Exception {

        String val;
        
        val = properties.getProperty(TestOptions.TIMEOUT);

        final long timeout = (val ==null ? 30 : Long.parseLong(val));

        val = properties.getProperty(TestOptions.NCLIENTS);

        final int nclients = (val == null ? 10 : Integer.parseInt(val));

        val = properties.getProperty(TestOptions.NTRIALS);

        final int ntrials = (val == null ? 10000 : Integer.parseInt(val));

        val = properties.getProperty(TestOptions.KEYLEN);

        final int keyLen = (val == null ? 4 : Integer.parseInt(val));

        val = properties.getProperty(TestOptions.NOPS);

        final int nops = (val == null ? 100 : Integer.parseInt(val));

        Journal journal = new Journal(properties);

        String msg = doConcurrentClientTest(journal, timeout, nclients, ntrials,
                keyLen, nops);

        journal.shutdown();
        
        if(journal.getFile()!=null) {
            
            journal.getFile().delete();
            
        }

        return msg;

    }

}
