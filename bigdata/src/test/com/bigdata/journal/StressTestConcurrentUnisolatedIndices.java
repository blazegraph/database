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
 * Created on Oct 9, 2007
 */

package com.bigdata.journal;

import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.WriteExecutorService.RetryException;
import com.bigdata.rawstore.Bytes;
import com.bigdata.test.ExperimentDriver;
import com.bigdata.test.ExperimentDriver.IComparisonTest;
import com.bigdata.test.ExperimentDriver.Result;

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

        doConcurrentClientTest(journal,//
                5,// timeout
                20,// nresources
                1, // minLocks
                3, // maxLocks
                100, // ntrials
                3, // keyLen
                100, // nops
                0.02d // failureRate
        );
        
    }

    /**
     * A stress test of concurrent writers on one or more named indices.
     * 
     * @param journal
     *            The database.
     * 
     * @param timeout
     *            The #of seconds before the test will terminate.
     * 
     * @param nresources
     *            The #of named indices that will be used by the tasks.
     * 
     * @param minLocks
     *            The minimum #of resources in which a writer will obtain a lock
     *            in [<i>0</i>:<i>nresources</i>].
     * 
     * @param maxLocks
     *            The maximum #of resources in which a writer will obtain a lock
     *            in [<i>minLocks</i>:<i>nresources</i>].
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
     * @param failureRate
     *            The percentage of {@link Writer}s that will throw a
     *            {@link SpuriousException} rather than completing normally.
     * 
     * @todo factor out the operation to be run as a test parameter?
     */
    static public Result doConcurrentClientTest(Journal journal, long timeout,
            int nresources, int minLocks, int maxLocks, int ntrials,
            int keyLen, int nops, double failureRate)
            throws InterruptedException {
        
        if(journal==null) throw new IllegalArgumentException();
        
        if(timeout<=0) throw new IllegalArgumentException();
        
        if(nresources<=0) throw new IllegalArgumentException();
        
        if(minLocks<0) throw new IllegalArgumentException();
        
        if(maxLocks<minLocks||maxLocks>nresources) throw new IllegalArgumentException();
        
        if(ntrials<1) throw new IllegalArgumentException();

        if(keyLen<1) throw new IllegalArgumentException();
        
        if(nops<0) throw new IllegalArgumentException();

        if(failureRate<0.0||failureRate>1.0) throw new IllegalArgumentException();
        
        Random r = new Random();

        /*
         * Setup the named resources/indices.
         */
        final String[] resources = new String[nresources];
        {
         
            for(int i=0; i<nresources; i++) {
            
                resources[i] = "index#"+i;
                
                journal.registerIndex(resources[i], BTree.create(journal,
                        new IndexMetadata(resources[i], UUID.randomUUID())));
                
            }
            
            journal.commit();
            
        }
        
        System.err.println("Created indices: "+Arrays.toString(resources));

        /*
         * Setup the tasks that we will submit.
         */
        
        Collection<AbstractTask> tasks = new HashSet<AbstractTask>(); 

        ConcurrentHashMap<IIndex, Thread> btrees = new ConcurrentHashMap<IIndex, Thread>();
        
        for(int i=0; i<ntrials; i++) {

            // choose nlocks and indices to use.
            
            int nlocks = r.nextInt(maxLocks-minLocks)+minLocks;
            
            assert nlocks>=minLocks && nlocks<=maxLocks;
            
            Collection<String> tmp = new HashSet<String>(nlocks);
            
            while(tmp.size()<nlocks) {

                tmp.add(resources[r.nextInt(nresources)]);
                
            }
            
            String[] resource = tmp.toArray(new String[nlocks]);
            
            tasks.add(new WriteTask(journal, resource, keyLen, nops,
                    failureRate, btrees));

        }

        /*
         * Run all tasks and wait for up to the timeout for them to complete.
         */

        System.err.println("Submitting "+tasks.size()+" tasks");
        
        final long begin = System.currentTimeMillis();

        final List<Future<Object>> results = journal.invokeAll(tasks, timeout, TimeUnit.SECONDS);

        final long elapsed = System.currentTimeMillis() - begin;

        /*
         * Examine the futures to see how things went.
         */
        Iterator<Future<Object>> itr = results.iterator();
        
        int nfailed = 0; // #of tasks that failed.
        int nretry = 0; // #of tasks that threw RetryException
        int ninterrupt = 0; // #of interrupted tasks.
        int ncommitted = 0; // #of tasks that successfully committed.
        int nuncommitted = 0; // #of tasks that did not complete in time.
        
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

                if(isInnerCause(ex, InterruptedException.class)) {

                    /*
                     * Note: Tasks will be interrupted if a timeout occurs when
                     * attempting to run the submitted tasks - this is normal.
                     */
                    
                    log.warn("Interrupted: "+ex);
                    
                    ninterrupt++;
                    
                } else if(isInnerCause(ex, SpuriousException.class)) {
                    
                    nfailed++;
                    
                } else if(isInnerCause(ex, RetryException.class)) {
                    
                    nretry++;
                    
                } else {
                
                    // Other kinds of exceptions are errors.
                    
                    fail("Not expecting: "+ex, ex);
                    
                }
                
            }
            
        }

        journal.shutdownNow();
        
        journal.deleteResources();
                
        /*
         * Compute bytes written per second.
         */
        
        long seconds = TimeUnit.SECONDS.convert(elapsed, TimeUnit.MILLISECONDS);
        
        long bytesWrittenPerSecond = journal.getRootBlockView().getNextOffset()
                / (seconds == 0 ? 1 : seconds);

        Result ret = new Result();
        
        // these are the results.
        ret.put("nfailed",""+nfailed);
        ret.put("nretry",""+nretry);
        ret.put("ncommitted",""+ncommitted);
        ret.put("ninterrupt",""+ninterrupt);
        ret.put("nuncommitted", ""+nuncommitted);
        ret.put("elapsed(ms)", ""+elapsed);
        ret.put("bytesWrittenPerSec", ""+bytesWrittenPerSecond);
        ret.put("tasks/sec", ""+(ncommitted * 1000 / elapsed));
        ret.put("maxRunning", ""+journal.getConcurrencyManager().writeService.getMaxRunning());
        ret.put("maxPoolSize", ""+journal.getConcurrencyManager().writeService.getMaxPoolSize());
        ret.put("maxLatencyUntilCommit", ""+journal.getConcurrencyManager().writeService.getMaxLatencyUntilCommit());
        ret.put("maxCommitLatency", ""+journal.getConcurrencyManager().writeService.getMaxCommitLatency());

        System.err.println(ret.toString(true/*newline*/));
        
        return ret;
       
    }
    
    /**
     * A task that writes on named unisolated index(s).
     */
    public static class WriteTask extends AbstractTask {

        private final int keyLen;
        private final int nops;
        private final double failureRate;
        private final ConcurrentHashMap<IIndex, Thread> btrees;
        
        final Random r = new Random();
        
        public WriteTask(IConcurrencyManager concurrencyManager,
                String[] resource, int keyLen, int nops, double failureRate,
                ConcurrentHashMap<IIndex, Thread> btrees) {

            super(concurrencyManager, ITx.UNISOLATED, resource);

            this.keyLen = keyLen;
            
            this.nops = nops;
            
            this.failureRate = failureRate;
            
            this.btrees = btrees;
            
        }

        /**
         * Executes random operation on a named unisolated index.
         * 
         * @return null
         */
        public Object doTask() throws Exception {

            // the index names on which the writer holds a lock.
            final String[] resource = getResource();
            
            IIndex[] indices = new IIndex[resource.length];
            
            for (int i = 0; i < resource.length; i++) {

                indices[i] = getJournal().getIndex(resource[i]);

                if (btrees.put(indices[i], Thread.currentThread()) != null) {

                    throw new AssertionError(
                            "Unisolated index already in use: " + resource[i]);

                }

            }
            
            try {

                // Random write operations on the named index(s).

                for (int i = 0; i < nops; i++) {

                    IIndex ndx = indices[i % resource.length];
                    
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

                if (r.nextDouble() < failureRate) {

                    throw new SpuriousException();

                }

                return null;
                
            } finally {

                for(int i=0; i<resource.length; i++) {

                    IIndex ndx = indices[i];
                    
                    if(ndx!=null) btrees.remove(ndx);
                    
                }

            }
            
        }
        
    }
    
    /**
     * Thrown by a {@link Writer} if it is selected for abort based on the
     * {@link TestOptions#FAILURE_RATE}.
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
     * @todo can we get to the point of being IO bound with lots of small write
     *       operations on lots of indices using Disk and a 2CPUs? 4CPUs? With
     *       larger write operations? With fewer indices?
     * 
     * @todo Try to make this a correctness test since there are lots of little
     *       ways in which things can go wrong.
     * 
     * @todo use the failureRate to vet a strategy where we checkpoint indices
     *       after each task and rollback to the prior checkpoint iff a task
     *       fails rather than discarding the entire commit group.
     * 
     * @see ExperimentDriver
     * @see GenerateExperiment
     */
    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();

        // avoids journal overflow when running out to 60 seconds.
        properties.put(Options.MAXIMUM_EXTENT, ""+Bytes.megabyte32*400);

//        properties.setProperty(Options.FORCE_ON_COMMIT,ForceEnum.No.toString());
        
//        properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient.toString());
        
//        properties.setProperty(Options.BUFFER_MODE, BufferMode.Direct.toString());

//        properties.setProperty(Options.BUFFER_MODE, BufferMode.Mapped.toString());

        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());

        properties.setProperty(Options.CREATE_TEMP_FILE, "true");
        
        properties.setProperty(TestOptions.TIMEOUT,"60");

        properties.setProperty(TestOptions.NRESOURCES,"10");

        properties.setProperty(TestOptions.MIN_LOCKS,"1");
        
        properties.setProperty(TestOptions.MAX_LOCKS,"3");

        properties.setProperty(TestOptions.NTRIALS,"20000");

        properties.setProperty(TestOptions.KEYLEN,"4");

        properties.setProperty(TestOptions.NOPS,"4");

        properties.setProperty(TestOptions.FAILURE_RATE,"0.00");

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
    public static interface TestOptions extends ConcurrencyManager.Options {

        /**
         * The timeout for the test (seconds).
         */
        public static final String TIMEOUT = "timeout";
        
        /**
         * The #of named resources from which {@link Writer}s may choosen the
         * indices on which they will write.
         */
        public static final String NRESOURCES = "nresources";

        /**
         * The minimum #of locks that a writer will obtain (0 or more, but a
         * writer with zero locks will not write on anything).
         */
        public static final String MIN_LOCKS = "minLocks";

        /**
         * The maximum #of locks that a writer will obtain (LTE
         * {@link #NRESOURCES}). A writer will write on each resource that it
         * locks.
         */
        public static final String MAX_LOCKS = "maxLocks";

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
         * The failure rate [0.0:1.0]. A {@link Writer} aborts by throwing a
         * {@link SpuriousException}.
         */
        public static final String FAILURE_RATE = "failureRate";
        
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

        final int nresources = Integer.parseInt(properties.getProperty(TestOptions.NRESOURCES));

        final int minLocks = Integer.parseInt(properties.getProperty(TestOptions.MIN_LOCKS));
        
        final int maxLocks = Integer.parseInt(properties.getProperty(TestOptions.MAX_LOCKS));

        final int ntrials = Integer.parseInt(properties.getProperty(TestOptions.NTRIALS));

        final int keyLen = Integer.parseInt(properties.getProperty(TestOptions.KEYLEN));

        final int nops = Integer.parseInt(properties.getProperty(TestOptions.NOPS));

        final double failureRate = Double.parseDouble(properties.getProperty(TestOptions.FAILURE_RATE));

        Result result = doConcurrentClientTest(journal, timeout, nresources, minLocks, maxLocks, ntrials,
                keyLen, nops, failureRate );

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

            // @todo vary nresources, minLocks, and maxLocks.
            defaultProperties.put(TestOptions.NRESOURCES,"10");

            defaultProperties.put(TestOptions.MIN_LOCKS,"1");
            
            defaultProperties.put(TestOptions.MAX_LOCKS,"3");

            defaultProperties.put(TestOptions.KEYLEN,"4");

            defaultProperties.put(TestOptions.NOPS,"100");

            defaultProperties.put(TestOptions.FAILURE_RATE,"0.02");

            /*
             * Build up the conditions.
             */
            
            List<Condition>conditions = new ArrayList<Condition>();

            conditions.add(new Condition(defaultProperties));
            
            // @todo also vary the maximum pool size.
            // @todo report the maximum pool size as a result along with maxrunning.
            conditions = apply(conditions,new NV[][]{
                    new NV[]{new NV(TestOptions.WRITE_SERVICE_CORE_POOL_SIZE,"1"),},
                    new NV[]{new NV(TestOptions.WRITE_SERVICE_CORE_POOL_SIZE,"10"),},
                    new NV[]{new NV(TestOptions.WRITE_SERVICE_CORE_POOL_SIZE,"20"),},
                    new NV[]{new NV(TestOptions.WRITE_SERVICE_CORE_POOL_SIZE,"50"),},
                    new NV[]{new NV(TestOptions.WRITE_SERVICE_CORE_POOL_SIZE,"100"),},
            });
            
//            conditions = apply(conditions,new NV[]{
//                    new NV(TestOptions.NOPS,"1"),
//                    new NV(TestOptions.NOPS,"10"),
//                    new NV(TestOptions.NOPS,"100"),
//                    new NV(TestOptions.NOPS,"1000"),
//            });
//            
//            conditions = apply(conditions,new NV[]{
//                    new NV(TestOptions.KEYLEN,"4"),
//                    new NV(TestOptions.KEYLEN,"8"),
////                    new NV(TestOptions.KEYLEN,"32"),
////                    new NV(TestOptions.KEYLEN,"64"),
////                    new NV(TestOptions.KEYLEN,"128"),
//            });
            
            conditions = apply(
                    conditions,
                    new NV[][] { //
                            new NV[] { new NV(Options.BUFFER_MODE,
                                    BufferMode.Transient), }, //
                            new NV[] { new NV(Options.BUFFER_MODE,
                                    BufferMode.Direct), }, //
                            new NV[] {
                                    new NV(Options.BUFFER_MODE, BufferMode.Direct),
                                    new NV(Options.FORCE_ON_COMMIT, ForceEnum.No
                                            .toString()), }, //
                            new NV[] { new NV(Options.BUFFER_MODE, BufferMode.Mapped), }, //
                            new NV[] { new NV(Options.BUFFER_MODE, BufferMode.Disk), }, //
                            new NV[] {
                                    new NV(Options.BUFFER_MODE, BufferMode.Disk),
                                    new NV(Options.FORCE_ON_COMMIT, ForceEnum.No
                                            .toString()), }, //
                    });
            
            Experiment exp = new Experiment(className,defaultProperties,conditions);

            // copy the output into a file and then you can run it later.
            System.err.println(exp.toXML());

        }
        
    }
    
}
