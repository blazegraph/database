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
 * Created on Oct 12, 2007
 */

package com.bigdata.journal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.journal.ConcurrencyManager.Options;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.test.ExperimentDriver;
import com.bigdata.test.ExperimentDriver.IComparisonTest;
import com.bigdata.test.ExperimentDriver.Result;

/**
 * Stress test of the group commit mechanism. This class may be used to tune the
 * performance of a variety of parameters that effect the throughput of group
 * commit. There are other stress tests that show the throughput for more
 * complex tasks or for isolated tasks (transactions). This test focuses purely
 * on the group commit mechanism itself.
 * <p>
 * The basic test submits a bunch of unisolated write tasks to a
 * {@link WriteExecutorService} of a known capacity. The write tasks are
 * designed to have with non-overlapping lock requirements so that they may run
 * with the maximum possible concurrency. The test the examines how many of the
 * tasks make it into the commit group (on average).
 * <p>
 * In order for there to be a commit, each task must write some data. In the
 * current design each task creates a named index. This means that there is some
 * data to write, but also that some synchronization is required on
 * {@link AbstractJournal#name2Addr}.
 * 
 * @todo another way to do this is to pre-generate all the indices, note the
 *       current commit counter, and then submit a bunch of simple index write
 *       tasks. This can be used to measure the effective index write
 *       throughput.
 * 
 * @todo yet another approach is to have each task perform an
 *       {@link IRawStore#write(java.nio.ByteBuffer)} which is the absolute
 *       minimum effort that a task can do to write on the store.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StressTestGroupCommit extends ProxyTestCase implements IComparisonTest {

    /**
     * 
     */
    public StressTestGroupCommit() {
        super();
    }

    /**
     * @param arg0
     */
    public StressTestGroupCommit(String arg0) {
        super(arg0);
    }

    /**
     * Measures the maximum rate at which a single thread can register named
     * indices. This provides an approximate lower bound for throughput using
     * group commit and the same index creation task. The actual throughput
     * could be somewhat higher or lower since access to the index in which the
     * metadata records for the named indices are stored is itself synchronized,
     * but there is other work to be performed by the task, such as the creation
     * of the named index and writing it onto the store to obtain its metadata
     * record.
     * <p>
     * Note: This is for data collection - it is not really a unit test.
     */
    public void test_singleThreadIndexCreationRate() {
        
        Properties properties = getProperties();
        
        Journal journal = new Journal(properties);

        // the initial value of the commit counter.
        final long beginCommitCounter = journal.getRootBlockView().getCommitCounter();

        final long begin = System.currentTimeMillis();
        
        int ntasks = 1000;
        
        for (int i = 0; i < ntasks; i++) {

            // resource names are non-overlapping.
            final String resource = ""+i;
            
            final UUID indexUUID = UUID.randomUUID();

            final BTree ndx = BTree.create(journal, new IndexMetadata(resource,
                    indexUUID));
            
            ndx.writeCheckpoint();
            
            journal.registerIndex(resource, ndx);

        }

        // unchanged (no commit was performed).
        assertEquals(beginCommitCounter,journal.getRootBlockView().getCommitCounter());
        
        final long now = System.currentTimeMillis();
        
        final long elapsed1 = now - begin;
        
        journal.commit();
        
        final long elapsed2 = now - begin;
        
        System.err.println("#tasks=" + ntasks + ", elapsed=" + elapsed1
                + ", #indices created per second="
                + (int)(1000d * ntasks / elapsed1) + ", commit=" + elapsed2 + "ms");
        
        journal.shutdownNow();
        
        journal.destroyAllResources();
        
    }

    /**
     * Measures the maximum rate at which two threads can register named
     * indices. This provides a good approximation for the throughput using
     * group commit and the same index creation task when the thread pool has
     * two threads.
     * <p>
     * Note: This is for data collection - it is not really a unit test.
     * 
     * @throws InterruptedException
     */
    public void test_twothreadIndexCreationRate() throws InterruptedException {

        Properties properties = getProperties();
        
        final Journal journal = new Journal(properties);

        // the initial value of the commit counter.
        final long beginCommitCounter = journal.getRootBlockView().getCommitCounter();

        final long begin = System.currentTimeMillis();
        
        final int ntasks = 1000;

        final Lock lock = new ReentrantLock();
        final AtomicInteger ndone = new AtomicInteger(0);
        final Condition done = lock.newCondition();
        
        Thread t1 = new Thread() {

            public void run() {
                
                for (int i = 0; i < ntasks/2; i++) {

                    // resource names are non-overlapping.
                    final String resource = ""+i;
                    
                    final UUID indexUUID = UUID.randomUUID();

                    final BTree ndx = BTree.create(journal, new IndexMetadata(
                            resource, indexUUID));
                    
                    journal.registerIndex(resource, ndx);

                }

                lock.lock();
                try {
                    ndone.incrementAndGet();
                    done.signal();
                }
                finally {
                    lock.unlock();
                }
                
            }
        
        };

        Thread t2 = new Thread() {
            
            public void run() {
                for (int i = ntasks/2; i < ntasks; i++) {

                    // resource names are non-overlapping.
                    final String resource = ""+i;
                    
                    final UUID indexUUID = UUID.randomUUID();

                    final BTree ndx = BTree.create(journal, new IndexMetadata(
                            resource, indexUUID));

                    journal.registerIndex(resource, ndx);

                }
                
                lock.lock();
                try {
                    ndone.incrementAndGet();
                    done.signal();
                }
                finally {
                    lock.unlock();
                }
                
            }

        };
        
        t1.setDaemon(true);
        t2.setDaemon(true);
        
        t1.start();
        t2.start();

        lock.lock();
        try {
            while(ndone.get()<2) {
                done.await();
            }
        } finally {
            lock.unlock();
        }
        
        // unchanged (no commit was performed).
        assertEquals(beginCommitCounter,journal.getRootBlockView().getCommitCounter());
        
        final long now = System.currentTimeMillis();
        
        final long elapsed1 = now - begin;
        
        journal.commit();
        
        final long elapsed2 = now - begin;
        
        System.err.println("#tasks=" + ntasks + ", elapsed=" + elapsed1
                + ", #indices created per second="
                + (int)(1000d * ntasks / elapsed1) + ", commit=" + elapsed2 + "ms");
        
        journal.shutdownNow();
        
        journal.destroyAllResources();

    }
    
    /**
     * Runs a single condition.
     *  
     * @throws Exception 
     */
    public void test_groupCommit() throws Exception {

        final int writeServiceCorePoolSize = 100;
        
        Properties properties = getProperties();

        properties.setProperty(TestOptions.TIMEOUT,"10");
        
        properties.setProperty(TestOptions.NTASKS,"1000");

        properties.setProperty(Options.WRITE_SERVICE_CORE_POOL_SIZE, ""+writeServiceCorePoolSize);

        properties.setProperty(Options.WRITE_SERVICE_MAXIMUM_POOL_SIZE, "1000");

        properties.setProperty(Options.WRITE_SERVICE_PRESTART_ALL_CORE_THREADS, "true");

        properties.setProperty(Options.WRITE_SERVICE_QUEUE_CAPACITY, "100");
        
        Result result = doComparisonTest(properties);
            
        /*
         * Note: You SHOULD expect 80%+ of the tasks to participate in each
         * commit group. However, the actual number can be lower due to various
         * startup costs so sometimes this test will fail - it is really a
         * stress test and should be done once the store is up and running
         * already.
         */

        final double tasksPerCommit = Double.parseDouble(result.get("tasks/commit"));
        
        assertTrue(
                "average group commit size is too small? size="
                        + tasksPerCommit + ", corePoolSize="
                        + writeServiceCorePoolSize,
                tasksPerCommit > writeServiceCorePoolSize * .5);
        
    }

    /**
     * Options understood by this stress test.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface TestOptions extends ConcurrencyManager.Options {
        
        /**
         * The timeout for the test (seconds).
         */
        public static final String TIMEOUT = "timeout";
        
        /**
         * The #of tasks to submit.
         */
        public static final String NTASKS = "ntasks";
        
        /**
         * The #of records to insert into the index -or- ZERO (0) to only
         * create the index (default 0).
         */
        public static final String NINSERT = "NINSERT";
        
        public static final String DEFAULT_NINSERT = "0";
        
    }
    
    public Result doComparisonTest(Properties properties) throws Exception {

        final int ntasks = Integer.parseInt(properties.getProperty(TestOptions.NTASKS));

        final long timeout = Long.parseLong(properties.getProperty(TestOptions.TIMEOUT));
        
        final long ninsert = Long.parseLong(properties.getProperty(TestOptions.NINSERT,TestOptions.DEFAULT_NINSERT));
        
        Journal journal = new Journal(properties);

        // the initial value of the commit counter.
        final long beginCommitCounter = journal.getRootBlockView().getCommitCounter();

        /*
         * Create the tasks.
         */
        Collection<AbstractTask> tasks = new HashSet<AbstractTask>(ntasks);

        // updated by each task that runs.
        final AtomicLong nrun = new AtomicLong(0);

        for (int i = 0; i < ntasks; i++) {

            // resource names are non-overlapping.
            final String resource = ""+i;
            
            final UUID indexUUID = UUID.randomUUID();
            
            tasks.add( SequenceTask.newSequence(new AbstractTask[]{

                    new RegisterIndexTask(journal, resource, 
                            new IndexMetadata(resource, indexUUID)),

                    new AbstractTask(journal, ITx.UNISOLATED, resource) {
                        
                        protected Object doTask() throws Exception {

                            if(ninsert>0) {

                                KeyBuilder keyBuilder = new KeyBuilder(4);
                            
                                IIndex ndx = getIndex(resource); 

                                // inserts are ordered, which is best case performance.
                                for(int i=0; i<ninsert; i++) {
                            
                                    ndx.insert(keyBuilder.reset().append(i).getKey(),keyBuilder.getKey());

                                }
                                
                            }

                            nrun.incrementAndGet();
                            
                            return null;

                        }

                    }
            }));

        }

        /*
         * Submit all tasks.
         */
        
        final long begin = System.currentTimeMillis();
        
        try {
            
            journal.invokeAll(tasks,timeout,TimeUnit.SECONDS);

        } catch(RejectedExecutionException ex) {
            
            log.warn("Some tasks could not be submitted (queue is full?)", ex);
            
        }

//        System.err.println("timeout="+timeout);
//        
//        // sleep until the tasks are done or the timeout is expired.
//        while(true) {
//        
//            if(nrun.get()==ntasks) break;
//            
//            final long elapsed = System.currentTimeMillis() - begin;
//            
//            if(timeout > elapsed) {
//                
//                System.err.println("Timeout exceeded: timeout="+timeout+", elapsed="+elapsed);
//                
//                break;
//                
//            }
//            
//            /*
//             * Note: Don't wait too long it or throws off the estimate of the elapsed
//             * time.
//             */
//            synchronized(this) {
//                wait(100);
//            }
//            
//        }

        /*
         * the actual run time.
         */
        final long elapsed = System.currentTimeMillis() - begin;
        
        // #of tasks run by this moment in time.
        final long ndone = nrun.get();
        
        // the commit counter at this moment in time.
        final long ncommits = journal.getRootBlockView().getCommitCounter() - beginCommitCounter; 
        
        assertTrue("Zero commits?",ncommits>0);
        
        // #of commits per second.
        final double commitsPerSecond = ncommits * 1000d / elapsed;

        // #of tasks per second.
        final double tasksPerSecond = ndone * 1000d / elapsed;

        final double tasksPerCommit = ((double)ndone) / ncommits;
        
        Result result = new Result();
        
        result.put("ndone", ""+ndone);
        result.put("ncommits", ""+ncommits);
        result.put("elapsed", ""+elapsed);
        result.put("tasks/sec", ""+tasksPerSecond);
        result.put("commits/sec", ""+commitsPerSecond);
        result.put("tasks/commit", ""+tasksPerCommit);
        result.put("maxRunning", ""+journal.getConcurrencyManager().writeService.getMaxRunning());
        result.put("maxLatencyUntilCommit", ""+journal.getConcurrencyManager().writeService.getMaxLatencyUntilCommit());
        result.put("maxCommitLatency", ""+journal.getConcurrencyManager().writeService.getMaxCommitLatency());
        result.put("poolSize",""+journal.getConcurrencyManager().writeService.getPoolSize());
        
        System.err.println(result.toString(true/*newline*/));

        /*
         * This is using shutdownNow so we don't wait for all tasks to complete --
         * ignore the messy warnings and errors once shutdown is called!
         */
        
        journal.shutdownNow();
        
        journal.destroyAllResources();
        
        return result;
        
    }

    public void setUpComparisonTest(Properties properties) throws Exception {
        
    }

    public void tearDownComparisonTest() throws Exception {
        
    }

    /**
     * Run the stress test configured in the code.
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();
        
        properties.setProperty(TestOptions.NTASKS,"10000");

        properties.setProperty(TestOptions.NINSERT,"100");

        properties.setProperty(TestOptions.TIMEOUT,"5");
        
        properties.setProperty(Options.WRITE_SERVICE_CORE_POOL_SIZE, "200");

        properties.setProperty(Options.WRITE_SERVICE_MAXIMUM_POOL_SIZE, "1000");

        properties.setProperty(Options.WRITE_SERVICE_PRESTART_ALL_CORE_THREADS, "true");

        properties.setProperty(Options.WRITE_SERVICE_QUEUE_CAPACITY, "1000");

        properties.setProperty(Options.CREATE_TEMP_FILE, "true");

//        properties.setProperty(Options.BUFFER_MODE,BufferMode.Transient.toString());
        
//        properties.setProperty(Options.BUFFER_MODE,BufferMode.Direct.toString());
        
//        properties.setProperty(Options.BUFFER_MODE,BufferMode.Mapped.toString());

        properties.setProperty(Options.BUFFER_MODE,BufferMode.Disk.toString());
        
//        properties.setProperty(Options.FORCE_ON_COMMIT,ForceEnum.No.toString());

        IComparisonTest test = new StressTestGroupCommit();
        
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
            String className = StressTestGroupCommit.class.getName();
            
            Map<String,String> defaultProperties = new HashMap<String,String>();

            // force delete of the files on close of the journal under test.
            defaultProperties.put(Options.CREATE_TEMP_FILE,"true");

            // avoids journal overflow when running out to 60 seconds.
            defaultProperties.put(Options.MAXIMUM_EXTENT, ""+Bytes.megabyte32*400);

            /* 
             * Set defaults for each condition.
             */
            
            defaultProperties.put(TestOptions.TIMEOUT,"5");

            defaultProperties.put(TestOptions.NTASKS,"10000");

            List<Condition>conditions = new ArrayList<Condition>();
            
            conditions.add(new Condition(defaultProperties));

//            conditions = apply(conditions, new NV[] {
//                    new NV(TestOptions.WRITE_SERVICE_CORE_POOL_SIZE, "1"),
//                    new NV(TestOptions.WRITE_SERVICE_CORE_POOL_SIZE, "10"),
//                    new NV(TestOptions.WRITE_SERVICE_CORE_POOL_SIZE, "100"),
//                    new NV(TestOptions.WRITE_SERVICE_CORE_POOL_SIZE, "1000")
//                    });
//            
//            conditions = apply(conditions, new NV[] {
//                    new NV(TestOptions.WRITE_SERVICE_QUEUE_CAPACITY, "1"),
//                    new NV(TestOptions.WRITE_SERVICE_QUEUE_CAPACITY, "10"),
//                    new NV(TestOptions.WRITE_SERVICE_QUEUE_CAPACITY, "100"),
//                    new NV(TestOptions.WRITE_SERVICE_QUEUE_CAPACITY, "1000")
//                    });
            
            // co-vary the core pool size and the queue capacity.
            conditions = apply(conditions, new NV[][] {
                    new NV[] {
                            new NV(TestOptions.WRITE_SERVICE_CORE_POOL_SIZE,
                                    "500"),
                            new NV(TestOptions.WRITE_SERVICE_MAXIMUM_POOL_SIZE,
                                    "500"),
                            new NV(TestOptions.WRITE_SERVICE_QUEUE_CAPACITY,
                                    "500") },
                    new NV[] {
                            new NV(TestOptions.WRITE_SERVICE_CORE_POOL_SIZE,
                                    "1000"),
                            new NV(TestOptions.WRITE_SERVICE_MAXIMUM_POOL_SIZE,
                                    "1000"),
                            new NV(TestOptions.WRITE_SERVICE_QUEUE_CAPACITY,
                                    "1000") },
                    new NV[] {
                            new NV(TestOptions.WRITE_SERVICE_CORE_POOL_SIZE,
                                    "1500"),
                            new NV(TestOptions.WRITE_SERVICE_MAXIMUM_POOL_SIZE,
                                    "1500"),
                            new NV(TestOptions.WRITE_SERVICE_QUEUE_CAPACITY,
                                    "1500") },
                    new NV[] {
                            new NV(TestOptions.WRITE_SERVICE_CORE_POOL_SIZE,
                                    "2000"),
                            new NV(TestOptions.WRITE_SERVICE_MAXIMUM_POOL_SIZE,
                                    "2000"),
                            new NV(TestOptions.WRITE_SERVICE_QUEUE_CAPACITY,
                                    "2000") }
                    });
            
//            conditions = apply(conditions, new NV[] {
//                    new NV(TestOptions.WRITE_SERVICE_PRESTART_ALL_CORE_THREADS, "true"),
//                    new NV(TestOptions.WRITE_SERVICE_PRESTART_ALL_CORE_THREADS, "false"),
//                    });
            
            conditions = apply(conditions, new NV[][] {
//                    new NV[]{new NV(TestOptions.BUFFER_MODE, BufferMode.Transient.toString())},
//                    new NV[]{new NV(TestOptions.BUFFER_MODE, BufferMode.Direct.toString())},
                    new NV[]{new NV(TestOptions.BUFFER_MODE, BufferMode.Disk.toString())},
//                    new NV[]{new NV(TestOptions.BUFFER_MODE, BufferMode.Disk.toString()),
//                                    new NV(TestOptions.FORCE_ON_COMMIT,ForceEnum.No.toString())},
//                    new NV[]{new NV(TestOptions.BUFFER_MODE, BufferMode.Mapped.toString())},
                    });
            
            Experiment exp = new Experiment(className,defaultProperties,conditions);

            // copy the output into a file and then you can run it later.
            System.err.println(exp.toXML());

        }
        
    }
    
}
