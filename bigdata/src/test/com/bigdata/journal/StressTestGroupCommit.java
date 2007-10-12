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
 * Created on Oct 12, 2007
 */

package com.bigdata.journal;

import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.ConcurrentJournal.Options;
import com.bigdata.rawstore.IRawStore;
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
 * @todo There can be HUGE variation in the group commit throughput for this
 *       test from between a few operations per commit to 1000 operations per
 *       commit. One of the key factors appears to be having a limited capacity
 *       for the write service queue, but even then the performance can vary
 *       widely from run to run.
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
 * FIXME refactor to get the {@link IComparisonTest} interface running.
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
     * 
     * @todo refactor and parameterize and then explore the parameter space.
     * 
     * @throws InterruptedException
     */
    public void test_groupCommit() throws InterruptedException {

        Properties properties = getProperties();

        final int ntasks = 1000;
        
//        properties.setProperty(Options.WRITE_SERVICE_CORE_POOL_SIZE, "100");

//        properties.setProperty(Options.WRITE_SERVICE_MAXIMUM_POOL_SIZE, "1000");

        // prestart the worker threads for this test to minimize time to get up to speed.
//        properties.setProperty(Options.WRITE_SERVICE_PRESTART_ALL_CORE_THREADS, "true");

//        properties.setProperty(Options.WRITE_SERVICE_QUEUE_CAPACITY, "100");

//        properties.setProperty(Options.BUFFER_MODE,BufferMode.Transient.toString());
        
//        properties.setProperty(Options.FORCE_ON_COMMIT,ForceEnum.No.toString());
        
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
                            new UnisolatedBTree(journal, indexUUID)),
     
                    new AbstractTask(journal, ITx.UNISOLATED,
                            false/* readOnly */, resource) {

                        protected Object doTask() throws Exception {

                            nrun.incrementAndGet();
                            
                            return null;

                        }

                    }
            }));

        }

        /*
         * Submit all tasks.
         */
        
        final long timeout = 5000;
        
        final long begin = System.currentTimeMillis();
        
        try {
//          List<Future<Object>> futures = 
            journal.invokeAll(tasks);
        } catch(RejectedExecutionException ex) {
            log.warn("Some tasks could not be submitted (queue is full?)", ex);
        }

        // sleep until the tasks are done or the timeout is expired.
        while(nrun.get()!=ntasks && (timeout - (System.currentTimeMillis()-begin)>0)) {
        
            Thread.sleep(100);
            
        }

        // the actual run time.
        final long elapsed = System.currentTimeMillis() - begin;
        
        // #of tasks run by this moment in time.
        final long ndone = nrun.get();
        
        // the commit counter at this moment in time.
        final long ncommits = journal.getRootBlockView().getCommitCounter() - beginCommitCounter; 
        
        // #of commits per second.
        final double commitsPerSecond = ncommits * 1000d / elapsed;

        // #of tasks per second.
        final double tasksPerSecond = ndone * 1000d / elapsed;

        final double tasksPerCommit = ((double)ndone) / ncommits;
        
        System.err.println("ntasks="+ntasks+", ndone="+ndone+", ncommits="+ncommits+", elapsed="+elapsed+"ms");
        
        System.err.println("tasks/sec="+tasksPerSecond);
        
        System.err.println("commits/sec="+commitsPerSecond);
        
        System.err.println("tasks/commit="+tasksPerCommit);

        // current
        System.err.print("poolSize="+journal.writeService.getPoolSize());

        // initial
        System.err.print(", corePoolSize="+journal.writeService.getCorePoolSize());

        // max. allowed.
        System.err.println(", maximumPoolSize="+journal.writeService.getMaximumPoolSize());
        
        journal.shutdown();
        
        journal.delete();

        /*
         * Note: You SHOULD expect 80%+ of the tasks to participate in each
         * commit group on average. However, the actual number can be lower due
         * to various startup costs so sometimes this test will fail - it is
         * really a stress test and should be done once the store is up and
         * running already.
         */
        
        assertTrue("average group commit size is too small? size="
                + tasksPerCommit, tasksPerCommit > journal.writeService
                .getCorePoolSize() * .5);
        
    }

    public void setUpComparisonTest(Properties properties) throws Exception {
        // TODO Auto-generated method stub
        
    }

    public Result doComparisonTest(Properties properties) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    public void tearDownComparisonTest() throws Exception {
        // TODO Auto-generated method stub
        
    }
    
}
