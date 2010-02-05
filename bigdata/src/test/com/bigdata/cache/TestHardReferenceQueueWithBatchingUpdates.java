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
 * Created on Jan 11, 2010
 */

package com.bigdata.cache;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase2;

import com.bigdata.journal.ConcurrencyManager;
import com.bigdata.test.ExperimentDriver;
import com.bigdata.test.ExperimentDriver.IComparisonTest;
import com.bigdata.test.ExperimentDriver.Result;
import com.bigdata.util.NV;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Test suite for {@link HardReferenceQueueWithBatchingUpdates}. The class under
 * test provides a thread-local buffer for updates
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo There is a lot more that could be tested here.
 */
public class TestHardReferenceQueueWithBatchingUpdates extends TestCase2
        implements IComparisonTest {

    /**
     * 
     */
    public TestHardReferenceQueueWithBatchingUpdates() {
    }

    /**
     * @param name
     */
    public TestHardReferenceQueueWithBatchingUpdates(String name) {
        super(name);
    }

    /**
     * Basic test verifies that eviction is deferred until a batch is ready.
     */
    public void test01() {

        final HardReferenceQueueEvictionListener<String> listener = null;

        // capacity of the shared queue.
        final int capacity = 27;

        // #of elements to test in each thread-local queue on add.
        final int threadLocalNSCan = 4;

        // capacity of the thread-local queue.
        final int threadLocalQueueCapacity = 4;
        
        // may be zero to disable tryLock() (makes test more deterministic).  
        final int threadLocalTryLockSize = 0;

        final String ref0 = "0";
        final String ref1 = "1";
        final String ref2 = "2";
        final String ref3 = "3";
        final String ref4 = "4";
        final String ref5 = "5";

        final IHardReferenceQueue<String> q = new HardReferenceQueueWithBatchingUpdates<String>(
                new HardReferenceQueue<String>(listener, capacity, 0/* nscan */),
//                listener, capacity,
                threadLocalNSCan, threadLocalQueueCapacity,
                threadLocalTryLockSize,
                null// batched updates listener.
                );

        // add ref, but not batched through.
        q.add(ref0);
        assertEquals(0, q.size());

        // same ref, so eliminated by scan.
        q.add(ref0);
        assertEquals(0, q.size());

        // 2nd ref.
        q.add(ref1);
        assertEquals(0, q.size());

        // 3rd ref.
        q.add(ref2);
        assertEquals(0, q.size());

        // 4th ref. causes batch eviction to the shared buffer before adding
        // the new element.
        q.add(ref3);
        assertEquals(3, q.size());
        
    }

    /**
     * XorShift
     *
     * @author Brian Goetz and Tim Peierls
     */
    public static class XorShift {
        
        static final AtomicInteger seq = new AtomicInteger(8862213);
        
        int x = -1831433054;

        public XorShift(int seed) {
            x = seed;
        }

        public XorShift() {
            this((int) System.nanoTime() + seq.getAndAdd(129));
        }

        public int next() {
            x ^= x << 6;
            x ^= x >>> 21;
            x ^= (x << 7);
            return x;
        }
    }
    
    /**
     * A default configuration of a parameterized stress test.
     * 
     * @throws ExecutionException
     * @throws TimeoutException
     * @throws BrokenBarrierException
     * @throws InterruptedException
     */
    public void test_stress() throws InterruptedException,
            BrokenBarrierException, TimeoutException, ExecutionException {

        final XorShift r = new XorShift();

//        final int spinMax = 0;//50000;
        final int ndistinct = 10000;
//        final long waitMillisMax = 20;
        
        // pre-populate array of reference objects since Integer.valueOf() blocks.
        final Integer[] vals = new Integer[ndistinct];
        for(int i=0; i<ndistinct; i++) {
            vals[i] = Integer.valueOf(i);
        }
        
        doStressTest(//
                10L,// timeout
                TimeUnit.SECONDS,// unit
                8, // threadPoolSize
                8000, // queue capacity
                0, // threadLocalNScan
                32,//64, // threadLocalQueueCapacity
                16,//32, // threadLocalTryLockSize
                new Callable<Object>() {
                    public Object call() throws Exception {
//                        final int nspin = spinMax == 0 ? 0 : Math.abs(r.next()
//                                % spinMax);
//                        for (int i = 0; i < nspin; i++) {
//                            // spin
                        // }
                        if (r.next() % 10 == 1) {
                            Thread.yield();
//                            final long waitMillis = Math.abs(r.next()
//                                    % waitMillisMax);
//                            if (waitMillis > 0)
//                                Thread.sleep(waitMillis);
                        }
                        return vals[Math.abs(r.next() % ndistinct)];
                    }
                });

    }

    /**
     * A parameterized stress test in which a pool of threads drives adds to the
     * queue. The adds will go onto the thread-local queues first and should
     * then be batched to the backing shared queue. The object references are
     * drawn from a pool of references. This tests thread safety and may also be
     * used to measure the impact of different parameters on throughput,
     * including the scan of the thread-local queue for matching references, the
     * capacity of the thread-local queue, the size of the thread-local queue at
     * which an attempt is made to barge in on the lock, and the effect of the
     * #of concurrent threads. A workload function may also be configured, in
     * which case the latency of the thread's primary operation and the effects
     * of blocking waits (which are similar to waiting on IO) may be explored.
     * 
     * @param timeout
     *            The duration of the stress test.
     * @param unit
     *            The unit in which the test duration is measured.
     * @param threadPoolSize
     *            The size of the thread pool.
     * @param capacity
     *            The capacity of the shared queue (does not include the
     *            capacity of the thread-local queues).
     * @param threadLocalNScan
     *            The #of references to scan on the thread-local queue.
     * @param threadLocalQueueCapacity
     *            The capacity of the thread-local queues in which the updates
     *            are gathered before they are batched into the shared queue.
     *            This must be at least
     * @param threadLocalTryLockSize
     *            Once the thread-local queue is this full an attempt will be
     *            made to barge in on the lock and batch the updates to the
     *            shared queue. This feature may be disabled by passing ZERO
     *            (0).
     * 
     * @return touches per unit.
     * 
     * @throws BrokenBarrierException
     * @throws InterruptedException
     * @throws TimeoutException
     * @throws ExecutionException
     */
    public long doStressTest(final long timeout, final TimeUnit unit,
            final int threadPoolSize, final int capacity,
            final int threadLocalNScan, final int threadLocalQueueCapacity,
            final int threadLocalTryLockSize, final Callable<?> worker)
            throws InterruptedException, BrokenBarrierException,
            TimeoutException, ExecutionException {

        final HardReferenceQueueWithBatchingUpdates<Object> queue = new HardReferenceQueueWithBatchingUpdates<Object>(
                new HardReferenceQueue<Object>(null/*listener*/, capacity, 0/* nscan */),
//                null/* listener */, capacity, 
                threadLocalNScan, threadLocalQueueCapacity,
                threadLocalTryLockSize, null// batched updates listener
                );

        final ExecutorService service = Executors.newFixedThreadPool(
                threadPoolSize, new DaemonThreadFactory(getName()));

        final AtomicLong ntouch = new AtomicLong(0L);
        final AtomicLong nadd = new AtomicLong(0L);
        
        final long elapsed;
        try {

            final AtomicBoolean done = new AtomicBoolean(false);

            final CyclicBarrier barrier = new CyclicBarrier(threadPoolSize + 1);

            final List<Future<Object>> futures = new ArrayList<Future<Object>>(
                    threadPoolSize);

            for (int i = 0; i < threadPoolSize; i++) {

                final Callable<Object> task = new Callable<Object>() {

                    long localTouchCount = 0L;
                    long localAddCount = 0L;

                    public Object call() throws Exception {

                        barrier.await();

                        try {

                            while (!done.get()) {

                                final Object ref = worker.call();

                                if (queue.add(ref))
                                    localAddCount++;

                                localTouchCount++;

                            }
                            
                        } catch (Throwable t) {

                            // Note failures eagerly.
                            System.err.println("Task failed: " + t
                                    + ", thread=" + Thread.currentThread());
                            
                            throw new RuntimeException(t);
                            
                        } finally {

                            // update the global counters.
                            ntouch.addAndGet(localTouchCount);

                            nadd.addAndGet(localAddCount);

                        }

                        // done.
                        return null;

                    }

                };

                futures.add(service.submit(task));

            }

            // wait for the barrier to break and then for the test to end.
            {

                assert !barrier.isBroken();

                barrier.await();

                // time when the test starts.
                final long begin = System.nanoTime();

                // time remaining in the test.
                final long nanos = unit.toNanos(timeout);
                long remaining = nanos;

                System.err.println("Running...");

                while (remaining >= 0) {
                
                    Thread.sleep(TimeUnit.NANOSECONDS.toMillis(remaining),
                            (int) Math.min(999999, remaining));

                    remaining = nanos - (System.nanoTime() - begin);
                    
                }

                final long end = System.nanoTime();

                elapsed = end - begin;

                // tell worker tasks to halt.
                done.set(true);

            }

            // shutdown the executor service.
            {

                // normal shutdown.
                service.shutdown();

                if (!service.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)) {

                    fail("Service did not terminate");

                }

                // verify no errors in the worker tasks.
                int nerrors = 0;
                for (Future<Object> f : futures) {

                    try {
                        
                        f.get();

                    } catch (ExecutionException t) {
                        
                        nerrors++;
                        
                        t.printStackTrace(System.err);
                        
                    }

                }

                if (nerrors > 0)
                    fail("There were " + nerrors + " errors.");

            }

            {
                final NumberFormat f = DecimalFormat.getNumberInstance();
                f.setGroupingUsed(true);
                final long elapsedUnits = unit.convert(elapsed,
                        TimeUnit.NANOSECONDS);
                final long touchesPerUnit = ntouch.get() / elapsedUnits;
                final long addsPerUnit = nadd.get() / elapsedUnits;
                System.err.println("timeout = " + f.format(timeout) + " "
                        + unit.toString());
                System.err.println("elapsed = " + f.format(elapsedUnits) + " "
                        + unit.toString());
                System.err.println("ntouches = " + f.format(ntouch) + ", per "
                        + unit + "=" + f.format(touchesPerUnit));
                System.err.println("nadded  = " + f.format(nadd) + ", per "
                        + unit + "=" + f.format(addsPerUnit));
                System.err.println("ndups   = "
                        + f.format(ntouch.get() - nadd.get()));

                return touchesPerUnit; 
            }

        } finally {

            // terminate the thread pool on error.
            service.shutdownNow();

        }

    }

    public void setUpComparisonTest(Properties properties) throws Exception {
        
//        queue = xxx;
        
    }
    
    public void tearDownComparisonTest() throws Exception {
    
        queue = null;

    }
    private IHardReferenceQueue<Object> queue;
    
    /**
     * Additional properties understood by this test.
     */
    public static interface TestOptions extends ConcurrencyManager.Options {

        /**
         * The timeout for the test.
         */
        public static final String TIMEOUT = "timeout";
        public static final String UNITS = "units";
        
        /**
         * The #of concurrent threads to run.
         */
        public static final String NTHREADS = "nthreads";

        /**
         * The capacity of the shared queue.
         */
        public static final String CAPACITY = "capacity";

        /**
         * The #of elements to scan on the thread local queue in order to
         * identify duplicates.
         */
        public static final String THREAD_LOCAL_NSCAN = "threadLocalNScan";

        /**
         * The capacity of the thread local queue. 
         */
        public static final String THREAD_LOCAL_CAPACITY = "threadLocalCapacity";

        /**
         * The size of the thread local queue at which an attempt will be made
         * to barge in on the lock and batch the updates to the shared queue.
         */
        public static final String THREAD_LOCAL_TRY_LOCK_SIZE = "threadLocalTryLockSize";

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

        final TimeUnit units = TimeUnit.valueOf(properties
                .getProperty(TestOptions.UNITS));

        final int threadPoolSize = Integer.parseInt(properties
                .getProperty(TestOptions.NTHREADS));
        
        final int capacity = Integer.parseInt(properties
                .getProperty(TestOptions.CAPACITY));
        
        final int threadLocalNScan= Integer.parseInt(properties
                .getProperty(TestOptions.THREAD_LOCAL_NSCAN));
        
        final int threadLocalCapacity = Integer.parseInt(properties
                .getProperty(TestOptions.THREAD_LOCAL_CAPACITY));
        
        final int threadLocalTryLockSize = Integer.parseInt(properties
                .getProperty(TestOptions.THREAD_LOCAL_TRY_LOCK_SIZE));

        final Result result = new Result();

        /*
         * Setup the worker.
         */
        final XorShift r = new XorShift();

        final int ndistinct = 10000;

        // pre-populate array of reference objects since Integer.valueOf()
        // blocks.
        final Integer[] vals = new Integer[ndistinct];
        for (int i = 0; i < ndistinct; i++) {
            vals[i] = Integer.valueOf(i);
        }

        final long touchesPerUnit = doStressTest(timeout,
                units, //
                threadPoolSize, //
                capacity,//
                threadLocalNScan, threadLocalCapacity, threadLocalTryLockSize,
                new Callable<Object>() {
                    public Object call() throws Exception {
//                        final int nspin = spinMax == 0 ? 0 : Math.abs(r.next()
//                                % spinMax);
//                        for (int i = 0; i < nspin; i++) {
//                            // spin
                        // }
                        if (r.next() % 100 == 1) {
                            Thread.yield();
//                            final long waitMillis = Math.abs(r.next()
//                                    % waitMillisMax);
//                            if (waitMillis > 0)
//                                Thread.sleep(waitMillis);
                        }
                        return vals[Math.abs(r.next() % ndistinct)];
                    }});

        result.put("touches/unit", "" + touchesPerUnit);

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
         *            ignored.
         */
        public static void main(final String[] args) throws Exception {

            // this is the test to be run.
            final String className = TestHardReferenceQueueWithBatchingUpdates.class.getName();
            
            /* 
             * Set defaults for each condition.
             */
            
            final Map<String,String> defaultProperties = new HashMap<String,String>();

            defaultProperties.put(TestOptions.TIMEOUT, "10");
            
            defaultProperties.put(TestOptions.UNITS, TimeUnit.SECONDS.name());

            defaultProperties.put(TestOptions.CAPACITY,"8000");

            defaultProperties.put(TestOptions.THREAD_LOCAL_NSCAN,"0");

            /*
             * Build up the conditions.
             */
            
            List<Condition>conditions = new ArrayList<Condition>();

            conditions.add(new Condition(defaultProperties));

            conditions = apply(conditions, new NV[] {
                    new NV(TestOptions.NTHREADS, "1"),
                    new NV(TestOptions.NTHREADS, "2"),
                    new NV(TestOptions.NTHREADS, "4"),
                    new NV(TestOptions.NTHREADS, "8"),
                    new NV(TestOptions.NTHREADS, "16"), });

            conditions = apply(
                    conditions,
                    new NV[][] { //
                            new NV[] {
                                    new NV(TestOptions.THREAD_LOCAL_CAPACITY,
                                            "16"),
                                    new NV(
                                            TestOptions.THREAD_LOCAL_TRY_LOCK_SIZE,
                                            "8"), }, //
                            new NV[] {
                                    new NV(TestOptions.THREAD_LOCAL_CAPACITY,
                                            "32"),
                                    new NV(
                                            TestOptions.THREAD_LOCAL_TRY_LOCK_SIZE,
                                            "16"), }, //
                            new NV[] {
                                    new NV(TestOptions.THREAD_LOCAL_CAPACITY,
                                            "64"),
                                    new NV(
                                            TestOptions.THREAD_LOCAL_TRY_LOCK_SIZE,
                                            "32"), }, //
                            new NV[] {
                                    new NV(TestOptions.THREAD_LOCAL_CAPACITY,
                                            "128"),
                                    new NV(
                                            TestOptions.THREAD_LOCAL_TRY_LOCK_SIZE,
                                            "64"), }, //
                    //
                    });
                        
            final Experiment exp = new Experiment(className, defaultProperties,
                    conditions);

            // copy the output into a file and then you can run it later.
            System.err.println(exp.toXML());

        }
        
    }

}
