/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on May 11, 2010
 */

package com.bigdata.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.bigdata.LRUNexus;
import com.bigdata.LRUNexus.AccessPolicyEnum;
import com.bigdata.LRUNexus.CacheSettings;
import com.bigdata.concurrent.TestLockManager;
import com.bigdata.io.FixedByteArrayBuffer;
import com.bigdata.io.IDataRecordAccess;
import com.bigdata.io.IFixedDataRecord;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.WormAddressManager;
import com.bigdata.test.ExperimentDriver;
import com.bigdata.test.ExperimentDriver.IComparisonTest;
import com.bigdata.test.ExperimentDriver.Result;
import com.bigdata.util.NV;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Suite of stress tests of the {@link IGlobalLRU} implementations. These tests
 * are focused on throughput comparisons for the {@link IGlobalLRU}
 * implementation strategies. Since a random access pattern is used, the tests
 * can be used to characterize the implementations in terms of efficiency and
 * concurrency (CAS and lock contention), but not in terms of the utility of the
 * access policy provided by the cache for the application.  
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StressTestGlobalLRU extends TestCase implements IComparisonTest {

    public static final Logger log = Logger.getLogger(StressTestGlobalLRU.class);

    /**
     * 
     */
    public StressTestGlobalLRU() {
        super();
    }

    public StressTestGlobalLRU(String name) {
        super(name);
    }
    
    /**
     * Test driver. 
     */
    public Result doComparisonTest(final Properties properties)
            throws Exception {

        // 
        final long timeout = Long.parseLong(properties.getProperty(
                TestOptions.TIMEOUT, TestOptions.DEFAULT_TIMEOUT));

        // #of threads (default is one more than the #of cores).
        final int nthreads = Integer.parseInt(properties.getProperty(
                TestOptions.NTHREADS, Integer.toString(Runtime.getRuntime()
                        .availableProcessors() + 1)));

        // #of operations to execute.
        final long nops = Long.parseLong(properties
                .getProperty(TestOptions.NOPS));

        // max distinct records per store.
        final int nrecords = Integer.parseInt(properties.getProperty(
                TestOptions.NRECORDS, TestOptions.DEFAULT_NRECORDS));

        // #of stores.
        final int nstores = Integer.parseInt(properties.getProperty(
                TestOptions.NSTORES, TestOptions.DEFAULT_NSTORES));

        final CacheSettings cacheSettings = new CacheSettings(properties);

        final IGlobalLRU<Long, Object> lru = cacheSettings.newInstance();

        assertNotNull("Could not create cache", lru);

        /*
         * get, put, remove, clearCache, deleteCache, discardAllCaches.
         * 
         * @todo config the operator generation.
         * 
         * @todo any clearing of caches is might be too much based on the
         * XorShift pseudo-random generator. Certainly, I am seeing too much
         * when those parameters are non-zero.
         * 
         * Note: deleteCache and discardAllCaches DO NOT guarantee consistency
         * if there are concurrent operations against the cache.
         */
        final Op gen = new Op(.8f, .2f, .005f, .0001f, .00005f, .00001f);
////        final Op gen = new Op(.8f, .2f, .005f, 0f, 0f, 0f);
        
        final RunCounters runCounters = doStressTest(timeout, nthreads, nops,
                nrecords, nstores, gen, lru);

        System.err.println(lru.getCounterSet().toString());

        // The #of operations across all threads.
        long opCount = 0L;
        for (int i = 0; i < nthreads; i++) {

            System.err.println("Thread[" + i + "]=" + runCounters.opCounters[i]);
            
            opCount += runCounters.opCounters[i].n;
            
        }

        // The #of operations per millisecond (throughput).
        final long opsPerMs = (opCount / TimeUnit.NANOSECONDS
                .toMillis(runCounters.elapsed));
        
        final Result result = new Result();

        result.put("ops/ms", "" + opsPerMs);
        result.put("opCount", "" + opCount);
        result.put("elapsed", "" + runCounters.elapsed);
        result.put("class", lru.getClass().getName());
        result.put("concurrencyLevel", "" + cacheSettings.concurrencyLevel);
        result.put("accessPolicy", "" + cacheSettings.accessPolicy);
        result.put("threadLocalBufferCapacity", ""
                + cacheSettings.threadLocalBufferCapacity);

        System.err.println(result.toString(true/* newline */));

        return result;
        
    }

    /**
     * Options for {@link TestLockManager#doComparisonTest(Properties)}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface TestOptions extends LRUNexus.Options {

        /**
         * Maximum amount of time that the test will run (seconds).
         */
        String TIMEOUT = "testTimeout";
        
        /**
         * The default is no timeout for the test.
         */
        String DEFAULT_TIMEOUT = "" + Long.MAX_VALUE;

        /**
         * The #of concurrent threads (default is <code>ncores+1</code>).
         */
        String NTHREADS = "nthreads";

        /**
         * Total #of tasks to execute.
         */
        String NOPS = "nops";

        /** The maximum #of distinct records per store. */
        String NRECORDS = "nrecords";

        String DEFAULT_NRECORDS = "10000";
        
        /**
         * The #of stores (the {@link IGlobalLRU} has one cache per
         * {@link IRawStore}).
         */
        String NSTORES = "nstores";
        
        String DEFAULT_NSTORES = "5";
        
    }

    /** NOP */
    public void setUpComparisonTest(Properties properties) throws Exception {

    }

    /** NOP */
    public void tearDownComparisonTest() throws Exception {
        
    }

    /**
     * Run an experiment using the <code>StressTestGlobalLRU.xml</code> file in
     * the local directory.
     * 
     * @throws Exception 
     */
    public static void main(final String[] args) throws Exception {

        ExperimentDriver.main(new String[] { "bigdata/src/test/com/bigdata/cache/StressTestGlobalLRU.xml" });

    }
    
    /**
     * Wrap a byte[] as a mock data record.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class MockDataRecord implements IDataRecordAccess {

        private final byte[] data;
        
        public MockDataRecord(final byte[] data) {
            
            assert data != null;
            
            this.data = data;
            
        }
        
        public IFixedDataRecord data() {

            return FixedByteArrayBuffer.wrap(data);
            
        }
        
    }

    /**
     * Report both the total #of operations and the #of each type of operation.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    static class OpCounters {
        /** Total #of operations. */
        long n;
        long get;
        long put;
        long remove;
        long clearCache;
        long deleteCache;
        long discardAllCaches;
        boolean timeout = false;

        public String toString() {
            return "{timeout=" + timeout + ",n=" + n + ",get=" + get + ",put="
                    + put + ",remove=" + remove + ",clearCache=" + clearCache
                    + ",deleteCache=" + deleteCache + ",discardAllCaches="
                    + discardAllCaches + "}";
        }
    }

    /**
     * Counters for a stress test run, including the per-thread counters.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class RunCounters {

        /**
         * The total elapsed run time.
         */
        public final long elapsed;

        /**
         * The per-thread operation counters.
         */
        public final OpCounters[] opCounters;

        public RunCounters(final long elapsed, final OpCounters[] opCounters) {
        
            this.elapsed = elapsed;
            
            this.opCounters = opCounters;
            
        }
        
    }

    /**
     * Stress test for cache with concurrent operations. This hammers on the
     * cache and reports data on throughput and may be used to analyze the
     * {@link IGlobalLRU} implementation for concurrency issues but it does not
     * provide correctness testing. Because this test uses a random access
     * pattern, it can only be used to measure throughput (or alternatively,
     * contention) of the implementation but not the utility of the cache for
     * the application.
     * 
     * @param timeout
     *            The test timeout (seconds).
     * @param nthreads
     *            The #of threads to use.
     * @param nops
     *            The #of operations to execute.
     * @param nrecords
     *            The maximum #of records in a mock "store".
     * @param nstores
     *            The #of mock stores.
     * @param gen
     *            The operation generator.
     * @param lru
     *            The cache implementation under test.
     *            
     * @return Performance results.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    static protected RunCounters doStressTest(final long timeout,
            final int nthreads, final long nops, final int nrecords,
            final int nstores, final Op gen, final IGlobalLRU<Long, Object> lru)
            throws InterruptedException, ExecutionException {

        // separate array to avoid any synchronization on access to the UUIDs.
        final UUID[] storeUUIDs = new UUID[nstores];

        final IAddressManager am = new WormAddressManager(
                WormAddressManager.MIN_OFFSET_BITS);
        
        for (int i = 0; i < nstores; i++) {
        
            storeUUIDs[i] = UUID.randomUUID();
            
        }
        
        final LinkedList<Callable<OpCounters>> tasks = new LinkedList<Callable<OpCounters>>();
        
        final OpCounters[] perThreadCounters = new OpCounters[nthreads];

        for (int i = 0; i < nthreads; i++) {

            final OpCounters c = perThreadCounters[i] = new OpCounters();
            
            final Callable<OpCounters> task = new Callable<OpCounters>() {

                public OpCounters call() throws Exception {

                    // thread-local randomness
                    final XorShift rnd = new XorShift();
                    while (c.n < nops) {
                        c.n++;
                        switch (gen.nextOp(rnd)) {
                        case Op.GET: {
                            c.get++;
                            final long k = Math.abs(rnd.next() % nrecords);
                            final int s = Math.abs(rnd.next() % nstores);
//                            System.err.println("k="+k+", s="+s);
                            lru.getCache(storeUUIDs[s], am).get(k);
                            break;
                        }
                        case Op.PUT: {
                            c.put++;
                            final long k = Math.abs(rnd.next() % nrecords);
                            final int s = Math.abs(rnd.next() % nstores);
                            lru.getCache(storeUUIDs[s], am).putIfAbsent(
                                    k,
                                    new MockDataRecord(new byte[Math.abs(rnd
                                            .next() % 10)]));
                            break;
                        }
                        case Op.REMOVE: {
                            c.remove++;
                            final long k = Math.abs(rnd.next() % nrecords);
                            final int s = Math.abs(rnd.next() % nstores);
                            lru.getCache(storeUUIDs[s], am).remove(k);
                            break;
                        }
                        case Op.CLEAR_CACHE: {
                            c.clearCache++;
                            final int s = Math.abs(rnd.next() % nstores);
//                            System.err.println("Clearing cache: "+s);
                            lru.getCache(storeUUIDs[s], am).clear();
                            break;
                        }
                        case Op.DELETE_CACHE: {
                            c.deleteCache++;
                            final int s = Math.abs(rnd.next() % nstores);
//                            System.err.println("Deleting cache: "+s);
                            lru.deleteCache(storeUUIDs[s]);
                            break;
                        }
                        case Op.DISCARD_ALL_CACHES: {
                            /*
                             * Note: The API DOES NOT guarantee coherence if
                             * this method is invoked with concurrent operations
                             * against the cache. Therefore it is not a good
                             * idea to have a non-zero probability for this
                             * operation. Some implementations provide enough
                             * locking to make the operation coherent, but many
                             * do not.
                             */
                            c.discardAllCaches++;
//                            System.err.println("Discarding all caches");
                            lru.discardAllCaches();
                            break;
                        }
                        default:
                            throw new AssertionError();
                        }

                        // Note: enable iff debugging.
                        if (false && c.n % 1000000 == 0) {
                            System.err
                                    .println(Thread.currentThread() + ":" + c);
                        }

                    } // while(...)

                    System.err.println(Thread.currentThread() + ":done:" + c);
                    
                    // done.
                    return c;

                } // call()

            }; // cls

            tasks.add(task);

        }
        
        final ExecutorService service = Executors
                .newCachedThreadPool(DaemonThreadFactory.defaultThreadFactory());

        final List<Future<OpCounters>> futures;
        
        final long begin = System.nanoTime();
        
        try {

            futures = service.invokeAll(tasks, timeout/* timeout */,
                    TimeUnit.SECONDS/* unit */);

        } finally {

            service.shutdownNow();

        }

        for (int i = 0; i < nthreads; i++) {

            final Future<OpCounters> f = futures.get(i);

            // look for errors.
            try {

                f.get();

            } catch (CancellationException ex) {

                perThreadCounters[i].timeout = true;

            }

        }

        final long elapsed = System.nanoTime() - begin;

        return new RunCounters(elapsed, perThreadCounters);
        
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
     * Helper class generates a random sequence of operation codes obeying the
     * probability distribution described in the constructor call.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class Op {
        
        static public final int GET = 0;
        static public final int PUT = 1;
        static public final int REMOVE = 2;
        static public final int CLEAR_CACHE = 3;
        static public final int DELETE_CACHE = 4;
        static public final int DISCARD_ALL_CACHES = 5;
        
        /**
         * The last defined operator.
         */
        static final int lastOp = DISCARD_ALL_CACHES;
        
        final private float[] _dist;

        /*
         * get, put, remove, clearCache, deleteCache, discardAllCaches
         */
        public Op(float getRate, float putRate, float removeRate,
                float clearCacheRate, float deleteCacheRate,
                float discardAllCachesRate) {
            
            if (getRate < 0 || putRate < 0 || removeRate < 0
                    || clearCacheRate < 0 || deleteCacheRate < 0
                    || discardAllCachesRate < 0) {
                throw new IllegalArgumentException("negative rate");
            }
            float total = getRate + putRate + removeRate + clearCacheRate
                    + deleteCacheRate + discardAllCachesRate;
            if( total == 0.0 ) {
                throw new IllegalArgumentException("all rates are zero.");
            }
            /*
             * Convert to normalized distribution in [0:1].
             */
            getRate/= total;
            putRate /= total;
            removeRate /= total;
            clearCacheRate /= total;
            deleteCacheRate /= total;
            discardAllCachesRate /= total;
            /*
             * Save distribution.
             */
            int i = 0;
            _dist = new float[lastOp+1];
            _dist[ i++ ] = getRate;
            _dist[ i++ ] = putRate;
            _dist[ i++ ] = removeRate;
            _dist[ i++ ] = clearCacheRate;
            _dist[ i++ ] = deleteCacheRate;
            _dist[ i++ ] = discardAllCachesRate;

            /*
             * Checksum.
             */
            float sum = 0f;
            for( i = 0; i<_dist.length; i++ ) {
                sum += _dist[ i ];
            }
            if( Math.abs( sum - 1f) > 0.01 ) {
                throw new AssertionError("sum of distribution is: "+sum+", but expecting 1.0");
            }
            
        }
        
        /**
         * Return the name of the operator.
         * 
         * @param op
         * @return
         */
        public String getName( final int op ) {
            if( op < 0 || op > lastOp ) {
                throw new IllegalArgumentException();
            }
            /*
             * isNull, length, get, copy, search, iterator, recode.
             */
            switch (op) {
            case GET:
                return "get        ";
            case PUT:
                return "put        ";
            case REMOVE:
                return "remove     ";
            case CLEAR_CACHE:
                return "clearCache ";
            case DELETE_CACHE:
                return "deleteCache";
            case DISCARD_ALL_CACHES:
                return "discardAll ";
            default:
                throw new AssertionError();
            }
        }
        
        /**
         * An array of normalized probabilities assigned to each operator. The
         * array may be indexed by the operator, e.g., dist[{@link #fetch}]
         * would be the probability of a fetch operation.
         * 
         * @return The probability distribution over the defined operators.
         */
        public float[] getDistribution() {
            return _dist;
        }

        /**
         * Generate a random operator according to the distribution described to
         * to the constructor.
         * 
         * @return A declared operator selected according to a probability
         *         distribution.
         */
//        public int nextOp(final Random r) {
        public int nextOp(final XorShift rnd) {
            int randInt;
            while ((randInt = rnd.next()) <= 0);
            final float rand = (float) ((double) randInt / (double) Integer.MAX_VALUE);
//            final float rand = r.nextFloat(); // [0:1)
            float cumprob = 0f;
            for (int i = 0; i < _dist.length; i++) {
                cumprob += _dist[i];
                if (rand <= cumprob) {
                    return i;
                }
            }
            throw new AssertionError("rand="+rand+", cumprob="+cumprob);
        }
        
    }

    /**
     * Tests of the {@link Op} test helper class.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class TestOp extends TestCase {

//        private final Random r = new Random();
        
        public void test_Op() {
            /*
             * get, put, remove, clearCache, deleteCache, discardAllCaches.
             */
            Op gen = new Op(.8f, .1f, .01f, 05f, .001f, .001f);
            doOpTest(gen);
        }

        public void test_Op2() {
            /*
             * get, put, remove, clearCache, deleteCache, discardAllCaches.
             */
            Op gen = new Op(0f,0f,0f,1f,0f,0f);
            doOpTest(gen);
        }

        /**
         * Correct rejection test when all rates are zero.
         */
        public void test_correctRejectionAllZero() {
            /*
             * get, put, remove, clearCache, deleteCache, discardAllCaches.
             */
            try {
                new Op(0f,0f,0f,0f,0f,0f);
                fail("Expecting: "+IllegalArgumentException.class);
            }
            catch(IllegalArgumentException ex) {
                log.info("Ignoring expected exception: "+ex);
            }
        }

        /**
         * Correct rejection test when one or more rates are negative.
         */
        public void test_correctRejectionNegativeRate() {
            /*
             * get, put, remove, clearCache, deleteCache, discardAllCaches.
             */
            try {
                new Op(0f,0f,0f,-1f,0f,1f);
                fail("Expecting: "+IllegalArgumentException.class);
            }
            catch(IllegalArgumentException ex) {
                log.info("Ignoring expected exception: "+ex);
            }
        }

        /**
         * Verifies the {@link Op} class given an instance with some probability
         * distribution.
         */
        void doOpTest(final Op gen) {
            final int limit = 10000;
            int[] ops = new int[limit];
            int[] sums = new int[Op.lastOp + 1];
            final XorShift rnd = new XorShift();
            for (int i = 0; i < limit; i++) {
                int op = gen.nextOp(rnd);
                assertTrue(op >= 0);
                assertTrue(op <= Op.lastOp);
                ops[i] = op;
                sums[op]++;
            }
            float[] expectedProbDistribution = gen.getDistribution();
            float[] actualProbDistribution = new float[Op.lastOp + 1];
            float sum = 0f;
            for (int i = 0; i <= Op.lastOp; i++) {
                sum += expectedProbDistribution[i];
                actualProbDistribution[i] = (float) ((double) sums[i] / (double) limit);
                float diff = Math.abs(actualProbDistribution[i]
                        - expectedProbDistribution[i]);
                System.err.println("expected[i=" + i + "]="
                        + expectedProbDistribution[i] + ", actual[i=" + i
                        + "]=" + actualProbDistribution[i] + ", diff="
                        + ((int) (diff * 1000)) / 10f + "%");
                assertTrue(diff < 0.02); // difference is less than 2% percent.
            }
            assertTrue(Math.abs(sum - 1f) < 0.01); // essential 1.0
        }

    }

    /**
     * Generates an XML file that can be used to (re-)run the stress tests. The
     * outputs are appended to a file so you can see how performance and
     * collected counters change from run to run.
     * 
     * @see ExperimentDriver
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    static public class Generate extends ExperimentDriver {
        
        /**
         * Generates an XML file that can be run by {@link ExperimentDriver}.
         * 
         * @param args
         */
        public static void main(final String[] args) throws Exception {
            
            // this is the test to be run.
            final String className = StressTestGlobalLRU.class.getName();
            
            final Map<String, String> defaultProperties = new HashMap<String, String>();

            /*
             * Set defaults. These will be the base case from which each
             * condition is derived.
             */
            
            // #of seconds to run the test.
            defaultProperties.put(TestOptions.TIMEOUT,"20"); // standard run.
//            defaultProperties.put(TestOptions.TIMEOUT,""+Long.MAX_VALUE); // when profiling.
            
            // #of operations to execute
            defaultProperties.put(TestOptions.NOPS,"10000000000"); // lots of stress (will run until timeout). 
//            defaultProperties.put(TestOptions.NOPS,"100000000"); // should finish w/in 20s.

            // a small heap. the records are mock objects with small sizes.
            defaultProperties.put(TestOptions.MAX_HEAP, ""+ (10 * Bytes.kilobyte));

            // clear at least 25% of the memory.
            defaultProperties.put(TestOptions.PERCENT_CLEARED, ".25");

            defaultProperties.put(TestOptions.MIN_CACHE_SET_SIZE, "0");

            defaultProperties.put(TestOptions.INITIAL_CAPACITY, "16");

            defaultProperties.put(TestOptions.LOAD_FACTOR, ".75");

            // degree of lock striping.
            defaultProperties.put(TestOptions.CONCURRENCY_LEVEL, "16");

            // #of test driver threads to use.
            defaultProperties.put(TestOptions.NTHREADS, "8");

            final List<Condition>conditions = new ArrayList<Condition>();

            /*
             * Configuration options for the different implementation classes.
             */

            //
            // The bigdata BCHM cache impl.
            //

            conditions.add(getCondition(defaultProperties, new NV[] {//
                    new NV(TestOptions.CLASS,BCHMGlobalLRU2.class.getName()),//
                    new NV(TestOptions.THREAD_LOCAL_BUFFERS,"false"),//
                    new NV(TestOptions.THREAD_LOCAL_BUFFER_CAPACITY,"128"),//
                    new NV(TestOptions.ACCESS_POLICY,AccessPolicyEnum.LRU.toString()),//
                }));
           
            // Note: concurrencyLevel := 0 means true thread-local buffers.
            conditions.add(getCondition(defaultProperties, new NV[] {//
                new NV(TestOptions.CLASS,BCHMGlobalLRU2.class.getName()),//
                new NV(TestOptions.THREAD_LOCAL_BUFFERS,"true"),//
                new NV(TestOptions.THREAD_LOCAL_BUFFER_CAPACITY,"128"),//
                new NV(TestOptions.ACCESS_POLICY,AccessPolicyEnum.LRU.toString()),//
            }));
            
            //
            // Based on the infinispan BCHM.
            //
            
            conditions.add(getCondition(defaultProperties, new NV[] {//
                    new NV(TestOptions.CLASS,BCHMGlobalLRU.class.getName()),//
                    new NV(TestOptions.ACCESS_POLICY,AccessPolicyEnum.LRU.toString()),//
                }));

            conditions.add(getCondition(defaultProperties, new NV[] {//
                    new NV(TestOptions.CLASS,BCHMGlobalLRU.class.getName()),//
                    new NV(TestOptions.ACCESS_POLICY,AccessPolicyEnum.LIRS.toString()),//
                }));

            final Experiment exp = new Experiment(className, defaultProperties,
                    conditions);

            // copy the output into a file and then you can run it later.
            System.err.println(exp.toXML());

        }

    }
    
}
