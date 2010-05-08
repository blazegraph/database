/*

Copyright (C) SYSTAP, LLC 2006-2009.  All rights reserved.

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
 * Created on Oct 16, 2009
 */

package com.bigdata.cache;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
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
import junit.framework.TestCase2;

import com.bigdata.cache.IGlobalLRU.ILRUCache;
import com.bigdata.io.FixedByteArrayBuffer;
import com.bigdata.io.IDataRecordAccess;
import com.bigdata.io.IFixedDataRecord;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.rawstore.WormAddressManager;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractHardReferenceGlobalLRUTest extends TestCase2 {

    public AbstractHardReferenceGlobalLRUTest() {
        
    }
    
    public AbstractHardReferenceGlobalLRUTest(String name) {
        
        super(name);
        
    }
    
    protected IHardReferenceGlobalLRU<Long, Object> lru;

    protected IRawStore store1 = new SimpleMemoryRawStore();

    // Note: not used for this test suite, not defined for memStore.
    protected IAddressManager am1 = null;
    
    protected IRawStore store2 = new SimpleMemoryRawStore();

    // Note: not used for this test suite, not defined for memStore.
    protected IAddressManager am2 = null;
    
    protected void tearDown() throws Exception {
        
        store1 = store2 = null;
        
        lru = null;
        
    }
    
    /**
     * Unit test explores ability to recover the {@link ILRUCache}
     * implementation for a given {@link IRawStore}.
     */
    public void test_cacheSet() {

        assertFalse(store1.getUUID().equals(store2.getUUID()));
        
        assertEquals(0, lru.getCacheSetSize());
        
        final ILRUCache<Long, Object> cache1 = lru.getCache(store1.getUUID(), am1);

        assertTrue(cache1 == lru.getCache(store1.getUUID(), am1));

        assertEquals(1, lru.getCacheSetSize());

        final ILRUCache<Long, Object> cache2 = lru.getCache(store2.getUUID(), am2);

        assertTrue(cache1 == lru.getCache(store1.getUUID(), am1));

        assertTrue(cache2 == lru.getCache(store2.getUUID(), am2));

        assertTrue(cache1 != cache2);
        
        assertEquals(2, lru.getCacheSetSize());

        lru.deleteCache(store1.getUUID());

        assertEquals(1, lru.getCacheSetSize());

        assertTrue(cache1 != lru.getCache(store1.getUUID(), am1));

        assertTrue(cache2 == lru.getCache(store2.getUUID(), am2));

        assertEquals(2, lru.getCacheSetSize());

    }

    /**
     * Unit test explores tracking of the {@link ILRUCache#size()} and the
     * bytesInMemory for the {@link IGlobalLRU}.
     */
    public void test_counters() {
        
        if(lru instanceof BCHMGlobalLRU2||
                lru instanceof BCHMGlobalLRU) {

            /*
             * Note: This test will <em>fail</em> if the {@link IGlobalLRU}
             * buffers references since the counters will not be updated until
             * the reference is batched through to the backing access policy
             * even though the cache map has been updated. {@link BCHMGlobalLRU}
             * and {@link BCHMGlobalLRU2} CAN NOT pass this test, but this does
             * not mean that these cache implementations are broken.
             */

            if (log.isInfoEnabled())
                log.info("Skipping unit test: " + lru.getClass());
            
            return;
            
        }

        // initial state.
        assertEquals(0, lru.getRecordCount());
        assertEquals(0, lru.getBytesInMemory());
        assertEquals(0, lru.getEvictionCount());

        // create one cache.
        final ILRUCache<Long, Object> cache1 = lru.getCache(store1.getUUID(), am1);
        assertEquals(0, lru.getRecordCount());
        assertEquals(0, lru.getBytesInMemory());
        assertEquals(0, lru.getEvictionCount());

        // add a first record.
        final IDataRecordAccess e0;
        assertNull(cache1.putIfAbsent(1L, e0 = new MockDataRecord(new byte[1])));
        assertEquals(1, lru.getRecordCount());
        assertEquals(1, lru.getBytesInMemory());
        assertEquals(0, lru.getEvictionCount());

        // add a 2nd record.
        assertNull(cache1.putIfAbsent(2L, new MockDataRecord(new byte[2])));
        assertEquals(2, lru.getRecordCount());
        assertEquals(3, lru.getBytesInMemory());
        assertEquals(0, lru.getEvictionCount());

        // verify putIfAbsent returned the other entry and no change in the counters.
        assertTrue(e0 == cache1
                .putIfAbsent(1L, new MockDataRecord(new byte[3])));
        assertEquals(2, lru.getRecordCount());
        assertEquals(3, lru.getBytesInMemory());
        assertEquals(0, lru.getEvictionCount());

        // remove one record.
        assertTrue(e0 == cache1.remove(1L));
        assertEquals(1, lru.getRecordCount());
        assertEquals(2, lru.getBytesInMemory());
        assertEquals(0, lru.getEvictionCount());

        // replace the cache entry with a different record.
        assertTrue(null == cache1.putIfAbsent(1L, new MockDataRecord(
                new byte[3])));
        assertEquals(2, lru.getRecordCount());
        assertEquals(5, lru.getBytesInMemory());
        assertEquals(0, lru.getEvictionCount());

        // now clear the cache and verify the counters all go to zero and
        // that the entries in the cache were removed.
        cache1.clear();
        assertEquals("recordCount", 0, lru.getRecordCount());
        assertEquals("bytesInMemory", 0, lru.getBytesInMemory());
        assertEquals("evictionCount", 0, lru.getEvictionCount());
        assertNull(cache1.get(1L));
        assertNull(cache1.get(2L));
        
    }

    /**
     * Stress test for clearing a cache.
     */
    public void test_clearCache() {

        final Random r = new Random();

        final ILRUCache<Long, Object> cache1 = lru.getCache(store1.getUUID(), am1);

        // how many inserts to perform.
        final int limit = 1000000;
        
        // the range from which the record identifiers are drawn.
        final int range = limit / 2;

        // add a bunch of records.
        for (int i = 0; i < limit; i++) {
         
            cache1.putIfAbsent((long) r.nextInt(range), new byte[3]);

        }
        // The #of records in the cache after all have been inserted (some may have been evicted).
        final int recordCount0 = lru.getRecordCount();

        // remove ~ 1/2 of the records.
        int nremoved = 0;
        for (int i = 0; i < limit / 2; i++) {

            if (cache1.remove((long) r.nextInt(range)) != null) {
                nremoved++;
            }
        }
        final int remaining = recordCount0 - nremoved;
        assertEquals("remaining records(nremoved=" + nremoved + ")", remaining,
                lru.getRecordCount());

//        // before clear
//        System.out.println(lru.toString());

        // clear the cache and verify all counters were cleared.
        cache1.clear();
        assertEquals("recordCount", 0, lru.getRecordCount());
        assertEquals("bytesInMemory", 0, lru.getBytesInMemory());
        assertEquals("evictionCount", 0, lru.getEvictionCount());

//        // after clear.
//        System.out.println(lru.toString());

        /*
         * Obtain a cache for a different store and insert some records and
         * verify the counters are still correct.
         */
        final ILRUCache<Long, Object> cache2 = lru.getCache(store2.getUUID(), am2);
        
        // add a first record.
        assertNull(cache2.putIfAbsent(1L, new MockDataRecord(new byte[1])));
        assertEquals("recordCount", 1, lru.getRecordCount());
        assertEquals("bytesInMemory", 1, lru.getBytesInMemory());
        assertEquals("evictionCount", 0, lru.getEvictionCount());

        // add a 2nd record.
        assertNull(cache2.putIfAbsent(2L, new MockDataRecord(new byte[2])));
        assertEquals("recordCount", 2, lru.getRecordCount());
        assertEquals("bytesInMemory", 3, lru.getBytesInMemory());
        assertEquals("evictionCount", 0, lru.getEvictionCount());

//        // before clear
//        System.out.println(lru.toString());

        // clear the cache and verify all counters were cleared.
        cache2.clear();
        assertEquals("recordCount", 0, lru.getRecordCount());
        assertEquals("bytesInMemory", 0, lru.getBytesInMemory());
        assertEquals("evictionCount", 0, lru.getEvictionCount());

//        // after clear.
//        System.out.println(lru.toString());

    }

//    protected static class MockDataRecord implements IDataRecordAccess {
//
//        private final byte[] data;
//        
//        public MockDataRecord(final byte[] data) {
//            assert data != null;
//            this.data = data;
//        }
//        
//        public IFixedDataRecord data() {
//
//            return FixedByteArrayBuffer.wrap(data);
//            
//        }
//        
//    }

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
     * Stress test for cache consistency with concurrent operations.
     * 
     * @throws InterruptedException 
     * @throws ExecutionException 
     * 
     * @todo should hammer at the implementation and report on throughput. might
     *       not be able to measure correctness.
     * 
     * @todo verify final iterate over all caches and clear of each leaves
     *       bytesInMemory, bytesOnDisk, and recordCount at zero.
     */
    public void test_concurrentOperations() throws InterruptedException, ExecutionException {

        /*
         * get, put, remove, clearCache, deleteCache, discardAllCaches.
         * 
         * @todo any clearing of caches is might be too much based on the
         * XorShift pseudo-random generator. Certainly, I am seeing too much
         * when those parameters are non-zero.
         */
//        final Op gen = new Op(.8f, .2f, .005f, .0001f, .00005f, .00001f);
        final Op gen = new Op(.8f, .2f, .005f, 0f, 0f, 0f);
        
        // max distinct records per store.
        final int nrecords = 10000;
        
        // #of stores.
        final int nstores = 5;
        
        // #of concurrent threads
        final int nthreads = Runtime.getRuntime().availableProcessors() + 1;

        // #of operations to execute
        final long nops = 10000000000L;// lots of stress (will timeout).
//        final long nops = 100000000;// standard stress.

        // test run time.
//        final long timeout = Long.MAX_VALUE;// when profiling
        final long timeout = 20L;// standard stress
        
//        // #of operations which have been executed so far.
//        final AtomicLong opCount = new AtomicLong();
        
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

        boolean isTimeout = false;
        for (int i = 0; i < nthreads; i++) {

            final Future<OpCounters> f = futures.get(i);

            // look for errors.
            try {

                f.get();

            } catch (CancellationException ex) {

                perThreadCounters[i].timeout = isTimeout = true;

            }

        }

        final long elapsed = System.nanoTime() - begin;
        
        System.err.println(lru.getCounterSet().toString());

        long opCount = 0L;
        for (int i = 0; i < nthreads; i++) {

            System.err.println("Thread[" + i + "]=" + perThreadCounters[i]);
            
            opCount += perThreadCounters[i].n;
            
        }

        System.err
                .println("elapsed="
                        + TimeUnit.NANOSECONDS.toMillis(elapsed)
                        + "ms, #ops="
                        + opCount
                        + ", ops/ms="
                        + (opCount / TimeUnit.NANOSECONDS.toMillis(elapsed))
                        + ", lruClass="
                        + lru.getClass().getName()
                        + (lru instanceof BCHMGlobalLRU2 ? ("concurrencyLevel=" + ((BCHMGlobalLRU2) lru)
                                .getConcurrencyLevel())
                                : ""));

        if (isTimeout && log.isInfoEnabled())
            log.info("Test timed out");
        
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
    
}
