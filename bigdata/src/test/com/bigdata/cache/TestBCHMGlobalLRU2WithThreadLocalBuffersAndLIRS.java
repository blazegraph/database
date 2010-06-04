/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Sep 16, 2009
 */

package com.bigdata.cache;

import java.util.UUID;

import com.bigdata.LRUNexus.AccessPolicyEnum;
import com.bigdata.cache.BCHMGlobalLRU2.DLNLirs;
import com.bigdata.cache.BCHMGlobalLRU2.LIRSAccessPolicy;
import com.bigdata.cache.BCHMGlobalLRU2.LRUCacheImpl;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IAddressManager;

/**
 * Some unit tests for the {@link BCHMGlobalLRU2} using true thread local
 * buffers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestBCHMGlobalLRU2WithThreadLocalBuffers.java 2792 2010-05-09
 *          20:54:39Z thompsonbry $
 * 
 * @see TestBCHMGlobalLRU2WithStripedLocks
 */
public class TestBCHMGlobalLRU2WithThreadLocalBuffersAndLIRS extends
        AbstractHardReferenceGlobalLRUTest {

    /**
     * 
     */
    public TestBCHMGlobalLRU2WithThreadLocalBuffersAndLIRS() {
    }

    /**
     * @param name
     */
    public TestBCHMGlobalLRU2WithThreadLocalBuffersAndLIRS(String name) {
        super(name);
    }

    private final AccessPolicyEnum accessPolicy = AccessPolicyEnum.LIRS;

    private final static long maximumBytesInMemory = 10 * Bytes.kilobyte;

    // clear at least 25% of the memory.
    private final static long minCleared = maximumBytesInMemory / 4;

    private final static int minimumCacheSetCapacity = 0;

    private final static int initialCacheCapacity = 16;

    private final static float loadFactor = .75f;

    private final static int concurrencyLevel = 16;

    private final static boolean threadLocalBuffers = true;

    private final static int threadLocalBufferCapacity = 128;
    
    protected void setUp() throws Exception {

        super.setUp();

        lru = new BCHMGlobalLRU2<Long, Object>(accessPolicy,
                maximumBytesInMemory, minCleared, minimumCacheSetCapacity,
                initialCacheCapacity, loadFactor, concurrencyLevel,
                threadLocalBuffers, threadLocalBufferCapacity);

    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: The {@link #threadLocalBufferCapacity} is overridden for this unit
     * test to ONE (1) so that the counter updates are synchronous.
     */
    public void test_counters() {

        lru = new BCHMGlobalLRU2<Long, Object>(accessPolicy,
                maximumBytesInMemory, minCleared, minimumCacheSetCapacity,
                initialCacheCapacity, loadFactor, concurrencyLevel,
                threadLocalBuffers, 1/* threadLocalBufferCapacity */);

        super.test_counters();
        
    }
    
    /**
     * {@inheritDoc}
     * <p>
     * Note: The {@link #threadLocalBufferCapacity} is overridden for this unit
     * test to ONE (1) so that the counter updates are synchronous.
     */
    public void test_clearCache() {

        lru = new BCHMGlobalLRU2<Long, Object>(accessPolicy,
                maximumBytesInMemory, minCleared, minimumCacheSetCapacity,
                initialCacheCapacity, loadFactor, concurrencyLevel,
                threadLocalBuffers, 1/* threadLocalBufferCapacity */);

        super.test_counters();

    }

    /**
     * A unit test based on the example in <a
     * href="http://portal.acm.org/citation.cfm?doid=511334.511340">LIRS: an
     * efficient low inter-reference recency set replacement policy to improve
     * buffer cache performance</a> and <a
     * href="http://www.ece.eng.wayne.edu/~sjiang/Projects/LIRS/sig02.ppt" >LIRS
     * : An Efficient Replacement Policy to Improve Buffer Cache
     * Performance.</a>
     * 
     * <h2>Example</h2>
     * 
     * The LIRS cache has been configured with sizeOf(lirs) := 2 and
     * sizeOf(hirs) := 1. This means that there will be two LIR blocks in memory
     * once the cache reaches steady state and only one resident HIR block.
     * There may in addition be non-resident HIR blocks on the LRU stack).
     * 
     * <pre>
     * BLOCK  | block touched at virtual time (t)     || recency | IRR
     * -------+---+---+---+---+---+---+---+---+---+---++---------+-----
     *   E    |   |   |   |   |   |   |   |   | x |   ||    0    | inf
     *   D    |   | x |   |   |   |   | x |   |   |   ||    2    |  3 
     *   C    |   |   |   | x |   |   |   |   |   |   ||    4    | inf  
     *   B    |   |   | x |   | x |   |   |   |   |   ||    3    |  1 
     *   A    | x |   |   |   |   | x |   | x |   |   ||    1    |  1  
     * -----------------------------------------------++---------+-----
     *        | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 |10 ||
     * </pre>
     * 
     * Each moment of discrete virtual time corresponds to a touch against a
     * single block. An "x" in the table body indicates that this block was
     * references at that time. The table is a snapshot <em>before</em> the
     * reference touch at virtual time <code>10</code>, which is why the "10"
     * column is empty.
     * <p>
     * <code>Recency</code> is defined as the number of distinct other blocks
     * touched since the last touch of a given block. Therefore the recency of
     * <code>C</code> in the table is FOUR (4) because the intervening touches
     * are <code>(B,A,D,A,E)</code> which is to say that FOUR (4)
     * <em>distinct</em> blocks have been touched since <code>C</code> was last
     * touched.
     * <p>
     * Likewise, <code>IRR</code> is defined as the number of (distinct) other
     * blocks touched since the between the last and the penultimate touches of
     * a block.
     * <p>
     * In practice, both recency and IRR are represented by the number of
     * intervening nodes in the LRU stack.
     * 
     * <h2>Example 1</h2>
     * <p>
     * In this future history, there is a reference to block D at time t := 10.
     * 
     * <h2>Example 2</h2>
     * <p>
     * In this future history, there is a reference to block C at time t := 10.
     * 
     */
    public void test_LIRS() {

        /*
         * This is set to 3 to place a constraint on the sizeOf(lirs+hirs)
         * resident blocks of three. Each block will have a byte[] with a single
         * byte.
         */
        final long maximumBytesInMemory = 3;
        
        /*
         * A minCleared of 1 ensures that we evict only a single block at a
         * time.
         */
        final long minCleared = 1;

        final Long addrA = Long.valueOf(0);
        final Long addrB = Long.valueOf(1);
        final Long addrC = Long.valueOf(2);
        final Long addrD = Long.valueOf(3);
        final Long addrE = Long.valueOf(4);
        
        final MockDataRecord A = new MockDataRecord(new byte[]{0});
        final MockDataRecord B = new MockDataRecord(new byte[]{1});
        final MockDataRecord C = new MockDataRecord(new byte[]{2});
        final MockDataRecord D = new MockDataRecord(new byte[]{3});
        final MockDataRecord E = new MockDataRecord(new byte[]{4});
        
        final UUID storeUUID = store1.getUUID();
        final IAddressManager am = am1;
        
        lru = new BCHMGlobalLRU2<Long, Object>(accessPolicy,
                maximumBytesInMemory, minCleared, minimumCacheSetCapacity,
                initialCacheCapacity, loadFactor, concurrencyLevel,
                threadLocalBuffers, 1/* threadLocalBufferCapacity */);

        final LRUCacheImpl<Long, Object> storeCache = (LRUCacheImpl<Long, Object>) lru
                .getCache(storeUUID, am);
        
        final LIRSAccessPolicy<Long, Object> accessPolicy = (LIRSAccessPolicy<Long, Object>) storeCache
                .getAccessPolicy();
        
        /*
         * Insert sequence: A, D, B, C, B, A, D, A, E
         */
        assertEquals(0,lru.getRecordCount());
        assertEquals(0, accessPolicy.getLRUSize());
        assertEquals(0, accessPolicy.getHIRSize()); // @todo carry this forward.

        storeCache.putIfAbsent(addrA, A); // t := 1
        assertEquals(1,lru.getRecordCount());
        assertEquals(1, accessPolicy.getLRUSize());
        
        storeCache.putIfAbsent(addrD, D); // t := 2
        assertEquals(2,lru.getRecordCount());
        assertEquals(2, accessPolicy.getLRUSize());

        storeCache.putIfAbsent(addrB, B); // t := 3
        assertEquals(3,lru.getRecordCount());
        assertEquals(3, accessPolicy.getLRUSize());

        storeCache.putIfAbsent(addrC, C); // t := 4
        assertEquals(3,lru.getRecordCount());
        assertEquals(3, accessPolicy.getLRUSize());

        storeCache.putIfAbsent(addrB, B); // t := 5
        assertEquals(3,lru.getRecordCount());
        assertEquals(3, accessPolicy.getLRUSize());
        
        storeCache.putIfAbsent(addrA, A); // t := 6
        assertEquals(3,lru.getRecordCount());
        assertEquals(3, accessPolicy.getLRUSize());
        
        storeCache.putIfAbsent(addrD, D); // t := 7
        assertEquals(3,lru.getRecordCount());
        assertEquals(3, accessPolicy.getLRUSize());
        
        storeCache.putIfAbsent(addrA, A); // t := 8
        assertEquals(3,lru.getRecordCount());
        assertEquals(3, accessPolicy.getLRUSize());
        
        storeCache.putIfAbsent(addrE, E); // t := 9
        assertEquals(3,lru.getRecordCount());
        assertEquals(3, accessPolicy.getLRUSize());
        
        DLNLirs<Long, Object> a = (DLNLirs<Long, Object>) storeCache
                .inspect(addrA);

    }

}
