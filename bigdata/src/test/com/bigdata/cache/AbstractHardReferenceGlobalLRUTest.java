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

import java.util.Random;

import junit.framework.TestCase2;

import com.bigdata.cache.IGlobalLRU.ILRUCache;
import com.bigdata.io.FixedByteArrayBuffer;
import com.bigdata.io.IDataRecordAccess;
import com.bigdata.io.IFixedDataRecord;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

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
     * bytesInMemory for the {@link HardReferenceGlobalLRU}.
     */
    public void test_counters() {

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
        assertEquals(0, lru.getRecordCount());
        assertEquals(0, lru.getBytesInMemory());
        assertEquals(0, lru.getEvictionCount());
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
        assertEquals(recordCount0 - nremoved, lru.getRecordCount());

//        // before clear
//        System.out.println(lru.toString());

        // clear the cache and verify all counters were cleared.
        cache1.clear();
        assertEquals(0, lru.getRecordCount());
        assertEquals(0, lru.getBytesInMemory());
        assertEquals(0, lru.getEvictionCount());

//        // after clear.
//        System.out.println(lru.toString());

        /*
         * Obtain a cache for a different store and insert some records and
         * verify the counters are still correct.
         */
        final ILRUCache<Long, Object> cache2 = lru.getCache(store2.getUUID(), am2);
        
        // add a first record.
        assertNull(cache2.putIfAbsent(1L, new MockDataRecord(new byte[1])));
        assertEquals(1, lru.getRecordCount());
        assertEquals(1, lru.getBytesInMemory());
        assertEquals(0, lru.getEvictionCount());

        // add a 2nd record.
        assertNull(cache2.putIfAbsent(2L, new MockDataRecord(new byte[2])));
        assertEquals(2, lru.getRecordCount());
        assertEquals(3, lru.getBytesInMemory());
        assertEquals(0, lru.getEvictionCount());

//        // before clear
//        System.out.println(lru.toString());

        // clear the cache and verify all counters were cleared.
        cache2.clear();
        assertEquals(0, lru.getRecordCount());
        assertEquals(0, lru.getBytesInMemory());
        assertEquals(0, lru.getEvictionCount());

//        // after clear.
//        System.out.println(lru.toString());

    }

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

}
