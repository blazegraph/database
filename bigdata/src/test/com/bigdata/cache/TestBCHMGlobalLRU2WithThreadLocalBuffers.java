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

import com.bigdata.LRUNexus.AccessPolicyEnum;
import com.bigdata.rawstore.Bytes;

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
public class TestBCHMGlobalLRU2WithThreadLocalBuffers extends
        AbstractHardReferenceGlobalLRUTest {

    /**
     * 
     */
    public TestBCHMGlobalLRU2WithThreadLocalBuffers() {
    }

    /**
     * @param name
     */
    public TestBCHMGlobalLRU2WithThreadLocalBuffers(String name) {
        super(name);
    }

    private final AccessPolicyEnum accessPolicy = AccessPolicyEnum.LRU;

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

}
