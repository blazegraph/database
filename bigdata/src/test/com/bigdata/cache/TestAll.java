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
 * Created on Apr 21, 2006
 */
package com.bigdata.cache;

import java.io.File;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.bigdata.test.ExperimentDriver;

/**
 * Aggregates unit tests into dependency order.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestAll extends TestCase {

    /**
     * 
     */
    public TestAll() {
        super();
    }

    /**
     * @param arg0
     */
    public TestAll(String arg0) {
        super(arg0);
    }

    public static junit.framework.Test suite() {
        
        final TestSuite suite = new TestSuite("cache");
        
        suite.addTestSuite(TestRingBuffer.class);
        
        suite.addTestSuite(TestHardReferenceQueue.class);

        suite.addTestSuite(TestHardReferenceQueueWithBatchingUpdates.class);
        
//        // Test all ICacheEntry implementations.
//        retval.addTestSuite( TestCacheEntry.class );

        // Test LRU semantics.
        suite.addTestSuite(TestLRUCache.class);

        // Test cache semantics with weak/soft reference values.
        suite.addTestSuite(TestWeakValueCache.class);

        suite.addTestSuite(TestStoreAndAddressLRUCache.class);

        suite.addTestSuite(TestHardReferenceGlobalLRU.class);

        suite.addTestSuite(TestHardReferenceGlobalLRURecycler.class);

        suite.addTestSuite(TestHardReferenceGlobalLRURecyclerExplicitDeleteRequired.class);

        /*
         * A high concurrency cache based on the infinispan project w/o support
         * for memory cap. This implementation has the disadvantage that we can
         * not directly manage the amount of memory which will be used by the
         * cache.  It has pretty much been replaced by the BCHMGlobalLRU2, 
         * which gets tested below.
         */
        suite.addTestSuite(TestBCHMGlobalLRU.class);

        /*
         * These are test suites for the same high concurrency cache with
         * support for memory cap. The cache can be configured with thread-lock
         * buffers or striped locks, so we test it both ways.
         */
        suite.addTestSuite(TestBCHMGlobalLRU2WithThreadLocalBuffers.class);
        suite.addTestSuite(TestBCHMGlobalLRU2WithStripedLocks.class);

        /*
         * Run the stress tests.
         */
        suite.addTestSuite(StressTests.class);

        return suite;
    }

    /**
     * Glue class used to execute the stress tests.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class StressTests extends TestCase {

        public StressTests() {

        }

        public StressTests(String name) {
            super(name);
        }

        /**
         * FIXME Modify the stress test configuration file to run each condition
         * of interest. It is only setup for a few conditions right now.
         */
        public void test() throws Exception {
            ExperimentDriver
                    .doMain(
                            new File(
                                    "bigdata/src/test/com/bigdata/cache/StressTestGlobalLRU.xml"),
                            1/* nruns */, true/* randomize */);
        }
    }

}
