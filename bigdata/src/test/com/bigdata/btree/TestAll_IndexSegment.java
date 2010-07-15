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
 * Created on Jan 31, 2009
 */

package com.bigdata.btree;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates the unit tests for the {@link IndexSegment} and its related
 * classes, all of which are in the same package as the {@link BTree}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAll_IndexSegment extends TestCase {

    public TestAll_IndexSegment() {
    }

    public TestAll_IndexSegment(String arg0) {
        super(arg0);
    }

    /**
     * Returns a test that will run each of the implementation specific test
     * suites in turn.
     */
    public static Test suite()
    {
        
        final TestSuite suite = new TestSuite("IndexSegment");

        /*
         * test index segment builds.
         * 
         * Note: the fast forward and fast reverse leaf scans are testing at the
         * same time that we test the index segment builds.
         * 
         * See DumpIndexSegment.
         */
        
        // test static methods for the index builder.
        suite.addTestSuite(TestIndexSegmentPlan.class);
        // test encoding and decoding of child node/leaf addresses.
        suite.addTestSuite(TestIndexSegmentAddressManager.class);
        // test write and read back of the index segment metadata record.
        suite.addTestSuite(TestIndexSegmentCheckpoint.class);
        // test with small known examples in detail.
        suite.addTestSuite(TestIndexSegmentBuilderWithSmallTree.class);
        // test ability to pre-populate the cache with records for the generated index segment.
        suite.addTestSuite(TestIndexSegmentBuilderCacheInteraction.class);
        // test fence posts for incremental builds with deleted index entries.
        suite.addTestSuite(TestIndexSegmentBuilderWithIncrementalBuild.class);
        // test fence posts for compacting merges with deleted index entries.
        suite.addTestSuite(TestIndexSegmentBuilderWithCompactingMerge.class);
        // test multi-block iterators for the index segment.
        suite.addTestSuite(TestIndexSegmentMultiBlockIterators.class);
        // test iterators for the index segment.
        suite.addTestSuite(TestIndexSegmentCursors.class);
        // stress test with larger random input trees and a variety of branching
        // factors.
        suite.addTestSuite(TestIndexSegmentBuilderWithLargeTrees.class);
        // test of the bloom filter integration.
        suite.addTestSuite(TestIndexSegmentWithBloomFilter.class);

        return suite;

    }

}
