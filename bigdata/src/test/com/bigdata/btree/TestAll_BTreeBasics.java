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
 * Aggregates the unit tests for the core B+Tree operations, all of which are in
 * the same package as the {@link BTree}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAll_BTreeBasics extends TestCase {

    public TestAll_BTreeBasics() {
    }

    public TestAll_BTreeBasics(String arg0) {
        super(arg0);
    }

    /**
     * Returns a test that will run each of the implementation specific test
     * suites in turn.
     */
    public static Test suite()
    {
        
        final TestSuite suite = new TestSuite("B+Tree basics");

        /*
         * test btree fundamentals.
         */
        // test static and instance utility methods on AbstractNode and ArrayType.
        suite.addTestSuite(TestUtilMethods.class);
        // test finding a child of a node by its key.
        suite.addTestSuite(TestFindChild.class);
        // test insert, lookup, and remove for root leaf w/o splitting it.
        suite.addTestSuite(TestInsertLookupRemoveKeysInRootLeaf.class);
        // test splitting the root leaf.
        suite.addTestSuite(TestSplitRootLeaf.class);
        // test splitting and joining the root leaf (no more than two levels).
        suite.addTestSuite(TestSplitJoinRootLeaf.class);
        // test splitting and joining with more than two levels.
        suite.addTestSuite(TestSplitJoinThreeLevels.class);
        // test edge cases in finding the shortest separator key for a leaf.
        suite.addTestSuite(TestLeafSplitShortestSeparatorKey.class);
        // test indexOf, keyAt, valueAt.
        suite.addTestSuite(TestLinearListMethods.class);
        // test getCounter()
        suite.addTestSuite(TestIndexCounter.class);

        // test imposing constraint on a fromKey or toKey based on an index
        // partition's boundaries.
        suite.addTestSuite(TestConstrainKeys.class);
        
        // test iterator semantics.
        suite.addTest(TestAll_Iterators.suite());

        // test delete semantics (also see the isolation package).
        suite.addTestSuite(TestRemoveAll.class);
        // test contract for BTree#touch(node) w/o IO.
        suite.addTestSuite(TestTouch.class);
        // stress test basic tree operations w/o IO.
        suite.addTestSuite(TestBTree.class);
        // @todo test child address serialization (keep or discard?).
        suite.addTestSuite(TestAddressSerializer.class);
        suite.addTestSuite(TestPackedAddressSerializer.class);
        // test node/leaf serialization.
//        suite.addTestSuite( TestNodeSerializer.class );
        
        // test iterator semantics for visiting only "dirty" nodes or leaves.
        suite.addTestSuite(TestDirtyIterators.class);

        // test incremental write of leaves and nodes.
        suite.addTestSuite(TestIncrementalWrite.class);
        // test copy-on-write scenarios.
        suite.addTestSuite(TestCopyOnWrite.class);

        /*
         * test with delete markers.
         * 
         * Note: tests with timestamps and delete markers are done in the
         * isolation package.
         * 
         * FIXME We should verify correct maintenance of the min/max and per
         * tuple version timestamps here. The raba coder tests already verify
         * correct coding and decoding IFF the data are being correctly
         * maintained.
         */
        suite.addTestSuite(TestDeleteMarkers.class);

        /*
         * test persistence protocols. 
         */
        // test the commit protocol.
        suite.addTestSuite(TestCommit.class);
        // test the dirty event protocol.
        suite.addTestSuite(TestDirtyListener.class);
        // test the close/reopen protocol for releasing index buffers.
        suite.addTestSuite(TestReopen.class);
        // test of storing null values under a key with persistence.
        suite.addTestSuite(TestNullValues.class);

        /*
         * test of transient BTree's (no backing store).
         */
        suite.addTestSuite(TestTransientBTree.class);

        /*
         * Test bloom filters for a BTree (vs an IndexSegment, which is handled
         * below).
         */
        suite.addTestSuite(TestBloomFilter.class);
        suite.addTestSuite(TestBTreeWithBloomFilter.class);

        return suite;

    }

}
