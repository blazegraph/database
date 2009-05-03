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
package com.bigdata.btree;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates test suites into increasing dependency order.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAll extends TestCase {

    /**
     * 
     */
    public TestAll() {
    }

    /**
     * @param arg0
     */
    public TestAll(String arg0) {
        super(arg0);
    }

    /**
     * Returns a test that will run each of the implementation specific test
     * suites in turn.
     */
    public static Test suite()
    {

        final TestSuite suite = new TestSuite(TestAll.class.getPackage()
                .getName());

        // test low level variable length byte[] operations.
        suite.addTestSuite( TestBytesUtil.class );
        
        // unsigned byte[] key encoding and decoding.
        suite.addTest(com.bigdata.btree.keys.TestAll.suite());

        // key[] and value[] compression.
        suite.addTest(com.bigdata.btree.compression.TestAll.suite());
        
        // test mutable key buffer.
        suite.addTestSuite(TestMutableKeyBuffer.class);
        // test immutable key buffer.
        suite.addTestSuite(TestImmutableKeyBuffer.class);
        // test key search routines on the key buffer implementations.
        suite.addTestSuite(TestKeyBufferSearch.class);

        /*
         * test btree fundementals.
         */
        // test static and instance utility methods on AbstractNode and ArrayType.
        suite.addTestSuite( TestUtilMethods.class );
        // test assertions that test for node/leaf invariants.
        suite.addTestSuite( TestInvariants.class );
        // test finding a child of a node by its key.
        suite.addTestSuite( TestFindChild.class );
        // test insert, lookup, and remove for root leaf w/o splitting it.
        suite.addTestSuite( TestInsertLookupRemoveKeysInRootLeaf.class );
        // test splitting the root leaf.
        suite.addTestSuite( TestSplitRootLeaf.class );
        // test splitting and joining the root leaf (no more than two levels).
        suite.addTestSuite( TestSplitJoinRootLeaf.class );
        // test splitting and joining with more than two levels.
        suite.addTestSuite( TestSplitJoinThreeLevels.class );
        // test indexOf, keyAt, valueAt.
        suite.addTestSuite( TestLinearListMethods.class );
        // test getCounter()
        suite.addTestSuite( TestIndexCounter.class );
        
        // test iterator semantics.
        suite.addTestSuite(TestBTreeLeafCursors.class);
        suite.addTestSuite(TestIterators.class);
        suite.addTestSuite(TestReadOnlyBTreeCursors.class);
        suite.addTestSuite(TestMutableBTreeCursors.class);
        // stackable tuple filters
        suite.addTest(com.bigdata.btree.filter.TestAll.suite());
        // chunked iterators.
        suite.addTestSuite(TestChunkedIterators.class);

        // test delete semantics (also see the isolation package).
        suite.addTestSuite( TestRemoveAll.class );
        // test contract for BTree#touch(node) w/o IO.
        suite.addTestSuite( TestTouch.class );
        // stress test basic tree operations w/o IO.
        suite.addTestSuite( TestBTree.class );
        // test child address serialization.
        suite.addTestSuite( TestAddressSerializer.class );
        suite.addTestSuite( TestPackedAddressSerializer.class );
        // test node/leaf serialization.
        suite.addTestSuite( TestNodeSerializer.class );
        
        // test iterator semantics for visiting only "dirty" nodes or leaves.
        suite.addTestSuite( TestDirtyIterators.class );
        
        // test incremental write of leaves and nodes.
        suite.addTestSuite( TestIncrementalWrite.class );
        // test copy-on-write scenarios.
        suite.addTestSuite( TestCopyOnWrite.class );
        
        /*
         * test with delete markers.
         * 
         * Note: tests with timestamps and delete markers are done in the
         * isolation package.
         */
        suite.addTestSuite( TestDeleteMarkers.class );

        /*
         * test persistence protocols. 
         */
        // test the commit protocol.
        suite.addTestSuite(TestCommit.class);
        // test the dirty event protocol.
        suite.addTestSuite(TestDirtyListener.class);
        // test the close/reopen protocol for releasing index buffers.
        suite.addTestSuite(TestReopen.class);
        
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

        // pick up the index segment test suites.
        suite.addTest(TestAll_IndexSegment.suite());

        /*
         * test fused views, including iterators for the fused view.
         */
        suite.addTestSuite( TestFusedView.class );
        
        /*
         * test the Map and Set implementations.
         */
        suite.addTestSuite( TestBigdataMap.class );
        suite.addTestSuite( TestBigdataSet.class );
        
        // @todo this test belongs in the isolation package.
//      suite.addTestSuite(TestIsolatedFusedViewCursors.class);

        // test index procedures.
        suite.addTest( com.bigdata.btree.proc.TestAll.suite());
        
        return suite;
        
    }
    
}
