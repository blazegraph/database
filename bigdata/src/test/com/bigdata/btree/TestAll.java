/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

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

        TestSuite suite = new TestSuite("BTree");
        
        /*
         * test fast DataOutput and DataInput implementations.
         */
        suite.addTestSuite(TestDataOutputBuffer.class);
        suite.addTestSuite(TestShortPacker.class);
        suite.addTestSuite(TestLongPacker.class);
        
        /*
         * test key encoding and comparison support.
         */
        // test methods that compute the successor for various data types.
        suite.addTestSuite( TestSuccessorUtil.class );
        // test low level variable length byte[] operations.
        suite.addTestSuite( TestBytesUtil.class );
        // test key encoding operations.
        suite.addTestSuite(TestKeyBuilder.class);
        // test mutable key buffer.
        suite.addTestSuite(TestMutableKeyBuffer.class);
        // test immutable key buffer.
        suite.addTestSuite(TestImmutableKeyBuffer.class);
        // test key search routines on the key buffer implementations.
        suite.addTestSuite( TestKeyBufferSearch.class );
        // test key buffer (de-)serialization.
        suite.addTestSuite( TestKeyBufferSerializer.class );
        
        /*
         * test value serializers and compression methods.
         */
        // test serialization of byte[] values.
        suite.addTestSuite( TestByteArrayValueSerializer.class );
        
        /*
         * test record compression support.
         */
        // test bulk data compression.
        suite.addTestSuite( TestRecordCompressor.class );

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
        // test insert, lookup, and remove for root leaf w/o splitting it using the batch api.
        suite.addTestSuite( TestInsertLookupRemoveOnRootLeafWithBatchApi.class );
        // test splitting the root leaf.
        suite.addTestSuite( TestSplitRootLeaf.class );
        // test splitting and joining the root leaf (no more than two levels).
        suite.addTestSuite( TestSplitJoinRootLeaf.class );
        // test splitting and joining with more than two levels.
        suite.addTestSuite( TestSplitJoinThreeLevels.class );
        // test indexOf, keyAt, valueAt.
        suite.addTestSuite( TestLinearListMethods.class );
        // test iterator semantics.
        suite.addTestSuite( TestIterators.class );
        // test delete semantics (also see the isolation package).
        suite.addTestSuite( TestRemoveAll.class );
        // test contract for BTree#touch(node) w/o IO.
        suite.addTestSuite( TestTouch.class );
        // stress test basic tree operations w/o IO.
        suite.addTestSuite( TestBTree.class );
        // test fused view operations on ordered list of trees.
        suite.addTestSuite( TestFusedView.class );
        // test of user-defined functions.
        suite.addTestSuite( TestUserDefinedFunction.class );
        // test node/leaf serialization.
        suite.addTestSuite( TestNodeSerializer.class );
        // test iterator semantics for visiting only "dirty" nodes or leaves.
        suite.addTestSuite( TestDirtyIterators.class );
        // test incremental write of leaves and nodes.
        suite.addTestSuite( TestIncrementalWrite.class );
        // test copy-on-write scenarios.
        suite.addTestSuite( TestCopyOnWrite.class );
        
        /*
         * test atomic commit
         */
        // test the commit protocol.
        suite.addTestSuite( TestCommit.class );
        // verify that a store is restart-safe iff it commits.
        suite.addTestSuite( TestRestartSafe.class );
        // test the close/reopen protocol for releasing index buffers.
        suite.addTestSuite( TestReopen.class );

        /*
         * index rebuilding.
         */
        // test static methods for the index builder.
        suite.addTestSuite( TestIndexSegmentPlan.class );
        // test encoding and decoding of child node/leaf addresses.
        suite.addTestSuite( TestIndexSegmentAddressSerializer.class );
        // test with small known examples in detail.
        suite.addTestSuite( TestIndexSegmentBuilderWithSmallTree.class );
        // stress test with larger random input trees and a variety of branching factors.
        suite.addTestSuite( TestIndexSegmentBuilderWithLargeTrees.class );
        // test of the bloom filter integration.
        suite.addTestSuite( TestIndexSegmentWithBloomFilter.class );
        // test of the fast forward and reverse leaf scans.
        suite.addTestSuite( TestIndexSegmentFastLeafScan.class );
        // test compacting merge of two index segments.
        suite.addTestSuite( TestIndexSegmentMerger.class );

        /*
         * @todo use of btree to support column store (in another package)
         * 
         * @todo handle column names and timestamp as part of the key.
         * 
         * @todo test version expiration based on age
         * 
         * @todo test version expiration based on #of versions.
         * 
         * @todo test on partitioned index. 
         */
        
        return suite;
        
    }
    
}
