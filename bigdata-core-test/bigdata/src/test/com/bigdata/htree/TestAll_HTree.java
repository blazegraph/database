/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.htree;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.bigdata.btree.TestIndexCounter;

/**
 * Aggregates test suites into increasing dependency order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAll_HTree extends TestCase {

    /**
     * 
     */
    public TestAll_HTree() {
    }

    /**
     * @param arg0
     */
    public TestAll_HTree(String arg0) {
        super(arg0);
    }

    /**
     * Returns a test that will run each of the implementation specific test
     * suites in turn.
     */
    public static Test suite()
    {

        final TestSuite suite = new TestSuite("HTree");

        suite.addTestSuite(TestHTreeUtil.class);

        // unit tests for bootstrap of an HTree instance.
        suite.addTestSuite(TestHTree_init.class);
        
        suite.addTestSuite(TestHTree.class);

        // Unit test for a simple add level scenario.
        suite.addTestSuite(TestHTree_addLevel.class);

        // Stress tests for insert with varying addressBits w/o persistence.
        suite.addTestSuite(TestHTree_stressInsert.class);

        // test getCounter()
        suite.addTestSuite(TestIndexCounter.class);
        
        // test iterator semantics for visiting only "dirty" nodes or leaves.
        suite.addTestSuite(TestDirtyIterators.class);

        // test incremental write of leaves and nodes.
        suite.addTestSuite(TestIncrementalWrite.class);
        
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
        // test duplicate keys (with index checkpoint).
        suite.addTestSuite(TestDuplicates.class);

        /*
         * test of transient HTree's (no backing store).
         */
        suite.addTestSuite(TestTransientHTree.class);

        /*
         * test index with raw record support enabled.
         */
        suite.addTestSuite(TestRawRecords.class);

        /*
         * test index with removed tuples.
         */
        suite.addTestSuite(TestRemovals.class);

//        /* TODO Support bloom filters?
//        
//         * Test bloom filters for a BTree (vs an IndexSegment, which is handled
//         * in the IndexSegment test suite).
//         */
//        suite.addTestSuite(TestBloomFilter.class);
//        suite.addTestSuite(TestBTreeWithBloomFilter.class);

        // Integration stress test w/ Memory manager.
        suite.addTestSuite(TestHTreeWithMemStore.class);
        
        suite.addTestSuite(TestHTreeRecycle.class);
        
        return suite;

	}

}
