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
package com.bigdata.htree;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.bigdata.btree.BTree;
import com.bigdata.btree.TestIndexCounter;
import com.bigdata.journal.Journal;
import com.bigdata.rwstore.RWStore;
import com.bigdata.rwstore.sector.MemStore;

/**
 * Aggregates test suites into increasing dependency order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO Write integration and stress tests for the {@link HTree}
 *          against the {@link MemStore} and {@link RWStore}.
 *          <p>
 *          RWStore with "RAM" mostly. Converts to disk backed if uses all those
 *          buffers. Possibly just give the WriteCacheService a bunch of write
 *          cache buffers (10-100) and have it evict to disk *lazily* rather
 *          than eagerly (when the #of free buffers is down to 20%).
 * 
 *          TODO Write integration tests for the {@link HTree} against a
 *          {@link Journal}. These tests will have to verify the commit protocol
 *          which is still partly wired for the {@link BTree}.
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
//
//        // test incremental write of leaves and nodes.
//        suite.addTestSuite(TestIncrementalWrite.class);
//        // test copy-on-write scenarios.
//        suite.addTestSuite(TestCopyOnWrite.class);

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

//        /*
//         * test of transient HTree's (no backing store).
//         */
//        suite.addTestSuite(TestTransientHTree.class);

        /*
         * test index with raw record support enabled.
         */
        suite.addTestSuite(TestRawRecords.class);

//        /* TODO Support blooom filters?
//        
//         * Test bloom filters for a BTree (vs an IndexSegment, which is handled
//         * in the IndexSegment test suite).
//         */
//        suite.addTestSuite(TestBloomFilter.class);
//        suite.addTestSuite(TestBTreeWithBloomFilter.class);

        // Integration stress test w/ Memory manager.
        suite.addTestSuite(TestHTreeWithMemStore.class);
        
        return suite;

	}

}
