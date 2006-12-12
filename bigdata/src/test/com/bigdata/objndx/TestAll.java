package com.bigdata.objndx;

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

        // test key search routines (linear and binary and various key types).
        suite.addTest( TestSearch.suite() );
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
        // test iterator semantics.
        suite.addTestSuite( TestIterators.class );
        // test contract for BTree#touch(node) w/o IO.
        suite.addTestSuite( TestTouch.class );
        // stress test basic tree operations w/o IO.
        suite.addTestSuite( TestBTree.class );
        // test checksum computations (used by serialization).
        suite.addTestSuite( TestChecksumUtility.class );
        // test index entry serialization
        suite.addTestSuite( TestIndexEntrySerializer.class );
        // test node/leaf serialization.
        suite.addTestSuite( TestNodeSerializer.class );
        // test iterator semantics for visiting only "dirty" nodes or leaves.
        suite.addTestSuite( TestDirtyIterators.class );
        // test incremental write of leaves and nodes.
        suite.addTestSuite( TestIncrementalWrite.class );
        // test copy-on-write scenarios.
        suite.addTestSuite( TestCopyOnWrite.class );
        // test the commit protocol. @todo expand tests.
        suite.addTestSuite( TestCommit.class );
        // stress test using journal as the backing store.
        suite.addTestSuite( TestBTreeWithJournal.class );
        // test alternative key types (String and long[]) for simple RDF data model.
        suite.addTestSuite( TestTripleStore.class );
        // test index is restart safe.
        // @todo test tree operations for correct transaction isolation and GC.
        // @todo test journal commit semantics for index.
        // @todo test journal abort semantics for index.
        // @todo test journal restart semantics w/o shutdown.
        // @todo stress test (correctness as object index for store for each journal mode).
        // @todo test journal transaction isolation using the new object index.
        // @todo test journal restart semantics once persistent allocation index is implemented.

        return suite;
        
    }
    
}
