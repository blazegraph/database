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

        TestSuite suite = new TestSuite("Object Index");

        // test search of int[] keys.
        suite.addTestSuite( TestSearch.class );
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
        // stress test basic tree operations w/o IO.
        suite.addTestSuite( TestBTree.class );
        // test checksum computations (used by serialization).
        suite.addTestSuite( TestChecksumUtility.class );
        // test index entry serialization
        suite.addTestSuite( TestIndexEntrySerializer.class );
        // test node/leaf serialization.
        suite.addTestSuite( TestNodeSerializer.class );
        // test the commit protocol w/o incremental leaf eviction @todo expand tests.
        suite.addTestSuite( TestCommit.class );
        // test incremental leaf eviction and copy-on-write scenarios.
        suite.addTestSuite( TestEviction.class );
        // @todo test copy-on-write semantics with post-commit or re-loaded tree.
        // stress test using journal as the backing store.
        suite.addTestSuite( TestBTreeWithJournal.class );
        // @todo test tree operations for correct isolation and GC behaviors.
        // @todo test journal commit semantics for index.
        // @todo test journal abort semantics for index.
        // @todo test journal restart semantics w/o shutdown.
        // @todo stress test (correctness as object index for store for each journal mode).
        // @todo test journal transaction isolation using the new object index.
        // @todo test journal restart semantics once persistent allocation index is implemented.

        return suite;
        
    }
    
}
