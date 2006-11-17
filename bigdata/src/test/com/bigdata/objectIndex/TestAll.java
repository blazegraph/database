package com.bigdata.objectIndex;

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

        // test utility classes.
        suite.addTestSuite( TestSearch.class );
        // test basic tree operations.
        suite.addTestSuite( TestSimpleBTree.class );
        // test checksum computations (used by serialization).
        suite.addTestSuite( TestChecksumUtility.class );
        // test serialization
        suite.addTestSuite( TestNodeSerializer.class );
        // test the commit protocol w/o incremental leaf eviction.
        suite.addTestSuite( TestCommit.class );
        // @todo test incremental leaf eviction.
        // @todo test copy-on-write semantics with post-commit tree.
        // @todo test copy-on-write semantics with incremental leaf eviction.
        // @todo stress test against IRawStore on journal (vs SimpleRawStore).
        suite.addTestSuite( TestBTreeWithJournal.class );
        // @todo test tree operations for correct isolation and GC behaviors.
        // @todo test journal commit semantics for index.
        // @todo test journal abort semantics for index.
        // @todo test journal restart semantics w/o shutdown.
        // @todo stress test (correctness).
        // @todo test journal transaction isolation using the new object index.
        // @todo test journal restart semantics once persistent allocation index is implemented.

        return suite;
        
    }
    
}
