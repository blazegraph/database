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

        // @todo test index node cache operations.
        // @todo test basic tree operations (insert, find, key scan)
        // @todo test basic tree manipulations (insert, rotate, split, etc).
        // @todo test copy-on-write semantics.
        // @todo test tree operations in journal context.
        // @todo test tree operations for correct isolation and GC behaviors.
        // @todo test journal commit semantics for index.
        // @todo test journal abort semantics for index.
        // @todo test journal restart semantics w/o shutdown.
        // @todo stress test (correctness).
        
//        suite.addTestSuite( TestChecksumUtility.class );
//        suite.addTestSuite( TestNodeSerializer.class );
        suite.addTestSuite( TestSearch.class );
        suite.addTestSuite( TestSimpleBTree.class );
               
        return suite;
        
    }
    
}
