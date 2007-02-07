package com.bigdata.scaleup;

import com.bigdata.scaleup.TestMetadataIndex;

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

        TestSuite suite = new TestSuite("scaleup");

        /*
         * journal overflow
         * 
         * @todo test overflow triggers near journal capacity
         * 
         * @todo test overflow will abort transactions if necessary, e.g., after
         * a grace period and possible journal extension.
         */
        
        /*
         * management of partitioned indices.
         * 
         * @todo test overflow resulting in parition merge or split.
         * 
         * @todo test DistributedBTree (reads through to active index segments
         * if miss on BTree in the journal). there is a lot to test here
         * including all of the transactional semantics.
         * 
         * @todo test metadata management for index segments.
         */
        suite.addTestSuite(TestMetadataIndex.class);
       
        return suite;
        
    }
    
}
