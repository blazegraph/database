package com.bigdata.rdf.graph.impl.sail;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates test suites into increasing dependency order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestAll.java 6116 2012-03-13 20:39:17Z thompsonbry $
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

        final TestSuite suite = new TestSuite("SAIL Graph Engine");

        /*
         * Only core functionality tests for the SAIL backend belong here. The
         * analytic test suites are written to the SAIL backend so there is good
         * coverage there.
         */

        suite.addTestSuite(TestGather.class);
        
        return suite;
        
    }
    
}
