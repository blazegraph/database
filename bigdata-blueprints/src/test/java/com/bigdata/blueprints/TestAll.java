package com.bigdata.blueprints;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Aggregates test suites in increase dependency order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestAll extends TestCase {
    
    /**
     * Static flags to HA and/or Quorum to be excluded, preventing hangs in CI
     */

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
     * Aggregates the tests in increasing dependency order.
     */
    public static Test suite()
    {

        /*
         * log4j defaults to DEBUG which will produce simply huge amounts of
         * logging information when running the unit tests. Therefore we
         * explicitly set the default logging level to WARN. If you are using a
         * log4j configuration file then this is unlikely to interact with your
         * configuration, and in any case you can override specific loggers.
         */
        {

            final Logger log = Logger.getRootLogger();

            if (log.getLevel().equals(Level.DEBUG)) {

                log.setLevel(Level.WARN);

                log.warn("Defaulting debugging level to WARN for the unit tests");

            }
            
        }
        
        final TestSuite suite = new TestSuite("blueprints");

        suite.addTestSuite(com.bigdata.blueprints.TestPathConstraints.class);
        suite.addTestSuite(com.bigdata.blueprints.TestBigdataGraphClientNSS.class);
        suite.addTestSuite(com.bigdata.blueprints.TestBigdataGraphFactoryNSS.class);
        suite.addTestSuite(com.bigdata.blueprints.TestBigdataGraphEmbeddedTransactional.class);
        suite.addTestSuite(com.bigdata.blueprints.TestBigdataGraphClientInMemorySail.class);
        suite.addTestSuite(com.bigdata.blueprints.TestPathConstraints.class);
        
        return suite;
        
    }
    
}
