/*
 * Copyright SYSTAP, LLC 2006-2007.  All rights reserved.
 * 
 * Contact:
 *      SYSTAP, LLC
 *      4501 Tower Road
 *      Greensboro, NC 27410
 *      phone: +1 202 462 9888
 *      email: licenses@bigdata.com
 *
 *      http://www.systap.com/
 *      http://www.bigdata.com/
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/*
 * Created on Nov 7, 2007
 */

package com.bigdata.rdf.sail;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Test suite.
 * 
 * FIXME integrate the Sesame 2 TCK (technology compatibility kit).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAll extends TestCase {

    /**
     * 
     */
    public TestAll() {
        super();
    }

    /**
     * @param arg0
     */
    public TestAll(String arg0) {
        super(arg0);
    }

    public static Test suite() {

        /*
         * log4j defaults to DEBUG which will produce simply huge amounts of
         * logging information when running the unit tests. Therefore we
         * explicitly set the default logging level to WARN unless it has
         * already been set to another value. If you are using a log4j
         * configuration file then this is unlikely to interact with your
         * configuration, and in any case you can override specific loggers.
         */
        {

            Logger log = Logger.getRootLogger();

            if (log.getLevel().equals(Level.DEBUG)) {

                log.setLevel(Level.WARN);

                log.warn("Defaulting debugging level to WARN for the unit tests");

            }
            
        }
        
        TestSuite suite = new TestSuite("Sesame 2.x integration");
       
        // bootstrap tests for the BigdataSail
        suite.addTestSuite(TestBigdataSail.class);

        // test of the search magic predicate
        suite.addTestSuite(TestSearchQuery.class);
        
        // high-level query tests.
        suite.addTestSuite(TestQuery.class);

        // test of high-level query on a graph with statements about statements.
        suite.addTestSuite(TestProvenanceQuery.class);
        
// Restore the following tests after adapting to Sesame 2.x
//        
//        /*
//         * Pickup the Sesame 1.x test suite.
//         * 
//         * Note: This test suite requires access to the Sesame 1.x test suite
//         * classes, not just the Sesame JARs.
//         * 
//         * @todo bundle that test suite.
//         */
//        try {
//
//            Class.forName("org.openrdf.sesame.sail.RdfRepositoryTest");
//
//            suite.addTestSuite(TestRdfRepository.class);
//
//        } catch (ClassNotFoundException ex) {
//
//            System.err.println("Will not run the Sesame 1.x integration test suite.");
//
//        }
//
//        try {
//
//            Class.forName("org.openrdf.sesame.sail.RdfSchemaRepositoryTest");
//
//            suite.addTestSuite(TestSchemaRdfRepository.class);
//
//        } catch (ClassNotFoundException ex) {
//
//            System.err.println("Will not run the Sesame 1.x integration test suite.");
//
//        }

        return suite;

    }

//    /**
//     * Integration for the Sesame 1.x repository test suite.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static class TestRdfRepository extends TestBigdataRdfRepository {
//
//        public TestRdfRepository(String arg0) {
//            super(arg0);
//        }
//
//        public Properties getProperties() {
//            
//            Properties properties = new Properties();
//            
//            properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());
//
//            properties.setProperty(Options.CREATE_TEMP_FILE,"true");
//
//            properties.setProperty(Options.DELETE_ON_EXIT,"true");
//
//            properties.setProperty(com.bigdata.rdf.store.LocalTripleStore.Options.ISOLATABLE_INDICES,"false");
//
//            properties.setProperty(com.bigdata.rdf.sail.BigdataSail.Options.STORE_CLASS,LocalTripleStore.class.getName());
//            
//            return properties;
//            
//        }
//
//    }
//
//    /**
//     * Integration for the Sesame 1.x repository test suite.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static class TestSchemaRdfRepository extends TestBigdataRdfSchemaRepository {
//
//        public TestSchemaRdfRepository(String arg0) {
//            super(arg0);
//        }
//
//        public Properties getProperties() {
//            
//            Properties properties = new Properties();
//            
//            properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());
//
//            properties.setProperty(Options.CREATE_TEMP_FILE,"true");
//
//            properties.setProperty(Options.DELETE_ON_EXIT,"true");
//
//            properties.setProperty(com.bigdata.rdf.store.LocalTripleStore.Options.ISOLATABLE_INDICES,"false");
//
//            properties.setProperty(com.bigdata.rdf.sail.BigdataSail.Options.STORE_CLASS,LocalTripleStore.class.getName());
//            
//            return properties;
//            
//        }
//
//    }

}
