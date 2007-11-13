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


import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Options;
import com.bigdata.rdf.store.LocalTripleStore;

/**
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
         * explicitly set the default logging level to WARN. If you are using a
         * log4j configuration file then this is unlikely to interact with your
         * configuration, and in any case you can override specific loggers.
         */
        {

            Logger log = Logger.getRootLogger();

            if (log.getLevel().equals(Level.DEBUG)) {

                log.setLevel(Level.WARN);

                log.warn("Defaulting debugging level to WARN for the unit tests");

            }
            
        }
        
        TestSuite suite = new TestSuite("Sesame 1.x integration");
       
        // test suite for SAIL transaction semantics.
        suite.addTestSuite(TestSAILTransactionSemantics.class);

        /*
         * @todo write a test to verify that concurrent attempts to start a
         * transactio using startTransaction() will be serialized such that only
         * a single writer is allowed.
         * 
         * @todo write BigdataReadCommittedRdf(Schema)Repository classes and
         * verify that concurrent readers against those classes are allowed
         * while writers are serialized by the BigdataRdf(Schema)Repository.
         * 
         * @todo if we support full transactions, then concurrent readers and
         * writers should be allowed by the SAIL. in this case use thread local
         * variables to associate the tx with the thread. readers will still use
         * read-committed semantics since there is no means to create a
         * read-only transaction with the SAIL.
         */
        
        // test suite for access to the statement type.
        suite.addTestSuite(TestStatementWithType.class);
        
        // test suite for RDFS closure correctness.
        suite.addTestSuite(TestRDFSClosure.class);

        // test suite for RDFS closure correctness with incremental load (TM).
        suite.addTestSuite(TestRDFSIncrementalClosure.class);

        // test suite for RDFS closure correctness with incremental delete (TM).
        suite.addTestSuite(TestRDFSTruthMaintenance.class);

        /*
         * Pickup the Sesame 1.x test suite.
         * 
         * Note: This test suite requires access to the Sesame 1.x test suite
         * classes, not just the Sesame JARs.
         * 
         * @todo bundle that test suite.
         */
        try {

            Class.forName("org.openrdf.sesame.sail.RdfRepositoryTest");

            suite.addTestSuite(TestRdfRepository.class);

        } catch (ClassNotFoundException ex) {

            System.err.println("Will not run the Sesame 1.x integration test suite.");

        }

        try {

            Class.forName("org.openrdf.sesame.sail.RdfSchemaRepositoryTest");

            suite.addTestSuite(TestSchemaRdfRepository.class);

        } catch (ClassNotFoundException ex) {

            System.err.println("Will not run the Sesame 1.x integration test suite.");

        }

        return suite;

    }

    /**
     * Integration for the Sesame 1.x repository test suite.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class TestRdfRepository extends TestBigdataRdfRepository {

        public TestRdfRepository(String arg0) {
            super(arg0);
        }

        public Properties getProperties() {
            
            Properties properties = new Properties();
            
            properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());

            properties.setProperty(Options.CREATE_TEMP_FILE,"true");

            properties.setProperty(Options.DELETE_ON_EXIT,"true");

            properties.setProperty(com.bigdata.rdf.store.LocalTripleStore.Options.ISOLATABLE_INDICES,"false");

            properties.setProperty(com.bigdata.rdf.sail.BigdataRdfRepository.Options.STORE_CLASS,LocalTripleStore.class.getName());
            
            return properties;
            
        }

    }

    /**
     * Integration for the Sesame 1.x repository test suite.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class TestSchemaRdfRepository extends TestBigdataRdfSchemaRepository {

        public TestSchemaRdfRepository(String arg0) {
            super(arg0);
        }

        public Properties getProperties() {
            
            Properties properties = new Properties();
            
            properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());

            properties.setProperty(Options.CREATE_TEMP_FILE,"true");

            properties.setProperty(Options.DELETE_ON_EXIT,"true");

            properties.setProperty(com.bigdata.rdf.store.LocalTripleStore.Options.ISOLATABLE_INDICES,"false");

            properties.setProperty(com.bigdata.rdf.sail.BigdataRdfRepository.Options.STORE_CLASS,LocalTripleStore.class.getName());
            
            return properties;
            
        }

    }

}
