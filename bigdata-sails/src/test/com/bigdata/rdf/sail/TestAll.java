/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Nov 7, 2007
 */

package com.bigdata.rdf.sail;


import java.util.Properties;

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

        TestSuite suite = new TestSuite("Sesame 1.x integration");
        
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
