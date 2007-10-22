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
 * Created on Oct 18, 2007
 */

package com.bigdata.rdf.store;

import java.io.File;
import java.util.Properties;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;

import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Options;
import com.bigdata.rdf.sail.TestBigdataRdfRepository;

/**
 * Proxy test suite for {@link LocalTripleStore} when the backing indices are
 * {@link UnisolatedBTree}. This configuration supports full transactions since
 * the various indices are all isolatable.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestLocalTripleStoreWithIsolatableIndices extends AbstractTestCase {

    /**
     * 
     */
    public TestLocalTripleStoreWithIsolatableIndices() {
    }

    public TestLocalTripleStoreWithIsolatableIndices(String name) {
        super(name);
    }
    
    public static Test suite() {

        final TestLocalTripleStoreWithIsolatableIndices delegate = new TestLocalTripleStoreWithIsolatableIndices(); // !!!! THIS CLASS !!!!

        /*
         * Use a proxy test suite and specify the delegate.
         */

        ProxyTestSuite suite = new ProxyTestSuite(delegate,
                "Isolatable Local Triple Store Test Suite");

        /*
         * List any non-proxied tests (typically bootstrapping tests).
         */
        
        /*
         * Pickup the basic triple store test suite. This is a proxied test
         * suite, so all the tests will run with the configuration specified in
         * this test class and its optional .properties file.
         */
        
        suite.addTest(TestTripleStoreBasics.suite());

        /*
         * Pickup the Sesame 1.x test suite.
         * 
         * Note: This test suite requires access to the Sesame 1.x test suite
         * classes, not just the Sesame JARs.
         */
        try {
            
            Class.forName("org.openrdf.sesame.sail.RdfRepositoryTest");
            
            suite.addTestSuite( TestRdfRepository.class );
            
        } catch(ClassNotFoundException ex) {
            
            log.warn("Will not run the Sesame 1.x integration test suite.");
            
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

            properties.setProperty(com.bigdata.rdf.store.LocalTripleStore.Options.ISOLATABLE_INDICES,"true");
            
            properties.setProperty(com.bigdata.rdf.sail.Options.STORE_CLASS,LocalTripleStore.class.getName());
            
            return properties;
            
        }

    }

    /**
     * Properties used by tests in the file and in this proxy suite.
     */
    public Properties getProperties() {

        Properties properties = super.getProperties();

        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());

        properties.setProperty(Options.CREATE_TEMP_FILE,"true");

        properties.setProperty(Options.DELETE_ON_EXIT,"true");

        properties.setProperty(com.bigdata.rdf.store.LocalTripleStore.Options.ISOLATABLE_INDICES,"true");

        return properties;

    }

    protected AbstractTripleStore getStore() {
        
        return new LocalTripleStore( getProperties() );
        
    }
 
    /**
     * Re-open the same backing store.
     * 
     * @param store
     *            the existing store.
     * 
     * @return A new store.
     * 
     * @exception Throwable
     *                if the existing store is closed, or if the store can not
     *                be re-opened, e.g., from failure to obtain a file lock,
     *                etc.
     */
    protected AbstractTripleStore reopenStore(AbstractTripleStore store) {
        
        // close the store.
        store.close();
        
        if (!((LocalTripleStore) store).store.isStable()) {
            
            throw new UnsupportedOperationException("The backing store is not stable");
            
        }
        
        // Note: clone to avoid modifying!!!
        Properties properties = (Properties)getProperties().clone();
        
        // Turn this off now since we want to re-open the same store.
        properties.setProperty(Options.CREATE_TEMP_FILE,"false");
        
        // The backing file that we need to re-open.
        File file = ((LocalTripleStore) store).store.getFile();
        
        assertNotNull(file);
        
        // Set the file property explictly.
        properties.setProperty(Options.FILE,file.toString());
        
        return new LocalTripleStore( properties );
        
    }

}
