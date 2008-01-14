/*

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Oct 18, 2007
 */

package com.bigdata.rdf.store;

import java.io.File;
import java.util.Properties;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;

import com.bigdata.service.EmbeddedBigdataClient;
import com.bigdata.service.EmbeddedBigdataFederation;
import com.bigdata.service.IBigdataClient;

/**
 * Proxy test suite for {@link ScaleOutTripleStore} running against an embedded
 * federation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestScaleOutTripleStoreWithEmbeddedFederation extends AbstractTestCase {

    /**
     * 
     */
    public TestScaleOutTripleStoreWithEmbeddedFederation () {
    }

    public TestScaleOutTripleStoreWithEmbeddedFederation (String name) {
        super(name);
    }
    
    public static Test suite() {

        final TestScaleOutTripleStoreWithEmbeddedFederation delegate = new TestScaleOutTripleStoreWithEmbeddedFederation(); // !!!! THIS CLASS !!!!

        /*
         * Use a proxy test suite and specify the delegate.
         */

        ProxyTestSuite suite = new ProxyTestSuite(delegate,
                "Scale-Out Triple Store Test Suite");

        /*
         * List any non-proxied tests (typically bootstrapping tests).
         */
        
//        // writes on the term:id and id:term indices.
//        suite.addTestSuite(TestTermAndIdsIndex.class);
//
//        // writes on the statement indices.
//        suite.addTestSuite(TestStatementIndex.class);
               
        /*
         * Proxied test suite for use only with the LocalTripleStore.
         * 
         * @todo test unisolated operation semantics.
         */

//        suite.addTestSuite(TestLocalTripleStoreTransactionSemantics.class);

        /*
         * Pickup the basic triple store test suite. This is a proxied test
         * suite, so all the tests will run with the configuration specified in
         * this test class and its optional .properties file.
         */
        
        suite.addTest(TestTripleStoreBasics.suite());

        return suite;

    }

    /**
     * Properties used by tests in the file and in this proxy suite.
     */
    public Properties getProperties() {

        Properties properties = new Properties( super.getProperties() );

//         Note: this reduces the disk usage at the expense of memory usage.
//        properties.setProperty(EmbeddedBigdataFederation.Options.BUFFER_MODE,
//                BufferMode.Transient.toString());

//        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());

//        properties.setProperty(Options.CREATE_TEMP_FILE,"true");

//        properties.setProperty(Options.DELETE_ON_EXIT,"true");

        /*
         * Note: there are also properties to control the #of data services
         * created in the embedded federation.
         */
        
        return properties;

    }

    /**
     * An embedded federation is setup and torn down per unit test.
     */
    IBigdataClient client;

    /**
     * Data files are placed into a directory named by the test. If the
     * directory exists, then it is removed before the federation is set up.
     */
    public void setUp(ProxyTestCase testCase) throws Exception {
    
        super.setUp(testCase);
        
        File dataDir = new File( testCase.getName() );
        
        if(dataDir.exists() && dataDir.isDirectory()) {

            recursiveDelete( dataDir );
            
        }

        Properties properties = new Properties(getProperties());
        
        // Note: directory named for the unit test (name is available from the
        // proxy test case).
        properties.setProperty(EmbeddedBigdataFederation.Options.DATA_DIR,
                testCase.getName());
        
        client = new EmbeddedBigdataClient(properties);
        
    }
    
    public void tearDown(ProxyTestCase testCase) throws Exception {

        // Note: close() is disconnecting from the embedded federation.
//        client.terminate();

        // delete on disk federation (if any).
        recursiveDelete(new File(testCase.getName()));
        
        super.tearDown();
        
    }
    
    /**
     * Recursively removes any files and subdirectories and then removes the
     * file (or directory) itself.
     * 
     * @param f A file or directory.
     */
    private void recursiveDelete(File f) {
        
        if(f.isDirectory()) {
            
            File[] children = f.listFiles();
            
            for(int i=0; i<children.length; i++) {
                
                recursiveDelete( children[i] );
                
            }
            
        }
        
        if (f.exists()) {

            log.warn("Removing: " + f);

            if (!f.delete()) {

                throw new RuntimeException("Could not remove: " + f);

            }

        }

    }
    
    protected AbstractTripleStore getStore() {
        
        // Connect to the federation.
        ScaleOutTripleStore store = new ScaleOutTripleStore(client.connect(),
                getProperties());
        
        // register indices.
        store.registerIndices();
        
        return store;
        
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

        /*
         * @todo closing the embedded federation causes it to be disconnected
         * and its data services shutdown but re-opening an existing embedded
         * federation is not yet supported so we have to either report an
         * exception here (since we can not reopen the client) or silently
         * return the same store.
         */
        
        log.warn("Embedded federation re-connect is not supported");
        
        return store;
        
//        // close the store.
//        store.close();
//        
//        if (!store.isStable()) {
//            
//            throw new UnsupportedOperationException("The backing store is not stable");
//            
//        }
//        
//        // Note: clone to avoid modifying!!!
//        Properties properties = (Properties)getProperties().clone();
//        
//        // Turn this off now since we want to re-open the same store.
//        properties.setProperty(Options.CREATE_TEMP_FILE,"false");
//        
//        // The backing file that we need to re-open.
//        File file = ((ScaleOutTripleStore) store).store.getFile();
//        
//        assertNotNull(file);
//        
//        // Set the file property explictly.
//        properties.setProperty(Options.FILE,file.toString());
//        
//        return new ScaleOutTripleStore( properties );
        
    }

}
