/**

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

import com.bigdata.btree.BTree;
import com.bigdata.journal.Options;

/**
 * Proxy test suite for {@link LocalTripleStore} when the backing indices are
 * {@link BTree}s. This configuration does NOT support transactions since the
 * various indices are NOT isolatable.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestLocalTripleStore extends AbstractTestCase {

    /**
     * 
     */
    public TestLocalTripleStore() {
    }

    public TestLocalTripleStore(String name) {
        super(name);
    }
    
    public static Test suite() {

        final TestLocalTripleStore delegate = new TestLocalTripleStore(); // !!!! THIS CLASS !!!!

        /*
         * Use a proxy test suite and specify the delegate.
         */

        ProxyTestSuite suite = new ProxyTestSuite(delegate,
                "Local Triple Store Test Suite");

        /*
         * List any non-proxied tests (typically bootstrapping tests).
         */

        // ...
        
        /*
         * Proxied test suite for use only with the LocalTripleStore.
         */

        suite.addTestSuite(TestLocalTripleStoreTransactionSemantics.class);

        /*
         * Pickup the basic triple store test suite. This is a proxied test
         * suite, so all the tests will run with the configuration specified in
         * this test class and its optional .properties file.
         */

        suite.addTest(TestTripleStoreBasics.suite());
        
        return suite;
        
    }

    public Properties getProperties() {

        /*
         * Note: nothing needs to be overriden.  This test corresponds to the
         * default configuration for the LocalTripleStore.
         */
        
        return super.getProperties();
        
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
