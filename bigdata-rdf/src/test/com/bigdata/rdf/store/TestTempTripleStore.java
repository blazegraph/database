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

import java.util.Properties;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;

/**
 * Proxy test suite for {@link TempTripleStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTempTripleStore extends AbstractTestCase {

    /**
     * 
     */
    public TestTempTripleStore() {
    }

    public TestTempTripleStore(String name) {
        super(name);
    }
    
    public static Test suite() {

        final TestTempTripleStore delegate = new TestTempTripleStore(); // !!!! THIS CLASS !!!!

        /*
         * Use a proxy test suite and specify the delegate.
         */

        final ProxyTestSuite suite = new ProxyTestSuite(delegate,
                "Temporary Triple Store Test Suite");

        /*
         * List any non-proxied tests (typically bootstrapping tests).
         */
        
        /*
         * Pickup the basic triple store test suite. This is a proxied test
         * suite, so all the tests will run with the configuration specified in
         * this test class and its optional .properties file.
         */
        
        // basic test suite.
        suite.addTest(TestTripleStoreBasics.suite());
        
        // rules, inference, and truth maintenance test suite.
        suite.addTest( com.bigdata.rdf.rules.TestAll.suite() );

        return suite;

    }

    /**
     * Properties for tests in this file and this proxy suite (if any).
     */
    public Properties getProperties() {

        Properties properties = super.getProperties();

        return properties;

    }
    
    protected AbstractTripleStore getStore(Properties properties) {
        
        return new TempTripleStore( properties );
        
    }
 
    /**
     * Re-open the same backing store.
     * 
     * @param store
     *            the existing store.
     * 
     * @return A new store.
     * 
     * @exception UnsupportedOperationException
     *                The {@link TempTripleStore} can not be re-opened.
     */
    protected AbstractTripleStore reopenStore(AbstractTripleStore store) {

        throw new UnsupportedOperationException(TempTripleStore.class.getName()
                + " can not be re-opened.");
        
    }

}
