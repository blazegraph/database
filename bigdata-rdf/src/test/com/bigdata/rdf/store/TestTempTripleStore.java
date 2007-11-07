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

        ProxyTestSuite suite = new ProxyTestSuite(delegate,
                "Temporary Triple Store Test Suite");

        /*
         * List any non-proxied tests (typically bootstrapping tests).
         */
        
        /*
         * Pickup the basic triple store test suite. This is a proxied test
         * suite, so all the tests will run with the configuration specified in
         * this test class and its optional .properties file.
         */
        
        suite.addTest(TestTripleStoreBasics.suite());

        return suite;

    }

    /**
     * Properties for tests in this file and this proxy suite (if any).
     */
    public Properties getProperties() {

        Properties properties = super.getProperties();

        return properties;

    }
    
    protected AbstractTripleStore getStore() {
        
        return new TempTripleStore( getProperties() );
        
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
