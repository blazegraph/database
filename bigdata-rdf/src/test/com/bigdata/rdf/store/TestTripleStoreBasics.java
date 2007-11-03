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

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;
import junit.framework.TestCase2;
import junit.framework.TestSuite;


/**
 * Aggregates test that are run for each {@link ITripleStore} implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTripleStoreBasics extends TestCase2 {

    /**
     * Aggregates the test suites into something approximating increasing
     * dependency. This is designed to run as a <em>proxy test suite</em> in
     * which all tests are run using a common configuration and a delegatation
     * mechanism. You MUST add the returned {@link Test} into a properly
     * configured {@link ProxyTestSuite}.
     * 
     * @see ProxyTestSuite
     */
    public static Test suite() {

        TestSuite suite = new TestSuite(TestTripleStoreBasics.class.getPackage()
                .getName());

        // test adding terms and statements.
        suite.addTestSuite(TestTripleStore.class);

        // test adding terms and statements is restart safe.
        suite.addTestSuite(TestRestartSafe.class);

        // test suite for the access path api.
        suite.addTestSuite(TestAccessPath.class);

        // somewhat dated test of sustained insert rate on synthetic data.
        suite.addTestSuite(TestInsertRate.class);

        // test suite for the rio parser and data loading integration.
        suite.addTest(com.bigdata.rdf.rio.TestAll.suite());

        // test suite for the inference engine.
        suite.addTest(com.bigdata.rdf.inf.TestAll.suite());

        return suite;
        
    }
    
}
