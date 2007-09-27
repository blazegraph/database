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
 * Created on Feb 4, 2007
 */

package com.bigdata;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates test suites in increase dependency order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAll extends TestCase {

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

        TestSuite suite = new TestSuite("bigdata");

        suite.addTest( com.bigdata.cache.TestAll.suite() );
        suite.addTest( com.bigdata.io.TestAll.suite() );
        suite.addTest( com.bigdata.util.TestAll.suite() );
        suite.addTest( com.bigdata.rawstore.TestAll.suite() );
        suite.addTest( com.bigdata.btree.TestAll.suite() );
        suite.addTest( com.bigdata.isolation.TestAll.suite() );
        suite.addTest( com.bigdata.sparse.TestAll.suite() );
        suite.addTest( com.bigdata.journal.TestAll.suite() );
        suite.addTest( com.bigdata.scaleup.TestAll.suite() );

        /*
         * Note: The service tests require that Jini is running, that you have
         * specified a suitable security policy, and that the codebase parameter
         * is set correctly. See the test suites for more detail on how to setup
         * to run these tests.
         */

        suite.addTest( com.bigdata.service.TestAll.suite() );
        suite.addTest( com.bigdata.service.mapReduce.TestAll.suite() );

        return suite;
        
    }

}
