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
 * Created on Jun 26, 2006
 */
package com.bigdata.service;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates tests in dependency order.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestAll extends TestCase {

    public TestAll() {}
    
    public TestAll(String name) {super(name);}
    
    public static Test suite()
    {

        TestSuite suite = new TestSuite(TestAll.class.getName());

        suite.addTestSuite( TestServiceDiscovery.class );

        /*
         * Test of a single client talking to a single data service instance
         * without the use of the metadata service or a transaction manager.
         */
        suite.addTestSuite( TestDataServer0.class );

        /*
         * Test of a single client talking to a single metadata service
         * instance.
         */
        suite.addTestSuite( TestMetadataServer0.class );

        //        suite.addTestSuite( TestServer.class ); // Does not implement TestCase.

        return suite;
        
    }
    
}
