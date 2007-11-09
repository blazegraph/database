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
        suite.addTest( com.bigdata.concurrent.TestAll.suite() );
        suite.addTest( com.bigdata.journal.TestAll.suite() );
        suite.addTest( com.bigdata.scaleup.TestAll.suite() );

        if (Boolean.parseBoolean(System.getProperty("maven.test.services.skip",
                "false"))) {

            /*
             * Note: The service tests require that Jini is running, that you
             * have specified a suitable security policy, and that the codebase
             * parameter is set correctly. See the test suites for more detail
             * on how to setup to run these tests.
             */

            suite.addTest(com.bigdata.service.TestAll.suite());
            suite.addTest(com.bigdata.service.mapReduce.TestAll.suite());

        }

        return suite;
        
    }

}
