/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.gom;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

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
     * Returns a test that will run each of the implementation specific test
     * suites in turn.
     */
    public static Test suite()
    {

        final TestSuite suite = new TestSuite("GOM");

        // Embedded GOM test suite.
        suite.addTest(com.bigdata.gom.LocalGOMTestCase.suite());
        
        // Remote GOM test suite
        suite.addTest(com.bigdata.gom.RemoteGOMTestCase.suite());
        
        
 
        suite.addTestSuite(com.bigdata.gom.TestOWLSkin.class);

        // JavaScript GOM test suite.
        suite.addTestSuite(com.bigdata.gom.TestJavaScriptGOM.class);

//        suite.addTestSuite(com.bigdata.gom.TestNumericBNodes.class);
        
        return suite;

    }

}
