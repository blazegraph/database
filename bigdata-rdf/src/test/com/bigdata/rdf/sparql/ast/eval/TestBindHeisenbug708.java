/**

Copyright (C) SYSTAP, LLC 2013.  All rights reserved.

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
package com.bigdata.rdf.sparql.ast.eval;

import junit.framework.AssertionFailedError;

/**
 * Test suite for a hesienbug involving BIND.
 * Unlike the other issues this sometimes happens, and is sometimes OK,
 * so we run the test in a loop 20 times.
 * 
 * @version $Id$
 */
public class TestBindHeisenbug708 extends AbstractDataDrivenSPARQLTestCase {

    public TestBindHeisenbug708() {
    }

    public TestBindHeisenbug708(String name) {
        super(name);
    }

    public void test_heisenbug708() throws Exception {
        int cnt = 0;
        int max = 10;
        for (int i=0; i<max; i++) {
        	try {
        		new TestHelper(
        				"heisenbug-708",// testURI
        				"heisenbug-708.rq", // queryURI
        				"heisenbug-708.ttl", // dataURI
        				"heisenbug-708.srx" // resultURI
        				).runTest();
        	}
        	catch (AssertionFailedError e) {
        		cnt++;
        	}
        }
        assertTrue("Test failed " + cnt + "/" + max + " times", cnt==0);
    }
   
    
}
