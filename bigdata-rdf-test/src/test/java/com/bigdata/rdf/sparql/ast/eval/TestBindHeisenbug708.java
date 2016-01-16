/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2013.  All rights reserved.

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
package com.bigdata.rdf.sparql.ast.eval;

import junit.framework.AssertionFailedError;

/**
 * Test suite for a hesienbug involving BIND. Unlike the other issues this
 * sometimes happens, and is sometimes OK, so we run the test in a loop 20
 * times.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/708">
 *      Heisenbug </a>
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
//        		return; // stop @ success
        	}
        	catch (AssertionFailedError e) {
        		cnt++;
//        		throw e;// halt @ 1st failure.
        	}
        }
        assertTrue("Test failed " + cnt + "/" + max + " times", cnt==0);
    }

    /**
     * Demonstrates the same problem using two BIND()s. This rules out the
     * JOIN as the problem.
     */
    public void test_heisenbug708_doubleBind() throws Exception {
        int cnt = 0;
        int max = 10;
        for (int i=0; i<max; i++) {
            try {
                new TestHelper(
                        "heisenbug-708-doubleBind",// testURI
                        "heisenbug-708-doubleBind.rq", // queryURI
                        "heisenbug-708-doubleBind.ttl", // dataURI
                        "heisenbug-708-doubleBind.srx" // resultURI
                        ).runTest();
//              return; // stop @ success
            }
            catch (AssertionFailedError e) {
                cnt++;
//              throw e;// halt @ 1st failure.
            }
        }
        assertTrue("Test failed " + cnt + "/" + max + " times", cnt==0);
    }

    
}
