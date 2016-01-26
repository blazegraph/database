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
/*
 * Created on Nov 7, 2007
 */

package com.bigdata.rdf.sail.webapp;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * A version of the test suite that is intended for local debugging and is NOT
 * run in CI. This is intended just to make it easier to run specific proxied
 * test suites.
 * <p>
 * TO USE: Comment in/out those tests suites that you want to run in
 * {@link #suite()} and also in
 * {@link TestNanoSparqlServerWithProxyIndexManager2}.
 * 
 * @see TestAll
 */
public class TestAll2 extends TestCase {

    /**
     * 
     */
    public TestAll2() {
        super();
    }

    /**
     * @param arg0
     */
    public TestAll2(String arg0) {
        super(arg0);
    }

    public static Test suite() {

        final TestSuite suite = new TestSuite("WebApp (local debugging)");

        suite.addTest(TestNanoSparqlServerWithProxyIndexManager2.suite(TestMode.triples));
        
        suite.addTest(TestNanoSparqlServerWithProxyIndexManager2.suite(TestMode.sids));
        
        suite.addTest(TestNanoSparqlServerWithProxyIndexManager2.suite(TestMode.quads));
        
        return suite;

    }

}
