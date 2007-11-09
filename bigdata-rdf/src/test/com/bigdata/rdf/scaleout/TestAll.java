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
package com.bigdata.rdf.scaleout;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates test suites into increasing dependency order.
 * <p>
 * Note: The tests in this suite setup a bigdata federation for each test. In
 * order for these tests to succeed you MUST specify at least the following
 * properties to the JVM and have access to the resources in
 * <code>src/resources/config</code>.
 * 
 * <pre>
 * -Djava.security.policy=policy.all -Djava.rmi.server.codebase=http://proto.cognitiveweb.org/maven-repository/bigdata/jars/
 * </pre>
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
     * Returns a test that will run each of the implementation specific test
     * suites in turn.
     */
    public static Test suite()
    {

        TestSuite suite = new TestSuite("scale-out");

        // @todo refactor into the basic test suite?
        suite.addTestSuite(TestTermAndIdsIndex.class);
        suite.addTestSuite(TestStatementIndex.class);
        
        suite.addTestSuite(TestDistributedTripleStoreLoadRate.class);
        
        return suite;
        
    }
    
}
