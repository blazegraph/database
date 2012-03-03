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
package com.bigdata.rdf.sparql.ast.eval.service;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates test suites into increasing dependency order.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          TODO The resources for the search test suite should be moved into
 *          this package. However, we will then need to qualify the resource
 *          names.
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

        final TestSuite suite = new TestSuite("AST Service Evaluation");

        /*
         * Data driven tests.
         */
        
        // Test suites for SPARQL 1.1 Federated Query
        suite.addTestSuite(TestServiceInternal.class);
        suite.addTestSuite(TestServiceExternal.class);
        /*
         * FIXME Test suite for REMOTE SPARQL SERVICE evaluation against
         * embedded HTTP end points.
         * 
         * TODO We could also issue some remote requests against well known
         * service end points (dbPedia, LOD cloud, etc).
         */
       
        // Full text search
        suite.addTestSuite(TestSearch.class);

        return suite;
        
    }
    
}
