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
        
        /*
         * Internal service support (bigdata aware services in the same JVM).
         */

        suite.addTestSuite(TestBigdataNativeServiceEvaluation.class);

        /*
         * Bundled "internal" services.
         */

        // Full text search
        suite.addTestSuite(TestSearch.class);

        /*
         * External service support (openrdf services in the same JVM).
         */

        suite.addTestSuite(TestOpenrdfNativeServiceEvaluation.class);
        
        /*
         * Test suite for building a valid SPARQL expression for a remote
         * service end point. This test suite includes validation of the parsed
         * AST model in order to verify that the expected SPARQL query was
         * generated.
         */
        
        suite.addTestSuite(TestRemoteSparql10QueryBuilder.class);
        
        suite.addTestSuite(TestRemoteSparql11QueryBuilder.class);
        
        /*
         * Test suite for choosing the right implementation to vector solutions
         * to a remote end point. This decision depends on the capabilities of
         * the end point as well as factors such as the presence of blank nodes
         * in the solutions to be vectored to the end point.
         */

        suite.addTestSuite(TestRemoteSparqlBuilderFactory.class);

        /*
         * Note: See the NanoSparqlServer test suite for REMOTE SPARQL SERVICE
         * evaluation against embedded HTTP end points.
         */

        /*
         * Test suite for registering and managing services.
         */
        suite.addTestSuite(TestServiceRegistry.class);
        
        /*
         * GeoSpatial Service test cases
         */
        suite.addTestSuite(TestGeoSpatialServiceEvaluation.class);
        suite.addTestSuite(TestGeoSpatialServiceEvaluationQuads.class);
        suite.addTestSuite(TestGeoSpatialServiceConfiguration.class);
        
        return suite;
        
    }
    
}
