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
package com.bigdata.rdf.sparql.ast;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates test suites into increasing dependency order.
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

        final TestSuite suite = new TestSuite("AST");

        // Test suite for AST manipulation (mainly for use by hand).
        suite.addTestSuite(TestAST.class);

        // Test suite for methods which determine must/may/not bound vars.
        suite.addTestSuite(TestStaticAnalysis.class);

        // Test suite for canJoin() and canJoinUsingConstraints().
        suite.addTestSuite(TestStaticAnalysis_CanJoin.class);

        // Test suite for AST query rewriters/optimizers.
        suite.addTest(com.bigdata.rdf.sparql.ast.optimizers.TestAll.suite());

        // Test suite for AST evaluation helpers.
        suite.addTest(com.bigdata.rdf.sparql.ast.eval.TestAll.suite());

        // Unit tests for query evaluation written to the AST layer.
        suite.addTestSuite(TestASTTriplesModeEvaluation.class);

        // Unit tests for named solution set references.
        suite.addTestSuite(TestNamedSolutionSetRef.class);
        
        return suite;
        
    }
    
}
