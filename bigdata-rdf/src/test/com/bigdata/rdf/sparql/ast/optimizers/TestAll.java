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
package com.bigdata.rdf.sparql.ast.optimizers;

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

        final TestSuite suite = new TestSuite("AST Optimizers");

        /*
         * Test suite for AST rewrite which replaces a variable bound to a
         * constant in an input solution with that constant.
         */
        suite.addTestSuite(TestASTBindingAssigner.class);
        
        // Unit tests for AST rewrite of DESCRIBE into CONSTRUCT.
        suite.addTestSuite(TestDescribeOptimizer.class);

        // Unit tests for AST rewrite of the PROJECTION for a CONSTRUCT query.
        suite.addTestSuite(TestDescribeOptimizer.class);

        // Unit tests for elimination of unnecessary join groups.
        suite.addTestSuite(TestASTEmptyGroupOptimizer.class);
        
        // Unit tests for identifying badly designed left joins.
        suite.addTestSuite(TestBadlyDesignedLeftJoinUtil.class);
        
        return suite;

    }

}
