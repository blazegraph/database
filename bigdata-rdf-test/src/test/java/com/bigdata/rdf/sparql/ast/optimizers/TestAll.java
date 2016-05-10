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
package com.bigdata.rdf.sparql.ast.optimizers;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.bigdata.rdf.sparql.ast.QueryHints;

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
         * Test suite for converting from IValueExpressionNodes to
         * IValueExpressions. Value expressions which can be evaluated to a
         * constant are replaced by that constant during the conversion.
         */
        suite.addTestSuite(TestASTSetValueExpressionOptimizer.class);

        /*
         * Test suite for FILTER normalization optimizer
         */
        suite.addTestSuite(TestASTFilterNormalizationOptimizer.class);
        
        /*
         * Test suite for AST rewrite which replaces a variable bound to a
         * constant in an input solution with that constant.
         */
        suite.addTestSuite(TestASTStaticBindingsOptimizer.class);
        
        // Unit tests for binding query hints on the AST nodes.
        suite.addTestSuite(TestASTQueryHintOptimizer.class);

        // Unit tests for "SELECT (DISTINCT|REDUCED) *" 
        suite.addTestSuite(TestASTWildcardProjectionOptimizer.class);

        // Unit tests for AST rewrite of DESCRIBE into CONSTRUCT.
        suite.addTestSuite(TestASTDescribeOptimizer.class);

        // Unit tests for AST rewrite of the PROJECTION for a CONSTRUCT query.
        suite.addTestSuite(TestASTConstructOptimizer.class);

        // Unit tests for elimination of unnecessary join groups.
        suite.addTestSuite(TestASTEmptyGroupOptimizer.class);

        // Unit tests for flattening of UNIONs.
        suite.addTestSuite(TestASTFlattenUnionsOptimizer.class);

        // Unit tests for pushing down filters into the join groups of a UNION.
        suite.addTestSuite(TestASTUnionFiltersOptimizer.class);

        // Unit tests for rewrites of GRAPH ... { ... } patterns.
        suite.addTestSuite(TestASTGraphGroupOptimizer.class);

        // Unit tests for assigning join variables for sub-groups.
        suite.addTestSuite(TestASTSubGroupJoinVarOptimizer.class);

        // Unit tests for assigning join variables for named subquery includes.
        suite.addTestSuite(TestASTNamedSubqueryOptimizer.class);
        
        // Unit tests for assigning join variables for named solution sets includes.
        suite.addTestSuite(TestASTNamedSubqueryOptimizer2.class);
        
        // Unit tests for lifting of simple optionals into the parent group.
        suite.addTestSuite(TestASTSimpleOptionalOptimizer.class);

        // Unit tests for optimization of complex optionals.
        suite.addTestSuite(TestASTComplexOptionalOptimizer.class);

        // Unit tests for lifting pre-filters into the parent group.
        suite.addTestSuite(TestASTLiftPreFiltersOptimizer.class);

        // Unit tests for enforcing bottom-up evaluation semantics.
        suite.addTestSuite(TestASTBottomUpOptimizer.class);

        // Unit tests for fulltext search magic predicate rewrite
        suite.addTestSuite(TestASTFulltextSearchOptimizer.class);

        suite.addTestSuite(TestASTPropertyPathOptimizer.class);

        // Unit tests for the BIGDATA "SEARCH" service optimizer.
        suite.addTestSuite(TestASTSearchOptimizer.class);

        // Unit tests ServiceNode optimizer.
        suite.addTestSuite(TestASTServiceNodeOptimizer.class);

        // Unit tests for SPARQL 1.1 subquery optimizations.
        suite.addTestSuite(TestASTSparql11SubqueryOptimizer.class);

        // Unit tests for optimizer which pushes down sub-groups for hash joins.
        suite.addTestSuite(TestASTHashJoinOptimizer.class);

        // Unit tests for the runFirst/runLast query hints.
        suite.addTestSuite(TestASTRunFirstRunLastOptimizer.class);

        // Unit tests for static join ordering optimizer.
        suite.addTestSuite(TestASTStaticJoinOptimizer.class);

        // Unit tests for optimizer which attaches join filters to SPs.
        suite.addTestSuite(TestASTAttachJoinFiltersOptimizer.class);
        

        suite.addTestSuite(TestASTExistsAndJoinOrderByTypeOptimizers.class);

        // Unit tests for optimizer which attaches join filters to SPs.
        suite.addTestSuite(TestASTRangeOptimizer.class);

        // Test suite for resolving mock IVs.
        suite.addTestSuite(TestASTBatchResolveTermsOptimizer.class);
        
        suite.addTestSuite(TestASTFlattenJoinGroupsOptimizer.class);
        
        suite.addTestSuite(TestALPPinTrac773.class);
        
        // unit tests for the TestIVariableBindingRequirements interface
        suite.addTestSuite(TestIVariableBindingRequirements.class);
        
        // unit tests for ASTJoinGroupOrderOptimizer
        suite.addTestSuite(TestASTJoinGroupOrderOptimizer.class);
        
        // test partitioning utility classes
        suite.addTestSuite(TestASTJoinGroupPartitioning.class);
        
        /**
		 * Optimizes SELECT COUNT(*) { triple-pattern } using the fast range
		 * count mechanisms when that feature would produce exact results for
		 * the KB instance.
		 * 
		 * @see <a href="http://trac.blazegraph.com/ticket/1037" > Rewrite SELECT
		 *      COUNT(...) (DISTINCT|REDUCED) {single-triple-pattern} as ESTCARD
		 *      </a>
		 */
		if (QueryHints.DEFAULT_FAST_RANGE_COUNT_OPTIMIZER)
			suite.addTest(TestASTFastRangeCountOptimizer.suite());

		if (QueryHints.DEFAULT_DISTINCT_TERM_SCAN_OPTIMIZER)
			suite.addTest(TestASTDistinctTermScanOptimizer.suite());

                      
        suite.addTestSuite(TestASTOrderByAggregateFlatteningOptimizer.class);
               
                
        return suite;

    }

}
