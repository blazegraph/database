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
 * Created on Oct 27, 2015
 */

package com.bigdata.rdf.sparql.ast.eval;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.join.HTreePipelinedHashJoinUtility;
import com.bigdata.bop.join.HashIndexOpBase.Annotations;
import com.bigdata.bop.join.JVMPipelinedHashJoinUtility;
import com.bigdata.bop.join.PipelinedHashIndexAndSolutionSetJoinOp;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.QueryHints;

/**
 * Test suite for {@link PipelinedHashIndexAndSolutionSetJoinOp}, which implements a
 * pipelined hash join.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class TestPipelinedHashJoin extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestPipelinedHashJoin() {
    }

    /**
     * @param name
     */
    public TestPipelinedHashJoin(String name) {
        super(name);
    }

    /**
     * Use pipelined hash join for OPTIONAL when LIMIT specified.
     */
    public void testPipelinedHashJoinUsedForOptional() throws Exception {

        final ASTContainer astContainer = 
            new TestHelper(
                "pipelined-hashjoin-used-optional",// testURI
                "pipelined-hashjoin-used-optional.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-optional.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, false);

    }

    /**
     * Use pipelined hash join for MINUS when LIMIT specified.
     */
    public void testPipelinedHashJoinUsedForMinus() throws Exception {

        final ASTContainer astContainer = 
            new TestHelper(
                "pipelined-hashjoin-used-minus",// testURI
                "pipelined-hashjoin-used-minus.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-minus.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, false);

    }

    /**
     * Use pipelined hash join for ALP "*" node.
     */
    public void testPipelinedHashJoinUsedForALP01() throws Exception {

        final ASTContainer astContainer = 
            new TestHelper(
                "pipelined-hashjoin-used-pp01",// testURI
                "pipelined-hashjoin-used-pp01.rq", // queryURI
                "pipelined-hashjoin-pp.trig", // dataURI
                "pipelined-hashjoin-pp01.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, false);

    }

    /**
     * Use pipelined hash join for ALP "+" node.
     */
    public void testPipelinedHashJoinUsedForALP02() throws Exception {

        final ASTContainer astContainer = 
            new TestHelper(
                "pipelined-hashjoin-used-pp02",// testURI
                "pipelined-hashjoin-used-pp02.rq", // queryURI
                "pipelined-hashjoin-pp.trig", // dataURI
                "pipelined-hashjoin-pp02.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, false);

    }    
    
    /**
     * Use pipelined hash join for SPARQL 1.1 subquery.
     */
    public void testPipelinedHashJoinUsedForSubquery() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-used-subquery",// testURI
                "pipelined-hashjoin-used-subquery.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-subquery.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, false);

    }
    

    /**
     * Use pipelined hash join for inlined VALUES node.
     */
    public void testPipelinedHashJoinUsedForValues() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-used-values",// testURI
                "pipelined-hashjoin-used-values.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-values.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, false);

    }

    /**
     * Do use pipelined hash join for EXISTS clause if LIMIT in query.
     */
    public void testPipelinedHashJoinUsedForExists() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-used-exists",// testURI
                "pipelined-hashjoin-used-exists.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-exists.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, false);

    }
    
    
    /**
     * Do use pipelined hash join for NOT EXISTS clause if LIMIT in query.
     */
    public void testPipelinedHashJoinUsedForNotExists() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-used-notexists",// testURI
                "pipelined-hashjoin-used-notexists.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-notexists.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, false);

    }
    
    /**
     * Use pipelined hash join for OPTIONAL when LIMIT specified
     * and analytic mode.
     */
    public void testPipelinedHashJoinUsedForOptionalAnalyticMode() throws Exception {

        final ASTContainer astContainer = 
            new TestHelper(
                "pipelined-hashjoin-used-optional-analytic",// testURI
                "pipelined-hashjoin-used-optional-analytic.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-optional.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, true);

    }

    /**
     * Use pipelined hash join for MINUS when LIMIT specified and analytic mode.
     */
    public void testPipelinedHashJoinUsedForMinusAnalyticMode() throws Exception {

        final ASTContainer astContainer = 
            new TestHelper(
                "pipelined-hashjoin-used-minus-analytic",// testURI
                "pipelined-hashjoin-used-minus-analytic.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-minus.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, true);

    }

    /**
     * Use pipelined hash join for ALP "*" node and analytic mode.
     */
    public void testPipelinedHashJoinUsedForALP01AnalyticMode() throws Exception {

        final ASTContainer astContainer = 
            new TestHelper(
                "pipelined-hashjoin-used-pp01-analytic",// testURI
                "pipelined-hashjoin-used-pp01-analytic.rq", // queryURI
                "pipelined-hashjoin-pp.trig", // dataURI
                "pipelined-hashjoin-pp01.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, true);

    }

    /**
     * Use pipelined hash join for ALP "+" node and analytic mode.
     */
    public void testPipelinedHashJoinUsedForALP02AnalyticMode() throws Exception {

        final ASTContainer astContainer = 
            new TestHelper(
                "pipelined-hashjoin-used-pp02-analytic",// testURI
                "pipelined-hashjoin-used-pp02-analytic.rq", // queryURI
                "pipelined-hashjoin-pp.trig", // dataURI
                "pipelined-hashjoin-pp02.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, true);

    }    
    
    /**
     * Use pipelined hash join for SPARQL 1.1 subquery and analytic mode.
     */
    public void testPipelinedHashJoinUsedForSubqueryAnalyticMode() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-used-subquery-analytic",// testURI
                "pipelined-hashjoin-used-subquery-analytic.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-subquery.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, true);

    }
  
    /**
     * Use pipelined hash join for inlined VALUES node and analytic mode.
     */
    public void testPipelinedHashJoinUsedForValuesAnalyticMode() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-used-values-analytic",// testURI
                "pipelined-hashjoin-used-values-analytic.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-values.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, true);

    }

    /**
     * Do use pipelined hash join for EXISTS clause if LIMIT in query
     * and analytic mode.
     */
    public void testPipelinedHashJoinUsedForExistsAnalyticMode() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-used-exists-analytic",// testURI
                "pipelined-hashjoin-used-exists-analytic.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-exists.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, true);

    }
    
    
    /**
     * Do use pipelined hash join for NOT EXISTS clause if LIMIT in query
     * and analytic mode.
     */
    public void testPipelinedHashJoinUsedForNotExistsAnalyticMode() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-used-notexists-analytic",// testURI
                "pipelined-hashjoin-used-notexists-analytic.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-notexists.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, true);

    }


    /**
     * Make sure the pipelined hash join operator is not used as a standard for
     * non-LIMIT query. Query body is the same as for
     * testPipelinedHashJoinUsedForOptional(), but no LIMIT.
     */
    public void testPipelinedHashJoinDefaultUsedOptional01() throws Exception {

        final ASTContainer astContainer = 
            new TestHelper(
                "pipelined-hashjoin-notused-optional01",// testURI
                "pipelined-hashjoin-notused-optional01.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-optional.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(
            queryPlan, astContainer, QueryHints.DEFAULT_PIPELINED_HASH_JOIN, false);

    }

    /**
     * Make sure the pipelined hash join operator is not used as a standard for
     * LIMIT+OFFSET queries. Query body is the same as for
     * testPipelinedHashJoinUsedForOptional(), but including LIMIT *and* ORDER
     * BY.
     */
    public void testPipelinedHashJoinDefaultUsedOptionald02() throws Exception {

        final ASTContainer astContainer = 
            new TestHelper(
                "pipelined-hashjoin-notused-optional02",// testURI
                "pipelined-hashjoin-notused-optional02.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-optional.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(
            queryPlan, astContainer, QueryHints.DEFAULT_PIPELINED_HASH_JOIN, false);

    }

    /**
     * Use pipelined hash join for MINUS is not specified when no LIMIT is
     * present in the query.
     */
    public void testPipelinedHashJoinDefaultUsedForMinus() throws Exception {

        final ASTContainer astContainer = 
            new TestHelper(
                "pipelined-hashjoin-notused-minus",// testURI
                "pipelined-hashjoin-notused-minus.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-minus.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(
            queryPlan, astContainer, QueryHints.DEFAULT_PIPELINED_HASH_JOIN, false);

    }
    
    /**
     * Do not use pipelined hash join for ALP "*" node if no LIMIT in query.
     */
    public void testPipelinedHashJoinDefaultUsedForALP01() throws Exception {

        final ASTContainer astContainer = 
            new TestHelper(
                "pipelined-hashjoin-notused-pp01",// testURI
                "pipelined-hashjoin-notused-pp01.rq", // queryURI
                "pipelined-hashjoin-pp.trig", // dataURI
                "pipelined-hashjoin-pp01.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(
            queryPlan, astContainer, QueryHints.DEFAULT_PIPELINED_HASH_JOIN, false);

    }
    
    /**
     * Do not use pipelined hash join for ALP "+" node if no LIMIT in query.
     */
    public void testPipelinedHashJoinDefaultUsedForALP02() throws Exception {

        final ASTContainer astContainer = 
            new TestHelper(
                "pipelined-hashjoin-notused-pp02",// testURI
                "pipelined-hashjoin-notused-pp02.rq", // queryURI
                "pipelined-hashjoin-pp.trig", // dataURI
                "pipelined-hashjoin-pp02.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(
            queryPlan, astContainer, QueryHints.DEFAULT_PIPELINED_HASH_JOIN, false);

    }
    
    /**
     * Do *not* use pipelined hash join for SPARQL 1.1 subquery if no LIMIT in query.
     */
    public void testPipelinedHashJoinDefaultUsedForSubquery() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-notused-subquery",// testURI
                "pipelined-hashjoin-notused-subquery.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-subquery.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(
            queryPlan, astContainer, QueryHints.DEFAULT_PIPELINED_HASH_JOIN, false);

    }

    /**
     * Do *not* use pipelined hash join for VALUES clause if no LIMIT in query.
     */
    public void testPipelinedHashJoinDefaultUsedForValues() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-notused-values",// testURI
                "pipelined-hashjoin-notused-values.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-values.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(
            queryPlan, astContainer, QueryHints.DEFAULT_PIPELINED_HASH_JOIN, false);

    }
    
    /**
     * Do *not* use pipelined hash join for EXISTS clause if no LIMIT in query.
     */
    public void testPipelinedHashJoinDefaultUsedForExists() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-notused-exists",// testURI
                "pipelined-hashjoin-notused-exists.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-exists.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(
            queryPlan, astContainer, QueryHints.DEFAULT_PIPELINED_HASH_JOIN, false);

    }
    
    /**
     * Do *not* use pipelined hash join for NOT EXISTS clause if no LIMIT in query.
     */
    public void testPipelinedHashJoinDefaultUsedForNotExists() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-notused-notexists",// testURI
                "pipelined-hashjoin-notused-notexists.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-notexists.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(
            queryPlan, astContainer, QueryHints.DEFAULT_PIPELINED_HASH_JOIN, false);

    }
    
    
    /**
     * Make sure the pipelined hash join operator is not used as a standard for
     * non-LIMIT query. Query body is the same as for
     * testPipelinedHashJoinUsedForOptional(), but no LIMIT.
     * Verifies exemplarily for analytic mode.
     */
    public void testPipelinedHashJoinDefaultUsedOptional01Analytic() throws Exception {

        final ASTContainer astContainer = 
            new TestHelper(
                "pipelined-hashjoin-notused-optional01-analytic",// testURI
                "pipelined-hashjoin-notused-optional01-analytic.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-optional.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(
            queryPlan, astContainer, QueryHints.DEFAULT_PIPELINED_HASH_JOIN, true);

    }

    /**
     * Combination of OPTIONAL and enablement by query hint.
     */
    public void testPipelinedHashEnabledByQueryHintOptional() throws Exception {

        final ASTContainer astContainer = 
            new TestHelper(
                "pipelined-hashjoin-used-optional-hint",// testURI
                "pipelined-hashjoin-used-optional-hint.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-optional.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, false);

    }

    
    /**
     * Combination of MINUS and enablement by query hint.
     */
    public void testPipelinedHashEnabledByQueryHintMinus() throws Exception {

        final ASTContainer astContainer = 
                new TestHelper(
                    "pipelined-hashjoin-used-minus-hint",// testURI
                    "pipelined-hashjoin-used-minus-hint.rq", // queryURI
                    "pipelined-hashjoin.trig", // dataURI
                    "pipelined-hashjoin-minus.srx" // resultURI
            ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, false);

    }
    
    /**
     * Combination of ALP and enablement by query hint.
     */
    public void testPipelinedHashEnabledByQueryHintALP() throws Exception {

        final ASTContainer astContainer = 
                new TestHelper(
                    "pipelined-hashjoin-used-pp01-hint",// testURI
                    "pipelined-hashjoin-used-pp01-hint.rq", // queryURI
                    "pipelined-hashjoin-pp.trig", // dataURI
                    "pipelined-hashjoin-pp01.srx" // resultURI
            ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, false);

    }
    
    /**
     * Combination of subquery and enablement by query hint.
     */
    public void testPipelinedHashEnabledByQueryHintSubquery() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-used-subquery-hint",// testURI
                "pipelined-hashjoin-used-subquery-hint.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-subquery.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, false);

    }
    
    /**
     * Combination of VALUES and enablement by query hint.
     */
    public void testPipelinedHashEnabledByQueryHintValues() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-used-values-hint",// testURI
                "pipelined-hashjoin-used-values-hint.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-values.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, false);

    }
    
    /**
     * Combination of EXISTS and enablement by query hint.
     */
    public void testPipelinedHashEnabledByQueryHintExists() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-used-exists-hint",// testURI
                "pipelined-hashjoin-used-exists-hint.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-exists.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, false);

    }
    
    /**
     * Combination of NOT EXISTS and enablement by query hint.
     */
    public void testPipelinedHashEnabledByQueryHintNotExists() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-used-notexists-hint",// testURI
                "pipelined-hashjoin-used-notexists-hint.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-notexists.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, false);

    }
    
    /**
     * Combination of OPTIONAL and enablement by query hint and analytic mode.
     */
    public void testPipelinedHashEnabledByQueryHintOptionalAnalyticMode() throws Exception {

        final ASTContainer astContainer = 
            new TestHelper(
                "pipelined-hashjoin-used-optional-hint-analytic",// testURI
                "pipelined-hashjoin-used-optional-hint-analytic.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-optional.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, true);

    }

    /**
     * Combination of OPTIONAL and enablement by query hint.
     */
    public void testPipelinedHashDisabledByQueryHintOptional() throws Exception {
    
        final ASTContainer astContainer = 
            new TestHelper(
                "pipelined-hashjoin-used-optional-hintneg",// testURI
                "pipelined-hashjoin-used-optional-hintneg.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-optional.srx" // resultURI
        ).runTest();
    
        final PipelineOp queryPlan = astContainer.getQueryPlan();
    
        assertPipelinedPlanOrNot(queryPlan, astContainer, false, false);
    
    }

    /**
     * Combination of MINUS and enablement by query hint.
     */
    public void testPipelinedHashDisabledByQueryHintMinus() throws Exception {
    
        final ASTContainer astContainer = 
                new TestHelper(
                    "pipelined-hashjoin-used-minus-hintneg",// testURI
                    "pipelined-hashjoin-used-minus-hintneg.rq", // queryURI
                    "pipelined-hashjoin.trig", // dataURI
                    "pipelined-hashjoin-minus.srx" // resultURI
            ).runTest();
    
        final PipelineOp queryPlan = astContainer.getQueryPlan();
    
        assertPipelinedPlanOrNot(queryPlan, astContainer, false, false);
    
    }

    /**
     * Combination of ALP and enablement by query hint.
     */
    public void testPipelinedHashDisabledByQueryHintALP() throws Exception {
    
        final ASTContainer astContainer = 
                new TestHelper(
                    "pipelined-hashjoin-used-pp01-hintneg",// testURI
                    "pipelined-hashjoin-used-pp01-hintneg.rq", // queryURI
                    "pipelined-hashjoin-pp.trig", // dataURI
                    "pipelined-hashjoin-pp01.srx" // resultURI
            ).runTest();
    
        final PipelineOp queryPlan = astContainer.getQueryPlan();
    
        assertPipelinedPlanOrNot(queryPlan, astContainer, false, false);
    
    }

    /**
     * Combination of subquery and enablement by query hint.
     */
    public void testPipelinedHashDisabledByQueryHintSubquery() throws Exception {
    
        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-used-subquery-hintneg",// testURI
                "pipelined-hashjoin-used-subquery-hintneg.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-subquery.srx" // resultURI
        ).runTest();
    
        final PipelineOp queryPlan = astContainer.getQueryPlan();
    
        assertPipelinedPlanOrNot(queryPlan, astContainer, false, false);
    
    }

    /**
     * Combination of VALUES and enablement by query hint.
     */
    public void testPipelinedHashDisabledByQueryHintValues() throws Exception {
    
        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-used-values-hintneg",// testURI
                "pipelined-hashjoin-used-values-hintneg.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-values.srx" // resultURI
        ).runTest();
    
        final PipelineOp queryPlan = astContainer.getQueryPlan();
    
        assertPipelinedPlanOrNot(queryPlan, astContainer, false, false);
    
    }

    /**
     * Combination of EXISTS and enablement by query hint.
     */
    public void testPipelinedHashDisabledByQueryHintExists() throws Exception {
    
        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-used-exists-hintneg",// testURI
                "pipelined-hashjoin-used-exists-hintneg.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-exists.srx" // resultURI
        ).runTest();
    
        final PipelineOp queryPlan = astContainer.getQueryPlan();
    
        assertPipelinedPlanOrNot(queryPlan, astContainer, false, false);
    
    }

    /**
     * Combination of NOT EXISTS and enablement by query hint.
     */
    public void testPipelinedHashDisabledByQueryHintNotExists() throws Exception {
    
        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-used-notexists-hintneg",// testURI
                "pipelined-hashjoin-used-notexists-hintneg.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-notexists.srx" // resultURI
        ).runTest();
    
        final PipelineOp queryPlan = astContainer.getQueryPlan();
    
        assertPipelinedPlanOrNot(queryPlan, astContainer, false, false);
    
    }
    
    /**
     * Combination of OPTIONAL and enablement by query hint and analytic mode.
     */
    public void testPipelinedHashDisabledByQueryHintOptionalAnalyticMode() throws Exception {
    
        final ASTContainer astContainer = 
            new TestHelper(
                "pipelined-hashjoin-used-optional-hintneg-analytic",// testURI
                "pipelined-hashjoin-used-optional-hintneg-analytic.rq", // queryURI
                "pipelined-hashjoin.trig", // dataURI
                "pipelined-hashjoin-optional.srx" // resultURI
        ).runTest();
    
        final PipelineOp queryPlan = astContainer.getQueryPlan();
    
        assertPipelinedPlanOrNot(queryPlan, astContainer, false, true);
    
    }

    /**
     * Test query affected by 
     * PipelinedHashIndexAndSolutionSetOp.INCOMING_BINDINGS_BUFFER_THRESHOLD.
     */
    public void testPipelinedHashIncomingBindingsBufferThreshold() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-threshold-incoming-bindings-buffer",// testURI
                "pipelined-hashjoin-threshold-incoming-bindings-buffer.rq", // queryURI
                "pipelined-hashjoin-threshold.trig", // dataURI
                "pipelined-hashjoin-threshold-incoming-bindings-buffer.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, false);

    }
    
    /**
     * Test query affected by 
     * PipelinedHashIndexAndSolutionSetOp.DISTINCT_PROJECTION_BUFFER_THRESHOLD
     */
    public void testPipelinedHashDistinctProjectionBufferThreshold() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-threshold-distinct-projection-buffer",// testURI
                "pipelined-hashjoin-threshold-distinct-projection-buffer.rq", // queryURI
                "pipelined-hashjoin-threshold.trig", // dataURI
                "pipelined-hashjoin-threshold-distinct-projection-buffer.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, false);

    }    
    
    /**
     * Test query affected by 
     * PipelinedHashIndexAndSolutionSetOp.INCOMING_BINDINGS_BUFFER_THRESHOLD.
     */
    public void testPipelinedHashIncomingBindingsBufferThresholdAnalyticMode() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-threshold-incoming-bindings-buffer-analytic",// testURI
                "pipelined-hashjoin-threshold-incoming-bindings-buffer-analytic.rq", // queryURI
                "pipelined-hashjoin-threshold.trig", // dataURI
                "pipelined-hashjoin-threshold-incoming-bindings-buffer.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, true);

    }
    
    /**
     * Test query affected by 
     * PipelinedHashIndexAndSolutionSetOp.DISTINCT_PROJECTION_BUFFER_THRESHOLD
     */
    public void testPipelinedHashDistinctProjectionBufferThresholdAnalyticMode() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-threshold-distinct-projection-buffer-analytic",// testURI
                "pipelined-hashjoin-threshold-distinct-projection-buffer-analytic.rq", // queryURI
                "pipelined-hashjoin-threshold.trig", // dataURI
                "pipelined-hashjoin-threshold-distinct-projection-buffer.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, true);

    }
    
    /**
     * Check correct multiplicity for EXISTS.
     */
    public void testPipelinedHashJoinExistsMultiplicity() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-exists-multiplicity",// testURI
                "pipelined-hashjoin-exists-multiplicity.rq", // queryURI
                "pipelined-hashjoin-multiplicity.trig", // dataURI
                "pipelined-hashjoin-exists-multiplicity.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, false);

    }
    
    /**
     * Check correct multiplicity for EXISTS in analytic mode.
     */
    public void testPipelinedHashJoinExistsMultiplicityAnalyticMode() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-exists-multiplicity-analytic",// testURI
                "pipelined-hashjoin-exists-multiplicity-analytic.rq", // queryURI
                "pipelined-hashjoin-multiplicity.trig", // dataURI
                "pipelined-hashjoin-exists-multiplicity.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, true);

    }
    
    /**
     * Check correct multiplicity for NOT EXISTS.
     */
    public void testPipelinedHashJoinNotExistsMultiplicity() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-notexists-multiplicity",// testURI
                "pipelined-hashjoin-notexists-multiplicity.rq", // queryURI
                "pipelined-hashjoin-multiplicity.trig", // dataURI
                "pipelined-hashjoin-notexists-multiplicity.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, false);

    }
    
    /**
     * Check correct multiplicity for NOT EXISTS in analytic mode.
     */
    public void testPipelinedHashJoinNotExistsMultiplicityAnalyticMode() throws Exception {

        final ASTContainer astContainer = new TestHelper(
                "pipelined-hashjoin-notexists-multiplicity-analytic",// testURI
                "pipelined-hashjoin-notexists-multiplicity-analytic.rq", // queryURI
                "pipelined-hashjoin-multiplicity.trig", // dataURI
                "pipelined-hashjoin-notexists-multiplicity.srx" // resultURI
        ).runTest();

        final PipelineOp queryPlan = astContainer.getQueryPlan();

        assertPipelinedPlanOrNot(queryPlan, astContainer, true, true);

    }    
    
    /**
     * Asserts that a PipelinedHashIndexAndSolutionSetOp is contained in the
     * query plan if contains equals <code>true</code>, otherwise that it is
     * NOT contained.
     */
    protected void assertPipelinedPlanOrNot(final PipelineOp queryPlan, 
        final ASTContainer container, final boolean assertPipelined,
        final boolean analyticMode) {
        
        final Class<?> operatorClass = PipelinedHashIndexAndSolutionSetJoinOp.class;
        
        if (assertPipelined) {

            // verify the operator class is correctly set
            if (!BOpUtility.visitAll(queryPlan, operatorClass).hasNext()) {
                
                fail("Expecting " + operatorClass + " in the plan: "
                        + container.toString());

            }

            // verify that we're properly using the analytical vs. JVM version of the class
            final Class<?> expectedUtilClass = analyticMode ?
                    HTreePipelinedHashJoinUtility.class : JVMPipelinedHashJoinUtility.class;

            final PipelinedHashIndexAndSolutionSetJoinOp firstMatch = 
                (PipelinedHashIndexAndSolutionSetJoinOp)BOpUtility.visitAll(queryPlan, operatorClass).next();
            
            final Object utilFactory = firstMatch.annotations().get(Annotations.HASH_JOIN_UTILITY_FACTORY);
            final Class<?> observedUtilClass = utilFactory.getClass();
            
            /**
             * The string checking below is a workaround for dealing with the anonymous
             * inner classes, what we have here is something lie
             * 
             * observedUtilClass.toString() := class com.bigdata.bop.join.JVMPipelinedHashJoinUtility$1
             * expectedUtilClass.toString() := class com.bigdata.bop.join.JVMPipelinedHashJoinUtility
             */
            if (!observedUtilClass.toString().contains(expectedUtilClass.toString())) {
                fail("Expecting util factory of type " + expectedUtilClass + ", but used one is " + utilFactory.getClass());
            }
                        
        } else {
            
            if (BOpUtility.visitAll(queryPlan, operatorClass).hasNext()) {
                
                fail("Expecting *no* " + operatorClass + " in the plan: "
                        + container.toString());

            }
            
        }
    }

}
