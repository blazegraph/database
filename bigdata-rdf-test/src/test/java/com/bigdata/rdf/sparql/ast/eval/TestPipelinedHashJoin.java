/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
import com.bigdata.bop.join.PipelinedHashIndexAndSolutionSetOp;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.QueryHints;

/**
 * Test suite for {@link PipelinedHashIndexAndSolutionSetOp}, which implements a
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

        assertPipelinedPlanOrNot(queryPlan, astContainer, true);

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

        assertPipelinedPlanOrNot(queryPlan, astContainer, true);

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

        assertPipelinedPlanOrNot(queryPlan, astContainer, true);

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

        assertPipelinedPlanOrNot(queryPlan, astContainer, true);

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

        assertPipelinedPlanOrNot(queryPlan, astContainer, true);

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

        assertPipelinedPlanOrNot(queryPlan, astContainer, true);

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

        assertPipelinedPlanOrNot(queryPlan, astContainer, true);

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

        assertPipelinedPlanOrNot(queryPlan, astContainer, true);

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
            queryPlan, astContainer, QueryHints.DEFAULT_PIPELINED_HASH_JOIN);

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
            queryPlan, astContainer, QueryHints.DEFAULT_PIPELINED_HASH_JOIN);

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
            queryPlan, astContainer, QueryHints.DEFAULT_PIPELINED_HASH_JOIN);

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
            queryPlan, astContainer, QueryHints.DEFAULT_PIPELINED_HASH_JOIN);

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
            queryPlan, astContainer, QueryHints.DEFAULT_PIPELINED_HASH_JOIN);

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
            queryPlan, astContainer, QueryHints.DEFAULT_PIPELINED_HASH_JOIN);

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
            queryPlan, astContainer, QueryHints.DEFAULT_PIPELINED_HASH_JOIN);

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
            queryPlan, astContainer, QueryHints.DEFAULT_PIPELINED_HASH_JOIN);

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
            queryPlan, astContainer, QueryHints.DEFAULT_PIPELINED_HASH_JOIN);

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

        assertPipelinedPlanOrNot(queryPlan, astContainer, true);

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

        assertPipelinedPlanOrNot(queryPlan, astContainer, true);

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

        assertPipelinedPlanOrNot(queryPlan, astContainer, true);

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

        assertPipelinedPlanOrNot(queryPlan, astContainer, true);

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

        assertPipelinedPlanOrNot(queryPlan, astContainer, true);

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

        assertPipelinedPlanOrNot(queryPlan, astContainer, true);

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

        assertPipelinedPlanOrNot(queryPlan, astContainer, true);

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
    
        assertPipelinedPlanOrNot(queryPlan, astContainer, false);
    
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
    
        assertPipelinedPlanOrNot(queryPlan, astContainer, false);
    
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
    
        assertPipelinedPlanOrNot(queryPlan, astContainer, false);
    
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
    
        assertPipelinedPlanOrNot(queryPlan, astContainer, false);
    
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
    
        assertPipelinedPlanOrNot(queryPlan, astContainer, false);
    
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
    
        assertPipelinedPlanOrNot(queryPlan, astContainer, false);
    
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
    
        assertPipelinedPlanOrNot(queryPlan, astContainer, false);
    
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

        assertPipelinedPlanOrNot(queryPlan, astContainer, true);

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

        assertPipelinedPlanOrNot(queryPlan, astContainer, true);

    }    
    
    /**
     * Asserts that a PipelinedHashIndexAndSolutionSetOp is contained in the
     * query plan if contains equals <code>true</code>, otherwise that it is
     * NOT contained.
     */
    protected void assertPipelinedPlanOrNot(final PipelineOp queryPlan, 
        final ASTContainer container, final boolean assertPipelined) {
        
        if (assertPipelined) {
            
            if (!BOpUtility.visitAll(queryPlan,
                    PipelinedHashIndexAndSolutionSetOp.class).hasNext()) {
                
                fail("Expecting PipelinedHashIndexAndSolutionSetOp in the plan: "
                        + container.toString());

            }
            
        } else {
            
            if (BOpUtility.visitAll(queryPlan,
                    PipelinedHashIndexAndSolutionSetOp.class).hasNext()) {
                
                fail("Expecting *no* PipelinedHashIndexAndSolutionSetOp in the plan: "
                        + container.toString());

            }
            
        }
    }

}