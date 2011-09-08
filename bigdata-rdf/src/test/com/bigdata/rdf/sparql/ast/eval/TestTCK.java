/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
/*
 * Created on Sep 7, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import com.bigdata.bop.IConstraint;
import com.bigdata.rdf.internal.constraints.SPARQLConstraint;
import com.bigdata.rdf.internal.constraints.SparqlTypeErrorBOp;

/**
 * Test driver for debugging Sesame or DAWG manifest tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTCK extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestTCK() {
    }

    /**
     * @param name
     */
    public TestTCK(String name) {
        super(name);
    }

    /**
     * This is not a DAWG test. There is no data for the aggregation. Sesame is
     * expecting a solution set consisting of a single solution with
     * 0^^xsd:integer. We are producing an empty solution set.
     */
    public void test_sparql11_sum_02() throws Exception {

        new TestHelper(
                "sparql11-sum-02", // testURI,
                "sparql11-sum-02.rq",// queryFileURL
                "sparql11-sum-02.ttl",// dataFileURL
                "sparql11-sum-02.srx"// resultFileURL
                ).runTest();

    }

    /**
     * This is not a DAWG test. There is no data for the aggregation. Sesame is
     * expecting a solution set consisting of a single solution with
     * 0^^xsd:integer. We are producing an empty solution set.
     */
    public void test_sparql11_count_03() throws Exception {

        new TestHelper(
                "sparql11-count-03", // testURI,
                "sparql11-count-03.rq",// queryFileURL
                "sparql11-count-03.ttl",// dataFileURL
                "sparql11-count-03.srx"// resultFileURL
                ).runTest();

    }

    /**
     * This is not a DAWG test. We produce 10^^xsd:decimal while openrdf expects
     * 10.0^^xsd:decimal.
     */
    public void test_sparql11_sum_04() throws Exception {

        new TestHelper(
                "sparql11-sum-04", // testURI,
                "sparql11-sum-04.rq",// queryFileURL
                "sparql11-sum-04.ttl",// dataFileURL
                "sparql11-sum-04.srx"// resultFileURL
                ).runTest();

    }

    /**
     * I can not figure out why these "dataset" tests fail.
     */
    public void test_dataset_01() throws Exception {

        new TestHelper(
                "dataset-01", // testURI,
                "dataset-01.rq",// queryFileURL
                "data-g1.ttl",  // dataFileURL
                "dataset-01.ttl"// resultFileURL
                ).runTest();

    }

    /**
     * Test effective boolean value - optional.
     * 
     * <pre>
     * PREFIX  xsd: <http://www.w3.org/2001/XMLSchema#>
     * PREFIX  : <http://example.org/ns#>
     * SELECT  ?a
     * WHERE
     *     { ?a :p ?v . 
     *       OPTIONAL
     *         { ?a :q ?w } . 
     *       FILTER (?w) .
     *     }
     * 
     * PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
     * PREFIX : <http://example.org/ns#>
     * QueryType: SELECT
     * SELECT VarNode(a)
     *   JoinGroupNode {
     *     StatementPatternNode(VarNode(a), ConstantNode(TermId(1U)), VarNode(v), DEFAULT_CONTEXTS)
     *     JoinGroupNode[optional] {
     *       StatementPatternNode(VarNode(a), ConstantNode(TermId(2U)), VarNode(w), DEFAULT_CONTEXTS)
     *     }
     *     FILTER( VarNode(w) )
     *   }
     * INFO : 2984      main com.bigdata.rdf.sparql.ast.AST2BOpUtility.convert(AST2BOpUtility.java:359): pipeline:
     * com.bigdata.bop.bset.EndOp[6](ConditionalRoutingOp[5])[ com.bigdata.bop.BOp.bopId=6, com.bigdata.bop.BOp.evaluationContext=CONTROLLER]
     *   com.bigdata.bop.bset.ConditionalRoutingOp[5](PipelineJoin[4])[ com.bigdata.bop.BOp.bopId=5, com.bigdata.bop.bset.ConditionalRoutingOp.condition=com.bigdata.rdf.internal.constraints.SPARQLConstraint(com.bigdata.rdf.internal.constraints.EBVBOp(w))]
     * ==> Missing a materialization step here.
     *     com.bigdata.bop.join.PipelineJoin[4](PipelineJoin[3])[ com.bigdata.bop.BOp.bopId=4, com.bigdata.rdf.sail.Rule2BOpUtility.cost.scan=com.bigdata.bop.cost.ScanCostReport@8b677f{rangeCount=3,shardCount=1,cost=0.0}, com.bigdata.rdf.sail.Rule2BOpUtility.cost.subquery=null, com.bigdata.bop.BOp.evaluationContext=ANY, com.bigdata.bop.join.PipelineJoin.predicate=com.bigdata.rdf.spo.SPOPredicate(a=null, TermId(2U), w=null, ebffdb58-9c9e-40cb-9c04-0be4b74fe615=null)[ com.bigdata.bop.IPredicate.relationName=[kb.spo], com.bigdata.bop.IPredicate.timestamp=0, com.bigdata.bop.IPredicate.flags=[KEYS,VALS,READONLY,PARALLEL], com.bigdata.bop.IPredicate.optional=true, com.bigdata.bop.IPredicate.accessPathFilter=cutthecrap.utils.striterators.NOPFilter@37d490{annotations=null,filterChain=[com.bigdata.bop.rdf.filter.StripContextFilter(), com.bigdata.bop.ap.filter.DistinctFilter()]}]]
     *       com.bigdata.bop.join.PipelineJoin[3](StartOp[1])[ com.bigdata.bop.BOp.bopId=3, com.bigdata.rdf.sail.Rule2BOpUtility.cost.scan=com.bigdata.bop.cost.ScanCostReport@1647278{rangeCount=8,shardCount=1,cost=0.0}, com.bigdata.rdf.sail.Rule2BOpUtility.cost.subquery=null, com.bigdata.bop.BOp.evaluationContext=ANY, com.bigdata.bop.join.PipelineJoin.predicate=com.bigdata.rdf.spo.SPOPredicate[2](a=null, TermId(1U), v=null, 4b7d02c5-3a72-47ed-b57c-f2fb5852ea2f=null)[ com.bigdata.bop.IPredicate.relationName=[kb.spo], com.bigdata.bop.IPredicate.timestamp=0, com.bigdata.bop.IPredicate.flags=[KEYS,VALS,READONLY,PARALLEL], com.bigdata.bop.BOp.bopId=2, com.bigdata.rdf.sail.Rule2BOpUtility.originalIndex=POCS, com.bigdata.rdf.sail.Rule2BOpUtility.estimatedCardinality=8, com.bigdata.bop.IPredicate.accessPathFilter=cutthecrap.utils.striterators.NOPFilter@1972e3a{annotations=null,filterChain=[com.bigdata.bop.rdf.filter.StripContextFilter(), com.bigdata.bop.ap.filter.DistinctFilter()]}]]
     *         com.bigdata.bop.bset.StartOp[1]()[ com.bigdata.bop.BOp.bopId=1, com.bigdata.bop.BOp.evaluationContext=CONTROLLER]
     * </pre>
     * AST2BOpUtility addConditionals() should be inserting a materialization
     * step for this.  Check the post-filters.
     */
    public void test_sparql_bev_5() throws Exception {

        new TestHelper(
                "bev-5", // testURI,
                "bev-5.rq",// queryFileURL
                "bev-5.ttl",// dataFileURL
                "bev-5-result.ttl"// resultFileURL
                ).runTest();

    }

    /**
     * <code>Nested Optionals - 1</code>. Classic badly designed left join.
     */
    public void test_two_nested_opt() throws Exception {

        new TestHelper(
                "two-nested-opt", // testURI,
                "two-nested-opt.rq",// queryFileURL
                "two-nested-opt.ttl",// dataFileURL
                "two-nested-opt.srx"// resultFileURL
                ).runTest();

    }

//    public void test_filter_nested_1() throws Exception {
//
//        new TestHelper(
//                "filter-nested-1", // testURI,
//                "filter-nested-1.rq",// queryFileURL
//                "filter-nested-1.ttl",// dataFileURL 
//                "filter-nested-1.srx"// resultFileURL
//                ).runTest();
//
//    }

    /**
     * This was handled historically by
     * 
     * <pre>
     * 
     * If the scope binding names are empty we can definitely
     * always fail the filter (since the filter's variables
     * cannot be bound).
     * 
     *                 if (filter.getBindingNames().isEmpty()) {
     *                     final IConstraint bop = new SPARQLConstraint(SparqlTypeErrorBOp.INSTANCE);
     *                     sop.setBOp(bop);
     * </pre>
     * 
     * We need to figure out the variables that are in scope when the filter is
     * evaluated and then filter them out when running the group in which the
     * filter exists (it runs as a subquery). If there are NO variables that are
     * in scope, then just fail the filter per the code above.
     * 
     * @throws Exception
     */
    public void test_filter_nested_2() throws Exception {

        new TestHelper(
                "filter-nested-2", // testURI,
                "filter-nested-2.rq",// queryFileURL
                "filter-nested-2.ttl",// dataFileURL 
                "filter-nested-2.srx"// resultFileURL
                ).runTest();

    }

    /**
     * This will be fixed by the same issue as {@link #test_filter_nested_2()}.
     */
    public void test_filter_scope_1() throws Exception {

        new TestHelper(
                "filter-scope-1", // testURI,
                "filter-scope-1.rq",// queryFileURL
                "filter-scope-1.ttl",// dataFileURL 
                "filter-scope-1.srx"// resultFileURL
                ).runTest();

    }

    /**
     * Classic badly designed left join.
     */
    public void test_var_scope_join_1() throws Exception {

        new TestHelper(
                "var-scope-join-1", // testURI,
                "var-scope-join-1.rq",// queryFileURL
                "var-scope-join-1.ttl",// dataFileURL
                "var-scope-join-1.srx"// resultFileURL
                ).runTest();

    }

}
