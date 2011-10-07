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
 * Created on Sep 4, 2011
 */

package com.bigdata.rdf.sparql.ast.eval.bsbm.bi;

import com.bigdata.rdf.sparql.ast.eval.AbstractDataDrivenSPARQLTestCase;
import com.bigdata.rdf.sparql.ast.optimizers.ASTFlattenUnionsOptimizer;

/**
 * Data driven test suite for complex queries.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Verify against ground truth results.
 */
public class TestBsbmBIQuery extends AbstractDataDrivenSPARQLTestCase {

    /**
     * 
     */
    public TestBsbmBIQuery() {
    }

    /**
     * @param name
     */
    public TestBsbmBIQuery(String name) {
        super(name);
    }

    /**
     * PC 100 data set. This is the data set which was used to generate the
     * concrete instances of the queries referenced from within this class.
     */
     static private final String dataset = "bsbm/bsbm3_dataset_pc10.nt";

    /**
     * An empty data set. This may be used if you are simply examining the query
     * plans.
     */
//    static private final String dataset = "bsbm/emptyDataset.nt";

    public void test_bsbm_bi_query1() throws Exception {

        new TestHelper("query1", // name
                "bsbm/bi/query1.rq",// query
                dataset,//
                "bsbm/bi/empty.srx"// result
        ).runTest();

    }

    public void test_bsbm_bi_query2() throws Exception {

        new TestHelper("query2", // name
                "bsbm/bi/query2.rq",// query
                dataset,//
                "bsbm/bi/empty.srx"// result
        ).runTest();

    }

    public void test_bsbm_bi_query3() throws Exception {

        new TestHelper("query3", // name
                "bsbm/bi/query3.rq",// query
                dataset,//
                "bsbm/bi/empty.srx"// result
        ).runTest();

    }

    public void test_bsbm_bi_query4() throws Exception {

        new TestHelper("query4", // name
                "bsbm/bi/query4.rq",// query
                dataset,//
                "bsbm/bi/empty.srx"// result
        ).runTest();

    }

    public void test_bsbm_bi_query5() throws Exception {

        new TestHelper("query5", // name
                "bsbm/bi/query5.rq",// query
                dataset,//
                "bsbm/bi/empty.srx"// result
        ).runTest();

    }

    /**
     * <pre>
     * prefix bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
     * prefix bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
     * prefix rev: <http://purl.org/stuff/rev#>
     * prefix xsd: <http://www.w3.org/2001/XMLSchema#>
     * 
     * Select ?reviewer (avg(xsd:float(?score)) As ?reviewerAvgScore)
     * {
     *   { Select (avg(xsd:float(?score)) As ?avgScore)
     *     {
     *       ?product bsbm:producer <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Producer1> .
     *       ?review bsbm:reviewFor ?product .
     *       { ?review bsbm:rating1 ?score . } UNION
     *       { ?review bsbm:rating2 ?score . } UNION
     *       { ?review bsbm:rating3 ?score . } UNION
     *       { ?review bsbm:rating4 ?score . }
     *     }
     *   }
     *   ?product bsbm:producer <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Producer1> .
     *   ?review bsbm:reviewFor ?product .
     *   ?review rev:reviewer ?reviewer .
     *   { ?review bsbm:rating1 ?score . } UNION
     *   { ?review bsbm:rating2 ?score . } UNION
     *   { ?review bsbm:rating3 ?score . } UNION
     *   { ?review bsbm:rating4 ?score . }
     * }
     * Group By ?reviewer
     * Having (avg(xsd:float(?score)) > min(?avgScore) * 1.5)
     * </pre>
     * 
     * <pre>
     * PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
     * PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
     * PREFIX rev: <http://purl.org/stuff/rev#>
     * PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
     * QueryType: SELECT
     * SELECT ( VarNode(reviewer) AS VarNode(reviewer) ) ( com.bigdata.rdf.sparql.ast.FunctionNode(FunctionNode(com.bigdata.rdf.internal.constraints.FuncBOp(score)[ com.bigdata.rdf.internal.constraints.FuncBOp.namespace=kb.lex, com.bigdata.rdf.internal.constraints.FuncBOp.function=http://www.w3.org/2001/XMLSchema#float]))[ com.bigdata.rdf.sparql.ast.FunctionNode.functionURI=http://www.w3.org/2006/sparql-functions#average, valueExpr=com.bigdata.bop.rdf.aggregate.AVERAGE(com.bigdata.rdf.internal.constraints.FuncBOp(score)[ com.bigdata.rdf.internal.constraints.FuncBOp.namespace=kb.lex, com.bigdata.rdf.internal.constraints.FuncBOp.function=http://www.w3.org/2001/XMLSchema#float])] AS VarNode(reviewerAvgScore) )
     *   JoinGroupNode {
     *     JoinGroupNode {
     *       QueryType: SELECT
     *       SELECT ( com.bigdata.rdf.sparql.ast.FunctionNode(FunctionNode(com.bigdata.rdf.internal.constraints.FuncBOp(score)[ com.bigdata.rdf.internal.constraints.FuncBOp.namespace=kb.lex, com.bigdata.rdf.internal.constraints.FuncBOp.function=http://www.w3.org/2001/XMLSchema#float]))[ com.bigdata.rdf.sparql.ast.FunctionNode.functionURI=http://www.w3.org/2006/sparql-functions#average, valueExpr=com.bigdata.bop.rdf.aggregate.AVERAGE(com.bigdata.rdf.internal.constraints.FuncBOp(score)[ com.bigdata.rdf.internal.constraints.FuncBOp.namespace=kb.lex, com.bigdata.rdf.internal.constraints.FuncBOp.function=http://www.w3.org/2001/XMLSchema#float])] AS VarNode(avgScore) )
     *         JoinGroupNode {
     *           StatementPatternNode(VarNode(product), ConstantNode(TermId(695U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/producer]), ConstantNode(TermId(675U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Producer1]), DEFAULT_CONTEXTS)
     *           StatementPatternNode(VarNode(review), ConstantNode(TermId(1500U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor]), VarNode(product), DEFAULT_CONTEXTS)
     *           UnionNode {
     *             JoinGroupNode {
     *               StatementPatternNode(VarNode(review), ConstantNode(TermId(1523U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating1]), VarNode(score), DEFAULT_CONTEXTS)
     *             }
     *             JoinGroupNode {
     *               StatementPatternNode(VarNode(review), ConstantNode(TermId(1497U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2]), VarNode(score), DEFAULT_CONTEXTS)
     *             }
     *             JoinGroupNode {
     *               StatementPatternNode(VarNode(review), ConstantNode(TermId(1498U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating3]), VarNode(score), DEFAULT_CONTEXTS)
     *             }
     *             JoinGroupNode {
     *               StatementPatternNode(VarNode(review), ConstantNode(TermId(1524U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating4]), VarNode(score), DEFAULT_CONTEXTS)
     *             }
     *           }
     *         }
     *     }
     *     StatementPatternNode(VarNode(product), ConstantNode(TermId(695U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/producer]), ConstantNode(TermId(675U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Producer1]), DEFAULT_CONTEXTS)
     *     StatementPatternNode(VarNode(review), ConstantNode(TermId(1500U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor]), VarNode(product), DEFAULT_CONTEXTS)
     *     StatementPatternNode(VarNode(review), ConstantNode(TermId(1475U)[http://purl.org/stuff/rev#reviewer]), VarNode(reviewer), DEFAULT_CONTEXTS)
     *     UnionNode {
     *       JoinGroupNode {
     *         StatementPatternNode(VarNode(review), ConstantNode(TermId(1523U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating1]), VarNode(score), DEFAULT_CONTEXTS)
     *       }
     *       JoinGroupNode {
     *         StatementPatternNode(VarNode(review), ConstantNode(TermId(1497U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2]), VarNode(score), DEFAULT_CONTEXTS)
     *       }
     *       JoinGroupNode {
     *         StatementPatternNode(VarNode(review), ConstantNode(TermId(1498U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating3]), VarNode(score), DEFAULT_CONTEXTS)
     *       }
     *       JoinGroupNode {
     *         StatementPatternNode(VarNode(review), ConstantNode(TermId(1524U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating4]), VarNode(score), DEFAULT_CONTEXTS)
     *       }
     *     }
     *   }
     * group by ( VarNode(reviewer) AS VarNode(reviewer) )
     * having com.bigdata.rdf.sparql.ast.FunctionNode(FunctionNode(com.bigdata.bop.rdf.aggregate.AVERAGE(com.bigdata.rdf.internal.constraints.FuncBOp(score)[ com.bigdata.rdf.internal.constraints.FuncBOp.namespace=kb.lex, com.bigdata.rdf.internal.constraints.FuncBOp.function=http://www.w3.org/2001/XMLSchema#float])),FunctionNode(MULTIPLY(com.bigdata.bop.rdf.aggregate.MIN(avgScore), XSDDecimal(1.5))))[ com.bigdata.rdf.sparql.ast.FunctionNode.functionURI=http://www.w3.org/2005/xpath-functions#greater-than, valueExpr=com.bigdata.rdf.internal.constraints.CompareBOp(com.bigdata.bop.rdf.aggregate.AVERAGE(com.bigdata.rdf.internal.constraints.FuncBOp(score)[ com.bigdata.rdf.internal.constraints.FuncBOp.namespace=kb.lex, com.bigdata.rdf.internal.constraints.FuncBOp.function=http://www.w3.org/2001/XMLSchema#float]),MULTIPLY(com.bigdata.bop.rdf.aggregate.MIN(avgScore), XSDDecimal(1.5)))[ com.bigdata.rdf.internal.constraints.CompareBOp.op=GT]]
     * </pre>
     * 
     * Note: The first problem with this query was that we were not flattening
     * the UNIONs. I've introduced {@link ASTFlattenUnionsOptimizer} which takes
     * care of that. However, performance is still terrible unless the query is
     * rewritten such that the subquery runs first.  So, the next step is to
     * understand why we need to do that for this case.
     */
    public void test_bsbm_bi_query6() throws Exception {

        new TestHelper("query6", // name
                "bsbm/bi/query6.rq",// query
                dataset,//
                "bsbm/bi/empty.srx"// result
        ).runTest();

    }

    /**
     * A variant on Q6 where the sub-select is rewritten as a named subquery
     * such that it runs first. This runs MUCH faster, but it is still a slow
     * query (around 3s of query time on PC 10, which is a very small data set).
     */
    public void test_bsbm_bi_query6b() throws Exception {

        new TestHelper("query6b", // name
                "bsbm/bi/query6b.rq",// query
                dataset,//
                "bsbm/bi/empty.srx"// result
        ).runTest();

    }

    public void test_bsbm_bi_query7() throws Exception {

        new TestHelper("query7", // name
                "bsbm/bi/query7.rq",// query
                dataset,//
                "bsbm/bi/empty.srx"// result
        ).runTest();

    }

    public void test_bsbm_bi_query8() throws Exception {

        new TestHelper("query8", // name
                "bsbm/bi/query8.rq",// query
                dataset,//
                "bsbm/bi/empty.srx"// result
        ).runTest();

    }

}
