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
 * Created on Oct 20, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.BigdataStatics;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.OrderByExpr;
import com.bigdata.rdf.sparql.ast.OrderByNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.SliceNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Test suite for {@link ASTHashJoinOptimizer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestASTHashJoinOptimizer extends AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestASTHashJoinOptimizer() {
    }

    /**
     * @param name
     */
    public TestASTHashJoinOptimizer(String name) {
        super(name);
    }

    /**
     * This unit test is based on BSBM Q5.
     * 
     * <pre>
     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
     * 
     * SELECT DISTINCT ?product ?productLabel
     * WHERE { 
     *     ?product rdfs:label ?productLabel .
     *     FILTER (<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1092/Product53999> != ?product)
     *     <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1092/Product53999> bsbm:productFeature ?prodFeature .
     *     ?product bsbm:productFeature ?prodFeature .
     *     <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1092/Product53999> bsbm:productPropertyNumeric1 ?origProperty1 .
     *     ?product bsbm:productPropertyNumeric1 ?simProperty1 .
     *     FILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 > (?origProperty1 - 120))
     *     <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1092/Product53999> bsbm:productPropertyNumeric2 ?origProperty2 .
     *     ?product bsbm:productPropertyNumeric2 ?simProperty2 .
     *     FILTER (?simProperty2 < (?origProperty2 + 170) && ?simProperty2 > (?origProperty2 - 170))
     * }
     * ORDER BY ?productLabel
     * LIMIT 5
     * </pre>
     * 
     * TODO For this query, the only variable which is needed in the parent
     * group after the sub-groups have been pushed down is <code>?product</code>
     * . It is possible to recognize that we could turn those sub-groups into
     * sub-selects and use a DISTINCT projection of just the
     * <code>?product</code> variable. If the joins in those subgroups have more
     * than one result per product, then that additional transform could
     * eliminate a significiant amount of work.
     */
    @SuppressWarnings("unchecked")
    public void test_hashJoinOptimizer_BSBM_Q5() {

        /*
         * Resolve terms against the lexicon.
         */
        final BigdataValueFactory valueFactory = store.getLexiconRelation()
                .getValueFactory();

        final String rdfs = "http://www.w3.org/2000/01/rdf-schema#";
//        final String rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
        final String bsbm = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/";

        final String productInstance = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Product22";
        
        final BigdataURI rdfsLabel = valueFactory.createURI(rdfs + "label");

        final BigdataURI productFeature = valueFactory.createURI(bsbm
                + "productFeature");

        final BigdataURI productPropertyNumeric1 = valueFactory.createURI(bsbm
                + "productPropertyNumeric1");

        final BigdataURI productPropertyNumeric2 = valueFactory.createURI(bsbm
                + "productPropertyNumeric2");

        final BigdataURI product53999 = valueFactory.createURI(productInstance);
        
        final BigdataLiteral _120 = valueFactory.createLiteral("120",
                XSD.INTEGER);
        
        final BigdataLiteral _170 = valueFactory.createLiteral("170",
                XSD.INTEGER);

        final BigdataValue[] terms = new BigdataValue[] { rdfsLabel,
                productFeature, productPropertyNumeric1,
                productPropertyNumeric2, product53999, _120, _170};

        // resolve terms.
        store.getLexiconRelation()
                .addTerms(terms, terms.length, false/* readOnly */);

        for(BigdataValue bv : terms) {
            // Cache the Value on the IV.
            bv.getIV().setValue(bv);
        }

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            {
                final ProjectionNode projection = new ProjectionNode();
                given.setProjection(projection);

                projection.addProjectionVar(new VarNode("product"));
                projection.addProjectionVar(new VarNode("productLabel"));
            }

            final JoinGroupNode whereClause = new JoinGroupNode();
            given.setWhereClause(whereClause);

            // ?product rdfs:label ?productLabel .
            whereClause.addChild(new StatementPatternNode(
                    new VarNode("product"),
                    new ConstantNode(rdfsLabel.getIV()), new VarNode(
                            "productLabel"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            // FILTER
            // (<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1092/Product53999>
            // != ?product)
            whereClause.addChild(new FilterNode(FunctionNode.NE(
                    new ConstantNode(product53999.getIV()), new VarNode(
                            "product"))));
            
            // <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1092/Product53999>
            // bsbm:productFeature ?prodFeature .
            whereClause.addChild(new StatementPatternNode(
                    new ConstantNode(product53999.getIV()),
                    new ConstantNode(productFeature.getIV()), new VarNode(
                            "prodFeature"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            // ?product bsbm:productFeature ?prodFeature .
            whereClause.addChild(new StatementPatternNode(
                    new VarNode("product"),
                    new ConstantNode(productFeature.getIV()), new VarNode(
                            "prodFeature"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            // <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1092/Product53999>
            // bsbm:productPropertyNumeric1 ?origProperty1 .
            whereClause.addChild(new StatementPatternNode(new ConstantNode(
                    product53999.getIV()), new ConstantNode(
                    productPropertyNumeric1.getIV()), new VarNode(
                    "origProperty1"), null/* c */, Scope.DEFAULT_CONTEXTS));

            // ?product bsbm:productPropertyNumeric1 ?simProperty1 .
            whereClause.addChild(new StatementPatternNode(
                    new VarNode("product"), new ConstantNode(
                            productPropertyNumeric1.getIV()), new VarNode(
                            "simProperty1"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            // FILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 > (?origProperty1 - 120))
            {
                final ValueExpressionNode left = new FunctionNode(
                        FunctionRegistry.LT, null/* scalarArgs */,
                        new ValueExpressionNode[] {//
                                new VarNode("simProperty1"),//
                                new FunctionNode(FunctionRegistry.ADD,
                                        null/* scalarArgs */,
                                        new ValueExpressionNode[] {//
                                            new VarNode("origProperty1"),//
                                            new ConstantNode(_120.getIV())
                                }) //
                        });
                
                final ValueExpressionNode right = new FunctionNode(
                        FunctionRegistry.GT, null/* scalarArgs */,
                        new ValueExpressionNode[] {//
                                new VarNode("simProperty1"),//
                                new FunctionNode(FunctionRegistry.SUBTRACT,
                                        null/* scalarArgs */,
                                        new ValueExpressionNode[] {//
                                            new VarNode("origProperty1"),//
                                            new ConstantNode(_120.getIV())
                                }) //
                        });
                
                final ValueExpressionNode expr = new FunctionNode(
                        FunctionRegistry.AND, null/* scalarValues */,
                        new ValueExpressionNode[] { left, right });

                final FilterNode filter = new FilterNode(expr);
                
                whereClause.addChild(filter);
            }
            
            // <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1092/Product53999>
            // bsbm:productPropertyNumeric2 ?origProperty2 .
            whereClause.addChild(new StatementPatternNode(new ConstantNode(
                    product53999.getIV()), new ConstantNode(
                    productPropertyNumeric2.getIV()), new VarNode(
                    "origProperty2"), null/* c */, Scope.DEFAULT_CONTEXTS));

            // ?product bsbm:productPropertyNumeric2 ?simProperty2 .
            whereClause.addChild(new StatementPatternNode(
                    new VarNode("product"), new ConstantNode(
                            productPropertyNumeric2.getIV()), new VarNode(
                            "simProperty2"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            // FILTER (?simProperty2 < (?origProperty2 + 170) && ?simProperty2 > (?origProperty2 - 170))
            {
                final ValueExpressionNode left = new FunctionNode(
                        FunctionRegistry.LT, null/* scalarArgs */,
                        new ValueExpressionNode[] {//
                                new VarNode("simProperty2"),//
                                new FunctionNode(FunctionRegistry.ADD,
                                        null/* scalarArgs */,
                                        new ValueExpressionNode[] {//
                                            new VarNode("origProperty2"),//
                                            new ConstantNode(_170.getIV())
                                }) //
                        });
                
                final ValueExpressionNode right = new FunctionNode(
                        FunctionRegistry.GT, null/* scalarArgs */,
                        new ValueExpressionNode[] {//
                                new VarNode("simProperty2"),//
                                new FunctionNode(FunctionRegistry.SUBTRACT,
                                        null/* scalarArgs */,
                                        new ValueExpressionNode[] {//
                                            new VarNode("origProperty2"),//
                                            new ConstantNode(_170.getIV())
                                }) //
                        });
                
                final ValueExpressionNode expr = new FunctionNode(
                        FunctionRegistry.AND, null/* scalarValues */,
                        new ValueExpressionNode[] { left, right });

                final FilterNode filter = new FilterNode(expr);
                
                whereClause.addChild(filter);
            }
            
            {
                final OrderByNode orderByNode = new OrderByNode();
                given.setOrderBy(orderByNode);
                orderByNode.addExpr(new OrderByExpr(
                        new VarNode("productLabel"), true/* ascending */));
            }
            
            given.setSlice(new SliceNode(0L/* offset */, 5L/* limit */));
            
        }

        /*
         * The expected AST after the rewrite.
         * 
         * Note: Two sub-groups are extracted. Each sub-group is identified by
         * the presence of a join which does not have any explicitly shared
         * variables but where there is a constraint imposed through a FILTER
         * which runs with the 2nd statement pattern in the sub-group.
         * 
         * Note: For BSBM, it works out that these groups can be united with the
         * parent group using a hash join on [?product]. It would be a pretty
         * odd query if there were no such variable which could unite the groups.
         */
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            {
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);

                projection.addProjectionVar(new VarNode("product"));
                projection.addProjectionVar(new VarNode("productLabel"));
            }

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            // <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1092/Product53999>
            // bsbm:productFeature ?prodFeature .
            whereClause.addChild(new StatementPatternNode(
                    new ConstantNode(product53999.getIV()),
                    new ConstantNode(productFeature.getIV()), new VarNode(
                            "prodFeature"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            // ?product bsbm:productFeature ?prodFeature .
            whereClause.addChild(new StatementPatternNode(
                    new VarNode("product"),
                    new ConstantNode(productFeature.getIV()), new VarNode(
                            "prodFeature"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            
            // FILTER
            // (<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1092/Product53999>
            // != ?product)
            whereClause.addChild(new FilterNode(FunctionNode.NE(
                    new ConstantNode(product53999.getIV()), new VarNode(
                            "product"))));
            
            // ?product rdfs:label ?productLabel .
            whereClause.addChild(new StatementPatternNode(
                    new VarNode("product"),
                    new ConstantNode(rdfsLabel.getIV()), new VarNode(
                            "productLabel"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            {
                final JoinGroupNode subGroup = new JoinGroupNode();
                whereClause.addChild(subGroup);

                // <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1092/Product53999>
                // bsbm:productPropertyNumeric1 ?origProperty1 .
                subGroup.addChild(new StatementPatternNode(new ConstantNode(
                        product53999.getIV()), new ConstantNode(
                        productPropertyNumeric1.getIV()), new VarNode(
                        "origProperty1"), null/* c */, Scope.DEFAULT_CONTEXTS));

                // ?product bsbm:productPropertyNumeric1 ?simProperty1 .
                subGroup.addChild(new StatementPatternNode(new VarNode(
                        "product"), new ConstantNode(productPropertyNumeric1
                        .getIV()), new VarNode("simProperty1"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                // FILTER (?simProperty1 < (?origProperty1 + 120) &&
                // ?simProperty1 > (?origProperty1 - 120))
                {
                    final ValueExpressionNode left = new FunctionNode(
                            FunctionRegistry.LT, null/* scalarArgs */,
                            new ValueExpressionNode[] {//
                            new VarNode("simProperty1"),//
                                    new FunctionNode(FunctionRegistry.ADD,
                                            null/* scalarArgs */,
                                            new ValueExpressionNode[] {//
                                                    new VarNode("origProperty1"),//
                                                    new ConstantNode(_120
                                                            .getIV()) }) //
                            });

                    final ValueExpressionNode right = new FunctionNode(
                            FunctionRegistry.GT, null/* scalarArgs */,
                            new ValueExpressionNode[] {//
                            new VarNode("simProperty1"),//
                                    new FunctionNode(FunctionRegistry.SUBTRACT,
                                            null/* scalarArgs */,
                                            new ValueExpressionNode[] {//
                                                    new VarNode("origProperty1"),//
                                                    new ConstantNode(_120
                                                            .getIV()) }) //
                            });

                    final ValueExpressionNode expr = new FunctionNode(
                            FunctionRegistry.AND, null/* scalarValues */,
                            new ValueExpressionNode[] { left, right });

                    final FilterNode filter = new FilterNode(expr);

                    subGroup.addChild(filter);
                    
                }
            
            }
            
            {

                final JoinGroupNode subGroup = new JoinGroupNode();
                whereClause.addChild(subGroup);

                // <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1092/Product53999>
                // bsbm:productPropertyNumeric2 ?origProperty2 .
                subGroup.addChild(new StatementPatternNode(new ConstantNode(
                        product53999.getIV()), new ConstantNode(
                        productPropertyNumeric2.getIV()), new VarNode(
                        "origProperty2"), null/* c */, Scope.DEFAULT_CONTEXTS));

                // ?product bsbm:productPropertyNumeric2 ?simProperty2 .
                subGroup.addChild(new StatementPatternNode(new VarNode(
                        "product"), new ConstantNode(productPropertyNumeric2
                        .getIV()), new VarNode("simProperty2"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                // FILTER (?simProperty2 < (?origProperty2 + 170) &&
                // ?simProperty2 > (?origProperty2 - 170))
                {
                    final ValueExpressionNode left = new FunctionNode(
                            FunctionRegistry.LT, null/* scalarArgs */,
                            new ValueExpressionNode[] {//
                            new VarNode("simProperty2"),//
                                    new FunctionNode(FunctionRegistry.ADD,
                                            null/* scalarArgs */,
                                            new ValueExpressionNode[] {//
                                                    new VarNode("origProperty2"),//
                                                    new ConstantNode(_170
                                                            .getIV()) }) //
                            });

                    final ValueExpressionNode right = new FunctionNode(
                            FunctionRegistry.GT, null/* scalarArgs */,
                            new ValueExpressionNode[] {//
                            new VarNode("simProperty2"),//
                                    new FunctionNode(FunctionRegistry.SUBTRACT,
                                            null/* scalarArgs */,
                                            new ValueExpressionNode[] {//
                                                    new VarNode("origProperty2"),//
                                                    new ConstantNode(_170
                                                            .getIV()) }) //
                            });

                    final ValueExpressionNode expr = new FunctionNode(
                            FunctionRegistry.AND, null/* scalarValues */,
                            new ValueExpressionNode[] { left, right });

                    final FilterNode filter = new FilterNode(expr);

                    subGroup.addChild(filter);
                }

            }
            
            {
                final OrderByNode orderByNode = new OrderByNode();
                expected.setOrderBy(orderByNode);
                orderByNode.addExpr(new OrderByExpr(
                        new VarNode("productLabel"), true/* ascending */));
            }
            
            expected.setSlice(new SliceNode(0L/* offset */, 5L/* limit */));

        }

        final IASTOptimizer rewriter = new ASTHashJoinOptimizer();
        
        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        // Cache the value expressions for both ASTs.
        new ASTSetValueExpressionsOptimizer().optimize(context, 
              new QueryNodeWithBindingSet(given, bsets));
        new ASTSetValueExpressionsOptimizer().optimize(context, 
              new QueryNodeWithBindingSet(expected, bsets));
        
        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        /*
         * FIXME This is failing because the optimizer is not finished yet.
         */
        if (!BigdataStatics.runKnownBadTests)
            return;

        assertSameAST(expected, actual);

    }

}
