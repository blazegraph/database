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
 * Created on Aug 29, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.store.BDS;

/**
 * Test suite for {@link ASTServiceNodeOptimizer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestASTServiceNodeOptimizer extends AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestASTServiceNodeOptimizer() {
    }

    /**
     * @param name
     */
    public TestASTServiceNodeOptimizer(String name) {
        super(name);
    }

    /**
     * Given
     * 
     * <pre>
     * PREFIX bd: <http://www.bigdata.com/rdf/search#>
     * QueryType: SELECT
     * SELECT ( VarNode(subj) AS VarNode(subj) ) ( VarNode(score) AS VarNode(score) )
     *     JoinGroupNode {
     *       SERVICE <ConstantNode(TermId(0U)[http://www.bigdata.com/rdf/search#search])> {
     *         JoinGroupNode {
     *           StatementPatternNode(VarNode(lit), ConstantNode(TermId(0U)[http://www.bigdata.com/rdf/search#search]), ConstantNode(TermId(0L)[mike]), DEFAULT_CONTEXTS)
     *           StatementPatternNode(VarNode(lit), ConstantNode(TermId(0U)[http://www.bigdata.com/rdf/search#relevance]), VarNode(score), DEFAULT_CONTEXTS)
     *         }
     *       }
     *       StatementPatternNode(VarNode(subj), VarNode(p), VarNode(lit), DEFAULT_CONTEXTS)
     *         com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.estimatedCardinality=5
     *         com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.originalIndex=SPOC
     *     }
     * }
     * </pre>
     * 
     * The {@link ServiceNode} is lifted out into a {@link NamedSubqueryRoot},
     * yielding the following AST after the rewrite:
     * 
     * <pre>
     * WITH {
     *   QueryType: SELECT
     *   SELECT VarNode(lit) VarNode(score)
     *     JoinGroupNode {
     *       SERVICE <ConstantNode(TermId(0U)[http://www.bigdata.com/rdf/search#search])> {
     *         JoinGroupNode {
     *           StatementPatternNode(VarNode(lit), ConstantNode(TermId(0U)[http://www.bigdata.com/rdf/search#search]), ConstantNode(TermId(0U)[mike]), DEFAULT_CONTEXTS)
     *           StatementPatternNode(VarNode(lit), ConstantNode(TermId(0U)[http://www.bigdata.com/rdf/search#relevance]), VarNode(score), DEFAULT_CONTEXTS)
     *         }
     *       }
     *     }
     * } AS %-anon-service-call-0
     * QueryType: SELECT
     * SELECT VarNode(subj) VarNode(score)
     *   JoinGroupNode {
     *     INCLUDE %-anon-service-call-0
     *     StatementPatternNode(VarNode(subj), VarNode(p), VarNode(lit), DEFAULT_CONTEXTS)
     *   }
     * </pre>
     */
    public void test_serviceNodeOptimizer_01() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */

        @SuppressWarnings("rawtypes")
        final IV searchIV = makeIV(BDS.SEARCH);
        
        @SuppressWarnings("rawtypes")
        final IV relevanceIV = makeIV(BDS.RELEVANCE);

        @SuppressWarnings("rawtypes")
        final IV mikeIV = makeIV(store.getValueFactory().createLiteral("mike"));

        final IBindingSet[] bsets = new IBindingSet[] { //
                new ListBindingSet()
        };

        /**
         * The source AST.
         * 
         * <pre>
         * PREFIX bd: <http://www.bigdata.com/rdf/search#>
         * QueryType: SELECT
         * SELECT ( VarNode(subj) AS VarNode(subj) ) ( VarNode(score) AS VarNode(score) )
         *     JoinGroupNode {
         *       SERVICE <ConstantNode(TermId(0U)[http://www.bigdata.com/rdf/search#search])> {
         *         JoinGroupNode {
         *           StatementPatternNode(VarNode(lit), ConstantNode(TermId(0U)[http://www.bigdata.com/rdf/search#search]), ConstantNode(TermId(0L)[mike]), DEFAULT_CONTEXTS)
         *           StatementPatternNode(VarNode(lit), ConstantNode(TermId(0U)[http://www.bigdata.com/rdf/search#relevance]), VarNode(score), DEFAULT_CONTEXTS)
         *         }
         *       }
         *       StatementPatternNode(VarNode(subj), VarNode(p), VarNode(lit), DEFAULT_CONTEXTS)
         *         com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.estimatedCardinality=5
         *         com.bigdata.rdf.sparql.ast.eval.AST2BOpBase.originalIndex=SPOC
         *     }
         * }
         * </pre>
         */
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            given.setProjection(projection);

            projection.addProjectionVar(new VarNode("subj"));
            projection.addProjectionVar(new VarNode("score"));

            final JoinGroupNode whereClause = new JoinGroupNode();
            given.setWhereClause(whereClause);

            {

                final JoinGroupNode serviceGraphPattern = new JoinGroupNode();

                serviceGraphPattern.addChild(new StatementPatternNode(
                        new VarNode("lit"), new ConstantNode(searchIV),
                        new ConstantNode(mikeIV), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                serviceGraphPattern.addChild(new StatementPatternNode(
                        new VarNode("lit"), new ConstantNode(relevanceIV),
                        new VarNode("score"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                final ServiceNode serviceNode = new ServiceNode(
                        new ConstantNode(searchIV), serviceGraphPattern);

                whereClause.addChild(serviceNode);

            }

            whereClause.addChild(new StatementPatternNode(new VarNode("subj"),
                    new VarNode("p"), new VarNode("lit"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

        }

        /**
         * The expected AST after the rewrite.
         * 
         * <pre>
         * WITH {
         *   QueryType: SELECT
         *   SELECT VarNode(lit) VarNode(score)
         *     JoinGroupNode {
         *       SERVICE <ConstantNode(TermId(0U)[http://www.bigdata.com/rdf/search#search])> {
         *         JoinGroupNode {
         *           StatementPatternNode(VarNode(lit), ConstantNode(TermId(0U)[http://www.bigdata.com/rdf/search#search]), ConstantNode(TermId(0U)[mike]), DEFAULT_CONTEXTS)
         *           StatementPatternNode(VarNode(lit), ConstantNode(TermId(0U)[http://www.bigdata.com/rdf/search#relevance]), VarNode(score), DEFAULT_CONTEXTS)
         *         }
         *       }
         *     }
         * } AS %-anon-service-call-0
         * QueryType: SELECT
         * SELECT VarNode(subj) VarNode(score)
         *   JoinGroupNode {
         *     INCLUDE %-anon-service-call-0
         *     StatementPatternNode(VarNode(subj), VarNode(p), VarNode(lit), DEFAULT_CONTEXTS)
         *   }
         * </pre>
         */
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final String namedSubqueryName = "%-anon-service-call-0";

            {
                
                final ProjectionNode projection = new ProjectionNode();
                expected.setProjection(projection);

                projection.addProjectionVar(new VarNode("subj"));
                projection.addProjectionVar(new VarNode("score"));

            }

            {
                
                final JoinGroupNode whereClause = new JoinGroupNode();

                expected.setWhereClause(whereClause);

                whereClause
                        .addChild(new NamedSubqueryInclude(namedSubqueryName));

                whereClause.addChild(new StatementPatternNode(new VarNode(
                        "subj"), new VarNode("p"), new VarNode("lit"),
                        null/* c */, Scope.DEFAULT_CONTEXTS));

            }

            {

                final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
                        QueryType.SELECT, namedSubqueryName);
                
                expected.getNamedSubqueriesNotNull().add(nsr);
                
                {
                 
                    final ProjectionNode projection = new ProjectionNode();
                    nsr.setProjection(projection);

                    projection.addProjectionVar(new VarNode("lit"));
                    projection.addProjectionVar(new VarNode("score"));

                }

                {

                    final JoinGroupNode whereClause = new JoinGroupNode();
                    nsr.setWhereClause(whereClause);

                    final JoinGroupNode serviceGraphPattern = new JoinGroupNode();

                    serviceGraphPattern.addChild(new StatementPatternNode(
                            new VarNode("lit"), new ConstantNode(searchIV),
                            new ConstantNode(mikeIV), null/* c */,
                            Scope.DEFAULT_CONTEXTS));

                    serviceGraphPattern.addChild(new StatementPatternNode(
                            new VarNode("lit"), new ConstantNode(relevanceIV),
                            new VarNode("score"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));

                    final ServiceNode serviceNode = new ServiceNode(
                            new ConstantNode(searchIV), serviceGraphPattern);

                    whereClause.addChild(serviceNode);
                
                }

            }

        }

        final IASTOptimizer rewriter = new ASTServiceNodeOptimizer();

        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }

    /**
     * Unit test for correct rejection of the lift into a named subquery.
     * 
     * <pre>
     * PREFIX : <http://example.org/> 
     * 
     * SELECT ?s ?o1 ?o2 
     * {
     *   ?s ?p1 ?o1 .
     *   SERVICE <http://localhost:18080/openrdf/repositories/endpoint1> {
     *     ?s ?p2 ?o2
     *   }
     * }
     * </pre>
     */
    public void test_serviceNodeOptimizer_02() {
    
        /*
         * Note: DO NOT share structures in this test!!!!
         */

        @SuppressWarnings("rawtypes")
        final IV serviceUriIV = makeIV(store.getValueFactory().createURI(
                "http://localhost:18080/openrdf/repositories/endpoint1"));

        final IBindingSet[] bsets = new IBindingSet[] { //
                new ListBindingSet()
        };

        /**
         * The source AST.
         * 
         * <pre></pre>
         */
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            given.setProjection(projection);

            projection.addProjectionVar(new VarNode("s"));
            projection.addProjectionVar(new VarNode("o1"));
            projection.addProjectionVar(new VarNode("o2"));

            final JoinGroupNode whereClause = new JoinGroupNode();
            given.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p1"), new VarNode("o1"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            {

                final JoinGroupNode serviceGraphPattern = new JoinGroupNode();

                serviceGraphPattern.addChild(new StatementPatternNode(
                        new VarNode("s"), new VarNode("p2"),
                        new VarNode("o2"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                final ServiceNode serviceNode = new ServiceNode(
                        new ConstantNode(serviceUriIV), serviceGraphPattern);

                whereClause.addChild(serviceNode);

            }
            
        }

        /**
         * The expected AST after the rewrite (unchanged).
         */
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);

            projection.addProjectionVar(new VarNode("s"));
            projection.addProjectionVar(new VarNode("o1"));
            projection.addProjectionVar(new VarNode("o2"));

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p1"), new VarNode("o1"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            {

                final JoinGroupNode serviceGraphPattern = new JoinGroupNode();

                serviceGraphPattern.addChild(new StatementPatternNode(
                        new VarNode("s"), new VarNode("p2"),
                        new VarNode("o2"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

                final ServiceNode serviceNode = new ServiceNode(
                        new ConstantNode(serviceUriIV), serviceGraphPattern);

                whereClause.addChild(serviceNode);

            }

        }

        final IASTOptimizer rewriter = new ASTServiceNodeOptimizer();

        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }
    
}
