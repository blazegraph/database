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

import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.UnionNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Test suite for {@link ASTFlattenUnionsOptimizer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Add counter examples.
 */
public class TestASTFlattenUnionsOptimizer extends AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestASTFlattenUnionsOptimizer() {
    }

    /**
     * @param name
     */
    public TestASTFlattenUnionsOptimizer(String name) {
        super(name);
    }

    /**
     * Given
     * 
     * <pre>
     * Select ?score
     *     {
     *       ?product bsbm:producer <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Producer1> .
     *       ?review bsbm:reviewFor ?product .
     *       { ?review bsbm:rating1 ?score . } UNION
     *       { ?review bsbm:rating2 ?score . } UNION
     *       { ?review bsbm:rating3 ?score . } UNION
     *       { ?review bsbm:rating4 ?score . }
     *     }
     * </pre>
     * 
     * which turns into
     * 
     * <pre>
     *       SELECT VarNode(score)
     *         JoinGroupNode {
     *           StatementPatternNode(VarNode(product), ConstantNode(TermId(0U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/producer]), ConstantNode(TermId(0U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Producer1]), DEFAULT_CONTEXTS)
     *           StatementPatternNode(VarNode(review), ConstantNode(TermId(0U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor]), VarNode(product), DEFAULT_CONTEXTS)
     *           UnionNode { ### Find a UnionNode
     *             JoinGroupNode { ### The direct children are JoinGroupNodes
     *               StatementPatternNode(VarNode(review), ConstantNode(TermId(0U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating1]), VarNode(score), DEFAULT_CONTEXTS)
     *             }
     *             JoinGroupNode { ### Having child JoinGroupNode
     *               UnionNode {   ### Where JoinGroupNode has child UnionNode
     *                 JoinGroupNode { ### Lift child JoinGroupNode into parent UnionNode
     *                   StatementPatternNode(VarNode(review), ConstantNode(TermId(0U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2]), VarNode(score), DEFAULT_CONTEXTS)
     *                 }
     *                 JoinGroupNode { ### Lift child JoinGroupNode into parent UnionNode
     *                   UnionNode {
     *                     JoinGroupNode {
     *                       StatementPatternNode(VarNode(review), ConstantNode(TermId(0U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating3]), VarNode(score), DEFAULT_CONTEXTS)
     *                     }
     *                     JoinGroupNode {
     *                       StatementPatternNode(VarNode(review), ConstantNode(TermId(0U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating4]), VarNode(score), DEFAULT_CONTEXTS)
     *                     }
     *                   }
     *                 }
     *               }
     *             }
     *           }
     *         }
     * </pre>
     * 
     * Flatten the UNIONs such that we have:
     * 
     * <pre>
     *       SELECT ?score
     *         JoinGroupNode {
     *           StatementPatternNode(VarNode(product), ConstantNode(TermId(0U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/producer]), ConstantNode(TermId(0U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Producer1]), DEFAULT_CONTEXTS)
     *           StatementPatternNode(VarNode(review), ConstantNode(TermId(0U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor]), VarNode(product), DEFAULT_CONTEXTS)
     *           UnionNode {
     *             JoinGroupNode {
     *               StatementPatternNode(VarNode(review), ConstantNode(TermId(0U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating1]), VarNode(score), DEFAULT_CONTEXTS)
     *             }
     *             JoinGroupNode {
     *               StatementPatternNode(VarNode(review), ConstantNode(TermId(0U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2]), VarNode(score), DEFAULT_CONTEXTS)
     *             }
     *             JoinGroupNode {
     *               StatementPatternNode(VarNode(review), ConstantNode(TermId(0U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating3]), VarNode(score), DEFAULT_CONTEXTS)
     *             }
     *             JoinGroupNode {
     *               StatementPatternNode(VarNode(review), ConstantNode(TermId(0U)[http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating4]), VarNode(score), DEFAULT_CONTEXTS)
     *             }
     *           }
     *         }
     * </pre>
     */
    @SuppressWarnings("rawtypes")
    public void test_flattenUnions1() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        /*
         * Set up constants.
         */
        final IV producerIV = makeIV(new URIImpl(
                "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/producer"));

        final IV producer1IV = makeIV(new URIImpl(
                "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Producer1"));

        final IV reviewForIV = makeIV(new URIImpl(
                "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor"));

        final IV rating1IV = makeIV(new URIImpl(
                "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating1"));
        final IV rating2IV = makeIV(new URIImpl(
                "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2"));
        final IV rating3IV = makeIV(new URIImpl(
                "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating3"));
        final IV rating4IV = makeIV(new URIImpl(
                "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating4"));
        
        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            given.setProjection(projection);
            projection.addProjectionVar(new VarNode("score"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            given.setWhereClause(whereClause);
            
            whereClause.addChild(new StatementPatternNode(
                    new VarNode("product"), new ConstantNode(producerIV),
                    new ConstantNode(producer1IV), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            whereClause.addChild(new StatementPatternNode(
                    new VarNode("review"), new ConstantNode(reviewForIV),
                    new VarNode("product"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            final UnionNode union1 = new UnionNode(); // outer
            final UnionNode union2 = new UnionNode(); // middle
            final UnionNode union3 = new UnionNode(); // inner
            whereClause.addChild(union1);
            final JoinGroupNode group1a = new JoinGroupNode();
            final JoinGroupNode group1b = new JoinGroupNode();
            union1.addChild(group1a);
            union1.addChild(group1b);
            group1a.addChild(new StatementPatternNode(
                    new VarNode("review"), new ConstantNode(rating1IV),
                    new VarNode("score"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            group1b.addChild(union2);
            
            final JoinGroupNode group2a = new JoinGroupNode();
            final JoinGroupNode group2b = new JoinGroupNode();
            union2.addChild(group2a);
            union2.addChild(group2b);
            group2a.addChild(new StatementPatternNode(
                    new VarNode("review"), new ConstantNode(rating2IV),
                    new VarNode("score"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            group2b.addChild(union3);

            final JoinGroupNode group3a = new JoinGroupNode();
            final JoinGroupNode group3b = new JoinGroupNode();
            union3.addChild(group3a);
            union3.addChild(group3b);
            group3a.addChild(new StatementPatternNode(
                    new VarNode("review"), new ConstantNode(rating3IV),
                    new VarNode("score"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            group3b.addChild(new StatementPatternNode(
                    new VarNode("review"), new ConstantNode(rating4IV),
                    new VarNode("score"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {
            
            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);
            projection.addProjectionVar(new VarNode("score"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);
            
            whereClause.addChild(new StatementPatternNode(
                    new VarNode("product"), new ConstantNode(producerIV),
                    new ConstantNode(producer1IV), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            whereClause.addChild(new StatementPatternNode(
                    new VarNode("review"), new ConstantNode(reviewForIV),
                    new VarNode("product"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

            final UnionNode union1 = new UnionNode(); // outer
            whereClause.addChild(union1);
            final JoinGroupNode group1a = new JoinGroupNode();
            final JoinGroupNode group1b = new JoinGroupNode();
            final JoinGroupNode group1c = new JoinGroupNode();
            final JoinGroupNode group1d = new JoinGroupNode();
            union1.addChild(group1a);
            union1.addChild(group1b);
            union1.addChild(group1c);
            union1.addChild(group1d);
            group1a.addChild(new StatementPatternNode(
                    new VarNode("review"), new ConstantNode(rating1IV),
                    new VarNode("score"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            group1b.addChild(new StatementPatternNode(
                    new VarNode("review"), new ConstantNode(rating2IV),
                    new VarNode("score"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            group1c.addChild(new StatementPatternNode(
                    new VarNode("review"), new ConstantNode(rating3IV),
                    new VarNode("score"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            group1d.addChild(new StatementPatternNode(
                    new VarNode("review"), new ConstantNode(rating4IV),
                    new VarNode("score"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

        }

        final IASTOptimizer rewriter = new ASTFlattenUnionsOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }

    /**
     * A unit test which verifies a case where a child union can not be
     * flattened.
     * 
     * <pre>
     *  SELECT ?s ?o {
     *     { ?s ?p1 ?o } UNION
     *     { ?x ?y ?x .
     *       { ?s ?p2 ?o } UNION
     *       { ?s ?p3 ?o }
     *     }
     *  }
     * </pre>
     * 
     * which turns into:
     * 
     * <pre>
     * 
     * SELECT ( VarNode(s) AS VarNode(s) ) ( VarNode(o) AS VarNode(o) )
     *   JoinGroupNode {
     *     UnionNode {
     *       JoinGroupNode {
     *         StatementPatternNode(VarNode(s), VarNode(p1), VarNode(o), DEFAULT_CONTEXTS)
     *       }
     *       JoinGroupNode {
     *         StatementPatternNode(VarNode(x), VarNode(y), VarNode(x), DEFAULT_CONTEXTS)
     *         UnionNode {
     *           JoinGroupNode {
     *             StatementPatternNode(VarNode(s), VarNode(p2), VarNode(o), DEFAULT_CONTEXTS)
     *           }
     *           JoinGroupNode {
     *             StatementPatternNode(VarNode(s), VarNode(p3), VarNode(o), DEFAULT_CONTEXTS)
     *           }
     *         }
     *       }
     *     }
     *   }
     * </pre>
     * 
     * This case can not be flatten because the child UnionNode is not the sole
     * child of its parent group node.
     * 
     * @throws Exception
     */
    public void test_flattenUnions2() throws Exception {
        
        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            given.setProjection(projection);
            projection.addProjectionVar(new VarNode("s"));
            projection.addProjectionVar(new VarNode("o"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            given.setWhereClause(whereClause);
            
            final UnionNode union1 = new UnionNode(); // outer
            final UnionNode union3 = new UnionNode(); // inner
            whereClause.addChild(union1);
            final JoinGroupNode group1a = new JoinGroupNode();
            final JoinGroupNode group1b = new JoinGroupNode();
            union1.addChild(group1a);
            union1.addChild(group1b);
            group1a.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p1"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            
            final JoinGroupNode group2 = new JoinGroupNode();
            group1b.addChild(group2);
            group2.addChild(new StatementPatternNode(new VarNode("x"),
                    new VarNode("y"), new VarNode("x"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            group2.addChild(union3);

            final JoinGroupNode group3a = new JoinGroupNode();
            final JoinGroupNode group3b = new JoinGroupNode();
            union3.addChild(group3a);
            union3.addChild(group3b);
            group3a.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p2"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            group3b.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p3"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);
            projection.addProjectionVar(new VarNode("s"));
            projection.addProjectionVar(new VarNode("o"));
            
            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);
            
            final UnionNode union1 = new UnionNode(); // outer
            final UnionNode union3 = new UnionNode(); // inner
            whereClause.addChild(union1);
            final JoinGroupNode group1a = new JoinGroupNode();
            final JoinGroupNode group1b = new JoinGroupNode();
            union1.addChild(group1a);
            union1.addChild(group1b);
            group1a.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p1"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            
            final JoinGroupNode group2 = new JoinGroupNode();
            group1b.addChild(group2);
            group2.addChild(new StatementPatternNode(new VarNode("x"),
                    new VarNode("y"), new VarNode("x"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            group2.addChild(union3);

            final JoinGroupNode group3a = new JoinGroupNode();
            final JoinGroupNode group3b = new JoinGroupNode();
            union3.addChild(group3a);
            union3.addChild(group3b);
            group3a.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p2"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));
            group3b.addChild(new StatementPatternNode(new VarNode("s"),
                    new VarNode("p3"), new VarNode("o"), null/* c */,
                    Scope.DEFAULT_CONTEXTS));

        }

        final IASTOptimizer rewriter = new ASTFlattenUnionsOptimizer();
        
        final IQueryNode actual = rewriter.optimize(null/* AST2BOpContext */,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }
    
}
