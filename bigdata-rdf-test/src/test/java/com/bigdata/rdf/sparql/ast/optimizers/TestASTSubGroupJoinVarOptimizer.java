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
 * Created on Sep 13, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Collections;

import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.bop.aggregate.AggregateBase;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.GroupByNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.NamedSubqueryRoot;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Test suite for the {@link ASTSubGroupJoinVarOptimizer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestASTSubGroupJoinVarOptimizer extends
        AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestASTSubGroupJoinVarOptimizer() {
    }

    /**
     * @param name
     */
    public TestASTSubGroupJoinVarOptimizer(String name) {
        super(name);
    }

    /**
     * Unit test for assigning join variables to sub-groups.
     * 
     * <pre>
     * PREFIX p1: <http://www.rdfabout.com/rdf/schema/usgovt/>
     * PREFIX p2: <http://www.rdfabout.com/rdf/schema/vote/>
     * PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
     * PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
     * 
     * SELECT (SAMPLE(?_var9) AS ?_var1) ?_var2 ?_var3
     * WITH {
     *     SELECT DISTINCT ?_var3
     *     WHERE {
     *         ?_var3 rdf:type <http://www.rdfabout.com/rdf/schema/politico/Politician>.
     *         ?_var3 <http://www.rdfabout.com/rdf/schema/politico/hasRole> ?_var6. 
     *         ?_var6 <http://www.rdfabout.com/rdf/schema/politico/party> "Democrat".
     *     }
     * } AS %_set1
     *         WHERE {
     *             INCLUDE %_set1 . 
     *             OPTIONAL {
     *                 ?_var3 p1:name ?_var9
     *             }. 
     *             OPTIONAL {
     *                 ?_var10 p2:votedBy ?_var3. 
     *                 ?_var10 rdfs:label ?_var2.
     *             }
     *         }
     *         GROUP BY ?_var2 ?_var3
     * </pre>
     * 
     * Both the simple optional group and the complex optional group in this
     * query should have <code>_var3</code> specified as a join variable.
     */
    public void test_govtrack_21() {

        /*
         * Note: DO NOT share structures in this test!!!!
         */
        final IBindingSet[] bsets = new IBindingSet[]{};

        @SuppressWarnings("rawtypes")
        final IV a = makeIV(RDF.TYPE);
        @SuppressWarnings("rawtypes")
        final IV rdfsLabel = makeIV(RDFS.LABEL);
        @SuppressWarnings("rawtypes")
        final IV politician = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/politico/Politician"));
        @SuppressWarnings("rawtypes")
        final IV hasRole = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/politico/hasRole"));
        @SuppressWarnings("rawtypes")
        final IV party = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/politico/party"));
        @SuppressWarnings("rawtypes")
        final IV democrat = makeIV(new LiteralImpl("Democrat"));
        @SuppressWarnings("rawtypes")
        final IV name = makeIV(new URIImpl("http://www.rdfabout.com/rdf/schema/usgovt/name"));
        @SuppressWarnings("rawtypes")
        final IV votedBy = makeIV(new URIImpl(
                "http://www.rdfabout.com/rdf/schema/vote/votedBy"));

        // The source AST.
        final QueryRoot given = new QueryRoot(QueryType.SELECT);
        final String namedSet1 = "_set1";
        final IVariable<?>[] joinVars = new IVariable[] { Var.var("_var3") };
        {

            // NamedSubqueryRoot
            {

                final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
                        QueryType.SELECT, namedSet1);
                given.getNamedSubqueriesNotNull().add(nsr);

                {
                    final ProjectionNode projection = new ProjectionNode();
                    nsr.setProjection(projection);

                    projection.setDistinct(true);
                    projection.addProjectionVar(new VarNode("_var3"));
                }

                final JoinGroupNode whereClause = new JoinGroupNode();
                nsr.setWhereClause(whereClause);

                whereClause.addChild(new StatementPatternNode(new VarNode(
                        "_var3"), new ConstantNode(a), new ConstantNode(
                        politician), null/* c */, Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(new StatementPatternNode(new VarNode(
                        "_var3"), new ConstantNode(hasRole), new VarNode(
                        "_var6"), null/* c */, Scope.DEFAULT_CONTEXTS));

                whereClause.addChild(new StatementPatternNode(new VarNode(
                        "_var6"), new ConstantNode(party), new ConstantNode(
                        democrat), null/* c */, Scope.DEFAULT_CONTEXTS));

            }

            // Main Query
            {
                final ProjectionNode projection = new ProjectionNode();
                given.setProjection(projection);

                projection
                        .addProjectionExpression(new AssignmentNode(//
                                new VarNode("_var1"),// var
                                new FunctionNode(
                                        // expr
                                        FunctionRegistry.SAMPLE,
                                        Collections
                                                .singletonMap(
                                                        AggregateBase.Annotations.DISTINCT,
                                                        (Object) Boolean.FALSE)/* scalarValues */,
                                        new ValueExpressionNode[] { new VarNode(
                                                "_var9") })));
                projection.addProjectionVar(new VarNode("_var2"));
                projection.addProjectionVar(new VarNode("_var3"));

                final JoinGroupNode whereClause = new JoinGroupNode();
                given.setWhereClause(whereClause);

                whereClause.addChild(new NamedSubqueryInclude(namedSet1));

                whereClause.addChild(new JoinGroupNode(true/* optional */,
                        new StatementPatternNode(new VarNode("_var3"),
                                new ConstantNode(name), new VarNode("_var9"),
                                null/* c */, Scope.DEFAULT_CONTEXTS)));

                final JoinGroupNode complexOptGroup = new JoinGroupNode(true/* optional */);
                whereClause.addChild(complexOptGroup);
                {
                    complexOptGroup.addChild(new StatementPatternNode(
                            new VarNode("_var10"), new ConstantNode(votedBy),
                            new VarNode("_var3"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));

                    complexOptGroup.addChild(new StatementPatternNode(
                            new VarNode("_var10"), new ConstantNode(rdfsLabel),
                            new VarNode("_var2"), null/* c */,
                            Scope.DEFAULT_CONTEXTS));
                }

                final GroupByNode groupByNode = new GroupByNode();
                given.setGroupBy(groupByNode);
                groupByNode.addGroupByVar(new VarNode("_var2"));
                groupByNode.addGroupByVar(new VarNode("_var3"));

            }

        }

        // The expected AST after the rewrite.
        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final NamedSubqueryRoot nsr = new NamedSubqueryRoot(
                    QueryType.SELECT, namedSet1);
            expected.getNamedSubqueriesNotNull().add(nsr);

            {
                final ProjectionNode projection = new ProjectionNode();
                nsr.setProjection(projection);

                projection.setDistinct(true);
                projection.addProjectionVar(new VarNode("_var3"));
            }

            final JoinGroupNode whereClause = new JoinGroupNode();
            nsr.setWhereClause(whereClause);

            whereClause.addChild(new StatementPatternNode(new VarNode("_var3"),
                    new ConstantNode(a), new ConstantNode(politician),
                    null/* c */, Scope.DEFAULT_CONTEXTS));

            whereClause.addChild(new StatementPatternNode(new VarNode("_var3"),
                    new ConstantNode(hasRole), new VarNode("_var6"),
                    null/* c */, Scope.DEFAULT_CONTEXTS));

            whereClause.addChild(new StatementPatternNode(new VarNode("_var6"),
                    new ConstantNode(party), new ConstantNode(democrat),
                    null/* c */, Scope.DEFAULT_CONTEXTS));

        }

        // Main Query
        {
            final ProjectionNode projection = new ProjectionNode();
            expected.setProjection(projection);

            projection
                    .addProjectionExpression(new AssignmentNode(//
                            new VarNode("_var1"),// var
                            new FunctionNode(
                                    // expr
                                    FunctionRegistry.SAMPLE,
                                    Collections.singletonMap(
                                            AggregateBase.Annotations.DISTINCT,
                                            (Object) Boolean.FALSE)/* scalarValues */,
                                    new ValueExpressionNode[] { new VarNode(
                                            "_var9") })));
            projection.addProjectionVar(new VarNode("_var2"));
            projection.addProjectionVar(new VarNode("_var3"));

            final JoinGroupNode whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);

            whereClause.addChild(new NamedSubqueryInclude(namedSet1));

            final JoinGroupNode simpleOptGroup = new JoinGroupNode(
                    true/* optional */, new StatementPatternNode(new VarNode(
                            "_var3"), new ConstantNode(name), new VarNode(
                            "_var9"), null/* c */, Scope.DEFAULT_CONTEXTS));
            whereClause.addChild(simpleOptGroup);
            simpleOptGroup.setJoinVars(joinVars);
            simpleOptGroup.setProjectInVars(joinVars);

            final JoinGroupNode complexOptGroup = new JoinGroupNode(true/* optional */);
            whereClause.addChild(complexOptGroup);
            {
                complexOptGroup.addChild(new StatementPatternNode(new VarNode(
                        "_var10"), new ConstantNode(votedBy), new VarNode(
                        "_var3"), null/* c */, Scope.DEFAULT_CONTEXTS));

                complexOptGroup.addChild(new StatementPatternNode(new VarNode(
                        "_var10"), new ConstantNode(rdfsLabel), new VarNode(
                        "_var2"), null/* c */, Scope.DEFAULT_CONTEXTS));
                
                complexOptGroup.setJoinVars(joinVars);
                complexOptGroup.setProjectInVars(joinVars);
            }

            final GroupByNode groupByNode = new GroupByNode();
            expected.setGroupBy(groupByNode);
            groupByNode.addGroupByVar(new VarNode("_var2"));
            groupByNode.addGroupByVar(new VarNode("_var3"));

        }

        final IASTOptimizer rewriter = new ASTSubGroupJoinVarOptimizer();

        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
              new QueryNodeWithBindingSet(given, bsets)).getQueryNode();

        assertSameAST(expected, actual);

    }

}
