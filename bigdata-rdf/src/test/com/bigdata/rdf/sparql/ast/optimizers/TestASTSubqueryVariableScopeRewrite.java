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
 * Created on Sep 1, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import org.openrdf.query.algebra.StatementPattern.Scope;

import com.bigdata.rdf.sail.QueryType;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.SubqueryRoot;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Test suite for {@link ASTSubqueryVariableScopeRewrite}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestASTVariableScopeAnalysis.java 5119 2011-09-01 23:02:35Z
 *          thompsonbry $
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/232
 */
public class TestASTSubqueryVariableScopeRewrite extends AbstractASTEvaluationTestCase {

    public TestASTSubqueryVariableScopeRewrite() {
        super();
    }

    public TestASTSubqueryVariableScopeRewrite(String name) {
        super(name);
    }

    /**
     * Unit test verifies that BIND(?x as ?y) in a subquery causes the variable
     * ?x to be replaced by ?y within the subquery. This allows solutions
     * flowing through the parent group to propagate bindings into the subquery
     * for [x] whereas without this rewrite the subquery would run without those
     * bindings and then reject solutions which were not consistent only after
     * they were projected.
     * 
     * Given:
     * 
     * <pre>
     * SELECT ?y where {{SELECT (?x as ?y) where {?x ?p ?o}}}
     * </pre>
     * 
     * Rewrite:
     * 
     * <pre>
     * SELECT ?y where {{SELECT (?y as ?y) where {?y ?-alias-p-1 ?-alias-o-2}}}
     * </pre>
     */
    public void test_rename_bind_x_as_y_in_subquery() {

        /*
         * Note: DO NOT SHARE AST STRUCTURES IN THIS TEST! 
         */
//        final VarNode x = new VarNode("x");
//        final VarNode y = new VarNode("y");
//        final VarNode p = new VarNode("p");
//        final VarNode o = new VarNode("o");

        final QueryRoot queryRoot = new QueryRoot(QueryType.SELECT);
        {

            final SubqueryRoot subSelect;
            {
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("x"));
                queryRoot.setProjection(projection);

                final JoinGroupNode whereClause = new JoinGroupNode();
                queryRoot.setWhereClause(whereClause);

                subSelect = new SubqueryRoot(QueryType.SELECT);
                // whereClause.addChild(subSelect);

                final JoinGroupNode wrapperGroup = new JoinGroupNode();
                whereClause.addChild(wrapperGroup);
                wrapperGroup.addChild(subSelect);
            }
            {

                final ProjectionNode projection2 = new ProjectionNode();
                projection2.addExpr(new AssignmentNode(new VarNode("x"),
                        new VarNode("y")));
                subSelect.setProjection(projection2);

                final JoinGroupNode whereClause2 = new JoinGroupNode();
                subSelect.setWhereClause(whereClause2);

                whereClause2.addChild(new StatementPatternNode(
                        new VarNode("x"), new VarNode("p"), new VarNode("o"),
                        null/* c */, Scope.DEFAULT_CONTEXTS));

            }
 
        }

        final QueryRoot expected = new QueryRoot(QueryType.SELECT);
        {

            final SubqueryRoot subSelect;
            {
                final ProjectionNode projection = new ProjectionNode();
                projection.addProjectionVar(new VarNode("x"));
                expected.setProjection(projection);

                final JoinGroupNode whereClause = new JoinGroupNode();
                expected.setWhereClause(whereClause);

                subSelect = new SubqueryRoot(QueryType.SELECT);
                // whereClause.addChild(subSelect);

                final JoinGroupNode wrapperGroup = new JoinGroupNode();
                whereClause.addChild(wrapperGroup);
                wrapperGroup.addChild(subSelect);
            }
            {

                final ProjectionNode projection2 = new ProjectionNode();
                projection2.addExpr(new AssignmentNode(new VarNode("y"),
                        new VarNode("y")));
                subSelect.setProjection(projection2);

                final JoinGroupNode whereClause2 = new JoinGroupNode();
                subSelect.setWhereClause(whereClause2);

                whereClause2.addChild(new StatementPatternNode(
                        new VarNode("y"), new VarNode("-alias-p-0"),
                        new VarNode("-alias-o-1"), null/* c */,
                        Scope.DEFAULT_CONTEXTS));

            }
 
        }

        final AST2BOpContext context = new AST2BOpContext(queryRoot, store);

        final QueryRoot actual = (QueryRoot) new ASTSubqueryVariableScopeRewrite()
                .optimize(context, queryRoot, null/* bindingSets */);

        assertSameAST(expected, actual);

    }

//    /**
//     * In this query, the ?x projected from the first subquery should not be
//     * evaluated within/joined with the ?x within the second subquery, since the
//     * ?x within the second subquery is local to the subquery since it is not
//     * projected. This query would only join on ?s, since it is the only common
//     * var between the 2 subqueries.
//     */
//    public void test_rename_variables_not_projected()
//            throws MalformedQueryException {
//
//        final String queryStr = ""
//                + "PREFIX : <http://example.org/>\n"
//                + "SELECT ?s ?x\n"
//                + "WHERE {\n"
//                + "     {\n"
//                + "        SELECT ?s ?x { ?s :p ?x }\n"
//                + "     }\n"
//                + "     {\n" // LET ( ?x := expr ) == BIND( expr AS ?x )
////                + "        SELECT ?s ?fake1 ?fake2 { ?x :q ?s . BIND(1 AS ?fake1) . BIND(2 AS ?fake2) . }\n"
//                + "        SELECT ?s ?fake1 ?fake2 { ?x :q ?s . LET(?fake1 := 1) . LET(?fake2 := 2) . }\n"
//                + "     }\n" //
//                + "}\n"//
//        ;
//
//        final QueryRoot queryRoot = new Bigdata2ASTSPARQLParser(store)
//                .parseQuery2(queryStr, null/* baseURI */);
//
//        final AST2BOpContext context = new AST2BOpContext(queryRoot, store);
//
//        final QueryRoot actual = (QueryRoot) new ASTVariableScopeAnalysis()
//                .optimize(context, queryRoot, null/* bindingSets */);
//
//        if (log.isInfoEnabled())
//            log.info(queryRoot);
//        
//        fail("write test");
//
//    }

}
