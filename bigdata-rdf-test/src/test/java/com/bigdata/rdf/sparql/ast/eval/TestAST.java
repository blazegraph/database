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
 * Created on Sep 7, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import com.bigdata.rdf.internal.constraints.INeedsMaterialization;

/**
 * Port of unit tests originally written at the AST layer.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAST extends AbstractDataDrivenSPARQLTestCase {

    /**
     *
     */
    public TestAST() {
    }

    /**
     * @param name
     */
    public TestAST(String name) {
        super(name);
    }

    /**
     * <pre>
     * select ?s where { ?s ?p ?o }
     * </pre>
     *
     * Note: This is a port of TestASTTriplesModeEvaluation#testAST() (as are
     * all of the ast_01x methods).
     */
    public void test_ast_01() throws Exception {

        new TestHelper("ast_01").runTest();

    }

    /**
     * <pre>
     * select ?s where { ?s ?p ?o } order by ?s limit 1
     * </pre>
     *
     * Note: I had to add an ORDER BY into this method in order to make the
     * results stable. The original unit test only checked the #of results but
     * not their data.
     */
    public void test_ast_01b() throws Exception {

        new TestHelper("ast_01b").runTest();

    }

    /**
     * <pre>
     * select DISTINCT ?s where { ?s ?p ?o }
     * </pre>
     */
    public void test_ast_01c() throws Exception {

        new TestHelper("ast_01c").runTest();

    }

    /**
     * <pre>
     * select (count(*) as ?count) where { ?s ?p ?o } limit 1
     * </pre>
     */
    public void test_ast_01d() throws Exception {

        new TestHelper("ast_01d").runTest();

    }

    /**
     * <pre>
     * select ?s ?o
     * where {
     *    ?s rdf:type :C .
     *    OPTIONAL {
     *       ?s :B ?o
     *    }
     * }
     * </pre>
     *
     * Note: This is a port of
     * TestASTTriplesModeEvaluation#testOptionalDistinct() (as are all of the
     * ast_02x methods).
     */
    public void test_ast_02() throws Exception {

        new TestHelper("ast_02").runTest();

    }

    /**
     * <pre>
     * select DISTINCT ?s ?o
     * where {
     *    ?s rdf:type :C .
     *    OPTIONAL {
     *       ?s :B ?o
     *    }
     * }
     * </pre>
     */
    public void test_ast_02b() throws Exception {

        new TestHelper("ast_02b").runTest();

    }

    /**
     * <pre>
     * select ?index
     * where {
     *   ?s rdf:type :C .
     *   ?s :predicate1 ?o .
     * }
     * GROUP BY (?o+1 as ?index)
     * </pre>
     *
     * Note: This is a port of
     * TestASTTriplesModeEvaluation#testProjectedGroupWithConstant().
     */
    public void test_ast_03() throws Exception {

        new TestHelper("ast_03").runTest();

    }

    /**
     * Unit test developed to demonstrate a problem where the incorrect
     * materialization requirements are computed.
     * <p>
     * I just happened to run into an issue dealing with materialization. Say I
     * have a constraint that needs materialization, <code>Str(...)</code> If
     * that constraint has as its arg another constraint:
     * <code>Coalesce (?x,?y)</code> [this returns the first none null value in
     * the list of ?x,?y]
     * <p>
     * Its seems that the logic that determines the extra materialization steps
     * doesn't handle the fact that Coalesce (?x,?y) doesn't need any
     * materialization, so never sets the {@link INeedsMaterialization}
     * interface, so the result of that valueExpression is going to be either ?x
     * or ?y, and Str does need those variables materialized, but the
     * materialization steps would never have seem that lists of terms(?x,?y) as
     * something that needs materialization. Does the fact that a
     * ValueExpression returns a non computed IV value, ie returns one of its
     * arguments, have to be captured in order to somehow handle this?
     *
     * <pre>
     *
     * QueryType: SELECT
     * SELECT VarNode(index)
     *   JoinGroupNode {
     *     StatementPatternNode(VarNode(s), ConstantNode(TermId(4U)), ConstantNode(TermId(2U)), DEFAULT_CONTEXTS)
     *     StatementPatternNode(VarNode(s), ConstantNode(TermId(5U)), VarNode(o), DEFAULT_CONTEXTS)
     *     ( com.bigdata.rdf.sparql.ast.ValueExpressionNode()[ valueExpr=com.bigdata.rdf.internal.constraints.StrBOp(com.bigdata.rdf.internal.constraints.CoalesceBOp(s,o))[ com.bigdata.rdf.internal.constraints.StrBOp.namespace=kb]] AS VarNode(index) )
     *   }
     * </pre>
     *
     * Note: This is a port of
     * TestASTTriplesModeEvaluation#testProjectedGroupByWithNestedVars()
     */
    public void test_ast_04() throws Exception {

        new TestHelper("ast_04").runTest();

    }

    public void test_materialization_extensions() throws Exception {

    	new TestHelper("materialization-extensions").runTest();

    }
}
