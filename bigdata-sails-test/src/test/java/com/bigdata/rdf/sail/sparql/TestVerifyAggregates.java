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
 * Created on Jul 27, 2011
 */

package com.bigdata.rdf.sail.sparql;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase2;

import com.bigdata.bop.aggregate.AggregateBase.Annotations;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.FunctionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.bigdata.rdf.sparql.ast.GroupByNode;
import com.bigdata.rdf.sparql.ast.HavingNode;
import com.bigdata.rdf.sparql.ast.IValueExpressionNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.VarNode;

/**
 * Test suite for {@link VerifyAggregates}.
 * 
 * Note: This is a port of {@link com.bigdata.bop.solutions.TestGroupByState} that
 * does not depend on the blazegraph operator model. It was developed as part of
 * BLZG-1176 to decouple the SPARQL parser from the database.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 *
 * @see https://jira.blazegraph.com/browse/BLZG-1176
 */
public class TestVerifyAggregates extends TestCase2 {

    /**
     * 
     */
    public TestVerifyAggregates() {
    }

    /**
     * @param name
     */
    public TestVerifyAggregates(final String name) {
        super(name);
    }
    
    private GlobalAnnotations globals;
    
    @Override
    protected void setUp() throws Exception {

        super.setUp();
        
        globals = new GlobalAnnotations(getName(), ITx.READ_COMMITTED);
    }
    
    @Override
    protected void tearDown() throws Exception {
        
        globals = null;
        
        super.tearDown();
        
    }

    /**
     * Unit test with SELECT clause having one value expression, which is a
     * simple variable also appearing as the sole value expression in the
     * GROUP_BY clause.
     * <pre>
     * SELECT ?org
     * GROUP BY ?org
     * </pre>
     */
    public void test_aggregateExpr_01() {
        
        final VarNode org = new VarNode("org");
        
        final ProjectionNode select = new ProjectionNode();
        select.addProjectionVar(org);

        final GroupByNode groupBy = new GroupByNode();
        groupBy.addGroupByVar(org);

        final HavingNode having = null;

        new VerifyAggregates(select, groupBy, having);
        
    }

    /**
     * Unit test with SELECT clause having one value expression, which is a
     * simple variable also appearing as the sole value expression in the
     * GROUP_BY clause. However, in this case we rename the variable when it is
     * projected out of the SELECT expression.
     * 
     * <pre>
     * SELECT ?org as ?newVar
     * GROUP BY ?org
     * </pre>
     */
    public void test_aggregateExpr_02() {
        
        final VarNode org = new VarNode("org");
        final VarNode newVar = new VarNode("newVar");
        
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final ProjectionNode select = new ProjectionNode();
        select.addProjectionExpression(new AssignmentNode(newVar, org));

        final GroupByNode groupBy = new GroupByNode();
        groupBy.addGroupByVar(org);

        final HavingNode having = null;

        new VerifyAggregates(select, groupBy, having);

    }

    /**
     * Unit test with simple aggregate function in SELECT clause.
     * <pre>
     * SELECT ?org, SUM(?lprice) AS ?totalPrice
     * GROUP BY ?org
     * </pre>
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_simpleAggregate() {
        
        final VarNode org = new VarNode("org");
        final VarNode lprice = new VarNode("lprice");
        final VarNode totalPrice = new VarNode("totalPrice");
        
        final IValueExpressionNode totalPriceExprNode = new FunctionNode(
                FunctionRegistry.SUM, null, lprice);
        
        final ProjectionNode select = new ProjectionNode();
        select.addProjectionVar(org);
        select.addProjectionExpression(new AssignmentNode(totalPrice, totalPriceExprNode));

        final GroupByNode groupBy = new GroupByNode();
        groupBy.addGroupByVar(org);

        final HavingNode having = null;

        new VerifyAggregates(select, groupBy, having);
        
    }

    /**
     * Unit test with simple aggregate function in SELECT clause and no GROUP BY
     * clause (the aggregation is taken across all solutions as if they were a
     * single group).
     * 
     * <pre>
     * SELECT SUM(?lprice) AS ?totalPrice
     * </pre>
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_simpleAggregate_noGroupBy() {
        
        final VarNode lprice = new VarNode("lprice");
        final VarNode totalPrice = new VarNode("totalPrice");
        
        final IValueExpressionNode totalPriceExprNode = new FunctionNode(
                FunctionRegistry.SUM, null, lprice);
        
        final ProjectionNode select = new ProjectionNode();
        select.addProjectionExpression(new AssignmentNode(totalPrice, totalPriceExprNode));

        final GroupByNode groupBy = null;

        final HavingNode having = null;

        new VerifyAggregates(select, groupBy, having);
        
    }

    /**
     * Unit test for references to aggregate declared in GROUP_BY with AS.
     * <pre>
     * SELECT ?org2
     * GROUP BY UCASE(?org) as ?org2
     * </pre>
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_aggregateExpr_03() {
        
        // Note: UcaseBOp is what we need here.
        final VarNode org = new VarNode("org");
        final VarNode org2 = new VarNode("org2");

        final IValueExpressionNode ucaseExpr = new FunctionNode(FunctionRegistry.UCASE, null, org);

        final ProjectionNode select = new ProjectionNode();
        select.addProjectionVar(org2);

        final GroupByNode groupBy = new GroupByNode();
        groupBy.addExpr(new AssignmentNode(org2, ucaseExpr));

        final HavingNode having = null;

        new VerifyAggregates(select, groupBy, having);

    }

    /**
     * Unit test verifies that a constant within a group by clause does not
     * cause the group by clause to be interpreted as an aggregate.
     * 
     * <pre>
     * select ?index
     * group by (?o + 1 AS ?index)
     * </pre>
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_aggregateExpr_04() {
        
        final VarNode index = new VarNode("index");
        final VarNode o = new VarNode("o");

        final IValueExpressionNode mathExpr = FunctionNode.add(
                o,
                new ConstantNode(new XSDNumericIV(1)));

        final ProjectionNode select = new ProjectionNode();
        select.addProjectionVar(index);

        final GroupByNode groupBy = new GroupByNode();
        groupBy.addExpr(new AssignmentNode(index, mathExpr));

        final HavingNode having = null;

        new VerifyAggregates(select, groupBy, having);

    }
    
    /**
     * <pre>
     * SELECT SUM(?y) as ?x
     * GROUP BY ?z
     * HAVING ?x > 10
     * </pre>
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_simpleHavingClause() {
        
        final VarNode y = new VarNode("y");
        final VarNode x = new VarNode("x");
        final VarNode z = new VarNode("z");

        final IValueExpressionNode xExpr = new FunctionNode(
                FunctionRegistry.SUM, null, y);

        final ProjectionNode select = new ProjectionNode();
        select.addExpr(new AssignmentNode(x, xExpr));

        final GroupByNode groupBy = new GroupByNode();
        groupBy.addGroupByVar(z);

        final HavingNode having = new HavingNode();
        having.addExpr(FunctionNode.GT(
                x,
                new ConstantNode(new XSDNumericIV(10))
            ));

        new VerifyAggregates(select, groupBy, having);

    }

    /**
     * <pre>
     * SELECT SUM(?y) as ?x
     * GROUP BY ?z
     * HAVING SUM(?y) > 10
     * </pre>
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_complexHavingClause() {

        final VarNode y = new VarNode("y");
        final VarNode x = new VarNode("x");
        final VarNode z = new VarNode("z");

        final FunctionNode xExpr = new FunctionNode(
                FunctionRegistry.SUM, null, y);

        final ProjectionNode select = new ProjectionNode();
        select.addExpr(new AssignmentNode(x, xExpr));

        final GroupByNode groupBy = new GroupByNode();
        groupBy.addGroupByVar(new VarNode(z));

        final HavingNode having = new HavingNode();
        having.addExpr(FunctionNode.GT(
                xExpr,
                new ConstantNode(new XSDNumericIV(10))
            ));

        new VerifyAggregates(select, groupBy, having);

    }

    /**
     * <pre>
     * SELECT SUM(?x+MIN(?y)) as ?z
     * GROUP BY ?a
     * </pre>
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_nestedAggregates() {

        final VarNode a = new VarNode("a");

        final VarNode x = new VarNode("x");
        final VarNode y = new VarNode("y");
        final VarNode z = new VarNode("z");

        final IValueExpressionNode zExpr = new FunctionNode(FunctionRegistry.SUM, null,
            FunctionNode.add(x, new FunctionNode(
                FunctionRegistry.MIN, null, y)));

        final ProjectionNode select = new ProjectionNode();
        select.addExpr(new AssignmentNode(z, zExpr));

        final GroupByNode groupBy = new GroupByNode();
        groupBy.addGroupByVar(a);

        final HavingNode having = null;

        new VerifyAggregates(select, groupBy, having);

    }

    /**
     * Verify that a reference to a variable defined by a previous select
     * expression is allowed and that the select dependency is recognized.
     * 
     * <pre>
     * SELECT SUM(?y) as ?z, SUM(?x)+?z as ?a 
     * GROUP BY ?b
     * </pre>
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_reverseReference_allowed_aka_select_dependency() {

        final VarNode a = new VarNode("a");
        final VarNode b = new VarNode("b");
        final VarNode x = new VarNode("x");
        final VarNode y = new VarNode("y");
        final VarNode z = new VarNode("z");

        final IValueExpressionNode zExpr = new FunctionNode(FunctionRegistry.SUM, null,y);

        final IValueExpressionNode aExpr = 
                FunctionNode.add(new FunctionNode(FunctionRegistry.SUM, null,x), z);

        final ProjectionNode select = new ProjectionNode();
        select.addExpr(new AssignmentNode(z, zExpr));
        select.addExpr(new AssignmentNode(a, aExpr));

        final GroupByNode groupBy = new GroupByNode();
        groupBy.addGroupByVar(b);

        final HavingNode having = null;

        new VerifyAggregates(select, groupBy, having);

    }

    /**
     * Forward references to a variable are not allowed.
     * 
     * <pre>
     * SELECT SUM(?x)+?z as ?a, SUM(?y) as ?z ... (forward reference to z)
     * </pre>
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void test_forwardReference_not_allowed() {

        final VarNode a = new VarNode("a");
        final VarNode b = new VarNode("b");
        final VarNode x = new VarNode("x");
        final VarNode y = new VarNode("y");
        final VarNode z = new VarNode("z");

        final IValueExpressionNode zExpr = new FunctionNode(FunctionRegistry.SUM, null,y);

        final IValueExpressionNode aExpr = 
                FunctionNode.add(new FunctionNode(FunctionRegistry.SUM, null,x), z);

        final ProjectionNode select = new ProjectionNode();
        select.addExpr(new AssignmentNode(a, aExpr));
        select.addExpr(new AssignmentNode(z, zExpr));

        final GroupByNode groupBy = new GroupByNode();
        groupBy.addGroupByVar(b);

        final HavingNode having = null;

        try {
            new VerifyAggregates(select, groupBy, having);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (final IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex,ex);
        }

    }
    
    /**
     * Unit test for {@link IGroupByState#isAnyDistinct()) where the DISTINCT
     * keyword appears within an {@link IAggregate} in the SELECT clause.
     * <pre>
     * SELECT SUM(DISTINCT ?y) as ?x
     * GROUP BY ?z
     * HAVING ?x > 10
     * </pre>
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_isAnyDistinct_select() {
        
        final VarNode y = new VarNode("y");
        final VarNode x = new VarNode("x");
        final VarNode z = new VarNode("z");

        final Map<String, Object> scalarValues = new HashMap<>();
        scalarValues.put(Annotations.DISTINCT, Boolean.TRUE);
        final IValueExpressionNode xExpr = new FunctionNode(FunctionRegistry.SUM, scalarValues, y);

        final ProjectionNode select = new ProjectionNode();
        select.addExpr(new AssignmentNode(x, xExpr));

        final GroupByNode groupBy = new GroupByNode();
        groupBy.addGroupByVar(z);

        final HavingNode having = new HavingNode();
        having.addExpr(FunctionNode.GT(
                x,
                new ConstantNode(new XSDNumericIV(10))
            ));

        new VerifyAggregates(select, groupBy, having);

    }

    /**
     * Unit test for {@link IGroupByState#isAnyDistinct()) where the DISTINCT
     * keyword appears within an {@link IAggregate} in the SELECT clause.
     * <pre>
     * SELECT SUM(?y) as ?x
     * GROUP BY ?z
     * HAVING SUM(DISTINCT ?y) > 10
     * </pre>
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_isAnyDistinct_having() {
        
        final VarNode y = new VarNode("y");
        final VarNode x = new VarNode("x");
        final VarNode z = new VarNode("z");

        final IValueExpressionNode xExpr = new FunctionNode(FunctionRegistry.SUM, null, y);

        final ProjectionNode select = new ProjectionNode();
        select.addExpr(new AssignmentNode(x, xExpr));

        final GroupByNode groupBy = new GroupByNode();
        groupBy.addGroupByVar(z);

        final HavingNode having = new HavingNode();
        final Map<String, Object> scalarValues = new HashMap<>();
        scalarValues.put(Annotations.DISTINCT, Boolean.TRUE);
        having.addExpr(FunctionNode.GT(
                new FunctionNode(FunctionRegistry.SUM, scalarValues, y),
                new ConstantNode(new XSDNumericIV(10))
            ));

        new VerifyAggregates(select, groupBy, having);

    }
    
    /**
     * Unit test when projecting a constant
     * <pre>
     * SELECT 12 as ?x
     * </pre>
     */
    public void test_with_constant() {

        @SuppressWarnings({ "unchecked", "rawtypes" })
        final VarNode x = new VarNode("x");

        @SuppressWarnings({ "rawtypes", "unchecked" })
        final ConstantNode xExpr = new ConstantNode(
                new XSDNumericIV(12));

        final ProjectionNode select = new ProjectionNode();
        select.addExpr(new AssignmentNode(x, xExpr));

        final GroupByNode groupBy = null;

        final HavingNode having = null;

        new VerifyAggregates(select, groupBy, having);

    }

    /**
     * Unit test for bad arguments.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_correctRejection() {

        final VarNode y = new VarNode("y");
        final VarNode x = new VarNode("x");
        final VarNode z = new VarNode("z");

        final GroupByNode groupBy = new GroupByNode();
        groupBy.addGroupByVar(z);

        final HavingNode having = new HavingNode();
        having.addExpr(FunctionNode.LT(
                new FunctionNode(FunctionRegistry.SUM, null, y),
                new ConstantNode(new XSDNumericIV(10))
            ));

        // SELECT may not be null.
        try {
            new VerifyAggregates(null/* select */, groupBy, having);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (final IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex, ex);
        }

        // SELECT may not be empty.
        try {
            new VerifyAggregates(new ProjectionNode()/* select */, groupBy,
                    having);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (final IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex, ex);
        }
        
    }
    
}
