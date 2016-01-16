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

package com.bigdata.bop.solutions;

import java.util.LinkedHashSet;

import junit.framework.TestCase2;

import org.openrdf.query.algebra.Compare.CompareOp;

import com.bigdata.bop.Bind;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.bop.rdf.aggregate.MIN;
import com.bigdata.bop.rdf.aggregate.SUM;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.CompareBOp;
import com.bigdata.rdf.internal.constraints.MathBOp;
import com.bigdata.rdf.internal.constraints.MathBOp.MathOp;
import com.bigdata.rdf.internal.constraints.SPARQLConstraint;
import com.bigdata.rdf.internal.constraints.UcaseBOp;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

/**
 * Test suite for {@link GroupByState}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestGroupByState extends TestCase2 {

    /**
     * 
     */
    public TestGroupByState() {
    }

    /**
     * @param name
     */
    public TestGroupByState(String name) {
        super(name);
    }
    
    private GlobalAnnotations globals;
    
    protected void setUp() throws Exception {

        super.setUp();
        
        globals = new GlobalAnnotations(getName(), ITx.READ_COMMITTED);
    }
    
    protected void tearDown() throws Exception {
        
        globals = null;
        
        super.tearDown();
        
    }

    private static class MockGroupByState implements IGroupByState {

        final IValueExpression<?>[] groupBy;
        final LinkedHashSet<IVariable<?>> groupByVars;
        final IValueExpression<?>[] select;
        final LinkedHashSet<IVariable<?>> selectVars;
        final IConstraint[] having;
        final LinkedHashSet<IVariable<?>> columnVars;
//        final LinkedHashSet<IVariable<?>> distinctColumnVars;
        final boolean anyDistinct;
        final boolean selectDependency;
        final boolean nestedAggregates;
        final boolean simpleHaving;

        MockGroupByState(//
                final IValueExpression<?>[] groupBy,
                final LinkedHashSet<IVariable<?>> groupByVars,
                final IValueExpression<?>[] select,
                final LinkedHashSet<IVariable<?>> selectVars,
                final IConstraint[] having,//
                final LinkedHashSet<IVariable<?>> columnVars,//
//                final LinkedHashSet<IVariable<?>> distinctColumnVars,//
                final boolean anyDistinct,//
                final boolean selectDependency,//
                final boolean nestedAggregates,//
                final boolean simpleHaving//
                ) {
            this.groupBy = groupBy;
            this.groupByVars = groupByVars;
            this.select = select;
            this.selectVars = selectVars;
            this.having = having;
            this.columnVars = columnVars;
//            this.distinctColumnVars = distinctColumnVars;
            this.anyDistinct = anyDistinct;
            this.selectDependency = selectDependency;
            this.nestedAggregates = nestedAggregates;
            this.simpleHaving = simpleHaving;
        }
        
        public IValueExpression<?>[] getGroupByClause() {
            return groupBy;
        }

        public LinkedHashSet<IVariable<?>> getGroupByVars() {
            return groupByVars;
        }

        public IValueExpression<?>[] getSelectClause() {
            return select;
        }

        public LinkedHashSet<IVariable<?>> getSelectVars() {
            return selectVars;
        }
        
        public IConstraint[] getHavingClause() {
            return having;
        }

        public LinkedHashSet<IVariable<?>> getColumnVars() {
            return columnVars;
        }
        
//        public LinkedHashSet<IVariable<?>> getDistinctColumnVars() {
//            return distinctColumnVars;
//        }
        
        public boolean isAnyDistinct() {
            return anyDistinct;
        }

        public boolean isSelectDependency() {
            return selectDependency;
        }

        public boolean isNestedAggregates() {
            return nestedAggregates;
        }
        
        public boolean isSimpleHaving() {
            return simpleHaving;
        }

    }

    static void assertSameState(final IGroupByState expected,
            final IGroupByState actual) {

        assertEquals("groupByClause", expected.getGroupByClause(),
                actual.getGroupByClause());

        assertEquals("groupByVars", expected.getGroupByVars(),
                actual.getGroupByVars());

        assertEquals("selectClause", expected.getSelectClause(),
                actual.getSelectClause());

        assertEquals("selectVars", expected.getSelectVars(),
                actual.getSelectVars());

        assertEquals("havingClause", expected.getHavingClause(),
                actual.getHavingClause());

        assertEquals("columnVars", expected.getColumnVars(),
                actual.getColumnVars());

//        assertEquals("distinctColumnVars", expected.getDistinctColumnVars(),
//                actual.getDistinctColumnVars());

        assertEquals("anyDistinct", expected.isAnyDistinct(),
                actual.isAnyDistinct());
        
        assertEquals("selectDependency", expected.isSelectDependency(),
                actual.isSelectDependency());
        
        assertEquals("nestedAggregates", expected.isNestedAggregates(),
                actual.isNestedAggregates());
        
        assertEquals("simpleHaving", expected.isSimpleHaving(),
                actual.isSimpleHaving());
        
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
        
        final IVariable<?> org = Var.var("org");
        
        final IValueExpression<?>[] select = new IValueExpression[] { org };

        final IValueExpression<?>[] groupBy = new IValueExpression[] { org };

        final IConstraint[] having = null;

        final LinkedHashSet<IVariable<?>> groupByVars = new LinkedHashSet<IVariable<?>>();
        groupByVars.add(org);

        final LinkedHashSet<IVariable<?>> selectVars = new LinkedHashSet<IVariable<?>>();
        selectVars.add(org);

        final LinkedHashSet<IVariable<?>> columnVars = new LinkedHashSet<IVariable<?>>();

        final MockGroupByState expected = new MockGroupByState(groupBy,
                groupByVars, select, selectVars, having, columnVars,
                false/* anyDistinct */, false/* selectDependency */,
                false/* nestedAggregates */, true/* simpleHaving */);

        final IGroupByState actual = new GroupByState(select, groupBy, having);

        assertSameState(expected, actual);
        
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
        
        final IVariable<?> org = Var.var("org");
        final IVariable<?> newVar = Var.var("newVar");
        
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final IValueExpression<?>[] select = new IValueExpression[] { new Bind(
                newVar, org) };

        final IValueExpression<?>[] groupBy = new IValueExpression[] { org };

        final IConstraint[] having = null;

        final LinkedHashSet<IVariable<?>> groupByVars = new LinkedHashSet<IVariable<?>>();
        groupByVars.add(org);

        final LinkedHashSet<IVariable<?>> selectVars = new LinkedHashSet<IVariable<?>>();
        selectVars.add(newVar);

        final LinkedHashSet<IVariable<?>> columnVars = new LinkedHashSet<IVariable<?>>();

        final MockGroupByState expected = new MockGroupByState(groupBy,
                groupByVars, select, selectVars, having, columnVars,
                false/* anyDistinct */, false/* selectDependency */,
                false/* nestedAggregates */, true/* simpleHaving */);

        final IGroupByState actual = new GroupByState(select, groupBy, having);

        assertSameState(expected, actual);
        
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
        
        final IVariable<?> org = Var.var("org");
        final IVariable<?> lprice = Var.var("lprice");
        final IVariable<?> totalPrice = Var.var("totalPrice");
        
        final IValueExpression<?> totalPriceExpr = new /* Conditional */Bind(
                totalPrice, new SUM(false/* distinct */,
                        (IValueExpression<IV>) lprice));
        
        final IValueExpression<?>[] select = new IValueExpression[] { org, totalPriceExpr };

        final IValueExpression<?>[] groupBy = new IValueExpression[] { org };

        final IConstraint[] having = null;

        final LinkedHashSet<IVariable<?>> groupByVars = new LinkedHashSet<IVariable<?>>();
        groupByVars.add(org);

        final LinkedHashSet<IVariable<?>> selectVars = new LinkedHashSet<IVariable<?>>();
        selectVars.add(org);
        selectVars.add(totalPrice);

        final LinkedHashSet<IVariable<?>> columnVars = new LinkedHashSet<IVariable<?>>();
        columnVars.add(lprice);

        final MockGroupByState expected = new MockGroupByState(groupBy,
                groupByVars, select, selectVars, having, columnVars,
                false/* anyDistinct */, false/* selectDependency */,
                false/* nestedAggregates */, true/* simpleHaving */);

        final IGroupByState actual = new GroupByState(select, groupBy, having);

        assertSameState(expected, actual);
        
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
        
        final IVariable<?> lprice = Var.var("lprice");
        final IVariable<?> totalPrice = Var.var("totalPrice");
        
        final IValueExpression<?> totalPriceExpr = new /* Conditional */Bind(
                totalPrice, new SUM(false/* distinct */,
                        (IValueExpression<IV>) lprice));
        
        final IValueExpression<?>[] select = new IValueExpression[] { totalPriceExpr };

        final IValueExpression<?>[] groupBy = null;

        final IConstraint[] having = null;

        final LinkedHashSet<IVariable<?>> groupByVars = new LinkedHashSet<IVariable<?>>();

        final LinkedHashSet<IVariable<?>> selectVars = new LinkedHashSet<IVariable<?>>();
        selectVars.add(totalPrice);

        final LinkedHashSet<IVariable<?>> columnVars = new LinkedHashSet<IVariable<?>>();
        columnVars.add(lprice);

        final MockGroupByState expected = new MockGroupByState(groupBy,
                groupByVars, select, selectVars, having, columnVars,
                false/* anyDistinct */, false/* selectDependency */,
                false/* nestedAggregates */, true/* simpleHaving */);

        final IGroupByState actual = new GroupByState(select, groupBy, having);

        assertSameState(expected, actual);
        
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
        final IVariable<IV<?,?>> org = Var.var("org");
        final IVariable<IV<?,?>> org2 = Var.var("org2");
        
        // The namespace of the lexicon relation.
        final String namespace = "kb.lex"; 

        final IValueExpression<?> ucaseExpr = new Bind(org2, new UcaseBOp(org,
                globals));

        final IValueExpression<?>[] select = new IValueExpression[] { org2 };

        final IValueExpression<?>[] groupBy = new IValueExpression[] { ucaseExpr };

        final IConstraint[] having = null;

        final LinkedHashSet<IVariable<?>> groupByVars = new LinkedHashSet<IVariable<?>>();
        groupByVars.add(org2);

        final LinkedHashSet<IVariable<?>> selectVars = new LinkedHashSet<IVariable<?>>();
        selectVars.add(org2);

        final LinkedHashSet<IVariable<?>> columnVars = new LinkedHashSet<IVariable<?>>();

        final MockGroupByState expected = new MockGroupByState(groupBy,
                groupByVars, select, selectVars, having, columnVars,
                false/* anyDistinct */, false/* selectDependency */,
                false/* nestedAggregates */, true/* simpleHaving */);

        final IGroupByState actual = new GroupByState(select, groupBy, having);

        assertSameState(expected, actual);

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
        
        final IVariable<IV<?,?>> index = Var.var("index");
        final IVariable<IV<?,?>> o = Var.var("o");

        final IValueExpression<?> mathExpr = new Bind(index, //
                new MathBOp(o, new Constant(new XSDNumericIV(1)),
                        MathBOp.MathOp.PLUS,globals));

        final IValueExpression<?>[] select = new IValueExpression[] { new Bind(
                index, index) };

        final IValueExpression<?>[] groupBy = new IValueExpression[] { mathExpr };

        final IConstraint[] having = null;

        final LinkedHashSet<IVariable<?>> groupByVars = new LinkedHashSet<IVariable<?>>();
        groupByVars.add(index);

        final LinkedHashSet<IVariable<?>> selectVars = new LinkedHashSet<IVariable<?>>();
        selectVars.add(index);

        final LinkedHashSet<IVariable<?>> havingVars = new LinkedHashSet<IVariable<?>>();

        final MockGroupByState expected = new MockGroupByState(groupBy,
                groupByVars, select, selectVars, having, havingVars,
                false/* anyDistinct */, false/* selectDependency */,
                false/* nestedAggregates */, true/* simpleHaving */);

        final IGroupByState actual = new GroupByState(select, groupBy, having);

        assertSameState(expected, actual);

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
        
        final IVariable<IV> y = Var.var("y");
        final IVariable<IV> x = Var.var("x");
        final IVariable<IV> z = Var.var("z");

        final IValueExpression<IV> xExpr = new /* Conditional */Bind(x, new SUM(
                false/* distinct */, (IValueExpression<IV>) y));

        final IValueExpression<IV>[] select = new IValueExpression[] { xExpr };

        final IValueExpression<IV>[] groupBy = new IValueExpression[] { z };

        final IConstraint[] having = new IConstraint[] {//
        new SPARQLConstraint<XSDBooleanIV>(new CompareBOp(x,
                new Constant<XSDNumericIV>(new XSDNumericIV(10)), CompareOp.LT
                ))//
            };

        final LinkedHashSet<IVariable<?>> groupByVars = new LinkedHashSet<IVariable<?>>();
        groupByVars.add(z);

        final LinkedHashSet<IVariable<?>> selectVars = new LinkedHashSet<IVariable<?>>();
        selectVars.add(x);

        final LinkedHashSet<IVariable<?>> columnVars = new LinkedHashSet<IVariable<?>>();
        columnVars.add(y);

        final MockGroupByState expected = new MockGroupByState(groupBy,
                groupByVars, select, selectVars, having, columnVars,
                false/* anyDistinct */, false/* selectDependency */,
                false/* nestedAggregates */, true/* simpleHaving */);

        final IGroupByState actual = new GroupByState(select, groupBy, having);

        assertSameState(expected, actual);

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

        final IVariable<IV> y = Var.var("y");
        final IVariable<IV> x = Var.var("x");
        final IVariable<IV> z = Var.var("z");

        final IValueExpression<IV> xExpr = new /* Conditional */Bind(x,
                new SUM(false/* distinct */, (IValueExpression<IV>) y));

        final IValueExpression<IV>[] select = new IValueExpression[] { xExpr };

        final IValueExpression<IV>[] groupBy = new IValueExpression[] { z };

        final IConstraint[] having = new IConstraint[] {//
        new SPARQLConstraint<XSDBooleanIV>(new CompareBOp(new SUM(
                false/* distinct */, (IValueExpression<IV>) y),
                new Constant<XSDNumericIV>(new XSDNumericIV(10)), CompareOp.LT
                )) //
        };

        final LinkedHashSet<IVariable<?>> groupByVars = new LinkedHashSet<IVariable<?>>();
        groupByVars.add(z);

        final LinkedHashSet<IVariable<?>> selectVars = new LinkedHashSet<IVariable<?>>();
        selectVars.add(x);

        final LinkedHashSet<IVariable<?>> columnVars = new LinkedHashSet<IVariable<?>>();
        columnVars.add(y);

        final MockGroupByState expected = new MockGroupByState(groupBy,
                groupByVars, select, selectVars, having, columnVars,
                false/* anyDistinct */, false/* selectDependency */,
                false/* nestedAggregates */, false/* simpleHaving */);

        final IGroupByState actual = new GroupByState(select, groupBy, having);

        assertSameState(expected, actual);

    }

    /**
     * <pre>
     * SELECT SUM(?x+MIN(?y)) as ?z
     * GROUP BY ?a
     * </pre>
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_nestedAggregates() {

        final IVariable<IV> a = Var.var("a");

        final IVariable<IV> x = Var.var("x");
        final IVariable<IV> y = Var.var("y");
        final IVariable<IV> z = Var.var("z");

        final IValueExpression<IV> zExpr = new /* Conditional */Bind(z,
                new SUM(false/* distinct */,
                        (IValueExpression<IV>) new MathBOp(x, new MIN(
                                false/* distinct */, (IValueExpression<IV>) y),
                                MathOp.PLUS,globals)));

        final IValueExpression<IV>[] select = new IValueExpression[] { zExpr };

        final IValueExpression<IV>[] groupBy = new IValueExpression[] { a };

        final IConstraint[] having = null;

        final LinkedHashSet<IVariable<?>> groupByVars = new LinkedHashSet<IVariable<?>>();
        groupByVars.add(a);

        final LinkedHashSet<IVariable<?>> selectVars = new LinkedHashSet<IVariable<?>>();
        selectVars.add(z);

        final LinkedHashSet<IVariable<?>> columnVars = new LinkedHashSet<IVariable<?>>();
        columnVars.add(x);
        columnVars.add(y);

        final MockGroupByState expected = new MockGroupByState(groupBy,
                groupByVars, select, selectVars, having, columnVars,
                false/* anyDistinct */, false/* selectDependency */,
                true/* nestedAggregates */, true/* simpleHaving */);

        final IGroupByState actual = new GroupByState(select, groupBy, having);

        assertSameState(expected, actual);

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

        final IVariable<IV> a = Var.var("a");
        final IVariable<IV> b = Var.var("b");
        final IVariable<IV> x = Var.var("x");
        final IVariable<IV> y = Var.var("y");
        final IVariable<IV> z = Var.var("z");

        final IValueExpression<IV> zExpr = new /* Conditional */Bind(z,
                new SUM(false/* distinct */, (IValueExpression<IV>) y));

        final IValueExpression<IV> aExpr = new /* Conditional */Bind(a,
                new MathBOp(new SUM(false/* distinct */,
                        (IValueExpression<IV>) x), z, MathOp.PLUS,globals));

        final IValueExpression<IV>[] select = new IValueExpression[] { zExpr, aExpr };

        final IValueExpression<IV>[] groupBy = new IValueExpression[] { b };

        final IConstraint[] having = null;

        final LinkedHashSet<IVariable<?>> groupByVars = new LinkedHashSet<IVariable<?>>();
        groupByVars.add(b);

        final LinkedHashSet<IVariable<?>> selectVars = new LinkedHashSet<IVariable<?>>();
        selectVars.add(z);
        selectVars.add(a);

        final LinkedHashSet<IVariable<?>> columnVars = new LinkedHashSet<IVariable<?>>();
        columnVars.add(y);
        columnVars.add(x);

        final MockGroupByState expected = new MockGroupByState(groupBy,
                groupByVars, select, selectVars, having, columnVars,
                false/* anyDistinct */, true/* selectDependency */,
                false/* nestedAggregates */, true/* simpleHaving */);

        final IGroupByState actual = new GroupByState(select, groupBy, having);

        assertSameState(expected, actual);

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

        final IVariable<IV> a = Var.var("a");
        final IVariable<IV> b = Var.var("b");
        final IVariable<IV> x = Var.var("x");
        final IVariable<IV> y = Var.var("y");
        final IVariable<IV> z = Var.var("z");

        final IValueExpression<IV> zExpr = new /* Conditional */Bind(z,
                new SUM(false/* distinct */, (IValueExpression<IV>) y));

        final IValueExpression<IV> aExpr = new /* Conditional */Bind(a,
                new MathBOp(new SUM(false/* distinct */,
                        (IValueExpression<IV>) x), z, MathOp.PLUS,globals));

        final IValueExpression<IV>[] select = new IValueExpression[] { aExpr, zExpr };

        final IValueExpression<IV>[] groupBy = new IValueExpression[] { b };

        final IConstraint[] having = null;

        try {
            new GroupByState(select, groupBy, having);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
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
        
        final IVariable<IV> y = Var.var("y");
        final IVariable<IV> x = Var.var("x");
        final IVariable<IV> z = Var.var("z");

        final IValueExpression<IV> xExpr = new /* Conditional */Bind(x, new SUM(
                true/* distinct */, (IValueExpression<IV>) y));

        final IValueExpression<IV>[] select = new IValueExpression[] { xExpr };

        final IValueExpression<IV>[] groupBy = new IValueExpression[] { z };

        final IConstraint[] having = new IConstraint[] {//
        new SPARQLConstraint<XSDBooleanIV>(new CompareBOp(x,
                new Constant<XSDNumericIV>(new XSDNumericIV(10)), CompareOp.LT
                ))//
            };

        final LinkedHashSet<IVariable<?>> groupByVars = new LinkedHashSet<IVariable<?>>();
        groupByVars.add(z);

        final LinkedHashSet<IVariable<?>> selectVars = new LinkedHashSet<IVariable<?>>();
        selectVars.add(x);

        final LinkedHashSet<IVariable<?>> columnVars = new LinkedHashSet<IVariable<?>>();
        columnVars.add(y);

        final MockGroupByState expected = new MockGroupByState(groupBy,
                groupByVars, select, selectVars, having, columnVars,
                true/* anyDistinct */, false/* selectDependency */,
                false/* nestedAggregates */, true/* simpleHaving */);

        final IGroupByState actual = new GroupByState(select, groupBy, having);

        assertSameState(expected, actual);

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
        
        final IVariable<IV> y = Var.var("y");
        final IVariable<IV> x = Var.var("x");
        final IVariable<IV> z = Var.var("z");

        final IValueExpression<IV> xExpr = new /* Conditional */Bind(x, new SUM(
                false/* distinct */, (IValueExpression<IV>) y));

        final IValueExpression<IV>[] select = new IValueExpression[] { xExpr };

        final IValueExpression<IV>[] groupBy = new IValueExpression[] { z };

        final IConstraint[] having = new IConstraint[] {//
        new SPARQLConstraint<XSDBooleanIV>(new CompareBOp(
                new /* Conditional */Bind(x, new SUM(true/* distinct */,
                        (IValueExpression<IV>) y)), new Constant<XSDNumericIV>(
                        new XSDNumericIV(10)), CompareOp.LT)) //
        };

        final LinkedHashSet<IVariable<?>> groupByVars = new LinkedHashSet<IVariable<?>>();
        groupByVars.add(z);

        final LinkedHashSet<IVariable<?>> selectVars = new LinkedHashSet<IVariable<?>>();
        selectVars.add(x);

        final LinkedHashSet<IVariable<?>> columnVars = new LinkedHashSet<IVariable<?>>();
        columnVars.add(y);

        final MockGroupByState expected = new MockGroupByState(groupBy,
                groupByVars, select, selectVars, having, columnVars,
                true/* anyDistinct */, false/* selectDependency */,
                false/* nestedAggregates */, false/* simpleHaving */);

        final IGroupByState actual = new GroupByState(select, groupBy, having);

        assertSameState(expected, actual);

    }
    
    /**
     * Unit test when projecting a constant
     * <pre>
     * SELECT 12 as ?x
     * </pre>
     */
    public void test_with_constant() {

        @SuppressWarnings({ "unchecked", "rawtypes" })
        final IVariable<IV> x = Var.var("x");

        @SuppressWarnings({ "rawtypes", "unchecked" })
        final IValueExpression<IV> xExpr = new /* Conditional */Bind(x,
                new Constant<IV>(new XSDNumericIV(12)));

        @SuppressWarnings({ "rawtypes", "unchecked" })
        final IValueExpression<IV>[] select = new IValueExpression[] { xExpr };

        @SuppressWarnings("rawtypes")
        final IValueExpression<IV>[] groupBy = null;

        final IConstraint[] having = null;

        final LinkedHashSet<IVariable<?>> groupByVars = new LinkedHashSet<IVariable<?>>();

        final LinkedHashSet<IVariable<?>> selectVars = new LinkedHashSet<IVariable<?>>();
        selectVars.add(x);

        final LinkedHashSet<IVariable<?>> columnVars = new LinkedHashSet<IVariable<?>>();

        final MockGroupByState expected = new MockGroupByState(groupBy,
                groupByVars, select, selectVars, having, columnVars,
                false/* anyDistinct */, false/* selectDependency */,
                false/* nestedAggregates */, true/* simpleHaving */);

        final IGroupByState actual = new GroupByState(select, groupBy, having);

        assertSameState(expected, actual);

    }

    /**
     * Unit test for bad arguments.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_correctRejection() {

        final IVariable<IV> y = Var.var("y");
        final IVariable<IV> x = Var.var("x");
        final IVariable<IV> z = Var.var("z");

        final IValueExpression<IV>[] groupBy = new IValueExpression[] { z };

        final IConstraint[] having = new IConstraint[] {//
        new SPARQLConstraint<XSDBooleanIV>(new CompareBOp(
                new /* Conditional */Bind(x, new SUM(true/* distinct */,
                        (IValueExpression<IV>) y)), new Constant<XSDNumericIV>(
                        new XSDNumericIV(10)), CompareOp.LT)) //
        };

        // SELECT may not be null.
        try {
            new GroupByState(null/* select */, groupBy, having);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex, ex);
        }

        // SELECT may not be empty.
        try {
            new GroupByState(new IValueExpression[] {}/* select */, groupBy,
                    having);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex, ex);
        }
        
    }
    
}
