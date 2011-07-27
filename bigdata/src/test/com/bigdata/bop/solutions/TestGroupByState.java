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
import com.bigdata.bop.rdf.aggregate.SUM;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSDBooleanIV;
import com.bigdata.rdf.internal.XSDIntIV;
import com.bigdata.rdf.internal.constraints.CompareBOp;
import com.bigdata.rdf.internal.constraints.SPARQLConstraint;
import com.bigdata.rdf.internal.constraints.UcaseBOp;

/**
 * Test suite for {@link GroupByState}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * TODO Write test for isAnyDistinct and other optimizer hints.
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

    private static class MockGroupByState implements IGroupByState {

        final IValueExpression<?>[] groupBy;
        final LinkedHashSet<IVariable<?>> groupByVars;
        final IValueExpression<?>[] select;
        final LinkedHashSet<IVariable<?>> selectVars;
        final IConstraint[] having;
        final boolean anyDistinct;
        final boolean selectDependency;
        final boolean simpleHaving;

        MockGroupByState(//
                final IValueExpression<?>[] groupBy,
                final LinkedHashSet<IVariable<?>> groupByVars,
                final IValueExpression<?>[] select,
                final LinkedHashSet<IVariable<?>> selectVars,
                final IConstraint[] having,//
                final boolean anyDistinct,//
                final boolean selectDependency,//
                final boolean simpleHaving//
                ) {
            this.groupBy = groupBy;
            this.groupByVars = groupByVars;
            this.select = select;
            this.selectVars = selectVars;
            this.having = having;
            this.anyDistinct = anyDistinct;
            this.selectDependency = selectDependency;
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

        public boolean isAnyDistinct() {
            return anyDistinct;
        }

        public boolean isSelectDependency() {
            return selectDependency;
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

        assertEquals("anyDistinct", expected.isAnyDistinct(),
                actual.isAnyDistinct());
        
        assertEquals("selectDependency", expected.isSelectDependency(),
                actual.isSelectDependency());
        
        assertEquals("simpleHaving", expected.isSimpleHaving(),
                actual.isSimpleHaving());
        
    }
    
    /**
     * Unit test with SELECT clause having one value expression, which is a
     * simple variable also appearing as the sole value expression in the
     * GROUP_BY clause.
     * <pre>
     * SELECT ?org GROUP BY ?org
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

        final MockGroupByState expected = new MockGroupByState(groupBy,
                groupByVars, select, selectVars, having,
                false/* anyDistinct */, false/* selectDependency */, true/* simpleHaving */);

        final IGroupByState actual = new GroupByState(select, groupBy, having);

        assertSameState(expected, actual);
        
    }

    /**
     * Unit test with simple aggregate function in SELECT clause.
     * <pre>
     * SELECT ?org, SUM(?lprice) AS ?totalPrice
     * </pre>
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void test_aggregateExpr_02() {
        
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

        final MockGroupByState expected = new MockGroupByState(groupBy,
                groupByVars, select, selectVars, having,
                false/* anyDistinct */, false/* selectDependency */, true/* simpleHaving */);

        final IGroupByState actual = new GroupByState(select, groupBy, having);

        assertSameState(expected, actual);
        
    }

    /**
     * Unit test for references to aggregate declared in GROUP_BY with AS.
     * <pre>
     * SELECT ?org2 GROUP BY UCASE(?org) as ?org2
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
                namespace));

        final IValueExpression<?>[] select = new IValueExpression[] { org2 };

        final IValueExpression<?>[] groupBy = new IValueExpression[] { ucaseExpr };

        final IConstraint[] having = null;

        final LinkedHashSet<IVariable<?>> groupByVars = new LinkedHashSet<IVariable<?>>();
        groupByVars.add(org2);

        final LinkedHashSet<IVariable<?>> selectVars = new LinkedHashSet<IVariable<?>>();
        selectVars.add(org2);

        final MockGroupByState expected = new MockGroupByState(groupBy,
                groupByVars, select, selectVars, having,
                false/* anyDistinct */, false/* selectDependency */, true/* simpleHaving */);

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
                new Constant<XSDIntIV>(new XSDIntIV(10)), CompareOp.LT))//
            };

        final LinkedHashSet<IVariable<?>> groupByVars = new LinkedHashSet<IVariable<?>>();
        groupByVars.add(z);

        final LinkedHashSet<IVariable<?>> selectVars = new LinkedHashSet<IVariable<?>>();
        selectVars.add(x);

        final MockGroupByState expected = new MockGroupByState(groupBy,
                groupByVars, select, selectVars, having,
                false/* anyDistinct */, false/* selectDependency */, true/* simpleHaving */);

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
                new Constant<XSDIntIV>(new XSDIntIV(10)), CompareOp.LT)) //
        };

        final LinkedHashSet<IVariable<?>> groupByVars = new LinkedHashSet<IVariable<?>>();
        groupByVars.add(z);

        final LinkedHashSet<IVariable<?>> selectVars = new LinkedHashSet<IVariable<?>>();
        selectVars.add(x);

        final MockGroupByState expected = new MockGroupByState(groupBy,
                groupByVars, select, selectVars, having,
                false/* anyDistinct */, false/* selectDependency */, false/* simpleHaving */);

        final IGroupByState actual = new GroupByState(select, groupBy, having);

        assertSameState(expected, actual);

    }

    public void test_compositionOfAggregates() {
        // SELECT SUM(?x+AVG(?y)) as ?z ...
        fail("write test");
    }

    public void test_reverseReference_allowed() {
        // SELECT SUM(?y) as ?z, SUM(?x)+?z as ?a ... (backward reference to z).
        fail("write test");
    }

    /**
     * Forward references to a variable are not allowed.
     * 
     * <pre>
     * SELECT SUM(?x)+?z as ?a, SUM(?y) as ?z ... (forward reference to z)
     * </pre>
     */
    public void test_forwardReference_not_allowed() {
        fail("write test");
    }

    /*
     * TODO Test for bad arguments, including nulls when not allowed and also
     * the more complex cases where something is/is not an aggregate.
     */
    public void test_correctRejection() {
        fail("write test");
    }

}
