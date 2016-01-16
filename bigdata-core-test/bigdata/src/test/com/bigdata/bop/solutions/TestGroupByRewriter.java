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
 * Created on Jul 29, 2011
 */

package com.bigdata.bop.solutions;

import java.util.LinkedHashMap;

import junit.framework.TestCase2;

import org.openrdf.query.algebra.Compare.CompareOp;

import com.bigdata.bop.Bind;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableFactory;
import com.bigdata.bop.Var;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.bop.rdf.aggregate.MIN;
import com.bigdata.bop.rdf.aggregate.SUM;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.CompareBOp;
import com.bigdata.rdf.internal.constraints.MathBOp;
import com.bigdata.rdf.internal.constraints.SPARQLConstraint;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

/**
 * Test suite for {@link GroupByRewriter}.
 * <p>
 * Note: This test suite is actually written at the
 * {@link GroupByRewriter#rewrite(IValueExpression, IVariableFactory, LinkedHashMap)}
 * and
 * {@link GroupByRewriter#rewrite(IConstraint, IVariableFactory, LinkedHashMap)}
 * level (those are static methods). This is good, but we do not have unit tests
 * at the {@link GroupByRewriter} level.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestGroupByRewriter extends TestCase2 {

    /**
     * 
     */
    public TestGroupByRewriter() {
    }

    /**
     * @param name
     */
    public TestGroupByRewriter(String name) {
        super(name);
    }

    /** The lexicon namespace - required for {@link CompareBOp}. */
    private GlobalAnnotations globals;
    
    protected void setUp() throws Exception {

        super.setUp();
        
        globals = new GlobalAnnotations(getName(), ITx.READ_COMMITTED);
    }
    
    protected void tearDown() throws Exception {
        
        globals = null;
        
        super.tearDown();
        
    }

//    private static class MockRewriteState implements IGroupByRewriteState {
//        
//        private final LinkedHashMap<IAggregate<?>,IVariable<?>> aggExpr;
//        private final IConstraint[] having2;
//        private final IValueExpression<?>[] select2;
//
//        public LinkedHashMap<IAggregate<?>,IVariable<?>> getAggExpr() {
//            return aggExpr;
//        }
//
//        public IConstraint[] getHaving2() {
//            return having2;
//        }
//        
//        public IValueExpression<?>[] getSelect2() {
//            return select2;
//        }
//
//        public MockRewriteState(
//                final LinkedHashMap<IAggregate<?>,IVariable<?>> aggExpr,
//                final IConstraint[] having2,
//                final IValueExpression<?>[] select2
//                ) {
//            this.aggExpr = aggExpr;
//            this.having2 = having2;
//            this.select2 = select2;
//        }
//
//    }
//    
//    static void assertSameState(final IGroupByRewriteState expected,
//            final IGroupByRewriteState actual) {
//
//        assertEquals("aggExpr", expected.getAggExpr(), actual.getAggExpr());
//
//        assertEquals("having2", expected.getHaving2(), actual.getHaving2());
//        
//        assertEquals("select2", expected.getSelect2(), actual.getSelect2());
//
//    }

    /**
     * Provides sequential (and predictable) variable names.
     */
    private static class MockVariableFactory implements IVariableFactory {

        int i = 0;

        public IVariable<?> var() {

            return Var.var("_" + i++);
            
        }

    }

    private IGroupByRewriteState newFixture(final IGroupByState groupByState) {
        return new GroupByRewriter(groupByState) {
            private IVariableFactory vf;
            /**
             * Overridden to provide sequential (and predictable) variable
             * names.
             */
            @Override
            public IVariable<?> var() {
                if(vf == null) // odd, but otherwise not initalized?!?
                    vf = new MockVariableFactory();
                return vf.var();
            }
        };
    }

    /*
     * Unit tests at the rewrite() level.
     */
    
    /**
     * Test that a bare variable is NOT rewritten.
     * <pre>
     * SELECT ?x GROUP BY ?x
     * </pre>
     */
    public void test_variable_not_rewritten() {
        
        final IVariable<?> x = Var.var("x");

        final LinkedHashMap<IAggregate<?>, IVariable<?>> actualAggExpr = new LinkedHashMap<IAggregate<?>, IVariable<?>>();

        assertTrue(x == GroupByRewriter.rewrite(x, new MockVariableFactory(),
                actualAggExpr));
        
        assertTrue(actualAggExpr.isEmpty());

    }

    /**
     * Test that a bare constant is NOT rewritten.
     * <pre>
     * SELECT 12
     * </pre>
     */
    public void test_constant_not_rewritten() {
        
        final IConstant<?> x = new Constant(12);

        final LinkedHashMap<IAggregate<?>, IVariable<?>> actualAggExpr = new LinkedHashMap<IAggregate<?>, IVariable<?>>();

        assertTrue(x == GroupByRewriter.rewrite(x, new MockVariableFactory(),
                actualAggExpr));
        
        assertTrue(actualAggExpr.isEmpty());

    }

    /**
     * Test lifting of bare {@link IAggregate} expression:
     * <pre>
     * SELECT SUM(?x) as ?y
     * </pre>
     * is rewritten as
     * <pre>
     * aggExp := SUM(?x) as ?_0
     * select := ?_0 as ?y
     * </pre>
     */
    public void test_rewrite_lift_bare_aggregate_expression() {

        /*
         * Setup the aggregation operation.
         */

        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");

        final IValueExpression<?> sumX = new /* Conditional */Bind(y, new SUM(
                false/* distinct */, (IValueExpression<IV>) x));

        /*
         * Set up the expected answer.
         */

        final IAggregate<?> _sumX = new SUM(false/* distinct */,
                (IValueExpression<IV>) x);

        final IVariable<?> _0 = Var.var("_0");

        final IValueExpression<?> expectedExpr = new Bind(y, _0);

        final LinkedHashMap<IAggregate<?>, IVariable<?>> expectedAggExpr = new LinkedHashMap<IAggregate<?>, IVariable<?>>();
        expectedAggExpr.put(_sumX, _0);

        /*
         * Verify the actual outcome against the expected answer.
         */

        final LinkedHashMap<IAggregate<?>, IVariable<?>> actualAggExpr = new LinkedHashMap<IAggregate<?>, IVariable<?>>();

        final IValueExpression<?> actualExpr = GroupByRewriter.rewrite(sumX,
                new MockVariableFactory(), actualAggExpr);

        assertEquals(expectedExpr, actualExpr);

        assertEquals(expectedAggExpr, actualAggExpr);

    }
    
    /**
     * Test that {@link IAggregate} expressions are also lifted out of a HAVING
     * clause.
     * <pre>
     * SELECT ?org
     * GROUP BY ?org
     * HAVING sum(?x)>10
     * </pre>
     */
    public void test_with_having() {
       
        /*
         * Setup the aggregation operation.
         */
        
        final IVariable<IV> x = Var.var("x");

        final IConstraint constraint = new SPARQLConstraint(
                new CompareBOp(new SUM(false/* distinct */,
                        (IValueExpression<IV>) x), new Constant<IV>(
                        new XSDNumericIV(10)), CompareOp.GT));

        /*
         * Set up the expected answer.
         */

        final IAggregate<?> _sumX = new SUM(false/* distinct */,
                (IValueExpression<IV>) x);

        final IVariable<IV> _0 = Var.var("_0");

        final IConstraint expectedExpr = new SPARQLConstraint(new CompareBOp(
                _0, new Constant<IV>(new XSDNumericIV(10)), CompareOp.GT));

        final LinkedHashMap<IAggregate<?>, IVariable<?>> expectedAggExpr = new LinkedHashMap<IAggregate<?>, IVariable<?>>();
        expectedAggExpr.put(_sumX, _0);

        /*
         * Verify the actual outcome against the expected answer.
         */

        final LinkedHashMap<IAggregate<?>, IVariable<?>> actualAggExpr = new LinkedHashMap<IAggregate<?>, IVariable<?>>();

        final IConstraint actualExpr = GroupByRewriter.rewrite(constraint,
                new MockVariableFactory(), actualAggExpr);

        assertEquals(expectedExpr, actualExpr);

        assertEquals(expectedAggExpr, actualAggExpr);

    }

    /**
     * Test elimination of duplicate {@link IAggregate} expressions.</br>
     * <pre>
     * SELECT SUM(?x) as ?y, SUM(x) as ?z
     * </pre>
     */
    public void test_eliminate_duplicate_aggregate_expressions() {
        /*
         * Setup the aggregation operation.
         */

        final IVariable<?> x = Var.var("x");
        final IVariable<?> y = Var.var("y");
        final IVariable<?> z = Var.var("z");

        final IValueExpression<?> sumX1 = new /* Conditional */Bind(y, new SUM(
                false/* distinct */, (IValueExpression<IV>) x));

        final IValueExpression<?> sumX2 = new /* Conditional */Bind(z, new SUM(
                false/* distinct */, (IValueExpression<IV>) x));

        /*
         * Set up the expected answer.
         */

        final IAggregate<?> _sumX = new SUM(false/* distinct */,
                (IValueExpression<IV>) x);

        final IVariable<?> _0 = Var.var("_0");

        final IValueExpression<?> expectedExpr1 = new Bind(y, _0);
        final IValueExpression<?> expectedExpr2 = new Bind(z, _0);

        final LinkedHashMap<IAggregate<?>, IVariable<?>> expectedAggExpr = new LinkedHashMap<IAggregate<?>, IVariable<?>>();
        expectedAggExpr.put(_sumX, _0);

        /*
         * Verify the actual outcome against the expected answer.
         */

        final LinkedHashMap<IAggregate<?>, IVariable<?>> actualAggExpr = new LinkedHashMap<IAggregate<?>, IVariable<?>>();

        final IValueExpression<?> actualExpr1 = GroupByRewriter.rewrite(sumX1,
                new MockVariableFactory(), actualAggExpr);

        final IValueExpression<?> actualExpr2 = GroupByRewriter.rewrite(sumX2,
                new MockVariableFactory(), actualAggExpr);

        assertEquals(expectedExpr1, actualExpr1);

        assertEquals(expectedExpr2, actualExpr2);

        assertEquals(expectedAggExpr, actualAggExpr);

    }

    /**
     * Test lifting of {@link IAggregate} expressions within value expressions:
     * 
     * <pre>
     * SELECT 1+SUM(?x) as ?y
     * </pre>
     */
    public void test_lift_aggregate_expression_nested_in_plain_valueExpr() {
    
        /*
         * Setup the aggregation operation.
         */

        final IVariable<IV> x = Var.var("x");
        final IVariable<IV> y = Var.var("y");

        final IValueExpression<?> expr = new /* Conditional */Bind(y,
                new MathBOp(new Constant<IV>(new XSDNumericIV(1)), new SUM(
                        false/* distinct */, (IValueExpression<IV>) x),
                        MathBOp.MathOp.PLUS, globals));

        /*
         * Set up the expected answer.
         */

        final IAggregate<IV> _sumX = new SUM(false/* distinct */,
                (IValueExpression<IV>) x);

        final IVariable<IV> _0 = Var.var("_0");

        final IValueExpression<IV> expectedExpr = new Bind(y, new MathBOp(
                new Constant<IV>(new XSDNumericIV(1)), _0, MathBOp.MathOp.PLUS, globals));

        final LinkedHashMap<IAggregate<?>, IVariable<?>> expectedAggExpr = new LinkedHashMap<IAggregate<?>, IVariable<?>>();
        expectedAggExpr.put(_sumX, _0);

        /*
         * Verify the actual outcome against the expected answer.
         */

        final LinkedHashMap<IAggregate<?>, IVariable<?>> actualAggExpr = new LinkedHashMap<IAggregate<?>, IVariable<?>>();

        final IValueExpression<?> actualExpr = GroupByRewriter.rewrite(expr,
                new MockVariableFactory(), actualAggExpr);

        assertEquals(expectedExpr, actualExpr);

        assertEquals(expectedAggExpr, actualAggExpr);

    }

    /**
     * Test lifting of nested {@link IAggregate} expressions:
     * 
     * <pre>
     * SELECT SUM(x+MIN(x))
     * </pre>
     */
    public void test_lift_aggregate_expression_nested_in_aggregateExpr() {
        
        /*
         * Setup the aggregation operation.
         */

        final IVariable<IV> x = Var.var("x");
        final IVariable<IV> y = Var.var("y");

        final IValueExpression<IV> sumX = new /* Conditional */Bind(y, new SUM(
                false/* distinct */, new MathBOp(x, new MIN(
                        false/* distinct */, x), MathBOp.MathOp.PLUS, globals)));

        /*
         * Set up the expected answer.
         */

        final IVariable<IV> _0 = Var.var("_0");
        final IVariable<IV> _1 = Var.var("_1");

        final IAggregate<IV> _minX = new MIN(false/* distinct */, x);
        final IAggregate<IV> _sumX = new SUM(false/* distinct */, new MathBOp(
                x, _0, MathBOp.MathOp.PLUS,globals));

        final IValueExpression<IV> expectedExpr = new Bind(y, _1);

        final LinkedHashMap<IAggregate<?>, IVariable<?>> expectedAggExpr = new LinkedHashMap<IAggregate<?>, IVariable<?>>();
        expectedAggExpr.put(_minX, _0);
        expectedAggExpr.put(_sumX, _1);

        /*
         * Verify the actual outcome against the expected answer.
         */

        final LinkedHashMap<IAggregate<?>, IVariable<?>> actualAggExpr = new LinkedHashMap<IAggregate<?>, IVariable<?>>();

        final IValueExpression<?> actualExpr = GroupByRewriter.rewrite(sumX,
                new MockVariableFactory(), actualAggExpr);

        assertEquals(expectedAggExpr, actualAggExpr);

        assertEquals(expectedExpr, actualExpr);

    }
    
}
