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
package com.bigdata.bop.solutions;

import junit.framework.TestCase2;

import com.bigdata.bop.Bind;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.bop.aggregate.AggregateBase;

/**
 * Unit tests for {@link GroupByUtility}.
 * 
 * @author thompsonbry
 * 
 * @todo correct rejection when an aggregate function is used inside of another
 *       aggregate function. The following are legal: SUM(i), SUM(i+1),
 *       SUM(i)+1, SUM(i)+SUM(j). However, the following is not legal:
 *       SUM(i+SUM(j)).
 */
public class TestGroupByUtility extends TestCase2 {

	public TestGroupByUtility() {
	}

	public TestGroupByUtility(String name) {
		super(name);
	}

	/**
	 * Various correct rejection tests together with validation of correct
	 * acceptance when the arguments are legal.
	 * 
	 * TODO Verify that top-level elements of SELECT are either
	 * {@link IVariable}s or {@link Bind}s.
	 */
	public void test_groupBy_correct_rejection() {

		final IValueExpression<?>[] select = new IValueExpression[]{Var.var("x"),};
		final IValueExpression<?>[] groupBy = new IValueExpression[]{Var.var("x")};
		final IConstraint[] having = new IConstraint[]{};

		// correct rejection when select is null.
		try {
			new GroupByUtility(null/*select*/, groupBy, having);
			fail("Expecting: " + IllegalArgumentException.class);
		} catch (IllegalArgumentException ex) {
			if (log.isInfoEnabled())
				log.info("Ignoring expected exception: " + ex);
		}

		// correct rejection when select is empty.
		try {
			new GroupByUtility(new IValueExpression[]{}, groupBy, having);
			fail("Expecting: " + IllegalArgumentException.class);
		} catch (IllegalArgumentException ex) {
			if (log.isInfoEnabled())
				log.info("Ignoring expected exception: " + ex);
		}

		// correct rejection when select element is neither var nor bind().
		try {
			new GroupByUtility(new IValueExpression[] { new Constant<Integer>(
					12) }, groupBy, having);
			fail("Expecting: " + IllegalArgumentException.class);
		} catch (IllegalArgumentException ex) {
			if (log.isInfoEnabled())
				log.info("Ignoring expected exception: " + ex);
		}

		// correct rejection when groupBy is null.
		try {
			new GroupByUtility(select, null/*groupBy*/, having);
			fail("Expecting: " + IllegalArgumentException.class);
		} catch (IllegalArgumentException ex) {
			if (log.isInfoEnabled())
				log.info("Ignoring expected exception: " + ex);
		}

		// correct rejection when groupBy is empty.
		try {
			new GroupByUtility(select, new IValueExpression[]{}, having);
			fail("Expecting: " + IllegalArgumentException.class);
		} catch (IllegalArgumentException ex) {
			if (log.isInfoEnabled())
				log.info("Ignoring expected exception: " + ex);
		}

		// correct acceptance when having is null.
		new GroupByUtility(select, groupBy, null/* having */);

		// correct acceptance when having is empty.
		new GroupByUtility(select, groupBy, new IConstraint[] {});

	}

	/**
	 * <code>SELECT SUM(i) as sumi GROUP BY x</code><br/>
	 * detailExpr := null<br/>
	 * detailProject := {x,i};<br/>
	 * groupVars := {x}<br/>
	 * aggregateExprs := {Bind(sumi,SUM(i))}<br/>
	 * aggregateProject := {sumi};<br/>
	 * <p>
	 * Note: For this case, there are no "detail" expressions broken out from
	 * the SELECT or GROUP BY expressions since everything is expressed in terms
	 * of simple variables.
	 * 
	 * @todo version where [x] is also selected.<br/>
	 *       <code>SELECT x, SUM(i) as sumi GROUP BY x</code><br/>
	 *       detailExpr := null<br/>
	 *       detailProject := {x,i};<br/>
	 *       groupVars := {x}<br/>
	 *       aggregateExprs := {Bind(sumi,SUM(i))}<br/>
	 *       aggregateProject := {x,sumi};<br/>
	 * 
	 * @todo version with SUM(i+1) so we have to compute a new variable named
	 *       "i+1".<br/>
	 *       <code>SELECT x, SUM(i+1) as sumip1 GROUP BY x</code><br/>
	 *       detailExpr := {x,Bind(ip1,i+1)}<br/>
	 *       detailProject := {x,ip1};<br/>
	 *       groupVars := {x}<br/>
	 *       aggregateExprs := {bind(sumip1,SUM(i+1))}<br/>
	 *       aggregateProject := {x,sumip1}<br/>
	 * 
	 * @todo When the "GROUP BY" clause uses value expressions rather than
	 *       simple variables, then we need to rewrite it to simple variable
	 *       references.<br/>
	 *       <code>SELECT x, SUM(i) as sumi GROUP BY STR(x)</code><br/>
	 *       detailExpr := {x,Bind(str_x,str(x)}<br/>
	 *       detailProject := {x,i,str_x};<br/>
	 *       groupVars := {str_x}<br/>
	 *       aggregateExprs := {bind(sumi,SUM(i))}<br/>
	 *       aggregateProject := {x,sumi}<br/>
	 * 
	 * @todo When the "HAVING" clause uses value expressions rather than simple
	 *       variables.
	 * 
	 * @todo Variant with no solution groups (just aggregate functions over the
	 *       source solutions)? [Is this really a GROUP_BY operator at this
	 *       point?]
	 */
	public void test_groupByUtility() {

		/*
		 * SELECT (SUM(i) as sumi) GROUP BY x</code>
		 */
		final IValueExpression<?>[] select = new IValueExpression[] { //
		new Bind(Var.var("sumi"), //
				new AggregateBase(AggregateBase.FunctionCode.SUM,
						false/* distinct */, Var.var("i"))) //
		};

		final IValueExpression<?>[] groupBy = new IValueExpression[] { //
		Var.var("x") //
		};

		final IConstraint[] having = new IConstraint[] {//
		};

		final MockData expected = new MockData();
		
		// Nothing is added to the detail records.
		expected.detailExprs = new IValueExpression[0];
		// The variables projected out of the detailed records.
		expected.detailProject = new IVariable[] { Var.var("i"), Var.var("x") };
		// The aggregate functions computed over the solution groups.
		expected.aggregateExprs = new IValueExpression[] { //
				new Bind(Var.var("sumi"), //
						new AggregateBase(AggregateBase.FunctionCode.SUM,
								false/* distinct */, Var.var("i"))) //
				};
		// The variables defining the solution groups.
		expected.groupByVars = new IVariable[] { Var.var("x") };
		// No constraints on the solution groups are defined.
		expected.havingVars = new IVariable[0];
		// The variables projected out of the GROUP_BY operator.
		expected.project = new IVariable[] { Var.var("sumi") };
		
		doTest(select, groupBy, having, new IVariable[] { Var.var("sumi") },
				expected);
		
	}

	/**
	 * Test helper creates the {@link GroupByUtility} instance from the given
	 * data and then verifies it against the expected data.
	 * 
	 * @param select
	 *            The SELECT expression.
	 * @param groupBy
	 *            The GROUP BY expression (optional).
	 * @param having
	 *            The HAVING clause (optional).
	 * @param expected
	 *            The expected data.
	 */
	protected void doTest(final IValueExpression<?>[] select,
			final IValueExpression<?>[] groupBy, final IConstraint[] having,
			final IVariable<?>[] newvars,
			final MockData expected) {

		// Create the fixture from the given data.
		final GroupByUtility fixture = new GroupByUtility(select, groupBy,
				having) {
			private int nextVar = 0;

			@Override
			protected IVariable<?> newVar() {
				if (nextVar >= newvars.length) {
					throw new UnsupportedOperationException(
							"All variables have been consumed");
				}
				return newvars[nextVar++];
			}
		};

		// Verify the given data.
		assertEquals("select", select, fixture.select);
		assertEquals("groupBy", groupBy, fixture.groupBy);
		assertEquals("having", having, fixture.having);

		// Verify the computed data.
		assertSameData(expected, fixture);
		
	}
	
	/**
	 * Mock object with fields corresponding to the computed properties of a
	 * {@link GroupByUtility} instance.
	 */
	private static class MockData {
		// The expressions computed over the detail (source) records.
		IValueExpression<?>[] detailExprs;
		// The variables projected from those computations.
		IVariable<?>[] detailProject;
		// The aggregation expressions computed over the group(s).
		IValueExpression<?>[] aggregateExprs;
		// The variables used to compute the groups.
		IVariable<?>[] groupByVars;
		// The variables used to compute the having constraints (optional).
		IVariable<?>[] havingVars;
		// The variables projected out of the GROUP_BY operator.
		IVariable<?>[] project;
	}

	/**
	 * Helper utility for verifying the computed properties on a
	 * {@link GroupByUtility} instance.
	 * 
	 * @param expected
	 * @param actual
	 */
	static protected void assertSameData(MockData expected,
			GroupByUtility actual) {
	
		assertEquals("detailExprs", expected.detailExprs, actual.detailExprs);
		assertEquals("detailProject", expected.detailProject, actual.detailProject);
		assertEquals("aggregateExprs", expected.aggregateExprs, actual.aggregateExprs);
		assertEquals("groupByVars", expected.groupByVars, actual.groupByVars);
		assertEquals("havingVars", expected.havingVars, actual.havingVars);
		assertEquals("project", expected.project, actual.project);
		
	}

}
