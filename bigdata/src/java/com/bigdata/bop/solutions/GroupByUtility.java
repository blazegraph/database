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

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Bind;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.bop.aggregate.IAggregate;

/**
 * A helper class which rewrites the various {@link IValueExpression}s (from
 * SELECT and GROUP BY clauses) and the {@link IConstraint}s (from the HAVING
 * clause) into those aspects which are computed over the source solutions (and
 * hence do not include the aggregate functions), those which are computed over
 * the grouped solutions (the aggregate functions), and those variables which
 * will be projected out of the GROUP BY operator.
 * 
 * @author thompsonbry
 */
public class GroupByUtility {

	/*
	 * Given data.
	 */

	/**
	 * The value expressions to be projected out of the GROUP_BY operator. The
	 * top-level elements of this array MUST be either (a) an {@link IVariable}
	 * or (c) a {@link Bind}.
	 * <p>
	 * Note: According to SPARQL, all select expressions must be explicitly
	 * named. Therefore, even expressions which are, or which evaluate to, a
	 * constant must be wrapped by a {@link Bind}.
	 * <p>
	 * Note: The special {@link IVariable} <code>*</code> MAY NOT be used to
	 * project everything out since the GROUP_BY operator is only defined in the
	 * presence of expressions which define solution groups and aggregate
	 * functions are required in order to extract information from a solution
	 * group.
	 */
	final IValueExpression<?>[] select;

	/**
	 * The ordered array of variables which define the distinct groups to be
	 * aggregated.
	 */
	final IValueExpression<?>[] groupBy;

	/**
	 * Optional constraints applied to the aggregated solutions.
	 */
	final IConstraint[] having;

	/*
	 * Derived data
	 */

	/**
	 * An ordered array of {@link IValueExpression}s computed for each incoming
	 * solution. This array is built up from the {@link #select},
	 * {@link #groupBy}, and {@link #having} collections. Only that component of
	 * each {@link IValueExpression} which DOES NOT include the aggregate
	 * function(s) is captured within {@link #detailExprs}. The expressions are
	 * applied to a clone of the source {@link IBindingSet}s and MUST use
	 * {@link Bind} to cause the computed value to become bound on the solution.
	 * It is an error if {@link Bind} would overwrite a variable which is
	 * already bound.
	 */
	final IValueExpression<?>[] detailExprs;

//	/**
//	 * An ordered array of {@link IVariable}s correlated with the
//	 * {@link #detailExprs} array and used to assign the computed results into
//	 * the solution which will be inserted into a {@link Group} and aggregated
//	 * in a post-process step.
//	 */
//	final IVariable<?>[] detailVars;

	/**
	 * The set of {@link IVariable}s to be projected from the detailed records.
	 * This MUST include anything referenced in {@link #aggregateExprs} or
	 * {@link #groupByVars}.
	 * <p>
	 * {@link IVariable}s not required by downstream processing MUST NOT be
	 * included in this list since: (a) their presence could cause a
	 * <code>SELECT DISTINCT</code> to be answered incorrectly (due to the
	 * additional variety in the solutions); and (b) the bindings will never by
	 * used and should be dropped to reduce the storage requirements of the
	 * GROUP_BY operation.
	 */
	final IVariable<?>[] detailProject;

	/**
	 * An ordered array of {@link IValueExpression}s computed for each group of
	 * solutions. The top-level elements of this array MUST be either (a) an
	 * {@link IVariable}; or (b) a {@link Bind}.
	 * <p>
	 * This includes everything down to the (if present) aggregate function, but
	 * any aggregate functions will have been rewritten to reference the
	 * {@link #detailVars} corresponding to the value expression within the
	 * aggregate function. For example, <code>SUM(i+j)*2</code> would be broken
	 * down into <code>i+j</code>, which would be an element of
	 * {@link #detailVars}, and <code>SUM(<i>var</i>)*2</code>, which is the
	 * aggregate function to be applied to the computed values.
	 */
	final IValueExpression<?>[] aggregateExprs;
	
//	/**
//	 * An ordered array of {@link IVariable}s correlated with the
//	 * {@link #aggregateExprs} array and used to assign the aggregate solution
//	 * for each solution group.
//	 */
//	final IValueExpression<?>[] aggregateVars;

	/**
	 * An ordered array of {@link IVariable}s which define the solution groups.
	 * This is derived from {@link #groupBy} by replacing any appearances of a
	 * complex {@link IValueExpression} in {@link #groupBy} with a reference to
	 * an {@link IVariable} declared by {@link #detailExprs}.
	 * <p>
	 * The {@link IVariable}s in this array must either be present in the source
	 * solutions or declared by {@link #detailExprs}. In addition, they must
	 * appear in {@link #detailProject} to ensure that the variables are not
	 * dropped from the solutions before they are placed into groups.
	 */
	final IVariable<?>[] groupByVars;

	/**
	 * An ordered array of {@link IVariables} which define the constraints on
	 * the solution groups. The BEV of each variable will be taken to decide
	 * whether the solution group will pass or fail.
	 * <p>
	 * This array is derived from {@link #having} by replacing any appearances
	 * of a complex {@link IValueExpression} in {@link #having} with a reference
	 * to an {@link IVariable} retained by the aggregated solutions, e.g., one
	 * declared either by {@link #groupByVars} or by {@link #aggregateExprs}.
	 * 
	 * @todo SPARQL might have the additional constraint that HAVING may only
	 *       refer to expressions which are projected out of the GROUP BY
	 *       operator by SELECT. However, that constraint is not being enforced
	 *       here.
	 */
	final IVariable<?>[] havingVars;

	/**
	 * The variables which were specified by {@link #select} and will be
	 * projected out of the GROUP_BY operator.
	 */
	final IVariable<?>[] project;

	public GroupByUtility(final IValueExpression<?>[] select,
			final IValueExpression<?>[] groupBy, final IConstraint[] having) {

		/*
		 * The IValueExpressions to be SELECTed.
		 */
		
		if (select == null)
			throw new IllegalArgumentException();
		
		if (select.length == 0)
			throw new IllegalArgumentException();

		for (IValueExpression<?> e : select) {
			if (e instanceof IVariable<?>)
				continue;
			if (e instanceof Bind<?>)
				continue;
			throw new IllegalArgumentException(
					"SELECT: Must be variable or (expr as var): " + e);
		}

		/*
		 * The IValueExpressions defining the groups. This must be non-null, and
		 * non-empty array w/o dups.
		 * 
		 * @todo What code path/operator will be used to handle SELECT
		 * expressions without a GROUP BY clause?
		 */
		
		if (groupBy == null)
			throw new IllegalArgumentException();
		
		if (groupBy.length == 0)
			throw new IllegalArgumentException();

		// Save reference to the given data
		this.select = select;
		this.groupBy = groupBy;
		this.having = having; // may be null or empty[].
		
		// Derived
		{

			final List<IValueExpression<?>> detailExprs = new LinkedList<IValueExpression<?>>();
			final Set<IVariable<?>> detailProject = new LinkedHashSet<IVariable<?>>();
			final List<IValueExpression<?>> aggregateExprs = new LinkedList<IValueExpression<?>>();
			final List<IVariable<?>> groupByVars = new LinkedList<IVariable<?>>();
			final List<IVariable<?>> havingVars = new LinkedList<IVariable<?>>();
			final Set<IVariable<?>> project = new LinkedHashSet<IVariable<?>>();

			/*
			 * Anything variables referenced in the SELECT must be projected out
			 * of the detail records (in case they are bound there).
			 * 
			 * Any SELECTed variables must be projected out of the GROUP_BY
			 * operator regardless.
			 * 
			 * Any aggregate functions must be broken down into the expression
			 * inside of the aggregate (unless it is a simple variable) and the
			 * outer expression, include the aggregate itself. When the
			 * expression inside of the aggregate is not a simple variable, then
			 * it must be replaced by a simple variable whose value is bound by
			 * a value expression in [aggregateExpr].
			 */
			for(IValueExpression<?> e : select) {
				
				if (e instanceof IVariable<?>) {
					/*
					 * Project out top-level variables appearing in the SELECT
					 * clause.
					 */
					// from detail record.
					detailProject.add((IVariable<?>) e);
					// from the GROUP_BY operation.
					project.add((IVariable<?>) e);
					continue;
				}
				
				if(e instanceof Bind<?>) {
					/*
					 * Rewrite aggregate functions such that they only reference
					 * simple variables within the aggregate function and then
					 * project out the variables appearing appearing within the
					 * [expr] from the detail record.
					 */
					// e.g., rewrite SUM(i+1)*2 as SUM(newVar)*2.
					final IValueExpression<?> t = rewriteAggregateFunctions(e,
							detailExprs);
					// Add to set of expressions to compute over the groups.
					aggregateExprs.add(t);
					// Project referenced variables out of the detail record.
					final Iterator<IVariable<?>> vitr = BOpUtility
							.getSpannedVariables(((Bind<?>) t).getExpr());
					while (vitr.hasNext()) {
						detailProject.add(vitr.next());
					}
					/*
					 * Project out the bind(var,expr) variable. If it appears in
					 * the detail record then we will have a runtime exception
					 * since it can not be rebound by SELECT.
					 * 
					 * @todo Or is this detected by static analysis outside of
					 * the GROUP_BY operator?
					 */
					detailProject.add(((Bind<?>) e).getVar());
					/*
					 * Project out top-level variables appearing as [var] in
					 * (expr AS var) within a SELECT clause.
					 */
					project.add(((Bind<?>) e).getVar());
					continue;
				}

				throw new AssertionError("Not expecting: "
						+ e.getClass().getName());
				
			}
			
			this.detailExprs = detailExprs
					.toArray(new IValueExpression[detailExprs.size()]);

			this.detailProject = detailProject
					.toArray(new IVariable[detailProject.size()]);

			this.aggregateExprs = detailExprs
					.toArray(new IValueExpression[aggregateExprs.size()]);

			this.groupByVars = groupByVars.toArray(new IVariable[groupByVars
					.size()]);

			this.havingVars = groupByVars.toArray(new IVariable[havingVars
					.size()]);

			this.project = groupByVars.toArray(new IVariable[project.size()]);

		}
		
	}

	/**
	 * If <i>expr</i> embeds one or more aggregate functions, then those
	 * functions are rewritten. If the expression to be aggregated is more
	 * complex than a simple {@link IVariable}, then that expression is lifted
	 * into a {@link Bind} onto a new anonymous variable and the occurrence of
	 * that expression within the aggregate function is replaced by the
	 * reference to the anonymous variable. The {@link Bind}s so generated are
	 * added to the <i>detailExpr</i> list. The rewritten expression is
	 * returned.
	 * <p>
	 * For example, given:
	 * 
	 * <pre>
	 * SUM(i + j * 2) + 3
	 * </pre>
	 * 
	 * The expression would be rewritten as:
	 * 
	 * <pre>
	 * SUM(_var1) + 3
	 * </pre>
	 * 
	 * where <code>_var1</code> is an anonymous variable. That expression would
	 * then be returned to the caller. In addition, the expression
	 * <code>i+j*2</code> would be added to the <i>detailExpr</i> list as
	 * <code>Bind(_var,i+j*2)</code>.
	 * 
	 * @param expr
	 *            An expression, optionally involving one or more aggregate
	 *            functions.
	 * @param detailExpr
	 *            A list which will be populated by {@link Bind}s lifted out of
	 *            the aggregate function(s) in <i>expr</i>
	 * 
	 * @return The <i>expr</i>, which has been rewritten such that all aggregate
	 *         functions are evaluated over simple {@link IVariable}s.
	 */
	IValueExpression<?> rewriteAggregateFunctions(
			final IValueExpression<?> expr,
			final List<IValueExpression<?>> detailExpr) {

		if (expr instanceof IAggregate<?>) {

			/*
			 * An aggregate function whose inner expression is not a simple
			 * variable.
			 */

			return rewriteAggregateFunction((IAggregate<?>) expr, detailExpr);

		}

		return expr;

	}

	/**
	 * Rewrite an aggregate function if its inner expression is not a simple
	 * variable into an aggregate function whose inner expression is a simple
	 * variable. If the same expression already appears in the
	 * <i>detailExpr</i> list, then a {@link Bind} is generated for the
	 * existing variable which is the result of evaluating the desired
	 * expression. Otherwise, the complex expression is wrapped with a
	 * {@link Bind} and added to the caller's list.
	 * 
	 * @param expr
	 *            An aggregate expression
	 * @param detailExpr
	 *            A set of expressions extracted from aggregate functions. Each
	 *            element of this set will be a {@link Bind} which associates
	 *            the result of evaluating the expression with a variable.
	 * 
	 * @return The aggregate function, possibly rewritten.
	 */
	IValueExpression<?> rewriteAggregateFunction(final IAggregate<?> expr,
			final List<IValueExpression<?>> detailExpr) {

		final IValueExpression<?> e = expr.getExpression();

		if (e instanceof IVariable<?>) {
		
			// No rewrite required.
			return expr;
			
		}
		
		for(IValueExpression<?> t : detailExpr) {
			
			if (e.equals(((Bind<?>) t).getExpr())) {

				/*
				 * We found an existing expression which is the same as this
				 * one, so we use its already declared variable rather than
				 * computing the same expression twice.
				 */
				
				final IVariable anon = ((Bind<?>) t).getVar();

				// Replace the expression with the anonymous variable.
				return expr.setExpression(anon);

			}

		}

		// Obtain an anonymous variable.
		final IVariable anon = newVar();

		/*
		 * Wrap with Bind() and add to the list of aggregate expressions we need
		 * to evaluate.
		 */
		detailExpr.add(new Bind(anon, e));

		// Replace the expression with the anonymous variable.
		return expr.setExpression(anon);

	}

	/**
	 * Return a new anonymous variable (this is overridden by some unit tests in
	 * order to have predictable variable names).
	 */
	protected IVariable<?> newVar() {

		return Var.var();

	}

}
