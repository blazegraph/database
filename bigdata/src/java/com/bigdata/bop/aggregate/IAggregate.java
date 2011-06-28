package com.bigdata.bop.aggregate;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;

/**
 * An aggregate operator, such as SUM, COUNT, MIN, MAX, etc.
 * 
 * @author thompsonbry
 * 
 * @todo In order to assign nice labels to select expressions we need to know
 *       (or be able to generate) the original syntactic expression, e.g.,
 *       <code>i+j<code> or <code>SUM(i*2)+j</code>. The textual value of these
 *       expressions will be used as if they were variable names. Since a
 *       subquery could be part of a SELECT expression, this means that we need
 *       to be able to do this for any SPARQL query construct.  I do not believe
 *       that openrdf currently supports this.
 */
public interface IAggregate<E> extends IValueExpression<E>{

	/**
	 * <code>true</code> if the aggregate is to be applied to the distinct
	 * solutions within the group. E.g.,
	 * 
	 * <pre>
	 * COUNT(DISTINCT x)
	 * </pre>
	 * 
	 * <pre>
	 * COUNT(DISTINCT *)
	 * </pre>
	 * 
	 * or
	 * 
	 * <pre>
	 * SUM(DISTINCT x)
	 * </pre>
	 */
	boolean isDistinct();

	/**
	 * Return <code>true</code> iff the {@link IValueExpression} is the special
	 * variable <code>*</code> (but note that this is only allowed for COUNT).
	 */
	boolean isWildcard();
	
	/**
	 * Return the {@link IValueExpression} to be computed by the aggregate. For
	 * example, is the aggregate function is <code>SUM(i+2)</code>, then this
	 * expression would be <code>i+2</code>. For <code>COUNT</code> this may be
	 * the special variable <code>*</code>, which is interpreted to mean all
	 * variables declared in the source solutions. The "DISTINCT" keyword is
	 * reported separately by {@link #isDistinct()}.
	 */
	IValueExpression<E> getExpression();
	
	/**
	 * Return the current value of the aggregate (this has a side-effect on the
	 * internal state of the {@link IAggregate} operator).
	 */
	E get(IBindingSet bset);

	/**
	 * Return a new {@link IAggregate} where the expression has been replaced by
	 * the given expression (copy-on-write).
	 * 
	 * @param newExpr
	 *            The new expression.
	 *            
	 * @return The new {@link IAggregate}.
	 */
	IAggregate<E> setExpression(IValueExpression<E> newExpr);

}
