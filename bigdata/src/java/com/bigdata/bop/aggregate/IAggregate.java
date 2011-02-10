package com.bigdata.bop.aggregate;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;

/**
 * An aggregate operator, such as SUM, COUNT, MIN, MAX, etc.
 * 
 * @author thompsonbry
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
	 * Return the {@link IValueExpression} to be computed by the aggregate. For
	 * <code>COUNT</code> this may be the special variable <code>*</code>, which
	 * is interpreted to mean all variables declared in the source solutions.
	 */
	IValueExpression<E> getExpression();
	
	/**
	 * Return the current value of the aggregate (this has a side-effect on the
	 * internal state of the {@link IAggregate} operator).
	 */
	E get(IBindingSet bset);
	
}
