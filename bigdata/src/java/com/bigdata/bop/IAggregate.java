package com.bigdata.bop;

/**
 * An aggregate operator, such as SUM, COUNT, MIN, MAX, etc.
 * 
 * @author thompsonbry
 */
public interface IAggregate<E> extends IValueExpression<E>{

	/**
	 * Return the current value of the aggregate (this has a side-effect on the
	 * internal state of the {@link IAggregate} operator).
	 */
	E get(IBindingSet bset);
	
}
