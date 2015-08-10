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
	 * Return <code>true</code> iff the {@link IValueExpression} is the special
	 * variable <code>*</code> (but note that this is only allowed for COUNT).
	 */
	boolean isWildcard();
	
	/**
     * Reset the aggregate's internal state.
     */
    void reset();

    /**
     * Return the current value of the aggregate (this has a side-effect on the
     * internal state of the {@link IAggregate} operator). Functions which can
     * not produce an intermediate result, such as AVERAGE, MAY return
     * <code>null</code>.
     * <p>
     * Note: If evaluation of the {@link IAggregate} throws an error, then that
     * error must be "sticky" and reported out by {@link #done()} as well. This
     * contract is relied on to correctly propagate errors within a group when
     * using incremental (pipelined) evaluation of {@link IAggregate}s. The
     * error state is cleared by {@link #reset()}.
     */
    E get(IBindingSet bset);

    /**
     * Return the final value.
     * 
     * @throws RuntimeException
     *             If evaluation of {@link IAggregate#get(IBindingSet)} threw an
     *             error, then that error is "sticky" and the first such error
     *             encountered will be thrown out of {@link #done()} as well.
     */
    E done();

}
