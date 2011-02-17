package com.bigdata.bop.aggregate;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.ImmutableBOp;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;

/**
 * Abstract base class for aggregate functions.
 * 
 * @author thompsonbry
 *
 * @param <E>
 */
public class AggregateBase<E> extends ImmutableBOp implements IAggregate<E> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * A type safe enumeration of well known aggregate functions.
	 */
	static public enum FunctionCode {

		/**
		 * The count of the #of computed value expressions within the solution
		 * group. In combination with the special keyword DISTINCT, this is the
		 * #of distinct values from the computed value expression within the
		 * solution group. When given with the special variable <code>*</code>,
		 * this is the count of the #of solutions (or distinct solutions if also
		 * combined with DISTINCT) within the group.
		 */
		COUNT(0),

		/**
		 * The sum of the computed value expressions within the solution group.
		 * In combination with the special keyword DISTINCT, this is the sum of
		 * the distinct values from the computed value expressions within the
		 * solution group.
		 */
		SUM(1),

		/**
		 * The average is defined as
		 * <code>AVG(expr) := SUM(expr)/COUNT(expr)</code>. Note that both SUM
		 * and COUNT can be hash partitioned over a cluster, so it often makes
		 * sense to rewrite AVG(expr) internally in terms of COUNT and SUM. This
		 * may be combined with DISTINCT.
		 */
		AVG(2),

		/**
		 * MIN(expr) is the minimum observed value for the computed value
		 * expressions according to the ordering semantics of
		 * <code>ORDER BY expr ASC</code>. This may be combined with DISTINCT.
		 */
		MIN(3), 
		
		/**
		 * MAX(expr) is the maximum observed value for the computed value
		 * expressions according to the ordering semantics of
		 * <code>ORDER BY expr ASC</code>. This may be combined with DISTINCT.
		 */
		MAX(4),
		
		/**
		 * The combined values of the computed value expressions as a string.
		 * This may be combined with DISTINCT. 
		 */
		GROUP_CONCAT(5),

		/**
		 * This evaluates to an arbitrary value of the computed value
		 * expressions. This may be combined with DISTINCT to sample from the
		 * distinct computed values. While the implementation is not required to
		 * choose randomly among the values to be sampled, random sampling may
		 * prove more useful to some applications.
		 */
		SAMPLE(6);
		
		private FunctionCode(int code) {
			this.code = code;
		}

		final private int code;

		public int getCode() {
			return code;
		}
		
	}
	
	public interface Annotations extends ImmutableBOp.Annotations {

		/**
		 * The aggregate function identifier ({@link FunctionCode#COUNT},
		 * {@link FunctionCode#SUM}, etc).
		 */
		String FUNCTION_CODE = AggregateBase.class.getName() + ".functionCode";
		
		/**
		 * Optional boolean property indicates whether the aggregate applies to
		 * the distinct within group solutions (default
		 * {@value #DEFAULT_DISTINCT}).
		 */
		String DISTINCT = AggregateBase.class.getName() + ".distinct";

		boolean DEFAULT_DISTINCT = false;
		
	}

	public AggregateBase(BOpBase op) {
		super(op);
	}

	/**
	 * Core shallow copy constructor. The <i>distinct</i> option is modeled
	 * using {@link Annotations#DISTINCT}. The <i>expr</i> is modeled as the
	 * first argument for the aggregate function.
	 * 
	 * @param args
	 * @param annotations
	 */
	public AggregateBase(BOp[] args, Map<String, Object> annotations) {

		super(args, annotations);

	}

	/**
	 * @param functionCode
	 *            The type safe value identifying the desired aggregate
	 *            function.
	 * @param distinct
	 *            <code>true</code> iff the keyword DISTINCT was used, for
	 *            example <code>COUNT(DISTINCT y)</code>
	 * @param expr
	 *            The value expression to be computed, for example
	 *            <code>x</code> in <code>COUNT(DISTINCT x)</code> or
	 *            <code>y+x</code> in <code>MIN(x+y)</code>. Note that only
	 *            COUNT may be used with the special variable <code>*</code>.
	 */
	public AggregateBase(final FunctionCode functionCode,
			final boolean distinct, final IValueExpression<E> expr) {

		this(new BOp[] { expr }, NV.asMap(//
				new NV(Annotations.FUNCTION_CODE, functionCode), //
				new NV(Annotations.DISTINCT, distinct))//
		);

	}

	final public boolean isDistinct() {

		return getProperty(Annotations.DISTINCT, Annotations.DEFAULT_DISTINCT);

	}

	@SuppressWarnings("unchecked")
	final public IValueExpression<E> getExpression() {

		return (IValueExpression<E>) get(0);

	}

	public boolean isWildcard() {

		return get(0).equals(Var.var("*"));
		
	}

	/**
	 * Operation is not implemented by this class and must be overridden if the
	 * {@link AggregateBase} is to be directly evaluated. However, note that the
	 * computation of aggregate functions is often based on hard coded
	 * recognition of the appropriate function code.
	 */
	public E get(IBindingSet bset) {
		throw new UnsupportedOperationException();
	}

	public AggregateBase<E> setExpression(final IValueExpression<E> newExpr) {

		if (newExpr == null)
			throw new IllegalArgumentException();

		final AggregateBase<E> tmp = (AggregateBase<E>) this.clone();

		tmp._set(0, newExpr);

		return tmp;

	}

}
