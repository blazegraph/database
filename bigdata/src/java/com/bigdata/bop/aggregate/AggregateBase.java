package com.bigdata.bop.aggregate;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.ImmutableBOp;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.bop.BOp.Annotations;

/**
 * Abstract base class for aggregate functions.
 * 
 * @author thompsonbry
 *
 * @param <E>
 */
abstract public class AggregateBase<E> extends ImmutableBOp implements IAggregate<E> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public interface Annotations extends ImmutableBOp.Annotations {

		/**
		 * Optional boolean property indicates whether the aggregate applies to
		 * the distinct within group solutions (default
		 * {@value #DEFAULT_DISTINCT}).
		 */
		String DISTINCT = AggregateBase.class.getName()+".distinct";

		boolean DEFAULT_DISTINCT = false;
		
	}

	public AggregateBase(BOpBase op) {
		super(op);
	}

	public AggregateBase(BOp[] args, Map<String, Object> annotations) {

		super(args, annotations);

		if (!isWildcardAllowed() && getExpression() == Var.var("*")) {

			/*
			 * Only COUNT may use the wildcard '*' variable.
			 */

			throw new UnsupportedOperationException("'*' not permitted.");
			
		}
		
	}

	/**
	 * 
	 * @param distinct
	 *            <code>true</code> iff the keyword DISTINCT was used, for
	 *            example <code>COUNT(DISTINCT y)</code>
	 * @param expr
	 *            The value expression to be computed, for example
	 *            <code>x</code> in <code>COUNT(DISTINCT x)</code> or
	 *            <code>y+x</code> in <code>MIN(x+y)</code>.
	 */
	public AggregateBase(final boolean distinct, final IValueExpression<E> expr) {

		this(new BOp[] { expr }, distinct ? NV.asMap(new NV(
				Annotations.DISTINCT, true)) : null);

	}

	final public boolean isDistinct() {

		return getProperty(Annotations.DISTINCT, Annotations.DEFAULT_DISTINCT);

	}

	@SuppressWarnings("unchecked")
	final public IValueExpression<E> getExpression() {

		return (IValueExpression<E>) get(0);

	}

	/**
	 * Return <code>true</code> iff the {@link IValueExpression} may be the
	 * special variable <code>*</code>.  The default implementation always
	 * returns <code>false</code>.
	 */
	public boolean isWildcardAllowed() {

		return false;
		
	}
	
}
