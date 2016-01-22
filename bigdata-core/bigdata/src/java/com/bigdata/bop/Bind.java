package com.bigdata.bop;

import java.util.Map;

/**
 * Operator causes a variable to be bound to the result of its evaluation as a
 * side-effect.
 * 
 * @author thompsonbry
 */
public class Bind<E> extends ImmutableBOp implements IValueExpression<E>, IBind<E> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
	 */
	public Bind(Bind<E> op) {
		super(op);
	}

	/**
	 * @param var
	 *            The {@link IVariable} which will be bound to the result of
	 *            evaluating the associated value expression.
	 * @param expr
	 *            The {@link IValueExpression} to be evaluated.
	 */
	public Bind(IVariable<E> var, IValueExpression<E> expr) {

		this(new BOp[] { var, expr }, null/* annotations */);
		
	}
    
    /**
	 * Required shallow copy constructor.
	 * @param args
	 * @param annotations
	 */
	public Bind(BOp[] args, Map<String, Object> annotations) {
		super(args, annotations);
	}

	/**
	 * Return the variable which will be bound to the result of evaluating the
	 * associated value expression.
	 */
	@SuppressWarnings("unchecked")
    public IVariable<? extends E> getVar() {

        return (IVariable<? extends E>) get(0);

	}

	/**
	 * Return the value expression.
	 */
	@SuppressWarnings("unchecked")
	public IValueExpression<E> getExpr() {

		return (IValueExpression<E>) get(1);

	}

	public E get(final IBindingSet bindingSet) {

		final IVariable<? extends E> var = getVar();

		final IValueExpression<E> expr = getExpr();

		// evaluate the value expression.
		final E val = expr.get(bindingSet);

		if(val==null){
		      return null;
		}
		
		// bind the variable as a side-effect.
		bindingSet.set(var, new Constant<E>(val));

		// return the evaluated value
		return val;

	}

}
