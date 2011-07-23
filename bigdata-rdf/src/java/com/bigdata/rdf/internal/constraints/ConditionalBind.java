package com.bigdata.rdf.internal.constraints;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.ImmutableBOp;
import com.bigdata.rdf.internal.IV;

/**
 * Operator causes a variable to be bound to the result of its evaluation as a
 * side-effect.
 * 
 * @author thompsonbry
 */
public class ConditionalBind extends ImmutableBOp implements IValueExpression<IV> {

	private static final long serialVersionUID = 1L;

	/**
	 * Required deep copy constructor.
	 */
	public ConditionalBind(ConditionalBind op) {
		super(op);
	}

	/**
	 * @param var
	 *            The {@link IVariable} which will be bound to the result of
	 *            evaluating the associated value expression.
	 * @param expr
	 *            The {@link IValueExpression} to be evaluated.
	 */
	public ConditionalBind(IVariable<? extends IV> var, IValueExpression<? extends IV> expr) {

		this(new BOp[] { var, expr }, null/* annotations */);
		
	}
    
    /**
	 * Required shallow copy constructor.
	 * @param args
	 * @param annotations
	 */
	public ConditionalBind(BOp[] args, Map<String, Object> annotations) {
		super(args, annotations);
	}

	/**
	 * Return the variable which will be bound to the result of evaluating the
	 * associated value expression.
	 */
	@SuppressWarnings("unchecked")
	public IVariable<? extends IV> getVar() {

		return (IVariable<? extends IV>) get(0);

	}

	/**
	 * Return the value expression.
	 */
	@SuppressWarnings("unchecked")
	public IValueExpression<? extends IV> getExpr() {

		return (IValueExpression<? extends IV>) get(1);

	}

	public IV get(final IBindingSet bindingSet) {

		final IVariable<? extends IV> var = getVar();

		final IValueExpression<? extends IV> expr = getExpr();

		// evaluate the value expression.
		IV val = expr.get(bindingSet);
		
		IV existing = var.get(bindingSet);
		if(existing==null){

		    // bind the variable as a side-effect.
		    bindingSet.set(var, new Constant<IV>(val));
		
		      // return the evaluated value
	        return val;
		}else{
		    return (val.equals(existing))?val:null;
		}


	}

}
