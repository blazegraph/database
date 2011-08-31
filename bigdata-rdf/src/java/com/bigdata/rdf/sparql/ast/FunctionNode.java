package com.bigdata.rdf.sparql.ast;

import java.util.Collections;
import java.util.Map;

import org.openrdf.model.URI;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.ModifiableBOpBase;

/**
 * AST node for anything which is neither a constant nor a variable, including
 * math operators, string functions, etc.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class FunctionNode extends ValueExpressionNode {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    interface Annotations extends ValueExpressionNode.Annotations {
  
        /**
         * The function URI from the {@link FunctionRegistry}.
         */
        String FUNCTION_URI = FunctionNode.class.getName() + "functionURI";

        /**
         * The scalar values needed to construct the IValueExpression.
         */
        String SCALAR_VALS = FunctionNode.class.getName() + "scalarVals";

    }

    /**
     * Construct a function node in the AST.
     * 
     * @param functionURI
     *            the function URI. see {@link FunctionRegistry}
     * @param scalarValues
     *            One or more scalar values that are passed to the function
     * @param args
     *            the arguments to the function.
     * 
     *            FIXME Drop the [lex] argument and move the conversion from a
     *            {@link FunctionNode} into an {@link IValueExpression} into
     *            {@link AST2BOpContext}.
     */
	public FunctionNode(//final String lex, 
			final URI functionURI, 
	        final Map<String,Object> scalarValues,
			final ValueExpressionNode... args) {
		
//		super(FunctionRegistry.toVE(lex, functionURI, scalarValues, args));
		super(args, null);
		
		super.setProperty(Annotations.SCALAR_VALS, scalarValues);
		super.setProperty(Annotations.FUNCTION_URI, functionURI);
		
	}
	
	public URI getFunctionURI() {
		
		return (URI) getRequiredProperty(Annotations.FUNCTION_URI);
		
	}
	
	/**
	 * Returns an unmodifiable map because if the scalar values are modified,
	 * we need to clear the value expression cache.  This is handled correctly
	 * by {@link #setScalarValues(Map)}.
	 */
	public Map<String,Object> getScalarValues() {
		
		return Collections.unmodifiableMap(
				(Map<String,Object>) getRequiredProperty(Annotations.SCALAR_VALS));
		
	}
	
	/**
	 * Sets the scalar values and clears the cached value expression.
	 */
	public void setScalarValues(final Map<String,Object> scalarValues) {
		
		setProperty(Annotations.SCALAR_VALS, scalarValues);
		
	}
	
//    @Override
//    public boolean equals(final Object o) {
//
//        if (o == this)
//            return true;
//
//        if (!(o instanceof FunctionNode))
//            return false;
//        
//        if (!super.equals(o))
//            return false;
//
//        return true;
//
//    }

	/**
	 * If we destructively modify the AST node we also need to clear the
	 * cached value expression.
	 */
	@Override
	public ModifiableBOpBase setArg(final int index, final BOp newArg) {
		
		final ModifiableBOpBase bop = super.setArg(index, newArg);
		
		super.clearProperty(Annotations.VALUE_EXPR);
		
		return bop;
		
	}

	/**
	 * If we destructively modify the AST node we also need to clear the
	 * cached value expression.
	 */
	@Override
	public void addArg(final BOp newArg) {
		
		super.addArg(newArg);
		
		super.clearProperty(Annotations.VALUE_EXPR);
		
	}

	/**
	 * If we destructively modify the AST node we also need to clear the
	 * cached value expression.
	 */
	@Override
	public void addArgIfAbsent(final BOp arg) {
		
		super.addArgIfAbsent(arg);
		
		super.clearProperty(Annotations.VALUE_EXPR);
		
	}

	/**
	 * If we destructively modify the AST node we also need to clear the
	 * cached value expression.
	 */
	@Override
	public boolean removeArg(final BOp arg) {
		
		final boolean b = super.removeArg(arg);
		
		super.clearProperty(Annotations.VALUE_EXPR);
		
		return b;
		
	}

	/**
	 * If we destructively modify the AST node we also need to clear the
	 * cached value expression.
	 */
	@Override
	public ModifiableBOpBase copyAll(final Map<String, Object> anns) {
		
		final ModifiableBOpBase bop = super.copyAll(anns);
		
		super.clearProperty(Annotations.VALUE_EXPR);
		
		return bop;
		
	}

	/**
	 * If we destructively modify the AST node we also need to clear the
	 * cached value expression.
	 */
	@Override
	public ModifiableBOpBase setProperty(final String name, final Object value) {
		
		final ModifiableBOpBase bop = super.setProperty(name, value);
		if(!(Annotations.VALUE_EXPR.equals(name))){
		
		super.clearProperty(Annotations.VALUE_EXPR);
		
		}

		return bop;
		
	}

	/**
	 * If we destructively modify the AST node we also need to clear the
	 * cached value expression.
	 */
	@Override
	public ModifiableBOpBase setUnboundProperty(final String name, final Object value) {
		
		final ModifiableBOpBase bop = super.setUnboundProperty(name, value);
		
		if(!(Annotations.VALUE_EXPR.equals(name))){

		super.clearProperty(Annotations.VALUE_EXPR);
		
		}
		return bop;
		
	}

	/**
	 * If we destructively modify the AST node we also need to clear the
	 * cached value expression.
	 */
	@Override
	public ModifiableBOpBase clearProperty(final String name) {
		
		final ModifiableBOpBase bop = super.clearProperty(name);
		
		super.clearProperty(Annotations.VALUE_EXPR);
		
		return bop;
		
	}

	
}
