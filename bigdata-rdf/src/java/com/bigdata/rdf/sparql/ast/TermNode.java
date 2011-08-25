package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;

/**
 * Used to represent a variable or constant in the AST (a term in a statement
 * pattern for example).
 * 
 * @author mikepersonick
 */
public abstract class TermNode extends ValueExpressionNode {

	/**
     * 
     */
    private static final long serialVersionUID = 1L;

    public TermNode(final IVariableOrConstant<IV> term) {
		
		super(term);
		
	}
	
	/**
	 * Strengthen the return type.
	 */
	@Override
	public IVariableOrConstant<IV> getValueExpression() {
		
		return (IVariableOrConstant<IV>) super.getValueExpression();
	}
	
    public BigdataValue getValue() {
		
		final IVariableOrConstant<IV> ve = getValueExpression();
		
		if (ve instanceof IConstant) {
			
			final IV iv = ((IConstant<IV>) ve).get();
			
			if (iv.hasValue()) {
				
				return iv.getValue();
				
			}
			
		}
		
		return null;
		
	}
	
}
