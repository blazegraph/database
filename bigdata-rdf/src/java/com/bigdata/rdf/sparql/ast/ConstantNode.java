package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.BOpBase;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IConstant;
import com.bigdata.rdf.internal.IV;

/**
 * Used to represent a constant in the AST.
 * 
 * @author mikepersonick
 */
public class ConstantNode extends TermNode {

	/**
     * 
     */
    private static final long serialVersionUID = 1L;

	public ConstantNode(final IV val) {

		this(new Constant<IV>(val));
		
	}
	
	public ConstantNode(final IConstant<IV> val) {
		
		super(BOpBase.NOARGS, null);
		
		setValueExpression(val);
		
	}
	
	/**
	 * Strengthen return type.
	 */
	@Override
	public IConstant<IV> getValueExpression() {
		
		return (IConstant<IV>) super.getValueExpression();
		
	}
	
    @Override
    public String toString() {

        return "ConstantNode(" + getValueExpression().get() + ")";

    }

}
