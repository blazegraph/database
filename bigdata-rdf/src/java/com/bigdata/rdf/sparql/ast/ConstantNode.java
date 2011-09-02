package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.BOp;
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
		
        super(new BOp[] { val }, null);
		
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

        final IConstant<IV> c = getValueExpression();
        
        return "ConstantNode(" + c + ")";

    }

}
