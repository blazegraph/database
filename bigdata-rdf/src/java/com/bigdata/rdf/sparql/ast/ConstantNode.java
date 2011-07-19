package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IConstant;
import com.bigdata.rdf.internal.IV;

/**
 * Used to represent a constant in the AST.
 * 
 * @author mikepersonick
 */
public class ConstantNode extends TermNode {

	public ConstantNode(final IConstant<IV> val) {
		
		super(val);
		
	}
	
	public ConstantNode(final IV val) {
		
		super(new Constant<IV>(val));
		
	}
	
	public IConstant<IV> getVal() {
		
		return (IConstant<IV>) getValueExpression();
		
	}
	
}
