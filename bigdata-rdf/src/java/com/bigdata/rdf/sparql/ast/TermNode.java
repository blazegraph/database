package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
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
	private static final long serialVersionUID = 2050144811725774174L;

//    interface Annotations extends ASTBase.Annotations {
//  	  
//        /**
//         * The term's value, if it has one.
//         */
//        String VAL = TermNode.class.getName() + ".val";
//        
//        /**
//         * The term's var name, if it has one.
//         */
//        String VAR = TermNode.class.getName() + ".var";
//
//    }


	public TermNode(final BOp[] args, final Map<String, Object> anns) {
		
		super(args, anns);
		
	}
	
	/**
	 * Strengthen the return type.
	 */
	@Override
	public IVariableOrConstant<IV> getValueExpression() {
		
		return (IVariableOrConstant<IV>) super.getValueExpression();
		
	}
	
//	public IVariable<IV> getVar() {
//		
//		return (IVariable<IV>) getProperty(TermNode.Annotations.VAR);
//		
//	}
//	
//	public IConstant<IV> getVal() {
//		
//		return (IConstant<IV>) getProperty(TermNode.Annotations.VAL);
//		
//	}
	
    public BigdataValue getValue() {
	
    	final IVariableOrConstant<IV> val = getValueExpression();
    	
    	if (val != null && val instanceof IConstant) {
    		
    		final IV iv = val.get();
    		
    		if (iv.hasValue()) {
    			
    			return iv.getValue();
    			
    		}
    		
    	}
    	
    	return null;

    }
    
}
