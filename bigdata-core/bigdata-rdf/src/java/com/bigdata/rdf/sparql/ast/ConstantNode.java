package com.bigdata.rdf.sparql.ast;

import java.util.Map;

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

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public ConstantNode(ConstantNode op) {
        super(op);
    }
    
    /**
     * Required shallow copy constructor.
     */
    public ConstantNode(final BOp[] args, final Map<String, Object> anns) {
        
        super(args, anns);
        
    }

    @SuppressWarnings("rawtypes")
    public ConstantNode(final IV val) {

		this(new Constant<IV>(val));
		
	}
	
    @SuppressWarnings("rawtypes")
	public ConstantNode(final IConstant<IV> val) {
		
        super(new BOp[] { val }, null);
		
	}
	
	/**
	 * Strengthen return type.
	 */
	@SuppressWarnings("rawtypes")
    @Override
	public IConstant<IV> getValueExpression() {
		
		return (IConstant<IV>) super.getValueExpression();
		
	}
	
    @Override
    public String toString() {

        final IConstant<?> c = getValueExpression();
        
        return "ConstantNode(" + c + ")";

    }

}
