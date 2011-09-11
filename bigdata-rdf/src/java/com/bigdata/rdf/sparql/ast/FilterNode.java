package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.internal.IV;

/**
 * AST node models a value expression which imposes a constraint.
 */
public class FilterNode extends GroupMemberValueExpressionNodeBase {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Required deep copy constructor.
     */
    public FilterNode(FilterNode op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public FilterNode(BOp[] args, Map<String, Object> anns) {

        super(args, anns);

    }
    
    /**
     * 
     * @param ve
     *            A value expression which places a constraint on the query.
     */
	public FilterNode(final IValueExpressionNode ve) {
		
        super(new BOp[] { (BOp) ve }, null/* anns */);

	}
	
	public IValueExpression<? extends IV> getValueExpression() {

	    return getValueExpressionNode().getValueExpression();
	    
	}
	
    public IValueExpressionNode getValueExpressionNode() {

        return (IValueExpressionNode) get(0);
        
	}

    @Override
    public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder();

        sb.append("\n");
        sb.append(indent(indent));
        sb.append("FILTER( ").append(getValueExpressionNode()).append(" )");

        return sb.toString();

    }

}
