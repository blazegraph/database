package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.internal.IV;

public class ValueExpressionNode extends ASTBase implements
        IValueExpressionNode {

	/**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    interface Annotations extends ASTBase.Annotations {
  
        /**
         * The {@link IValueExpression}.
         * 
         * Note: This is really an instance of cached data. If the argument
         * corresponding the the {@link IValueExpressionNode} is updated, this
         * annotation must be re-computed.
         */
        String VALUE_EXPR = "valueExpr";

    }
    
//    private final IValueExpression<? extends IV> ve;

    /**
     * FIXME Just for compatibility with SOp2ASTUtility. Remove when done
     * with AST -> AST direct translation.
     */
    @Deprecated
    public ValueExpressionNode(final IValueExpression<? extends IV> ve) {
    	
        super(BOp.NOARGS, null/* anns */);

        setProperty(Annotations.VALUE_EXPR, ve);
        
    }
    
    public ValueExpressionNode(final BOp[] args, final Map<String, Object> anns) {

    	super(args, anns);

    }

    /**
     * Return <code>true</code> iff this is a {@link ConstantNode}.
     */
    public final boolean isConstant() {
        
        return this instanceof ConstantNode;
        
    }

    /**
     * Return <code>true</code> iff this is a {@link VarNode}.
     */
    public final boolean isVariable() {
        
        return this instanceof VarNode;
        
    }

    /**
     * Return <code>true</code> iff this is a {@link FunctionNode}.
     */
    public final boolean isFunction() {
        
        return this instanceof FunctionNode;
        
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public IValueExpression<? extends IV> getValueExpression() {

        return (IValueExpression) getProperty(Annotations.VALUE_EXPR);

    }
	
    /**
     * Called by AST2BOpUtility to populate the value expression nodes
     * with value expressions. 
     */
    @SuppressWarnings({ "rawtypes" })
    public void setValueExpression(IValueExpression<? extends IV> ve) {

        setProperty(Annotations.VALUE_EXPR, ve);

    }

    public void invalidate() {
        
        setProperty(Annotations.VALUE_EXPR, null);
        
    }
    
    public String toShortString() {

//        return super.toString();
        return getClass().getSimpleName() + "(" + getValueExpression() + ")";

    }
    
}
