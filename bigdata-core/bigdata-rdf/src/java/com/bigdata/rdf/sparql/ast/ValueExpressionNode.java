package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.internal.IV;

/**
 * AST node for value expressions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class ValueExpressionNode extends ASTBase implements
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

    /**
     * @deprecated This was just for compatibility with SOp2ASTUtility. It is
     *             only used by the test suite now. It should be removed now
     *             that we are done with the SPARQL to AST direct translation.
     */
    @Deprecated
    public ValueExpressionNode(final IValueExpression<? extends IV> ve) {
    	
        super(BOp.NOARGS, null/* anns */);

        setProperty(Annotations.VALUE_EXPR, ve);
        
    }
    
    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public ValueExpressionNode(final ValueExpressionNode op) {
        super(op);
    }

    /**
     * Required shallow copy constructor.
     */
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

    final public IValueExpression<? extends IV> getRequiredValueExpression() {

        final IValueExpression<? extends IV> valueExpr = getValueExpression();

        if (valueExpr == null)
            throw new IllegalStateException("ValueExpression not set: " + this);

        return valueExpr;

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
    public void setValueExpression(final IValueExpression<? extends IV> ve) {

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
