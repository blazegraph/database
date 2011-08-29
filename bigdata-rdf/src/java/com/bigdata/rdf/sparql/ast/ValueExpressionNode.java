package com.bigdata.rdf.sparql.ast;

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
         * FIXME This is really an instance of cached data. If the argument
         * corresponding the the {@link IValueExpressionNode} is updated, this
         * annotation must be re-computed.
         * <p>
         * In my opinion, the cached value should be replaced by on demand
         * generation of the IValueExpression WITHOUT caching, and that
         * conversion to IValueExpressions should be pushed into an
         * IASTOptimizer and defer until the query plan is being generated.
         */
        String VALUE_EXPR = "valueExpr";

    }
    
//    private final IValueExpression<? extends IV> ve;

    public ValueExpressionNode(final IValueExpression<? extends IV> valueExpr) {

        super(BOp.NOARGS, null/* anns */);

        setProperty(Annotations.VALUE_EXPR, valueExpr);

    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public IValueExpression<? extends IV> getValueExpression() {

        return (IValueExpression) getProperty(Annotations.VALUE_EXPR);

    }
	
    @SuppressWarnings({ "rawtypes" })
    public void setValueExpression(IValueExpression<? extends IV> ve) {

        setProperty(Annotations.VALUE_EXPR, ve);

    }
    
    public String toString() {

        return getClass().getSimpleName() + "(" + getValueExpression() + ")";

    }
    
//    @Override
//    public boolean equals(final Object o) {
//
//        if (o == this)
//            return true;
//
//        if (!(o instanceof ValueExpressionNode))
//            return false;
//        
//        if (!ve.equals(((ValueExpressionNode) o).ve))
//            return false;
//
//        return true;
//
//    }

}
