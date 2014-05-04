package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.internal.IV;

/**
 * An AST node which models a value expression.
 */
public interface IValueExpressionNode {

    /**
     * Return the cached {@link IValueExpression} if it exists on the node,
     * otherwise return null.
     */
    IValueExpression<? extends IV> getValueExpression();
    
    /**
     * Return the cached {@link IValueExpressionNode} if it exists on the node.
     * 
     * @return The {@link IValueExpressionNode} and never <code>null</code>.
     * 
     * @throws IllegalStateException
     *             if the {@link IValueExpressionNode} is not cached.
     */
    IValueExpression<? extends IV> getRequiredValueExpression();
    
	/**
	 * Cache the translated value expression on the node.
	 */
	void setValueExpression(final IValueExpression<? extends IV> ve);

	/**
	 * Invalidate the cached value.  It will be recomputed again on demand.
	 */
	void invalidate();
	
	/**
	 * A string representation of a recursive structure with pretty-print indent.
	 * @param indent
	 * @return
	 */
	String toString(final int indent);
	
}
