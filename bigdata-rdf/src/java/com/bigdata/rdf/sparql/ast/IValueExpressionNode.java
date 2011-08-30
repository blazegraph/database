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
	 * Cache the translated value expression on the node.
	 */
	void setValueExpression(final IValueExpression<? extends IV> ve);
	
}
