package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.internal.IV;

/**
 * An AST node which models a value expression.
 */
public interface IValueExpressionNode {

    /**
     * Return the {@link IValueExpression}.
     */
	IValueExpression<? extends IV> getValueExpression();
	
}
