package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.lexicon.LexiconRelation;

/**
 * An AST node which models a value expression.
 */
public interface IValueExpressionNode {

    /**
     * Return the {@link IValueExpression}.
     * 
     * @deprecated This needs to be replaced by the conversion of the
     *             {@link IValueExpressionNode}s by {@link AST2BOpUtility},
     *             which will have to provide sufficient context to resolve the
     *             {@link LexiconRelation}'s namespace as well.
     */
	IValueExpression<? extends IV> getValueExpression();
	
}
