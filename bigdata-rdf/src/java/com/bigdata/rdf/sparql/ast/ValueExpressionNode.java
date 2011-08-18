package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.internal.IV;

public class ValueExpressionNode implements IValueExpressionNode {
	
	private final IValueExpression<? extends IV> ve;
	
	public ValueExpressionNode(final IValueExpression<? extends IV> ve) {
		
		this.ve = ve;
		
	}
	
	public IValueExpression<? extends IV> getValueExpression() {
		
		return ve;
		
	}
	
    public String toString() {

        return getClass().getSimpleName() + "(" + ve + ")";

    }
    
}
