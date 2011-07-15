package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.internal.IV;

public interface IValueExpressionNode {

	IValueExpression<? extends IV> getValueExpression();
	
}
