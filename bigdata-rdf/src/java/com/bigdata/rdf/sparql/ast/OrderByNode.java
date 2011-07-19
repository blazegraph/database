package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.internal.IV;

public class OrderByNode {

	private final IValueExpressionNode ve;
	
	private final boolean ascending;
	
	public OrderByNode(final IValueExpressionNode ve, final boolean ascending) {
		
		this.ve = ve;
		this.ascending = ascending;
		
	}
	
	public IValueExpressionNode getValueExpressionNode() {
		return ve;
	}

	public IValueExpression<? extends IV> getValueExpression() {
		return ve.getValueExpression();
	}

	public boolean isAscending() {
		return ascending;
	}
	
	public String toString() {
		
		final StringBuilder sb = new StringBuilder();
		
		if (!ascending) {
			sb.append("desc(");
		}
		
		sb.append("?").append(ve.toString());
		
		if (!ascending) {
			sb.append(")");
		}
		
		return sb.toString();
		
	}

}
