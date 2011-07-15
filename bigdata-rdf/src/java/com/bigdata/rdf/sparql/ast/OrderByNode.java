package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;

public class OrderByNode {

	private final IVariable<IV> var;
	
	private final boolean ascending;
	
	public OrderByNode(final IVariable<IV> var, final boolean ascending) {
		
		this.var = var;
		this.ascending = ascending;
		
	}
	
	public IVariable<IV> getVar() {
		return var;
	}

	public boolean isAscending() {
		return ascending;
	}
	
	public String toString() {
		
		final StringBuilder sb = new StringBuilder();
		
		if (!ascending) {
			sb.append("desc(");
		}
		
		sb.append("?").append(var.getName());
		
		if (!ascending) {
			sb.append(")");
		}
		
		return sb.toString();
		
	}

}
