package com.bigdata.rdf.sparql.ast;

public abstract class QueryNodeBase implements IQueryNode {

	private IGroupNode parent;
	
	public QueryNodeBase() {
	}

	public void setParent(final IGroupNode parent) {
		this.parent = parent;
	}

	public IGroupNode getParent() {
		return parent;
	}
	
}
