package com.bigdata.rdf.sparql.ast;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Base class for AST group nodes.
 */
public abstract class GroupNodeBase extends QueryNodeBase implements IGroupNode {

	private final List<IQueryNode> children;
	
	private final boolean optional;
	
	protected GroupNodeBase(final boolean optional) {
		
		this.children = new LinkedList<IQueryNode>();
		
		this.optional = optional;
		
	}
	
	public Iterator<IQueryNode> iterator() {
		
		return children.iterator();
		
	}
	
	public IGroupNode addChild(final IQueryNode child) {
		
		children.add(child);
		
		child.setParent(this);
		
		return this;
		
	}
	
	public IGroupNode removeChild(final IQueryNode child) {
		
		children.remove(child);
		
		child.setParent(null);
		
		return this;
		
	}
	
	public int getChildCount() {
		
		return children.size();
		
	}
	
	public boolean isOptional() {
		
		return optional;
		
	}
	
}
