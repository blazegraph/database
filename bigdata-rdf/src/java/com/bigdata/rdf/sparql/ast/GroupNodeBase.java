package com.bigdata.rdf.sparql.ast;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Base class for AST group nodes.
 */
public abstract class GroupNodeBase<E extends IGroupMemberNode> extends
        GroupMemberNodeBase<E> implements IGroupNode<E> {

	private final List<E> children;
	
	private boolean optional;
	
	protected GroupNodeBase(final boolean optional) {
		
		this.children = new LinkedList<E>();
		
		this.optional = optional;
		
    }

	public Iterator<E> iterator() {
		
		return children.iterator();
		
	}

	public IGroupNode<E> addChild(final E child) {
		
		children.add(child);
		
		child.setParent(this);
		
		return this;
		
	}
	
	public IGroupNode<E> removeChild(final E child) {
		
		children.remove(child);
		
		child.setParent(null);
		
		return this;
		
	}
	
    public boolean isEmpty() {
        
        return children.isEmpty();
        
    }
    
	public int getChildCount() {
		
		return children.size();
		
	}
	
	public boolean isOptional() {
		
		return optional;
		
	}
	
	public void setOptional(final boolean optional) {
	    
	    this.optional = optional;
	    
	}
	
    public boolean equals(final Object o) {

        if (this == o)
            return true;

        if (!(o instanceof GroupNodeBase))
            return false;

        final GroupNodeBase<?> t = (GroupNodeBase<?>) o;

        if (optional != t.optional)
            return false;

        if (!children.equals(t.children))
            return false;
        
        return true;

    }
    
}
