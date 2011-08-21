package com.bigdata.rdf.sparql.ast;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Base class for AST group nodes.
 */
public abstract class GroupNodeBase extends GroupMemberNodeBase implements
        IGroupNode {

	private final List<IGroupMemberNode> children;
	
	private boolean optional;
	
	protected GroupNodeBase(final boolean optional) {
		
		this.children = new LinkedList<IGroupMemberNode>();
		
		this.optional = optional;
		
    }

	public Iterator<IGroupMemberNode> iterator() {
		
		return children.iterator();
		
	}
	
	public IGroupNode addChild(final IGroupMemberNode child) {
		
		children.add(child);
		
		child.setParent(this);
		
		return this;
		
	}
	
	public IGroupNode removeChild(final IGroupMemberNode child) {
		
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
	
	public void setOptional(final boolean optional) {
	    
	    this.optional = optional;
	    
	}
	
    public boolean equals(final Object o) {

        if (this == o)
            return true;

        if (!(o instanceof GroupNodeBase))
            return false;

        final GroupNodeBase t = (GroupNodeBase) o;

        if (optional != t.optional)
            return false;

        if (!children.equals(t.children))
            return false;
        
        return true;

    }
    
}
