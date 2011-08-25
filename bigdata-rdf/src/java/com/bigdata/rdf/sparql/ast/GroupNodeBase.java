package com.bigdata.rdf.sparql.ast;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.bigdata.bop.BOp;

import cutthecrap.utils.striterators.EmptyIterator;
import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.SingleValueIterator;
import cutthecrap.utils.striterators.Striterator;

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

    /**
     * Visits itself and then its children (recursively).
     */
    @SuppressWarnings("unchecked")
    public Iterator<IQueryNode> preOrderIterator() {

        return new Striterator(new SingleValueIterator(this))
                .append(preOrderIterator2(0, this));

    }

    /**
     * Visits the children (recursively) using pre-order traversal, but does NOT
     * visit this node.
     */
    @SuppressWarnings("unchecked")
    static private Iterator<BOp> preOrderIterator2(final int depth,
            final GroupNodeBase op) {

        /*
         * Iterator visits the direct children, expanding them in turn with a
         * recursive application of the pre-order iterator.
         */
        
        // mild optimization when no children are present.
        if (op.isEmpty())
            return EmptyIterator.DEFAULT;

        return new Striterator(op.children.iterator()).addFilter(new Expander() {

            private static final long serialVersionUID = 1L;

            /*
             * Expand each child in turn.
             */
            protected Iterator expand(final Object childObj) {

                /*
                 * A child of this node.
                 */

                final IGroupMemberNode child = (IGroupMemberNode) childObj;

                if (child instanceof GroupNodeBase<?>) {

                    /*
                     * The child is a Node (has children).
                     * 
                     * Visit the children (recursive pre-order traversal).
                     */

//                  System.err.println("Node["+depth+"]: "+op.getClass().getName());

                    final Striterator itr = new Striterator(
                            new SingleValueIterator(child));

                    // append this node in post-order position.
                    itr.append(preOrderIterator2(depth + 1,
                            (GroupNodeBase) child));

                    return itr;

                } else {

                    /*
                     * The child is a leaf.
                     */

//                  System.err.println("Leaf["+depth+"]: "+op.getClass().getName());
                    
                    // Visit the leaf itself.
                    return new SingleValueIterator(child);

                }

            }
        });

    }

}
