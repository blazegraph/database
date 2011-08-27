package com.bigdata.rdf.sparql.ast;

import java.util.Iterator;

import com.bigdata.bop.BOp;

/**
 * Base class for AST group nodes.
 */
public abstract class GroupNodeBase<E extends IGroupMemberNode> extends
        GroupMemberNodeBase<E> implements IGroupNode<E> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    interface Annotations extends GroupMemberNodeBase.Annotations {
    
        String OPTIONAL = "optional";

        boolean DEFAULT_OPTIONAL = false;

    }

//    private final List<E> children;
	
	protected GroupNodeBase(final boolean optional) {
		
		setOptional( optional );
		
    }

	@SuppressWarnings({ "unchecked", "rawtypes" })
    public Iterator<E> iterator() {
		
		return (Iterator) argIterator();
		
	}

	public IGroupNode<E> addChild(final E child) {
		
	    if(child == this)
	        throw new IllegalArgumentException();
	    
        addArg((BOp) child);

		child.setParent(this);
		
		return this;
		
	}
	
	public IGroupNode<E> removeChild(final E child) {

        if (child == null)
            throw new IllegalArgumentException();

        if (child == this)
            throw new IllegalArgumentException();

        if (removeArg((BOp) child))
            child.setParent(null);
		
		return this;
		
	}
	
    public boolean isEmpty() {
        
        return arity() == 0;
        
    }
    
	public int size() {
		
		return arity();
		
	}
	
	public boolean isOptional() {
		
        return getProperty(Annotations.OPTIONAL, Annotations.DEFAULT_OPTIONAL);
		
	}
	
	public void setOptional(final boolean optional) {
	    
	    setProperty(Annotations.OPTIONAL, optional);
	    
	}
	
//    public boolean equals(final Object o) {
//
//        if (this == o)
//            return true;
//
//        if (!(o instanceof GroupNodeBase))
//            return false;
//
//        final GroupNodeBase<?> t = (GroupNodeBase<?>) o;
//
//        if (optional != t.optional)
//            return false;
//
//        if (!children.equals(t.children))
//            return false;
//        
//        return true;
//
//    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to also clone the children and then set the parent reference
     * on the cloned children.
     */
    @Override
    public GroupNodeBase<E> clone() {

        @SuppressWarnings("unchecked")
        final GroupNodeBase<E> tmp = (GroupNodeBase<E>) super.clone();

        final int size = size();

        for (int i = 0; i < size; i++) {

            IGroupMemberNode aChild = (IGroupMemberNode) tmp.get(i);

            aChild = (IGroupMemberNode) ((ASTBase) aChild).clone();

            tmp.setArg(i, (ASTBase) aChild);

        }

        return tmp;

    }

}
