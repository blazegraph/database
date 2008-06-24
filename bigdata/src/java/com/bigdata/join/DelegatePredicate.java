package com.bigdata.join;

/**
 * Delegation pattern for an {@link IPredicate}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 */
public class DelegatePredicate<E> implements IPredicate<E> {
    
    private final IPredicate<E> src;

    public DelegatePredicate(IPredicate<E> src) {
        
        if (src == null)
            throw new IllegalArgumentException();
        
        this.src = src;
        
    }

    public int arity() {
        return src.arity();
    }

    public IPredicate<E> asBound(IBindingSet bindingSet) {
        return src.asBound(bindingSet);
    }

    public void copyValues(E e, IBindingSet bindingSet) {
        src.copyValues(e, bindingSet);
    }

    public boolean equals(IPredicate<E> other) {
        return src.equals(other);
    }

    public IVariableOrConstant get(int index) {
        return src.get(index);
    }

    public IPredicateConstraint<E> getConstraint() {
        return src.getConstraint();
    }

    public IRelation<E> getRelation() {
        return src.getRelation();
    }
    
    public IAccessPath<E> getAccessPath() {
        return src.getAccessPath();
    }

    public int getVariableCount() {
        return src.getVariableCount();
    }

    public boolean isConstant() {
        return src.isConstant();
    }

    public String toString() {
        return src.toString();
    }

    public String toString(IBindingSet bindingSet) {
        return src.toString(bindingSet);
    }
    
    
}