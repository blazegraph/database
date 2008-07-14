package com.bigdata.relation.rule.eval;

import java.util.Arrays;

import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.IVariableOrConstant;

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

    public boolean equals(IPredicate<E> other) {
        return src.equals(other);
    }

    public IVariableOrConstant get(int index) {
        return src.get(index);
    }

    public IElementFilter<E> getConstraint() {
        return src.getConstraint();
    }

    public String getOnlyRelationName() {
        return src.getOnlyRelationName();
    }
    
    public String getRelationName(int index) {
        return src.getRelationName(index);
    }
    
    public int getRelationCount() {
        return src.getRelationCount();
    }

    public int getVariableCount() {
        return src.getVariableCount();
    }

    public boolean isFullyBound() {
        return src.isFullyBound();
    }

    public String toString() {
        return src.toString();
    }

    public String toString(IBindingSet bindingSet) {
        return src.toString(bindingSet);
    }
       
}