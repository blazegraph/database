/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/
/*
 * Created on Jun 19, 2008
 */

package com.bigdata.join;


/**
 * A generic implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class Predicate<E> implements IPredicate<E> {

    private final int arity;
    
    /** #of unbound variables. */
    private final int nvars;

    private final IVariableOrConstant[] values;
    
    private final IPredicateConstraint<E> constraint;

    /**
     * 
     * @param values
     *            The values (order is important!).
     */
    public Predicate(IVariableOrConstant[] values) {
        
        this(null/* constraint */, values);
        
    }

    /**
     * 
     * @param constraint
     *            An optional constraint.
     * @param values
     *            The values (order is important!).
     */
    public Predicate(IPredicateConstraint<E> constraint,
            IVariableOrConstant[] values) {

        if (values == null)
            throw new IllegalArgumentException();

        this.arity = values.length;

        int nvars = 0;

        for (int i = 0; i < arity; i++) {

            final IVariableOrConstant<E> value = values[i];

            if (value == null)
                throw new IllegalArgumentException();

            if (value.isVar())
                nvars++;

        }

        this.nvars = nvars;

        this.values = values;
        
        this.constraint = constraint;
        
    }
    
    final public int arity() {
        
        return arity;
        
    }

    public IVariableOrConstant get(int index) {
        
        return values[index];
        
    }

    public IPredicateConstraint<E> getConstraint() {

        return constraint;
        
    }

    final public int getVariableCount() {

        return nvars;
        
    }

    final public boolean isConstant() {

        return false;
        
    }

    /**
     * Returns an ordered array of the values for this predicate with the given
     * bindings overriding any unbound variables.
     * 
     * @param bindingSet
     *            The bindings (optional).
     * 
     * @return
     */
    public IVariableOrConstant[] toArray(IBindingSet bindingSet) {
        
        final IVariableOrConstant[] values = new IVariableOrConstant[this.values.length];
        
        for(int i=0; i<arity; i++) {
            
            final IVariableOrConstant v = values[i];
            
            if (v.isVar() && bindingSet!=null && bindingSet.isBound(v.getName())) {

                values[i] = new Constant(bindingSet.get(v.getName()));
                
            } else {
                
                values[i] = this.values[i];
                
            }
            
        }

        return values;
        
    }
    
    /**
     * Note: easily implemented using {@link #toArray(IBindingSet)}.
     */
    abstract public Predicate<E> asBound(IBindingSet bindingSet);

    abstract public void copyValues(E e, IBindingSet bindingSet );
    
    public String toString() {
        
        return toString(null/* bindingSet */);
        
    }
    
    public String toString(IBindingSet bindingSet) {

        StringBuilder sb = new StringBuilder();

        sb.append("(");

        for (int i = 0; i < arity; i++) {

            if (i >= 0)
                sb.append(", ");

            final IVariableOrConstant<E> v = values[i];

            sb.append(v.isConstant() || bindingSet == null ? v.toString()
                    : bindingSet.get(v.getName()));

        }

        sb.append(")");

        return sb.toString();

    }

}
