/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.bop;

/**
 * A constant.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
final public class Constant<E> extends ImmutableBOp implements IConstant<E> {

    /**
     * 
     */
    private static final long serialVersionUID = -2967861242470442497L;
    
    final private E value;

    public interface Annotations extends ImmutableBOp.Annotations {

        /**
         * The {@link IVariable} which is bound to that constant value
         * (optional).
         */
        String VAR = (Constant.class.getName() + ".var").intern();

    }
    
    final public boolean isVar() {
        
        return false;
        
    }

    final public boolean isConstant() {
        
        return true;
        
    }

//    /**
//     * Required shallow copy constructor.
//     * 
//     * @param op
//     */
//    public Constant(final BOp[] args, final Map<String,Object> ann) {
//        super(args,ann);
//        this.value = null;
//    }

    /**
     * Required deep copy constructor.
     * 
     * @param op
     */
    public Constant(final Constant<E> op) {

        super(op);
        
        this.value = op.value;
        
    }

    public Constant(final IVariable<E> var, final E value) {

        super(BOp.NOARGS, NV.asMap(new NV(Annotations.VAR, var)));

        if (var == null)
            throw new IllegalArgumentException();

        if (value == null)
            throw new IllegalArgumentException();

        this.value = value;

    }

    public Constant(final E value) {

        super(BOp.NOARGS, BOp.NOANNS);

        if (value == null)
            throw new IllegalArgumentException();

        this.value = value;

    }

    /**
     * Clone is overridden to reduce heap churn.
     */
    final public Constant<E> clone() {

        return this;
        
    }

    public String toString() {
        
        return value.toString();
        
    }

    final public boolean equals(final IVariableOrConstant<E> o) {

        if (o.isConstant() && value.equals(o.get())) {

            return true;

        }

        return false;

    }
    
    final public boolean equals(final Object o) {

        if (this == o)
            return true;
        
        if(!(o instanceof IConstant<?>)) {

            /*
             * Incomparable types.
             * 
             * Note: This used to permit IVariableOrConstant, but it is not
             * possible to invoke get() on an IVariable without a bindingSet
             * against which to resolve its asBound value.
             * 
             * See https://sourceforge.net/apps/trac/bigdata/ticket/276
             */
            return false;
            
        }
        
        final Object otherValue = ((IConstant<?>) o).get();
        
        // handles reference equality, including when both are null.
        if (value == otherValue)
            return true;

        // handles value null when other is non-null.
        if (value == null)
            return false;

        // compares non-null value with the other value.
        return value.equals(otherValue);
        
    }
    
    final public int hashCode() {
        
//        return (int) (id ^ (id >>> 32));
        return value.hashCode();
        
    }

    final public E get() {
        
        return value;
        
    }

    final public E get(final IBindingSet bindingSet) {
        
        return value;

    }

    final public String getName() {
     
        throw new UnsupportedOperationException();
        
    }

}
