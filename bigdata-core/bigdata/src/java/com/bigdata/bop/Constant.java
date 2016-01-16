/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

import java.util.Collections;


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
         * <p>
         * {@link BOpContext#bind(IPredicate, IConstraint[], Object, IBindingSet)}
         * takes care of propagating the binding onto the variable for solutions
         * which join.
         * <p>
         * Note: The {@link Var} class in bigdata provides a guarantee of
         * reference testing for equality, which is why we can not simply attach
         * the constant to the variable and have the variable report its bound
         * value. *
         */
        String VAR = Constant.class.getName() + ".var";

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
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     * 
     * @param op
     */
    public Constant(final Constant<E> op) {

        super(op);
        
        this.value = op.value;
        
    }

    /**
     * Create a constant which models a variable bound to that constant. This
     * may be used when a variable has an external binding, such as when a
     * single binding set is provided as input to a query. By pairing the
     * {@link Constant} with the {@link IVariable} and handling this case when
     * solutions are joined the resulting solutions will have the variable with
     * its constant bound value.
     * <p>
     * Note: The {@link Var} class in bigdata provides a guarantee of reference
     * testing for equality, which is why we can not simply attach the constant
     * to the variable and have the variable report its bound value. *
     * <p>
     * Note: A very similar effect may be achieved by simply binding the
     * variable on the {@link IBindingSet} to the constant. However, there are
     * some (few) cases where we can not do that because the binding must be
     * applied for all solutions and we lack access to the input solutions.
     * <p>
     * See
     * {@link BOpContext#bind(IPredicate, IConstraint[], Object, IBindingSet)},
     * which takes care of propagating the binding onto the variable for
     * solutions which join.
     * 
     * @param var
     *            The variable.
     * @param value
     *            The bound value.
     */
    public Constant(final IVariable<E> var, final E value) {

        super(BOp.NOARGS, Collections.singletonMap(Annotations.VAR,
                (Object) var));
//        NV.asMap(new NV(Annotations.VAR, var)));

        if (var == null)
            throw new IllegalArgumentException();

        if (value == null)
            throw new IllegalArgumentException();

        if(value instanceof IConstant<?>) {
            // Recursive nesting of Constant is not allowed.
            throw new IllegalArgumentException();
        }
        
        this.value = value;

    }

    /**
     * Create a constant for the value.
     * 
     * @param value
     *            The value (may not be <code>null</code>).
     */
    public Constant(final E value) {

        super(BOp.NOARGS, BOp.NOANNS);

        if (value == null)
            throw new IllegalArgumentException();

        if(value instanceof IConstant<?>) {
            // Recursive nesting of Constant is not allowed.
            throw new IllegalArgumentException();
        }

        this.value = value;

    }

    /**
     * Clone is overridden to reduce heap churn.
     */
    final public Constant<E> clone() {

        return this;
        
    }

    public String toString() {
        
        @SuppressWarnings("unchecked")
        final IVariable<E> var = (IVariable<E>) getProperty(Annotations.VAR);

        if(var != null) {
            
            // A constant which is really an as-bound variable.
            return value.toString() + "[var=" + var + "]";
            
        }
        
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
        if(value.equals(otherValue))
            return true;

        return false;
        
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
    
    @SuppressWarnings("unchecked")
	final public IVariable<E> getVar() {
    	
    	return (IVariable<E>) getProperty(Annotations.VAR);
    	
    }

}
