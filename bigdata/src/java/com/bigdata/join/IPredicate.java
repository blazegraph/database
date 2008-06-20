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
 * An immutable constraint on the elements visited using an {@link IAccessPath}.
 * The slots in the predicate corresponding to variables are named and those
 * names establish binding patterns access {@link IPredicate}s in the context
 * of a {@link Rule}. Access is provided to slots by ordinal index regardless
 * of whether or not they are named variables.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IPredicate<E> extends Cloneable {

    /**
     * An optional constraint on the visitable elements.
     */
    public IPredicateConstraint<E> getConstraint();
    
    /**
     * Return true iff all arguments of the predicate are bound (vs
     * variables).
     */
    public boolean isConstant();
    
    /**
     * The #of arguments in the predicate that are variables (vs constants).
     */
    public int getVariableCount();
    
    /** The #of slots in the predicate. */
    public int arity();

    /**
     * Return the variable or constant at the specified index.
     * 
     * @param index
     *            The index.
     * 
     * @return The variable or constant at the specified index.
     * 
     * @throws IllegalArgumentException
     *             if the index is less than zero or GTE the {@link #arity()} of
     *             the {@link IPredicate}.
     */
    public IVariableOrConstant get(int index);
    
    /**
     * A copy of this {@link IPredicate} in which zero or more variables have
     * been bound to constants using the given {@link IBindingSet}.
     */
    public IPredicate<E> asBound(IBindingSet bindingSet);

    /**
     * Copy values from the visited element into the binding set.
     * 
     * @param e
     *            An element visited for this {@link IPredicate} using some
     *            {@link IAccessPath}.
     * @param bindingSet
     *            A set of bindings.
     * 
     * @todo perhaps make [E] extend an interface that defines this method. We
     *       might pass in the predicate since we only want to copy those
     *       bindings that are of interest. (In SQL you can select any bindings
     *       on the relations that participate in the JOIN).
     */
    public void copyValues(E e, IBindingSet bindingSet);
    
    /**
     * Representation of the predicate without variable bindings.
     */
    public String toString();
    
    /**
     * Representation of the predicate with variable bindings.
     * 
     * @param bindingSet
     *            The variable bindings
     */
    public String toString(IBindingSet bindingSet);

    /**
     * Compares the bindings of two predicates for equality.
     * 
     * @param other
     *            Another predicate.
     * @return true iff the predicate have the same arity and their ordered
     *         bindings are the same. when both predicates have a variable at a
     *         given index, the names of the variables must be the same.
     * 
     * @todo this may not be the best definition of equality to use here. it
     *       intentionally disregards any constraints and the generic type while
     *       requiring that unbound variables have the same name at a given
     *       index. This definition was motivated by the unit tests rather than
     *       any functional use for the behavior.
     */
    public boolean equals(IPredicate<E> other);

}
