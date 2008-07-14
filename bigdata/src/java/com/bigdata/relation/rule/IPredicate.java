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

package com.bigdata.relation.rule;

import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.ISolution;

/**
 * An immutable constraint on the elements visited using an {@link IAccessPath}.
 * The slots in the predicate corresponding to variables are named and those
 * names establish binding patterns access {@link IPredicate}s in the context
 * of a {@link IRule}. Access is provided to slots by ordinal index regardless
 * of whether or not they are named variables.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IPredicate<E> extends Cloneable {

    /**
     * Resource identifier (aka namespace) identifies the {@link IRelation}
     * associated with this {@link IPredicate}.
     * <p>
     * This is more or less ignored when the {@link IRule} is executed as a
     * query.
     * <p>
     * When the {@link IRule} is executed as an {@link ActionEnum#Insert} or
     * {@link ActionEnum#Delete} then this identifies the target
     * {@link IMutableRelation} on which the computed {@link ISolution}s will
     * be written.
     * 
     * @throws IllegalStateException
     *             if there is more than on element in the view.
     * 
     * @deprecated by {@link #getRelationName(int)}
     */
    public String getOnlyRelationName();

    /**
     * Return the ith element of the relation view. The view is an ordered array
     * of resource identifiers that describes the view for the relation.
     * 
     * @param index
     *            The index into the array of relation names in the view.
     * 
     * @todo is there any particular reason to use an ordered view? TM does not
     *       appear to require this and we always write on the relation that is
     *       associated with the head of the rule, and that should always be the
     *       "database" (vs the focusStore).
     */
    public String getRelationName(int index);
    
    /**
     * The #of elements in the relation view.
     */
    public int getRelationCount();
    
    /**
     * An optional constraint on the visitable elements.
     */
    public IElementFilter<E> getConstraint();
    
    /**
     * Return true iff all arguments of the predicate are bound (vs
     * variables).
     */
    public boolean isFullyBound();
    
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
     *            
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
