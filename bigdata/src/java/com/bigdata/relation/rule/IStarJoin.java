/*

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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

package com.bigdata.relation.rule;

import java.util.Iterator;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;

/**
 * Interface for a special type of {@link IPredicate} - the star join predicate.
 * This type of predicate bypasses the normal join operation and does a binding
 * set join of the matches to its star constraints from within the access path
 * operation.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 */
public interface IStarJoin<E> extends IPredicate<E> {

    /**
     * Add a star constraint for this star join.
     * 
     * @param constraint
     *          the star constraint
     */
    void addStarConstraint(IStarConstraint<E> constraint);
    
    /**
     * Returns an iterator over this star join's constraints.
     * 
     * @return
     *          the star constraints iterator
     */
    Iterator<IStarConstraint<E>> getStarConstraints();
    
    /**
     * Return the number of star constraints.
     * 
     * @return
     *          the number of star constraints
     */
    int getNumStarConstraints();
    
    /**
     * Returns an iterator over the variables used in this star join's 
     * constraints.
     * 
     * @return
     *          the star constraints' variables iterator
     */
    Iterator<IVariable> getConstraintVariables();

    /**
     * A star constraint specifies the shape of the star join.  Star constraints
     * will determine whether a particular element matches the constraint, and
     * will create variable bindings for that elements.
     */
    public static interface IStarConstraint<E> {

        /**
         * Return an as-bound version of this star constraint.
         * 
         * @param bindingSet
         *          the binding set from which to pull variable bindings
         * @return
         *          the as-bound version of this star constraint
         */
        IStarConstraint<E> asBound(IBindingSet bindingSet);
        
        /**
         * Return the number of variables used in this star constraint.
         * 
         * @return
         *          the number of variables
         */
        int getNumVars();
        
        /**
         * Return true if a particular element matches this star constraint.
         * 
         * @param e
         *          the element to test
         * @return
         *          true if there is a match
         */
        boolean isMatch(E e);
        
        /**
         * Adds variable bindings to the supplied binding set based on the
         * as-bound values of the supplied element.
         * 
         * @param bs
         *          the binding set to modify
         * @param e
         *          the element from which to pull variable bindings
         */
        void bind(IBindingSet bs, E e);
        
        /**
         * Return true if this star constraint is optional.
         * 
         * @return
         *          true if optional
         */
        boolean isOptional();
        
    }
    
}
