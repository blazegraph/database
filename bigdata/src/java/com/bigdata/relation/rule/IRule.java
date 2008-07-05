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
 * Created on Jun 23, 2008
 */

package com.bigdata.relation.rule;

import java.util.Iterator;
import java.util.Set;

import com.bigdata.relation.IRelation;
import com.bigdata.relation.IRelationName;

/**
 * Conjunctive query of N {@link IPredicate}s with optional {@link IConstraint}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo support joins across RDF and non-RDF data. This will require some
 *       relaxing of assumptions concerning the type of relations in the RDF
 *       rule execution.
 * 
 * @param E
 *            The generic type of the [E]lements materialized by the head of the
 *            rule. This should be the same as the generic type of the
 *            {@link IRelation} that can be materialized from the
 *            {@link IRelationName} associated with the {@link IPredicate} that
 *            is the head of the rule.
 */
public interface IRule<E> extends IStep {

    /**
     * The #of distinct variables declared by the rule.
     */
    public int getVariableCount();

    /**
     * The variables declared by the rule in no particular order.
     */
    public Iterator<IVariable> getVariables();

    /**
     * The #of {@link IPredicate}s in the body (aka tail) of the rule.
     */
    public int getTailCount();

    /**
     * The head of the rule.
     */
    public IPredicate getHead();

    /**
     * Iterator visits the {@link IPredicate}s in the body (ala tail) of the
     * rule.
     */
    public Iterator<IPredicate> getTail();

    /**
     * Return the predicate at the given index from the tail of the rule.
     * 
     * @param index
     *            The index.
     *            
     * @return The predicate at that index.
     * 
     * @throws IndexOutOfBoundsException 
     */
    public IPredicate getTail(int index);

    /**
     * The #of constraints on the legal states for bindings of the variables
     * declared by rule.
     */
    public int getConstraintCount();
    
    /**
     * The optional constraints.
     */
    public Iterator<IConstraint> getConstraints();

    /**
     * Return the constraint at the given index.
     * 
     * @param index
     *            The index.
     * 
     * @return The constraint.
     * 
     * @throws IndexOutOfBoundsException
     */
    public IConstraint getConstraint(int index);
    
    /**
     * The name of the rule.
     */
    public String getName();

    /**
     * Externalizes the rule displaying variable names and constants.
     */
    public String toString();

    /**
     * Externalizes the rule displaying variable names, their bindings, and
     * constants.
     * 
     * @param bindingSet
     *            When non-<code>null</code>, the current variable bindings
     *            will be displayed. Otherwise, the names of variables will be
     *            displayed rather than their bindings.
     */
    public String toString(IBindingSet bindingSet);

    /**
     * Specialize a rule - the name of the new rule will be derived from the
     * name of the old rule with an appended single quote to indicate that it is
     * a derived variant.
     * 
     * @param bindingSet
     *            Bindings for zero or more free variables in this rule. The
     *            rule will be rewritten such that the variable is replaced by
     *            the binding throughout the rule. An attempt to bind a variable
     *            not declared by the rule will be ignored.
     * @param constraints
     *            An array of additional constraints to be imposed on the rule
     *            (optional).
     * 
     * @return The specialized rule.
     * 
     * @throws IllegalArgumentException
     *             if <i>bindingSet</i> is <code>null</code>.
     */
    public IRule<E> specialize(IBindingSet bindingSet, IConstraint[] constraints);

    /**
     * Specialize a rule by binding zero or more variables and adding zero or
     * more constraints.
     * 
     * @param bindingSet
     *            Bindings for zero or more free variables in this rule. The
     *            rule will be rewritten such that the variable is replaced by
     *            the binding throughout the rule. An attempt to bind a variable
     *            not declared by the rule will be ignored.
     * @param constraints
     *            An array of additional constraints to be imposed on the rule
     *            (optional).
     * 
     * @return The specialized rule.
     * 
     * @exception IllegalArgumentException
     *                if <i>name</i> is <code>null</code>.
     * @exception IllegalArgumentException
     *                if <i>bindingSet</i> is <code>null</code>.
     */
    public IRule<E> specialize(String name, IBindingSet bindingSet,
            IConstraint[] constraints);

    /**
     * Return the variables in common for two {@link IPredicate}s.
     * 
     * @param index1
     *            The index of a predicate in the {@link #tail}.
     * 
     * @param index2
     *            The index of a different predicate in the {@link #tail}.
     * 
     * @return The variables in common -or- <code>null</code> iff there are no
     *         variables in common.
     * 
     * @throws IllegalArgumentException
     *             if the two predicate indices are the same.
     * @throws IndexOutOfBoundsException
     *             if either index is out of bounds.
     */
    public Set<IVariable> getSharedVars(int index1, int index2);

    /**
     * Return true iff the selected predicate is fully bound.
     * 
     * @param index
     *            The index of a predicate declared in the {@link #tail} of the
     *            rule.
     * @param bindingSet
     *            The variable bindings.
     * 
     * @return True iff it is fully bound (a mixture of constants and/or bound
     *         variables).
     * 
     * @throws IndexOutOfBoundsException
     *             if the <i>index</i> is out of bounds.
     * @throws IllegalArgumentException
     *             if <i>bindingSet</i> is <code>null</code>.
     */
    public boolean isFullyBound(int index, IBindingSet bindingSet);

    /**
     * If the rule is fully bound for the given bindings.
     * 
     * @param bindingSet
     *            The bindings.
     * 
     * @return true if there are no unbound variables in the rule given those
     *         bindings.
     */
    public boolean isFullyBound(IBindingSet bindingSet);

    /**
     * Return <code>true</code> unless the {@link IBindingSet} violates a
     * {@link IConstraint} declared for this {@link Rule}.
     * 
     * @param bindingSet
     *            The binding set.
     * 
     * @return <code>true</code> unless a constraint is violated by the
     *         bindings.
     */
    public boolean isConsistent(IBindingSet bindingSet);

    /**
     * Return <code>true</code> iff the rule declares this variable.
     * 
     * @param var
     *            Some variable.
     * 
     * @return True if the rule declares that variable.
     * 
     * @throws IllegalArgumentException
     *             if <i>var</i> is <code>null</code>.
     */
    public boolean isDeclared(IVariable var);
    
    /*
     * Behavior override.
     */
    
    /**
     * An optional {@link IRuleTaskFactory} that will be used in place of the
     * default {@link IRuleTaskFactory} to evaluate this rule (optional)
     * 
     * @return <code>null</code> unless custom evaluation is desired.
     */
    public IRuleTaskFactory getTaskFactory();
    
}
