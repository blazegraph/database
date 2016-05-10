/*

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
/*
 * Created on Jun 19, 2008
 */

package com.bigdata.bop;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

/**
 * Interface for a set of bindings. The set of variables values is extensible
 * and the bound values are loosely typed.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IBindingSet extends Cloneable, Serializable {

    /**
     * Return <code>true</code> iff the variable is bound.
     * 
     * @param var
     *            The variable.
     * 
     * @return <code>true</code> if the variable is bound.
     * 
     * @throws IllegalArgumentException
     *             if <i>var</i> is <code>null</code>.
     */
    public boolean isBound(IVariable var);
    
    /**
     * Bind the variable to the value.
     * 
     * @param var
     *            The variable.
     * @param val
     *            The value (MAY NOT be <code>null</code>).
     * 
     * @throws IllegalArgumentException
     *             if <i>var</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <i>val</i> is <code>null</code>.
     */
    public void set(IVariable var,IConstant val);
    
    /**
     * Return the binding for the variable.
     * 
     * @param var
     *            The variable.
     * 
     * @return the binding for the variable -or- <code>null</code> iff the
     *         variable is not bound.
     * 
     * @throws IllegalArgumentException
     *             if <i>var</i> is <code>null</code>.
     */
    public IConstant get(IVariable var);
    
    /**
     * Clear any binding for the variable.
     * 
     * @param var
     *            The variable.
     *            
     * @throws IllegalArgumentException
     *             if <i>var</i> is <code>null</code>.
     */
    public void clear(IVariable var);

    /**
     * Clear all bindings.
     */
    public void clearAll();

    /**
     * <code>true</code> iff there are no variable bindings in the binding set.
     */
    public boolean isEmpty();
    
    /**
     * The #of bound variables.
     */
    public int size();

	/**
	 * Visits the bindings.
	 */
    public Iterator<Map.Entry<IVariable,IConstant>> iterator();

    /**
     * Visits the bound variables.
     * 
	 * @todo The unit tests verify that the implementations do not permit
	 *       mutation using the iterator, but that is not actually specified by
	 *       the API as forbidden.
     */
    public Iterator<IVariable> vars();
    
    /**
     * Return a shallow copy of the binding set.
     */
    public IBindingSet clone();

	/**
	 * Return a shallow copy of the binding set, eliminating unnecessary
	 * variables.
	 * 
	 * @param variablesToKeep
	 *            When non-<code>null</code>, only the listed variables are
	 *            retained.
	 */
    public IBindingSet copy(IVariable[] variablesToKeep);
    
	/**
	 * Return a shallow copy of the binding set, eliminating unnecessary
	 * variables and error values (equal to Constant.errorValueConstant()).
	 * 
	 * @param variablesToKeep
	 *            When non-<code>null</code>, only the listed variables are
	 *            retained.
	 */
    public IBindingSet copyMinusErrors(IVariable[] variablesToKeep);
    
    
    /** 
     * @return true if this IBindingSet contains an assignment of an error value
     */
    public boolean containsErrorValues();
    
    /**
     * True iff the variables and their bound values are the same
     * for the two binding sets.
     * 
     * @param o
     *            Another binding set.
     */
    public boolean equals(Object o);

	/**
	 * The hash code of a binding is defined as the bit-wise XOR of the hash
	 * codes of the {@link IConstant}s for its bound variables. Unbound
	 * variables are ignored when computing the hash code. Binding sets are
	 * unordered collections, therefore the calculated hash code intentionally
	 * does not depend on the order in which the bindings are visited. The hash
	 * code reflects the current state of the bindings and must be recomputed if
	 * the bindings are changed.
	 */
    public int hashCode();

//	/**
//	 * Make a copy of the current symbol table (aka current variable bindings)
//	 * and push it onto onto the stack. Variable bindings will be made against
//	 * the current symbol table. The symbol table stack is propagated by
//	 * {@link #clone()} and {@link #copy(IVariable[])}. Symbols tables may be
//	 * used to propagate conditional bindings through a data flow until a
//	 * decision point is reached, at which point they may be either discarded or
//	 * committed. This mechanism may be used to support SPARQL style optional
//	 * join groups.
//	 * 
//	 * @throws UnsupportedOperationException
//	 *             if the {@link IBindingSet} is not mutable.
//	 * 
//	 * @see #pop(boolean)
//	 */
//	public void push();
//
//	/**
//	 * Pop the current symbol table off of the stack.
//	 * 
//	 * @param save
//	 *            When <code>true</code>, the bindings on the current symbol
//	 *            table are copied to the parent symbol table before the current
//	 *            symbol table is popped off of the stack. If <code>false</code>
//	 *            , any bindings associated with that symbol table are
//	 *            discarded.
//	 * 
//	 * @throws IllegalStateException
//	 *             if there is no nested symbol table.
//	 * 
//	 * @see #push()
//	 */
//	public void pop(boolean save);

//    /**
//     * Push the current symbol table, copying the bindings for only the named
//     * variables into a new symbol table on the top of the stack. Only the
//     * bindings in the current symbol table are visible. {@link #iterator()}
//     * WILL NOT visit bindings unless they are present at the top of the stack.
//     * <p>
//     * Note: push()/pop() model the change in lexical scope for variables when
//     * entering and leaving a subquery. Only those variables projected from a
//     * subquery may be bound on entry, and only those variables projected from a
//     * subquery should have their bindings preserved when the subquery
//     * completes. Since variables which were bound on entry will remain bound,
//     * the bindings will remain consistent throughout the subquery and pop() can
//     * not cause inconsistent bindings to be propagated into the parent's symbol
//     * table.
//     * 
//     * @param vars
//     *            The variable(s) whose bindings will be visible in the new
//     *            symbol table.
//     * 
//     * @throws UnsupportedOperationException
//     *             if the {@link IBindingSet} is not mutable.
//     * 
//     * @see #pop(IVariable[])
//     * 
//     * @deprecated It looks like we will not be using this facility. This
//     *             operation is handled directly by {@link SubqueryOp}.
//     */
//    public void push(IVariable[] vars);
//
//    /**
//     * Pop the current symbol table off of the stack, copying the bindings for
//     * the specified variables into the revealed symbol table.
//     * <p>
//     * The variables represent the projection of the subquery. For correct
//     * semantics, a pop() MUST balance a push(), providing the same variables in
//     * each case. Consistency is provided when push() and pop() are injected
//     * into a query plan since the query planner knows the projection and can
//     * plan the push and pop operations accordingly.
//     * 
//     * @param vars
//     *            The variables whose bindings will be copied into the uncovered
//     *            symbol table.
//     * 
//     * @throws IllegalStateException
//     *             if there is no nested symbol table.
//     * 
//     * @see #push(IVariable[])
//     * 
//     * @deprecated It looks like we will not be using this facility. This
//     *             operation is handled directly by {@link SubqueryOp}.
//     */
//    public void pop(IVariable[] vars);

}
