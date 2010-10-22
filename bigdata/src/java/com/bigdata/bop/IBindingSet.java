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
     * The #of bound variables.
     */
    public int size();
    
    /**
     * Visits the bindings.
     */
    public Iterator<Map.Entry<IVariable,IConstant>> iterator();

    /**
     * Visits the bound variables.
     */
    public Iterator<IVariable> vars();
    
    /**
     * Return a shallow copy of the binding set.
     */
    public IBindingSet clone();
    
    /**
     * Return a shallow copy of the binding set, eliminating unnecessary 
     * variables.
     */
    public IBindingSet copy(IVariable[] variablesToKeep);
    
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
     * does not dependent on the order in which the bindings are iterated over.
     * The hash code reflects the current state of the bindings and must be
     * recomputed if the bindings are changed.
     */
    public int hashCode();
    
}
