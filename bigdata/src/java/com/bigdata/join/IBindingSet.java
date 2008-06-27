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

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

/**
 * Interface for a set of bindings. The set of variables values is extensible
 * and the bound values are loosely typed.
 * 
 * @todo The variable positions in a binding set can be assigned an index by the
 *       order in which they are encountered across the predicates when the
 *       predicates are considered in execution order. This gives us a dense
 *       index in [0:nvars-1]. The index can be into an array. When the bindings
 *       are of a primitive type, as they are for the RDF DB, that array can be
 *       an array of the primitive type, e.g., long[nvars].
 *       <p>
 *       This change would require that the singleton factory for a variable was
 *       on the {@link Rule} (different rules would have different index
 *       assignments), it would require predicates to be cloned into a
 *       {@link Rule} so that the variables possessed the necessary index
 *       assignment, and that index assignment would have to be late - once the
 *       evaluation order was determined, so maybe the Rule is cloned into the
 *       {@link RuleState} once we have the evaluation order.
 *       <p>
 *       There would also need to be a type-specific means for copying bindings
 *       from a visited element into a bindingSet if a want to avoid autoboxing.
 *       <p>
 *       The {@link IConstant} interface might have to disappear for this as
 *       well. I am not convinced that it adds much.
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
     * Return a shallow copy of the binding set.
     */
    public IBindingSet clone();
    
    /**
     * True iff the variables and their bound values are the same
     * for the two binding sets.
     * 
     * @param o
     *            Another binding set.
     */
    public boolean equals(IBindingSet o);
    
}
