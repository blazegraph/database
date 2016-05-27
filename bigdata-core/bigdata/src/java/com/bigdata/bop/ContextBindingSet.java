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
/*
 * Created on Mar 14, 2012
 */

package com.bigdata.bop;

import java.util.Iterator;
import java.util.Map.Entry;

/**
 * Wraps an {@link IBindingSet} to provide access to the {@link BOpContext}. The
 * {@link BOpContext} information is <em>transient</em> and will not cross the
 * wire.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/513"> Expose
 *      the LexiconConfiguration to function BOPs </a>
 */
@SuppressWarnings("rawtypes") 
public class ContextBindingSet implements IBindingSet {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private transient BOpContextBase context;

    private final IBindingSet delegate;

    public BOpContextBase getBOpContext() {

        return context;

    }

    public ContextBindingSet(final BOpContextBase context,
            final IBindingSet delegate) {

        this.context = context;

        this.delegate = delegate;

    }

    public boolean isBound(final IVariable var) {
        return delegate.isBound(var);
    }

    public void set(final IVariable var, final IConstant val) {
        delegate.set(var, val);
    }

    public IConstant get(final IVariable var) {
        return delegate.get(var);
    }

    public void clear(IVariable var) {
        delegate.clear(var);
    }

    public void clearAll() {
        delegate.clearAll();
    }

    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    public int size() {
        return delegate.size();
    }

    public Iterator<Entry<IVariable, IConstant>> iterator() {
        return delegate.iterator();
    }

    public Iterator<IVariable> vars() {
        return delegate.vars();
    }

    public IBindingSet clone() {

        // Note: Keep wrapped when we clone().
        return new ContextBindingSet(context, delegate.clone());
        
    }

    public IBindingSet copy(final IVariable[] variablesToKeep) {
        
        // Note: Keep wrapped when copy() does a clone.
        return new ContextBindingSet(context, delegate.copy(variablesToKeep));
        
    }
   
    
    @Override
    @SuppressWarnings("rawtypes")
    public IBindingSet copyMinusErrors(final IVariable[] variablesToKeep) {
        return new ContextBindingSet(context,
                delegate.copyMinusErrors(variablesToKeep));
    }
    
       
    /** 
     * @return true if this IBindingSet contains an assignment of an error value
     */
    @Override
    public final boolean containsErrorValues() {  
        return delegate.containsErrorValues();
    }
        
    public boolean equals(final Object o) {
        return delegate.equals(o);
    }

    public int hashCode() {
        return delegate.hashCode();
    }

    public String toString() {
        return delegate.toString();
    }
    
}
