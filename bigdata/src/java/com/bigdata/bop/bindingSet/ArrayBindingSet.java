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
 * Created on Jun 20, 2008
 */

package com.bigdata.bop.bindingSet;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;

/**
 * An {@link IBindingSet} backed by an dense array (no gaps). This
 * implementation is more efficient for fixed or small N (N LTE ~20). It simples
 * scans the array looking for the variable using references tests for equality.
 * Since the #of variables is generally known in advance this can be faster and
 * lighter than {@link HashBindingSet} for most applications.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ArrayBindingSet implements IBindingSet {

    private static final long serialVersionUID = -6468905602211956490L;
    
    private static final Logger log = Logger.getLogger(ArrayBindingSet.class);

    /**
     * A dense array of the bound variables.
     */
    private final IVariable[] vars;
    /**
     * A dense array of the values bound to the variables (correlated with
     * {@link #vars}).
     */
    private final IConstant[] vals;

    private int nbound = 0;

    /**
     * Copy constructor.
     */
    protected ArrayBindingSet(final ArrayBindingSet bindingSet) {
        
        if (bindingSet == null)
            throw new IllegalArgumentException();
        
        nbound = bindingSet.nbound;
        
        vars = bindingSet.vars.clone();

        vals = bindingSet.vals.clone();
        
    }
    
    /**
     * Initialized with the given bindings (assumes for efficiency that all
     * elements of bound arrays are non-<code>null</code> and that no
     * variables are duplicated).
     * 
     * @param vars
     *            The variables.
     * @param vals
     *            Their bound values.
     */
    public ArrayBindingSet(final IVariable[] vars, final IConstant[] vals) {

        if (vars == null)
            throw new IllegalArgumentException();

        if (vals == null)
            throw new IllegalArgumentException();

        if(vars.length != vals.length)
            throw new IllegalArgumentException();

        // for (int i = 0; i < vars.length; i++) {
        //            
        // if (vars[i] == null)
        // throw new IllegalArgumentException();
        //
        // if (vals[i] == null)
        // throw new IllegalArgumentException();
        //            
        // }

        this.vars = vars;

        this.vals = vals;
        
        this.nbound = vars.length;

    }

    /**
     * Initialized with the given capacity.
     * 
     * @param capacity
     *            The capacity.
     * 
     * @throws IllegalArgumentException
     *             if the <i>capacity</i> is negative.
     */
    public ArrayBindingSet(final int capacity) {

        if (capacity < 0)
            throw new IllegalArgumentException();

        vars = new IVariable[capacity];

        vals = new IConstant[capacity];

    }

    public Iterator<IVariable> vars() {

        return Collections.unmodifiableList(Arrays.asList(vars)).iterator();
        
    }
    
    /**
     * Iterator does not support either removal or concurrent modification of 
     * the binding set.
     */
    public Iterator<Map.Entry<IVariable,IConstant>> iterator() {
       
        return new BindingSetIterator();
        
    }
    
    private class BindingSetIterator implements Iterator<Map.Entry<IVariable,IConstant>> {

        private int i = 0;
        
        public boolean hasNext() {

            return i < nbound;
            
        }

        public Entry<IVariable, IConstant> next() {

            // the index whose bindings are being returned.
            final int index = i++;

            return new Map.Entry<IVariable, IConstant>() {

                public IVariable getKey() {
                    
                    return vars[index];
                    
                }

                public IConstant getValue() {

                    return vals[index];
                    
                }

                public IConstant setValue(IConstant value) {

                    if (value == null)
                        throw new IllegalArgumentException();
                    
                    final IConstant t = vals[index];
                    
                    vals[index] = value;
                    
                    return t;
                    
                }

            };

        }

        public void remove() {

            throw new UnsupportedOperationException();
            
        }
        
    }
    
    public int size() {
        
        return nbound;
        
    }
    
    public void clearAll() {

        for (int i = nbound - 1; nbound > 0; i--, nbound--) {
            
            vars[i] = null;
            
            vals[i] = null;
            
        }

        // clear the hash code.
        hash = 0;
        
        assert nbound == 0;
        
    }

    /**
     * Since the array is dense (no gaps), {@link #clear(IVariable)} requires
     * that we copy down any remaining elements in the array by one position.
     */
    public void clear(final IVariable var) {

        if (var == null)
            throw new IllegalArgumentException();

        for (int i = 0; i < nbound; i++) {

            if (vars[i] == var) {

                final int nremaining = nbound-(i+1);
                
                if (nremaining >= 0) {
                    
                    // Copy down to close up the gap!
                    System.arraycopy(vars, i+1, vars, i, nremaining);

                    System.arraycopy(vals, i+1, vals, i, nremaining);
                    
                } else {

                    // Just clear the reference.
                    
                    vars[i] = null;

                    vals[i] = null;

                }
                
                // clear the hash code.
                hash = 0;
                
                nbound--;

                break;

            }

        }

    }

    public IConstant get(final IVariable var) {

        if (var == null)
            throw new IllegalArgumentException();

        for (int i = 0; i < nbound; i++) {

            if (vars[i] == var) {
        
                return vals[i];
                
            }
            
        }
        
        return null;
        
    }

    public boolean isBound(final IVariable var) {
        
        return get(var) != null;
        
    }

    public void set(final IVariable var, final IConstant val) {
        
        if (var == null)
            throw new IllegalArgumentException();
        
        if (val == null)
            throw new IllegalArgumentException();

        if (log.isTraceEnabled()) {

            log.trace("var=" + var + ", val=" + val + ", nbound=" + nbound
                    + ", capacity=" + vars.length);

        }
        
        for (int i = 0; i < nbound; i++) {

            if (vars[i] == var) {
        
                vals[i] = val;

                // clear the hash code.
                hash = 0;
                
                return;
                
            }
            
        }
        
        vars[nbound] = var;
        
        vals[nbound] = val;
        
        // clear the hash code.
        hash = 0;
        
        nbound++;
        
    }

    public String toString() {
        
        final StringBuilder sb = new StringBuilder();
        
        sb.append("{");

        for(int i=0; i<nbound; i++) {
            
            if(i>0) sb.append(", ");
            
            sb.append(vars[i]);
            
            sb.append("=");
            
            sb.append(vals[i]);
            
        }
        
        sb.append("}");
        
        return sb.toString();
        
    }
    
    public ArrayBindingSet clone() {

        return new ArrayBindingSet(this);
        
    }

    /**
     * Return a shallow copy of the binding set, eliminating unecessary 
     * variables.
     */
    public ArrayBindingSet copy(final IVariable[] variablesToKeep) {

        // bitflag for the old binding set
        final boolean[] keep = new boolean[nbound];
        
        // for each var in the old binding set, see if we need to keep it
        for (int i = 0; i < nbound; i++) {
            
            final IVariable v = vars[i];

            keep[i] = false;
            for (IVariable k : variablesToKeep) {
                if (v == k) {
                    keep[i] = true;
                    break;
                }
            }
            
        }
        
        // allocate the new vars
        final IVariable[] newVars = new IVariable[vars.length];
        
        // allocate the new vals
        final IConstant[] newVals = new IConstant[vals.length];
        
        // fill in the new binding set based on the keep bitflag
        int newbound = 0;
        for (int i = 0; i < nbound; i++) {
            if (keep[i]) {
                newVars[newbound] = vars[i];
                newVals[newbound] = vals[i];
                newbound++;
            }
        }
        
        ArrayBindingSet bs = new ArrayBindingSet(newVars, newVals);
        bs.nbound = newbound;
        
        return bs;
        
    }

    public boolean equals(final Object t) {
        
        if (this == t)
            return true;
        
        if(!(t instanceof IBindingSet))
            return false;
        
        final IBindingSet o = (IBindingSet)t;
        
        if (nbound != o.size())
            return false;
        
        for(int i=0; i<nbound; i++) {
            
            IConstant<?> o_val = o.get ( vars [ i ] ) ;
            if ( null == o_val || !vals[i].equals( o_val ))
                return false;
            
        }
        
        return true;
        
    }

    public int hashCode() {

        if (hash == 0) {

            int result = 0;

            for (int i = 0; i < nbound; i++) {

                if (vals[i] == null)
                    continue;

                result ^= vals[i].hashCode();

            }

            hash = result;

        }
        return hash;

    }
    private int hash;
    
}
