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

package com.bigdata.relation.rule;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;


/**
 * An {@link IBindingSet} backed by an dense array (no gaps). This
 * implementation is more efficient for fixed or small N (N LTE ~20). It simples
 * scans the array looking for the variable using references tests for equality.
 * Since the #of variables is generally known in advance this can be faster and
 * lighter than {@link HashBindingSet} for most applications.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo compact serialization.
 */
public class ArrayBindingSet implements IBindingSet {

    private static final long serialVersionUID = -6468905602211956490L;
    
    protected static final Logger log = Logger.getLogger(ArrayBindingSet.class);
    
    private final IVariable[] vars;
    private final IConstant[] vals;

    private int nbound = 0;

    /**
     * Copy constructor.
     */
    protected ArrayBindingSet(ArrayBindingSet bindingSet) {
        
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
    public ArrayBindingSet(IVariable[] vars, IConstant[] vals) {

//        if (vars == null)
//            throw new IllegalArgumentException();
//
//        if (vals == null)
//            throw new IllegalArgumentException();

        assert vars != null;
        assert vals != null;
        assert vars.length == vals.length;

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
    public ArrayBindingSet(int capacity) {

        if (capacity < 0)
            throw new IllegalArgumentException();

        vars = new IVariable[capacity];

        vals = new IConstant[capacity];

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

        assert nbound == 0;
        
    }

    /**
     * Since the array is dense (no gaps), {@link #clear(IVariable)} requires
     * that we copy down any remaining elements in the array by one position.
     */
    public void clear(IVariable var) {

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
                
                nbound--;

                break;

            }

        }

    }

    public IConstant get(IVariable var) {

        if (var == null)
            throw new IllegalArgumentException();

        for (int i = 0; i < nbound; i++) {

            if (vars[i] == var) {
        
                return vals[i];
                
            }
            
        }
        
        return null;
        
    }

    public boolean isBound(IVariable var) {
        
        return get(var) != null;
        
    }

    public void set(final IVariable var, final IConstant val) {
        
        if (var == null)
            throw new IllegalArgumentException();
        
        if (val == null)
            throw new IllegalArgumentException();

        if(log.isDebugEnabled()) {
            
            log.debug("var=" + var + ", val=" + val + ", nbound=" + nbound+", capacity="+vars.length);
            
        }
        
        for (int i = 0; i < nbound; i++) {

            if (vars[i] == var) {
        
                vals[i] = val;
        
                return;
                
            }
            
        }
        
        vars[nbound] = var;
        
        vals[nbound] = val;
        
        nbound++;
        
    }

    public String toString() {
        
        StringBuilder sb = new StringBuilder();
        
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

    public boolean equals(IBindingSet o) {
        
        if (o == this)
            return true;
        
        if (nbound != o.size())
            return false;
        
        for(int i=0; i<nbound; i++) {
            
//            if (!o.isBound(vars[i]))
//                return false;

            if (!vals[i].equals(o.get(vars[i])))
                return false;
            
        }
        
        return true;
        
    }

}
