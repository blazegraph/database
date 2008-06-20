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

package com.bigdata.join;

/**
 * An {@link IBindingSet} backed by an array. This implementation is more
 * efficient for fixed or small N (N LTE ~20). It simples scans the array
 * looking for the variable using references tests for equality. Since the #of
 * variables is generally known in advance this can be faster and lighter than
 * {@link HashBindingSet} for most applications.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo compact serialization for bindings for a relation.
 */
public class ArrayBindingSet implements IBindingSet {
    
    private final IVariable[] vars;
    private final IConstant[] vals;

    private int nbound = 0;

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
     */
    public ArrayBindingSet(int capacity) {

        if (capacity <= 0)
            throw new IllegalArgumentException();

        vars = new IVariable[capacity];

        vals = new IConstant[capacity];

    }
    
    public void clearAll() {

        for (int i = nbound - 1; nbound > 0; i--, nbound--) {
            
            vars[i] = null;
            
            vals[i] = null;
            
        }

        assert nbound == 0;
        
    }

    public void clear(IVariable var) {

        if (var == null)
            throw new IllegalArgumentException();

        for (int i = 0; i < nbound; i++) {

            if (vars[i] == var) {

                vars[i] = null;

                vals[i] = null;

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

    public void set(IVariable var, IConstant val) {
        
        if (var == null)
            throw new IllegalArgumentException();
        
        if (val == null)
            throw new IllegalArgumentException();

        for (int i = 0; i < nbound; i++) {

            if (vars[i] == var) {
        
                vals[i] = val;
                
            }
            
        }
        
        vars[nbound] = var;
        
        vals[nbound] = val;
        
        nbound++;
        
    }

    public String toString() {
        
        StringBuilder sb = new StringBuilder();
        
        sb.append("{ ");

        for(int i=0; i<nbound; i++) {
            
            if(i>0) sb.append(", ");
            
            sb.append(vars[i]);
            
            sb.append("=");
            
            sb.append(vals[i]);
            
        }
        
        sb.append(" }");
        
        return sb.toString();
        
    }
    
}
