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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

/**
 * {@link IBindingSet} backed by a {@link HashMap}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Since {@link Var}s allow reference testing, a faster implementation
 *       could be written based on a {@link LinkedList}. Just scan the list
 *       until the entry is found with the desired {@link Var} reference and
 *       then return it.
 */
public class HashBindingSet implements IBindingSet {

    private static final long serialVersionUID = -2989802566387532422L;
    
    /**
     * Note: A {@link LinkedHashMap} provides a fast iterator, which we use a
     * bunch.
     */
    private LinkedHashMap<IVariable, IConstant> map;

    /**
     * New empty binding set.
     */
    public HashBindingSet() {
        
        map = new LinkedHashMap<IVariable, IConstant>();
        
    }

    /**
     * Copy constructor.
     * 
     * @param src
     */
    protected HashBindingSet(final HashBindingSet src) {
        
        map = new LinkedHashMap<IVariable, IConstant>(src.map);
        
    }

    /**
     * Copy constructor.
     * 
     * @param src
     */
    public HashBindingSet(final IBindingSet src) {

        map = new LinkedHashMap<IVariable, IConstant>(src.size());

        final Iterator<Map.Entry<IVariable, IConstant>> itr = src.iterator();

        while (itr.hasNext()) {

            final Map.Entry<IVariable, IConstant> e = itr.next();

            map.put(e.getKey(), e.getValue());

        }

    }
    
    public HashBindingSet(final IVariable[] vars, final IConstant[] vals) {

        if (vars == null)
            throw new IllegalArgumentException();

        if (vals == null)
            throw new IllegalArgumentException();

        if (vars.length != vals.length)
            throw new IllegalArgumentException();

        map = new LinkedHashMap<IVariable, IConstant>(vars.length);

        for (int i = 0; i < vars.length; i++) {

            map.put(vars[i], vals[i]);

        }
        
    }
    
    public boolean isBound(final IVariable var) {
     
        if (var == null)
            throw new IllegalArgumentException();
        
        return map.containsKey(var);
        
    }
    
    public IConstant get(final IVariable var) {
   
        if (var == null)
            throw new IllegalArgumentException();
        
        return map.get(var);
        
    }

    public void set(final IVariable var, final IConstant val) {
        
        if (var == null)
            throw new IllegalArgumentException();
        
        if (val == null)
            throw new IllegalArgumentException();
        
        map.put(var,val);

        // clear the hash code.
        hash = 0;
        
    }

    public void clear(final IVariable var) {
        
        if (var == null)
            throw new IllegalArgumentException();
        
        map.remove(var);
        
        // clear the hash code.
        hash = 0;

    }
    
    public void clearAll() {
        
        map.clear();
    
        // clear the hash code.
        hash = 0;

    }

    public String toString() {
        
        final StringBuilder sb = new StringBuilder();
        
        sb.append("{ ");

        int i = 0;
        
        final Iterator<Map.Entry<IVariable, IConstant>> itr = map.entrySet()
                .iterator();

        while (itr.hasNext()) {

            if (i > 0)
                sb.append(", ");

            final Map.Entry<IVariable, IConstant> entry = itr.next();
            
            sb.append(entry.getKey());

            sb.append("=");

            sb.append(entry.getValue());

            i++;

        }

        sb.append(" }");

        return sb.toString();

    }

    /**
     * Iterator does not support removal, set, or concurrent modification.
     */
    public Iterator<Entry<IVariable, IConstant>> iterator() {

        return Collections.unmodifiableMap(map).entrySet().iterator();
        
    }

    public Iterator<IVariable> vars() {

        return Collections.unmodifiableSet(map.keySet()).iterator();
        
    }
    
    public int size() {

        return map.size();
        
    }

    public HashBindingSet clone() {
        
        return new HashBindingSet( this );
        
    }
    
    /**
     * Return a shallow copy of the binding set, eliminating unecessary 
     * variables.
     */
    public HashBindingSet copy(final IVariable[] variablesToKeep) {
        
        final HashBindingSet bs = new HashBindingSet();
        
        for (IVariable<?> var : variablesToKeep) {
            
            final IConstant<?> val = map.get(var);
            
            if (val != null) {
                
                bs.map.put(var, val);
                
            }
            
        }
        
        return bs;
        
    }
    
    public boolean equals(final Object t) {
        
        if (this == t)
            return true;
        
        if(!(t instanceof IBindingSet))
            return false;
        
        final IBindingSet o = (IBindingSet) t;

        if (size() != o.size())
            return false;
        
        final Iterator<Map.Entry<IVariable,IConstant>> itr = map.entrySet().iterator();
        
        while(itr.hasNext()) {

            final Map.Entry<IVariable,IConstant> entry = itr.next();
            
            final IVariable<?> var = entry.getKey();
            
            final IConstant<?> val = entry.getValue();
            
//            if (!o.isBound(vars[i]))
//                return false;

            if (!val.equals(o.get(var)))
                return false;
            
        }
        
        return true;
        
    }

    public int hashCode() {

        if (hash == 0) {

            int result = 0;

            for(IConstant<?> c : map.values()) {

                if (c == null)
                    continue;

                result ^= c.hashCode();

            }

            hash = result;

        }
        return hash;

    }
    private int hash;

}
