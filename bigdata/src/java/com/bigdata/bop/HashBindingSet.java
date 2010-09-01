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
import java.util.Map;
import java.util.Map.Entry;


/**
 * {@link IBindingSet} backed by a {@link HashMap}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
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
    
    public boolean isBound(IVariable var) {
     
        if (var == null)
            throw new IllegalArgumentException();
        
        return map.containsKey(var);
        
    }
    
    public IConstant get(IVariable var) {
   
        if (var == null)
            throw new IllegalArgumentException();
        
        return map.get(var);
        
    }

    public void set(IVariable var, IConstant val) {
        
        if (var == null)
            throw new IllegalArgumentException();
        
        if (val == null)
            throw new IllegalArgumentException();
        
        map.put(var,val);
        
    }

    public void clear(IVariable var) {
        
        if (var == null)
            throw new IllegalArgumentException();
        
        map.remove(var);
        
    }
    
    public void clearAll() {
        
        map.clear();
        
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
        
        for (IVariable var : variablesToKeep) {
            
            IConstant val = map.get(var);
            if (val != null) {
                bs.map.put(var, val);
            }
            
        }
        
        return bs;
        
    }
    
    public boolean equals(final IBindingSet o) {
        
        if (o == this)
            return true;
        
        if (size() != o.size())
            return false;
        
        final Iterator<Map.Entry<IVariable,IConstant>> itr = map.entrySet().iterator();
        
        while(itr.hasNext()) {

            final Map.Entry<IVariable,IConstant> entry = itr.next();
            
            final IVariable var = entry.getKey();
            
            final IConstant val = entry.getValue();
            
//            if (!o.isBound(vars[i]))
//                return false;

            if (!val.equals(o.get(var)))
                return false;
            
        }
        
        return true;
        
    }

}
