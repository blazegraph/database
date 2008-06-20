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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * {@link IBindingSet} backed by a {@link HashMap}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo compact serialization for bindings for a relation.
 */
public class HashBindingSet implements IBindingSet {

    private Map<IVariable,IConstant> map = new HashMap<IVariable, IConstant>();

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
        
        StringBuilder sb = new StringBuilder();
        
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

}
