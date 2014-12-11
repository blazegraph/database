/**
Copyright (C) SYSTAP, LLC 2006-Infinity.  All rights reserved.

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
package com.bigdata.blueprints;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BigdataSelection {

    private final List<Bindings> bindings;
    
    public BigdataSelection() {
        this.bindings = new LinkedList<Bindings>();
    }
    
    public Bindings newBindings() {
        final Bindings b = new Bindings();
        bindings.add(b);
        return b;
    }
    
    public List<Bindings> getBindings() {
        return bindings;
    }
    
    public static class Bindings implements Iterable<Map.Entry<String,Object>> {
        
        private final Map<String, Object> vals;
        
        private Bindings() {
            this.vals = new LinkedHashMap<String, Object>();
        }
        
        public void put(final String key, final Object val) {
            this.vals.put(key, val);
        }
        
        public Object get(final String key) {
            return this.vals.get(key);
        }

        public Set<String> getKeys() {
            return this.vals.keySet();
        }

        @Override
        public Iterator<Map.Entry<String,Object>> iterator() {
            return vals.entrySet().iterator();
        }

        @Override
        public String toString() {
            return "Bindings [vals=" + vals + "]";
        }
        
    }
    
}
