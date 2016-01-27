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
package com.bigdata.blueprints;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class BigdataBindingSet implements Iterable<Map.Entry<String,Object>> {
        
    private final Map<String, Object> vals;
    
    public BigdataBindingSet() {
        this.vals = new LinkedHashMap<String, Object>();
    }
    
    public void put(final String key, final Object val) {
        this.vals.put(key, val);
    }
    
    public Object get(final String key) {
        return this.vals.get(key);
    }
    
    public boolean isBound(final String key) {
        return this.vals.containsKey(key);
    }

    public Set<String> getKeys() {
        return this.vals.keySet();
    }
    
    public Map<String, Object> get() {
        return vals;
    }

    @Override
    public Iterator<Map.Entry<String,Object>> iterator() {
        return vals.entrySet().iterator();
    }

    @Override
    public String toString() {
        return "BigdataBindings [vals=" + vals + "]";
    }
    
}
