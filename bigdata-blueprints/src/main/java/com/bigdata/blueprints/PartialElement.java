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

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tinkerpop.blueprints.Element;

public class PartialElement implements Element {

    private final String id;
    
    private final Map<String, Object> properties = 
            new LinkedHashMap<String, Object>();
    
    public PartialElement(final String id) {
        this.id = id;
    }
    
    @Override
    public Object getId() {
        return id;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object getProperty(final String name) {
        return properties.get(name);
    }

    @Override
    public Set<String> getPropertyKeys() {
        return properties.keySet();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object removeProperty(final String key) {
        return properties.remove(key);
    }

    @Override
    public void setProperty(final String key, final Object value) {
        
        /*
         * Gracefully turn a single value property into a 
         * multi-valued property.
         */
        if (properties.containsKey(key)) {
        
            final Object o = properties.get(key);
            
            if (o instanceof List) {
                
                @SuppressWarnings("unchecked")
                final List<Object> list = (List<Object>) o;
                list.add(value);
                
            } else {
                
                final List<Object> list = new LinkedList<Object>();
                list.add(o);
                list.add(value);
                
                properties.put(key, list);
                
            }
            
        } else {
        
            properties.put(key, value);
            
        }
        
    }
    
    public void copyProperties(final PartialElement element) {
        properties.putAll(element.properties);
    }
    
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("id: " + id);
        sb.append(", props: ");
        appendProps(sb);
        return sb.toString();
    }
    
    protected void appendProps(final StringBuilder sb) {
        for (Map.Entry<String, Object> prop : properties.entrySet()) {
            sb.append(prop.getKey()).append("=").append(prop.getValue());
        }
    }

}
