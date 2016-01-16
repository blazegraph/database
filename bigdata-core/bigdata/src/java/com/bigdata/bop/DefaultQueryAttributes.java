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
 * Created on Aug 16, 2011
 */

package com.bigdata.bop;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Default implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultQueryAttributes implements IQueryAttributes {

    private final ConcurrentMap<Object, Object> map = new ConcurrentHashMap<Object, Object>();

    public Object get(Object key) {
        return map.get(key);
    }

    public Object put(Object key, Object val) {
        return map.put(key, val);
    }

    public Object putIfAbsent(Object key, Object val) {
        return map.putIfAbsent(key, val);
    }

    public Object remove(Object key) {
        return map.remove(key);
    }

    public boolean remove(Object key, Object value) {
        return map.remove(key, value);
    }

    public String toString() {
        return super.toString() + map;
    }
    
}
