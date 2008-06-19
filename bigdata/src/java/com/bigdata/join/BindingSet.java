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
import java.util.Map;

/**
 * Simple {@link IBindingSet} impl.
 * 
 * @todo use the generic data record for a compact and efficient impl but make
 *       sure that the data record access is not synchronized for this purpose.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BindingSet implements IBindingSet {

    private Map<String,Object> map = new HashMap<String,Object>();

    public boolean isBound(String name) {
        return map.containsKey(name);
    }
    
    public Object get(String name) {
        return map.get(name);
    }

    public double getDouble(String name) {
        return ((Double) map.get(name)).doubleValue();
    }

    public float getFloat(String name) {
        return ((Float) map.get(name)).floatValue();
    }

    public int getInt(String name) {
        return ((Integer) map.get(name)).intValue();
    }

    public long getLong(String name) {
        return ((Long) map.get(name)).longValue();
    }

    public void set(String name, Object val) {
        map.put(name,val);
    }

    public void setDouble(String name, double val) {
        map.put(name,val);
    }

    public void setFloat(String name, float val) {
        map.put(name,val);
    }

    public void setInt(String name, int val) {
        map.put(name,val);
    }

    public void setLong(String name, long val) {
        map.put(name,val);
    }

    public void clear(String name) {
        map.remove(name);
    }
    
    public void clearAll() {
        map.clear();
    }
    
}
