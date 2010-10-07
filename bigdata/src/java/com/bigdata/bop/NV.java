/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Aug 17, 2010
 */

package com.bigdata.bop;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A name-value pair.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NV implements Serializable, Comparable<NV> {
    
    /**
     * 
     */
    private static final long serialVersionUID = -6919300153058191480L;

    private final String name;
    
    private final Object value;
    
    public String getName() {
        
        return name;
        
    }
    
    public Object getValue() {
        
        return value;
        
    }

    public String toString() {
        
        return name + "=" + value;
        
    }
    
    /**
     * 
     * @throws IllegalArgumentException
     *             if the <i>name</i> is <code>null</code>.
     */
    public NV(final String name, final Object value) {

        if (name == null)
            throw new IllegalArgumentException();

//        if (value == null)
//            throw new IllegalArgumentException();

        this.name = name;

        this.value = value;
        
    }
    
    public int hashCode() {
        
        return name.hashCode();
        
    }
    
    public boolean equals(final Object o) {

        if (this == o)
            return true;

        if (!(o instanceof NV))
            return false;

        return name.equals(((NV) o).name) && value.equals(((NV) o).value);
        
    }

    /**
     * Places into order by <code>name</code>.
     */
    public int compareTo(final NV o) {
        
        return name.compareTo(o.name);
        
    }
    
    /**
     * Combines the two arrays, appending the contents of the 2nd array to the
     * contents of the first array.
     * 
     * @param a
     * @param b
     * @return
     */
    public static NV[] concat(final NV[] a, final NV[] b) {

        if (a == null && b == null)
            return a;

        if (a == null)
            return b;

        if (b == null)
            return a;

        final NV[] c = (NV[]) java.lang.reflect.Array.newInstance(a.getClass()
                .getComponentType(), a.length + b.length);

        System.arraycopy(a, 0, c, 0, a.length);

        System.arraycopy(b, 0, c, a.length, b.length);

        return c;

    }

    /**
     * Wrap an array name/value pairs as a {@link Map}.
     * 
     * @param a
     *            The array.
     * 
     * @return The map.
     */
    static public Map<String, Object> asMap(final NV... a) {

        final Map<String, Object> tmp = new LinkedHashMap<String, Object>(
                a.length);

        for (int i = 0; i < a.length; i++) {

            tmp.put(a[i].name, a[i].value);

        }

        return tmp;

    }

}
