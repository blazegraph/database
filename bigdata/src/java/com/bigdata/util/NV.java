package com.bigdata.util;

import java.io.Serializable;

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
    
    private final String value;
    
    public String getName() {
        
        return name;
        
    }
    
    public String getValue() {
        
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
    public NV(final String name, final String value) {

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

}
