package com.bigdata.util;

import java.io.Serializable;

/**
 * A name-value pair.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NV implements Serializable {
    
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
     *             if either parameter is <code>null</code>.
     */
    public NV(String name, String value) {

        if (name == null)
            throw new IllegalArgumentException();

        if (value == null)
            throw new IllegalArgumentException();

        this.name = name;

        this.value = value;
        
    }
    
    public int hashCode() {
        
        return name.hashCode();
        
    }
    
    public boolean equals(NV o) {
    
        return name.equals(o.name) && value.equals(o.value);
        
    }
    
}
