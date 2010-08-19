package com.bigdata.bop.ap;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Comparator for {@link E elements}.
 */
class EComparator implements Comparator<E>, Serializable {
    
    private static final long serialVersionUID = 1L;

    public int compare(final E o1, final E o2) {
     
        return o1.name.compareTo(o2.name);
        
    }
    
}