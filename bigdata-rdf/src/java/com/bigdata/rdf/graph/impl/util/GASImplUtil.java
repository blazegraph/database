package com.bigdata.rdf.graph.impl.util;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import com.bigdata.rdf.internal.IV;

/**
 * Some utility method for the GAS Engine implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class GASImplUtil {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static final Iterator<IV> EMPTY_VERTICES_ITERATOR = (Iterator) Collections.emptyIterator();

    /**
     * Compact a collection of vertices into an ordered frontier.
     * 
     * @param vertices
     *            The collection of vertices for the new frontier.
     * 
     * @return The compact, ordered frontier.
     */
    @SuppressWarnings("rawtypes")
    public static IV[] compactAndSort(final Set<IV> vertices) {
    
        final IV[] a;
    
        final int size = vertices.size();
    
        // TODO FRONTIER: Could reuse this array for each round!
        vertices.toArray(a = new IV[size]);
    
        /*
         * Order for index access. An ordered scan on a B+Tree is 10X faster
         * than random access lookups.
         * 
         * Note: This uses natural V order, which is also the index order.
         */
        java.util.Arrays.sort(a);
    
        return a;
        
    }

}
