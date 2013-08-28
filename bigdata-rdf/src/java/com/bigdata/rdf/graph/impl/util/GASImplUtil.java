package com.bigdata.rdf.graph.impl.util;

import java.util.Iterator;
import java.util.Set;

import com.bigdata.rdf.internal.IV;

import cutthecrap.utils.striterators.EmptyIterator;

/**
 * Some utility method for the GAS Engine implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class GASImplUtil {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static final Iterator<IV> EMPTY_VERTICES_ITERATOR = EmptyIterator.DEFAULT;

    /**
     * Compact a collection of vertices into an ordered frontier.
     * 
     * @param vertices
     *            The collection of vertices for the new frontier.
     * 
     * @return The compact, ordered frontier.
     * 
     * @deprecated This implementation fails to reuse/grow the array for each
     *             round. This causes a lot of avoidable heap pressure during
     *             the single-threaded execution between each round and is a
     *             large percentage of the total runtime costs of the engine!
     */
    @Deprecated
    @SuppressWarnings("rawtypes")
    public static IV[] compactAndSort(final Set<IV> vertices) {
    
        final IV[] a;
    
        final int size = vertices.size();
    
        /*
         * FIXME FRONTIER: Grow/reuse this array for each round! This is 15% of
         * all time in the profiler. The #1 hot spot with the CHMScheduler. We
         * need to reuse the target array!!!
         */
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

    /**
     * Compact a collection of vertices into an ordered frontier.
     * 
     * @param vertices
     *            The collection of vertices for the new frontier.
     * @param buffer
     *            The backing buffer for the new frontier - it will be resized
     *            if necessary.
     *            
     * @return A slice onto just the new frontier.
     */
    @SuppressWarnings("rawtypes")
    public static IArraySlice compactAndSort(final Set<IV> vertices,
            final IManagedArray<IV> buffer) {

        final int nvertices = vertices.size();

        // ensure buffer has sufficient capacity.
        buffer.ensureCapacity(nvertices);

        // backing array reference (presized above).
        final IV[] a = buffer.array();

        // copy frontier into backing array.
        int i = 0;
        for (IV v : vertices) {

            a[i++] = v;

        }

        /*
         * Order the frontier for efficient index access. An ordered scan on a
         * B+Tree is 10X faster than random access lookups.
         * 
         * Note: This uses natural V order, which is also the index order.
         * 
         * FIXME FRONTIER : We should parallelize this sort!
         */
        java.util.Arrays.sort(a, 0/* fromIndex */, nvertices/* toIndex */);

        // A view onto just the new frontier.
        return buffer.slice(0/* off */, nvertices/* len */);

    }

}
