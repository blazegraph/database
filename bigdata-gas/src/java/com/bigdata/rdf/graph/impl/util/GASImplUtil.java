/**
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.rdf.graph.impl.util;

import java.util.Set;

import org.openrdf.model.Value;


/**
 * Some utility method for the GAS Engine implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class GASImplUtil {

//    /**
//     * Compact a collection of vertices into an ordered frontier.
//     * 
//     * @param vertices
//     *            The collection of vertices for the new frontier.
//     * 
//     * @return The compact, ordered frontier.
//     * 
//     * @deprecated This implementation fails to reuse/grow the array for each
//     *             round. This causes a lot of avoidable heap pressure during
//     *             the single-threaded execution between each round and is a
//     *             large percentage of the total runtime costs of the engine!
//     */
//    @Deprecated
//    @SuppressWarnings("rawtypes")
//    public static Value[] compactAndSort(final Set<Value> vertices) {
//    
//        final Value[] a;
//    
//        final int size = vertices.size();
//    
//        /*
//         * FRONTIER: Grow/reuse this array for each round! This is 15% of
//         * all time in the profiler. The #1 hot spot with the CHMScheduler. We
//         * need to reuse the target array!!!
//         */
//        vertices.toArray(a = new Value[size]);
//    
//        /*
//         * Order for index access. An ordered scan on a B+Tree is 10X faster
//         * than random access lookups.
//         * 
//         * Note: This uses natural V order, which is also the index order.
//         */
//        java.util.Arrays.sort(a);
//    
//        return a;
//        
//    }

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
    public static IArraySlice<Value> compactAndSort(final Set<Value> vertices,
            final IManagedArray<Value> buffer) {

        final int nvertices = vertices.size();

        // ensure buffer has sufficient capacity.
        buffer.ensureCapacity(nvertices);

        // backing array reference (presized above).
        final Value[] a = buffer.array();

        // copy frontier into backing array.
        int i = 0;
        for (Value v : vertices) {

            a[i++] = v;

        }

        /*
         * Order the frontier for efficient index access. An ordered scan on a
         * B+Tree is 10X faster than random access lookups.
         * 
         * Note: This uses natural V order, which is also the index order.
         */
        java.util.Arrays.sort(a, 0/* fromIndex */, nvertices/* toIndex */);

        // A view onto just the new frontier.
        return buffer.slice(0/* off */, nvertices/* len */);

    }

}
