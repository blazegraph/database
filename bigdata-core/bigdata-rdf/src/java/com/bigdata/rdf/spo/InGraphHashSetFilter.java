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
package com.bigdata.rdf.spo;

import java.util.HashSet;

import com.bigdata.rdf.internal.IV;

/**
 * "IN" filter for the context position based on a native long hash set
 * containing the acceptable graph identifiers. While evaluation of the access
 * path will be ordered, the filter does not maintain evolving state so a hash
 * set will likely beat a binary search.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: InGraphHashSetFilter.java 3694 2010-09-30 14:54:59Z mrpersonick
 *          $
 * 
 * @see InGraphBinarySearchFilter
 */
@SuppressWarnings("rawtypes")
public final class InGraphHashSetFilter<E extends ISPO> extends SPOFilter<E> {

    /**
     * 
     */
    private static final long serialVersionUID = -6059009162692785772L;

    private final HashSet<IV> contextSet;
    
    /**
     * 
     * @param graphs
     *            The set of acceptable graph identifiers.
     */
    public InGraphHashSetFilter(final int initialCapacity,
            final Iterable<IV> graphs) {

        /*
         * Create a sorted array of term identifiers for the set of contexts
         * we will accept.
         */

        contextSet = new HashSet<IV>(initialCapacity);
        
        for (IV termId : graphs) {
        
            if (termId != null) {

                contextSet.add(termId);
                
            }
            
        }
        
    }

    @Override
    public boolean isValid(final Object o) {
        
        if (!canAccept(o)) {
            
            return true;
            
        }
        
        return accept((ISPO) o);
        
    }

    private boolean accept(final ISPO o) {
        
        final ISPO spo = (ISPO) o;
        
        return contextSet.contains(spo.c());
        
    }

    @Override
    public String toString() {

        return getClass().getName() + "{size=" + contextSet.size() + "}";
        
    }

}