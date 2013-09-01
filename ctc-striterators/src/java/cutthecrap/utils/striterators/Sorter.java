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
package cutthecrap.utils.striterators;

import java.util.Comparator;
import java.util.Iterator;

/**
 * <p>Used with Sorterator by Striterator to sort iterations.</p>
 *
 * <p>Cannot be instantiated directly since an implementation of the compare method is required.</p>
 *
 * <p>It should be noted, that the resulting sorting iterator cannot be produced incrementally
 *	since it must access all objects to complete the sort.  For this reason care should be taken
 *	for sets that may be very large.</p>
 */

abstract public class Sorter extends FilterBase implements Comparator {
 
//    /**
//     * Annotation giving the {@link Comparator} to be applied.
//     */
//    static final public String COMPARATOR = Sorter.class.getName()
//            + ".comparator";

    public Sorter() {
    }

//    public Sorter(Comparator c) {
//        
//        setProperty(COMPARATOR, c);
//        
//    }

    // -------------------------------------------------------------

    @Override
    final public Iterator filterOnce(Iterator src, Object context) {

        return new Sorterator(src, context, this);
        
    }

    // -------------------------------------------------------------

//    public Comparator getComparator() {
//    
//        return (Comparator) getRequiredProperty(COMPARATOR);
//        
//    }
    
    public abstract int compare(Object o1, Object o2);

}
