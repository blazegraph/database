/*
Striterator - transformation and mapping patterns over java Iterators

Copyright (C) SYSTAP, LLC 2010.  All rights reserved.

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
