/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Apr 21, 2009
 */

package com.bigdata.relation.accesspath;

import cern.colt.GenericSorting;
import cern.colt.Swapper;
import cern.colt.function.IntComparator;

/**
 * Utility for merge sort of chunks. Merge sorts are used when two or more
 * ordered chunks have been combined into a single chunk. Since the source
 * chunks were already ordered, the merge sort is less expensive than performing
 * a full sort on the combined chunk.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ChunkMergeSortHelper {

    /**
     * In place merge sort.
     * 
     * @param <E>
     *            The generic type of the elements in the chunk.
     * @param chunk
     *            The chunk.
     */
    public static <E> void mergeSort(final E[] chunk) {

        if (chunk == null)
            throw new IllegalArgumentException();

        if (chunk.length == 0)
            return;

        GenericSorting.mergeSort(
                0, // fromIndex
                chunk.length, // toIndex
                new MyIntComparator((Comparable[]) chunk),
                new MySwapper((Object[]) chunk));
        
        return;

    }

    /**
     * Implementation for data in an array whose element type implements
     * {@link Comparable}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    final static private class MyIntComparator implements IntComparator {

        private final Comparable[] a;

        public MyIntComparator(final Comparable[] a) {

            this.a = a;

        }

        @SuppressWarnings("unchecked")
        public int compare(int o1, int o2) {

            return a[o1].compareTo(a[o2]);

        }

    }

    /**
     * Implementation swaps object references in an array.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    final static private class MySwapper implements Swapper {

        private final Object[] a;

        public MySwapper(final Object[] a) {

            this.a = a;

        }

        public void swap(final int i, final int j) {

            final Object t = a[i];

            a[i] = a[j];

            a[j] = t;

        }

    }

}
