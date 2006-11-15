/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Nov 12, 2006
 */

package com.bigdata.objectIndex;


/**
 * Utility class for searching arrays that may be only partly filled with
 * values.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Search {

    /**
     * Binary search wins over linear search when there are this many keys.
     */
    public static final int BINARY_WINS = 48;
    
    /**
     * Search array for key.
     * 
     * @param key
     *            The key for the search.
     * 
     * @param keys
     *            The array of keys.
     * 
     * @param nkeys
     *            Only the first nkeys values are searched. This makes it
     *            possible to search arrays that are only partly filled.
     * 
     * @return index of the search key, if it is contained in the array;
     *         otherwise, <code>(-(insertion point) - 1)</code>. The
     *         insertion point is defined as the point at which the key would be
     *         inserted into the array. Note that this guarantees that the
     *         return value will be >= 0 if and only if the key is found.
     */
    static final public int search(final int key, final int[] keys,
            final int nkeys) {

        if (nkeys < BINARY_WINS) {

            // @todo backport this change to generic-data.

            return linearSearch(key, keys, nkeys);

        } else {

            return binarySearch(key, keys, nkeys);

        }

    }

    /**
     * Linear search implementation obeying the contract of
     * {@link #search(int, int[], int)}
     */
    static final public int linearSearch(final int key, final int[] keys,
            final int nkeys) {

        for (int i = 0; i < nkeys; i++) {

            int val = keys[i];

            if (val == key)
                return i;

            if (val > key)
                return -(i + 1);

        }

        return -(nkeys + 1);

    }

    /**
     * Binary search implementation obeying the contract of
     * {@link #search(int, int[], int)}
     */
    static final public int binarySearch(final int key, final int[] keys,
            final int nkeys) {

        int low = 0;

        int high = nkeys - 1;

        while (low <= high) {

            final int mid = (low + high) >> 1;

            final int midVal = keys[mid];

            if (midVal < key) {

                low = mid + 1;

            } else if (midVal > key) {

                high = mid - 1;

            } else {

                // Found: return offset.

                return mid;

            }

        }

        // Not found: return insertion point.

        return -(low + 1);

    }

}
