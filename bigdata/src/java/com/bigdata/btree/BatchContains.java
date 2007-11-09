/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Feb 12, 2007
 */

package com.bigdata.btree;

/**
 * Batch existence test operation. Existence tests SHOULD be used in place of
 * lookup tests to determine key existence if null values are allowed in an
 * index (lookup will return a null for both a null value and the absence of a
 * key in the index).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BatchContains implements IBatchOperation, IReadOnlyOperation {

    /**
     * The #of tuples to be processed.
     */
    public final int ntuples;
    
    /**
     * The keys for each tuple.
     */
    public final byte[][] keys;
    
    /**
     * The value corresponding to each key.
     */
    public final boolean[] contains;
    
    /**
     * The index of the tuple that is currently being processed.
     */
    public int tupleIndex = 0;
    
    public int getTupleCount() {
        return ntuples;
    }
    
    public byte[][] getKeys() {
        return keys;
    }
    
    /**
     * Create a batch existance test operation.
     * 
     * @param ntuples
     *            The #of tuples in the operation (in).
     * 
     * @param keys
     *            A series of keys paired to values (in). Each key is an
     *            variable length unsigned byte[]. The keys must be presented in
     *            sorted order in order to obtain maximum efficiency for the
     *            batch operation.
     * 
     * @param contains
     *            An array of boolean flags, one per tuple (in,out). On input,
     *            the tuple will be tested iff the corresponding element is
     *            <code>false</code> (this supports chaining of this operation
     *            on a view over multiple btrees). On output, the array element
     *            corresponding to a tuple will be true iff the key exists.
     * 
     * @exception IllegalArgumentException
     *                if the dimensions of the arrays are not sufficient for the
     *                #of tuples declared.
     *                
     * @todo consider returning the #of keys that were found in the btree.
     */
    public BatchContains(int ntuples, byte[][] keys, boolean[] contains) {
        
        if (ntuples <= 0)
            throw new IllegalArgumentException(Errors.ERR_NTUPLES_NON_POSITIVE);

        if (keys == null)
            throw new IllegalArgumentException(Errors.ERR_KEYS_NULL);

        if (keys.length < ntuples)
            throw new IllegalArgumentException(Errors.ERR_NOT_ENOUGH_KEYS);

        if (contains == null)
            throw new IllegalArgumentException(Errors.ERR_VALS_NULL);

        if (contains.length < ntuples)
            throw new IllegalArgumentException(Errors.ERR_NOT_ENOUGH_VALS);

        this.ntuples = ntuples;
        this.keys = keys;
        this.contains = contains;

    }

    /**
     * Applies the operation using {@link ISimpleBTree#contains(byte[])}.
     * 
     * @param btree
     */
    public void apply(ISimpleBTree btree) {
        
        while( tupleIndex < ntuples ) {

            // skip tuples already marked as true.
            if (contains[tupleIndex]) {

                tupleIndex++;

                continue;

            }

            contains[tupleIndex] = btree.contains(keys[tupleIndex]);

            tupleIndex ++;

        }

    }
    
}
