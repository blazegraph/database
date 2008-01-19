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
 * Batch removal of one or more tuples, returning their existing values by
 * side-effect.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BatchRemove implements IBatchOperation {

    /**
     * The #of tuples to be processed.
     */
    public final int n;
    
    /**
     * The keys for each tuple.
     */
    public final byte[][] keys;
    
    /**
     * The value corresponding to each key.
     */
    public final Object[] values;
    
    /**
     * The index of the tuple that is currently being processed.
     */
    public int tupleIndex = 0;

    public int getTupleCount() {
        return n;
    }
    
    public byte[][] getKeys() {
        return keys;
    }

    /**
     * Create batch remove operation.
     * 
     * @param ntuples
     *            The #of tuples in the operation (in).
     * @param keys
     *            A series of keys paired to values (in). Each key is an
     *            variable length unsigned byte[]. The keys must be presented in
     *            sorted order in order to obtain maximum efficiency for the
     *            batch operation.
     * @param values
     *            An array of values, one per tuple (out). The array element
     *            corresponding to a tuple will be null if the key did not exist
     *            -or- if the key existed with a null value (null values are
     *            used to mark deleted keys in an isolated btree).
     * 
     * @exception IllegalArgumentException
     *                if the dimensions of the arrays are not sufficient for the
     *                #of tuples declared.
     */
    public BatchRemove(int ntuples, byte[][] keys, Object[] values) {
        
        if (ntuples <= 0)
            throw new IllegalArgumentException(Errors.ERR_NTUPLES_NON_POSITIVE);

        if (keys == null)
            throw new IllegalArgumentException(Errors.ERR_KEYS_NULL);

        if (keys.length < ntuples)
            throw new IllegalArgumentException(Errors.ERR_NOT_ENOUGH_KEYS);

        if (values == null)
            throw new IllegalArgumentException(Errors.ERR_VALS_NULL);

        if (values.length < ntuples)
            throw new IllegalArgumentException(Errors.ERR_NOT_ENOUGH_VALS);

        this.n = ntuples;
        this.keys = keys;
        this.values = values;

    }

    /**
     * Applies the operation using {@link ISimpleBTree#remove(Object)}.
     * 
     * @param btree
     */
    public void apply(ISimpleBTree btree) {

        while( tupleIndex < n) {

            values[tupleIndex] = btree.remove(keys[tupleIndex]);

            tupleIndex ++;

        }

    }
    
}
