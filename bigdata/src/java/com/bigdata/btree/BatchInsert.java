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
 * Data for a batch insert operation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BatchInsert implements IBatchOperation {

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
     * Create a batch insert operation.
     * <p>
     * Batch insert operation of N tuples presented in sorted order. This
     * operation can be very efficient if the tuples are presented sorted by key
     * order.
     * 
     * @param ntuples
     *            The #of tuples that are being inserted(in).
     * @param keys
     *            A series of keys paired to values (in). Each key is an
     *            variable length unsigned byte[]. The keys must be presented in
     *            sorted order in order to obtain maximum efficiency for the
     *            batch operation.<br>
     *            The individual byte[] keys provided to this method MUST be
     *            immutable - if the content of a given byte[] in <i>keys</i>
     *            is changed after the method is invoked then the change MAY
     *            have a side-effect on the keys stored in leaves of the tree.
     *            While this constraint applies to the individual byte[] keys,
     *            the <i>keys</i> byte[][] itself may be reused from invocation
     *            to invocation without side-effect.
     * @param values
     *            Values (one element per key) (in/out). Null elements are
     *            allowed. On output, each element is either null (if there was
     *            no entry for that key) or the old value stored under that key
     *            (which may be null).
     */
    public BatchInsert(int ntuples, byte[][] keys, Object[] values) {

        if (ntuples <= 0)
            throw new IllegalArgumentException(Errors.ERR_NTUPLES_NON_POSITIVE);
            
        if (keys == null)
            throw new IllegalArgumentException(Errors.ERR_KEYS_NULL);

        if( keys.length < ntuples )
            throw new IllegalArgumentException(Errors.ERR_NOT_ENOUGH_KEYS);

        if (values == null)
            throw new IllegalArgumentException(Errors.ERR_VALS_NULL);

        if( values.length < ntuples )
            throw new IllegalArgumentException(Errors.ERR_NOT_ENOUGH_VALS);

        this.n = ntuples;
        this.keys = keys;
        this.values = values;
        
    }
    
    /**
     * Applies the operator using {@link ISimpleBTree#insert(Object, Object)}
     * 
     * @param btree
     */
    public void apply(ISimpleBTree btree) {
        
        while (tupleIndex < n) {

            values[tupleIndex] = btree.insert(keys[tupleIndex],
                    values[tupleIndex]);
            
            tupleIndex ++;

        }

    }
    
}
