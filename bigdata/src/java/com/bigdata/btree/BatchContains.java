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
