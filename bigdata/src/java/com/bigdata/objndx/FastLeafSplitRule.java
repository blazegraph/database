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
 * Created on Nov 27, 2006
 */

package com.bigdata.objndx;

/**
 * Fast implementation does not copy the keys.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FastLeafSplitRule implements ILeafSplitRule {

    final private int maxKeys;
    
    /**
     * 
     */
    public FastLeafSplitRule(int maxKeys) {
        
        this.maxKeys = maxKeys;
        
    }
    
    /**
     * <p>
     * A leaf is split by choosing the first key to copy to the new
     * rightSibling. The leaf is split into (leaf, rightSibling) and we
     * insert(separatorKey, rightSibling) into the parent. Since keys and values
     * are paired one-to-one in a leaf, all keys and values starting with the
     * split index are copied into the new right sibling.
     * </p>
     * <p>
     * This method chooses the index of the median of the keys in the leaf union
     * the insertKey. This guarentees that both leaves will satisify the
     * invariants. If we do not consider the incoming key, then splitting a leaf
     * with an odd number of keys (m := 3) in half can result in a 1-2 split and
     * if the new key goes into the high leaf then this produces a 1-3 split,
     * which means that the leaf with only one key is invalid. We prevent this
     * by computing the split index naievly and then adjusting it for whether or
     * not the insertKey would cause either leaf to remain deficient.
     * </p>
     * <p>
     * The split rule always chooses the index m/2 when m is even since the
     * result always produces two valid leaves. The complicated case is when m
     * is odd since the insert key must be considered as well.
     * </p>
     * <p>
     * When m is odd and we compute m/2 using integer math we get a splitIndex
     * that puts the extra key into the high leaf. E.g., when m := 3 we get
     * splitIndex := 3/2 = 1. This works out correctly when the insert key will
     * go into the lower leaf. However, when the insert key would go into the
     * high leaf, we must adjust the splitIndex up by one so that the low leaf
     * will have as many values after the split and insert operation as the high
     * leaf, which in turn guarentees that the invariants are satisified for
     * both leaves.
     * </p>
     * <p>
     * The following are examples of the post-conditions resulting when a split
     * occurs during an insert. These examples show the post-condition after the
     * insert operation has been completed. In all cases the separatorKey is the
     * key at index zero in the high leaf in the post-condition, but this method
     * is responsible for determining what the separatorKey needs to be in order
     * for that post-condition to be achieved by {@link #insert(int, Object)}.
     * </p>
     * <p>
     * Given: keys[ 3 5 7 ]
     * </p>
     * 
     * <pre>
     * insert(8), postcondition: [ 3 5 - ], [ 7 8 - ]
     * insert(6), postcondition: [ 3 5 - ], [ 6 7 - ]
     * insert(1), postcondition: [ 1 3 - ], [ 5 7 - ]
     * insert(4), postcondition: [ 3 4 - ], [ 5 7 - ]
     * </pre>
     * 
     * <p>
     * Given: keys [ 3 5 7 9 ]
     * </p>
     * 
     * <pre>
     * insert(10), postcondition: [ 3 5 - - ], [ 7 9 10 - ]
     * insert(6),  postcondition: [ 3 5 - - ], [ 6 7 9 - ]
     * insert(1),  postcondition: [ 1 3 5 - ], [ 7 9 - - ]
     * insert(4),  postcondition: [ 3 4 5 - ], [ 7 9 - - ]
     * </pre>
     * 
     * @param insertKey
     *            The key that is being inserted and which is driving the split.
     * 
     * @return The index at which to split the leaf (the index of the first key
     * to copy to the rightSibling).
     * 
     * @see #_separatorKey, which is set as a side-effect.
     * 
     * @see TestBTree#test_leafSplitRuleBranchingFactor3()
     * @see TestBTree#test_leafSplitRuleBranchingFactor4()
     */
    public int getSplitIndex(int[] keys,int insertKey) {

        assert maxKeys == keys.length;
        
        // compute m/2
        int splitIndex = maxKeys >> 1;
        
        // true iff m is odd (3, 5, 7, etc.)
        final boolean odd = (maxKeys & 1) == 1;
        
        /*
         * Iff m is odd then m/2 is correct iff the insertKey goes into the
         * lower leaf. Therefore if the insertKey is larger than the smallest
         * key in the upper leaf then we adjust the splitIndex to splitIndex +1
         * such that the insert goes into the high leaf.
         */

        if (odd && insertKey > keys[splitIndex]) {
     
            splitIndex++;
            
        }

        /*
         * Figure out whether or not the insertKey will also serve as the
         * separatorKey.
         * keys: [ 3 5 7 ]
         * insert(8), postcondition: [ 3 5 - ], [ 7 8 - ]
         * insert(6), postcondition: [ 3 5 - ], [ 6 7 - ]
         * insert(1), postcondition: [ 1 3 - ], [ 5 7 - ]
         * insert(4), postcondition: [ 3 4 - ], [ 5 7 - ]
         */

        final int separatorKey;
        
        if( insertKey < keys[splitIndex - 1] ) {
           
            /*
             * The insertKey will go into the low leaf so the separatorKey is
             * the key at the computed splitIndex.
             */
            separatorKey = keys[splitIndex];
            
        } else if( insertKey < keys[splitIndex] ) {

            /*
             * The insertKey is less than any existing key in the high leaf so
             * the separatorKey is the insertKey.
             */
            separatorKey = insertKey;
            
        } else {

            /*
             * The insert key will go into the high leaf at some position other
             * than the first index.  In this case the separatorKey is the key
             * at the computed splitIndex.
             */
            
            separatorKey = keys[splitIndex];
            
        }

        /*
         * Note: always exit this method by falling through so that we are
         * guarenteed that we set the _separatorKey as a side-effect.
         */
        _separatorKey = separatorKey;
        
        return splitIndex;

    }

    /**
     * Variable set as a side-effect by {@link #getSplitIndex(int)} and contains
     * the separatorKey that must be inserted into the parent.
     * 
     * For example, if we insert (4) into
     * 
     * <pre>
     *  keys : [ 1 3 5 ]
     * </pre>
     * 
     * the post-condition for the split and insert is:
     * 
     * <pre>
     *  keys : [ 1 3 - ], [ 4 5 - ]
     * </pre>
     * 
     * In this case, this field will be set to (4) and the insert key will
     * become the separatorKey in the parent.
     */
    private transient int _separatorKey;

    public int getSeparatorKey() {
        
        return _separatorKey;
        
    }

}
