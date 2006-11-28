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

import java.util.Arrays;

/**
 * Implementation copies the keys and the insert key into a shared buffer, sorts
 * the buffer, and the reports on the splitIndex and separatorKey based on the
 * position of the insertKey in the buffer.
 */
public class SlowNodeSplitRule implements INodeSplitRule {
    
    final private int maxKeys;
    final private int[] keys2;
//    final private int m2;
    final private int mp12;

    /**
     * 
     * @param maxKeys
     *            The maximum #of keys allowed in the node (this is always equal
     *            to <code>branchingFactor-1</code> for a {@link Node}).
     */
    public SlowNodeSplitRule(int maxKeys) {
    
        /*
         * Note: we allow maxKeys to be m-1 so that this class can be used with
         * a {@link Node} (which has only m-1 keys).
         */
        
        assert maxKeys >= ( BTree.MIN_BRANCHING_FACTOR - 1);
        
        this.maxKeys = maxKeys;
        
        keys2 = new int[maxKeys+1];
        
//        // (m / 2)
//        m2 = maxKeys >> 1;
        
        // (m + 1)/2
        mp12 = (maxKeys+1)>>1;

    }

    public int getSplitIndex(int[] keys, int insertKey) {

        if( keys.length != maxKeys ) {
            
            throw new IllegalArgumentException();
            
        }
        
        System.arraycopy(keys, 0, keys2, 0, keys.length);

        keys2[keys.length] = insertKey;
        
        Arrays.sort(keys2);

        int pos = Arrays.binarySearch(keys2, insertKey);

        // the insertKey MUST be found.
        assert pos >= 0;
        
        int splitIndex = pos > mp12 ? mp12 + 1 : mp12;

        // separator key (the lowest key allowed into rightSibling).
        _separatorKey = keys2[mp12];

        return splitIndex;

    }

    public int getSeparatorKey() {
        return _separatorKey;
    }

    private int _separatorKey;

}
