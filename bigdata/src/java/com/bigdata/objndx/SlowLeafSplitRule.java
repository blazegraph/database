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
public class SlowLeafSplitRule implements ILeafSplitRule {
    
    final private int maxKeys;
    final private int[] keys2;
    final private int m2;
    final private int m2p1;
    
    /**
     * 
     * @param maxKeys
     *            The branching factor for the leaf (the branching factor for a
     *            lead is always the same as the maximum #of keys allowed in the
     *            leaf).
     */
    public SlowLeafSplitRule(int maxKeys) {
            
        assert maxKeys >= BTree.MIN_BRANCHING_FACTOR;
        
        this.maxKeys = maxKeys;
        
        keys2 = new int[maxKeys+1];
        
        // (m/2)
        m2 = maxKeys >> 1;

        // (m/2)+1
        m2p1 = m2+1;

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
        
//        boolean odd = (keys.length & 1) == 1;

        /* index of the 1st key in keys[] to copy to the rightSibling. this
         * is adjusted by +1 if the #of keys is odd and the key will go into
         * the rightSibling since that will produce two nodes with m/2 keys
         * each.
         */
//        int splitIndex = (odd && pos > m2 ? m2 + 1 : m2);
        int splitIndex = pos > m2 ? m2 + 1 : m2;

        // separator key (the lowest key allowed into rightSibling).
        _separatorKey = keys2[m2p1];

        return splitIndex;

    }

    public int getSeparatorKey() {
        return _separatorKey;
    }

    private int _separatorKey;

}