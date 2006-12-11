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
 * Created on Dec 8, 2006
 */

package com.bigdata.objndx;

import java.nio.ByteBuffer;

import com.bigdata.journal.Bytes;

/**
 * Key (de-)serializer suitable for use with int32 object identifier keys. keys
 * are non-negative integers in the half-open range [1:{@link Integer#MAX_VALUE}).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo keys can be packed since they are non-negative.
 */
public class Int32OIdKeySerializer implements IKeySerializer {

    public static final IKeySerializer INSTANCE = new Int32OIdKeySerializer();

    /**
     * The value of the key that represents a null reference. This is the
     * minimum legal value for an int32 identifier.
     */
    static final int NEGINF = 0;
    
    public int getSize(int n) {
        
        return n*Bytes.SIZEOF_INT;
        
    }

    public void putKeys(ByteBuffer buf, Object keys, int nkeys) {

        final int[] a = (int[]) keys;
        
        int lastKey = NEGINF;
        
        for (int i = 0; i < nkeys; i++) {

            final int key = a[i];
            
            assert key > lastKey; // verify increasing and minimum.
            
//            assert key < BTree.POSINF; // verify maximum.

            buf.putInt(key);
            
            lastKey = key;
            
        }

    }

    public void getKeys(ByteBuffer buf, Object keys, int nkeys) {
        
        final int[] a = (int[])keys;

        int lastKey = NEGINF;

        for (int i = 0; i < nkeys; i++) {

            int key = buf.getInt();

            assert key > lastKey; // verify keys are in ascending order.

//            assert key < BTree.POSINF; // verify keys in legal range.

            a[i] = lastKey = key;

        }
        
    }

}
