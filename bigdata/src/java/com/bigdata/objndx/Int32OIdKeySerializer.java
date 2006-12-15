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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.journal.Bytes;

/**
 * Key (de-)serializer suitable for use with int32 object identifier keys. keys
 * are non-negative integers in the half-open range [1:{@link Integer#MAX_VALUE}).
 * The keys are packed. When the keys tend to be found in the lower part of the
 * positive {@link Integer} range this results in smaller serialization. Even if
 * the keys are evenly distributed through the positive {@link Integer} range
 * packing is more compact than staight serialization. However, it is possible
 * to serialize keys such that the result is less compact.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Handle prefix packing, which will be more efficient than
 * {@link LongPacker}, which is currently disabled since it is interacting with
 * the key distributions used by the {@link TestNodeSerializer} test suite.
 * doing. Note that things like prefix / suffix compression will break
 * serialization and suggests that they should be version changes for the
 * {@link NodeSerializer}.
 */
public class Int32OIdKeySerializer implements IKeySerializer {

    public static final IKeySerializer INSTANCE = new Int32OIdKeySerializer();

    /**
     * The value of the key that represents a null reference. This is the
     * minimum legal value for an int32 identifier.
     */
    static final int NEGINF = 0;
    
    public ArrayType getKeyType() {
        
        return ArrayType.INT;
        
    }
    
    public int getSize(int n) {
        
        return n*Bytes.SIZEOF_INT;
        
    }

    public void putKeys(DataOutputStream os, Object keys, int nkeys) throws IOException {

        final int[] a = (int[]) keys;
        
        int lastKey = NEGINF;
        
        for (int i = 0; i < nkeys; i++) {

            final int key = a[i];
            
            assert key > lastKey; // verify increasing and minimum.
            
//            assert key < BTree.POSINF; // verify maximum.

//            LongPacker.packLong(os, key);
            
            os.writeInt(key);
            
            lastKey = key;
            
        }

    }

    public void getKeys(DataInputStream is, Object keys, int nkeys)
        throws IOException
    {
        
        final int[] a = (int[])keys;

        int lastKey = NEGINF;

        for (int i = 0; i < nkeys; i++) {

            final int key = is.readInt();
            
//            final int key = (int)LongPacker.unpackLong(is);

            assert key > lastKey; // verify keys are in ascending order.

//            assert key < BTree.POSINF; // verify keys in legal range.

            a[i] = lastKey = key;

        }
        
    }

}
