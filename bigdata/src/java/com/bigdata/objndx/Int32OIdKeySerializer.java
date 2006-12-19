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
import org.CognitiveWeb.extser.ShortPacker;

import com.bigdata.journal.Bytes;

/**
 * Key (de-)serializer suitable for use with int32 object identifier keys. keys
 * are non-negative integers in the half-open range [1:{@link Integer#MAX_VALUE}).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Int32OIdKeySerializer implements IKeySerializer {

    /**
     * The value of the key that represents a null reference. This is the
     * minimum legal value for an int32 identifier.
     */
    static final int NEGINF = 0;
    
    /**
     * The keys are written as unpacked integer values.
     */
    static final public byte VERSION0 = (byte)0;
    
    /**
     * The keys are written as packed non-negative integer values. This often,
     * but not always, produces a more compact serialization that just writing
     * out the integer values.
     * 
     * @todo review why it is possible for this to take more space than 
     */
    static final public byte VERSION1 = (byte)1;
    
    /**
     * Prefix compression with packed integer values.
     * 
     * The value of the largest key is choosen as a prefix and written out once
     * as a packed integer. For each key, the difference between the prefix and
     * that key is computed and written out as a packed integer. This is always
     * a non-negative integer and it will be zero for the largest key.
     * 
     * @todo compare this with a strategy that computes the #of leading bytes
     *       (or bits) in common between the first and last keys, writes that
     *       out, and then masks off those bytes when writing out the keys. Once
     *       masked off, the keys will again be of a fixed length. Explore
     *       whether packing the masked off values is worth while and, if the
     *       #of remaining bits is 15 or less, then explore the
     *       {@link ShortPacker}. Finally, if the #of bits remaining is 8 or
     *       less than just write out one byte per key since we can not improve
     *       on that without bit packing all of the keys together (which is an
     *       option I guess).
     */
    static final public byte VERSION2 = (byte)2;
    
    /**
     * The version that will be used to write keys.
     */
    private final byte version;
    
    public byte getVersion() {
        
        return version;
        
    }

    /**
     * A public instance.
     * 
     * @todo compare various strategies, choose the best one, and make it the
     *       default here, i.e., write a performance test comparing their
     *       behavior on some realistic data.
     * 
     * @todo {@link #VERSION1} and {@link #VERSION2} can under-estimate the
     *       required space.
     */
    public static final IKeySerializer INSTANCE = new Int32OIdKeySerializer(
            VERSION0);
    
    /**
     * Create a key serializer that will use the specified serialization
     * version.
     * 
     * @param version
     *            The key serialization version. Note that the class always
     *            deserializes keys based on how the keys were actuall
     *            serialized but it serializes keys based on the choice declared
     *            to the constructor.
     * 
     * @see #VERSION0
     * @see #VERSION1
     * @see #VERSION2
     */
    public Int32OIdKeySerializer(byte version) {
        
        switch(version) {
        case VERSION0:
        case VERSION1:
        case VERSION2:
            break;
        default:
            throw new IllegalArgumentException();
        }
        this.version = version;
        
    }

    public ArrayType getKeyType() {
        
        return ArrayType.INT;
        
    }

    /**
     * Estimates for packed keys may err high on average but may actually be low
     * for some key patterns.
     */
    public int getSize(int n) {

        switch (version) {

        case VERSION0:
            // version code and N unpacked keys
            return 1 + n * Bytes.SIZEOF_INT;
        
        case VERSION1:
            // version code and N packed keys.
            return 1 + n * Bytes.SIZEOF_INT;
        
        case VERSION2:
            // version code, prefix, and N packed keys.
            return 1 + Bytes.SIZEOF_INT + n * Bytes.SIZEOF_INT;
        
        default:
            throw new AssertionError();
        
        }
        
    }

    public void putKeys(DataOutputStream os, Object keys, int nkeys) throws IOException {

        os.write(version);
        
        switch (version) {
        case VERSION0: {
            final int[] a = (int[]) keys;

            int lastKey = NEGINF;

            for (int i = 0; i < nkeys; i++) {

                final int key = a[i];

                assert key > lastKey; // verify increasing and minimum.

                os.writeInt(key);

                lastKey = key;

            }
            break;
        }
        case VERSION1: {
            final int[] a = (int[]) keys;

            int lastKey = NEGINF;

            for (int i = 0; i < nkeys; i++) {

                final int key = a[i];

                assert key > lastKey; // verify increasing and minimum.

                LongPacker.packLong(os, key);

                lastKey = key;

            }
            break;
        }
        case VERSION2: {

            final int[] a = (int[]) keys;

            // the largest key.
            final int prefix = a[nkeys-1];

            LongPacker.packLong(os, prefix);
            
            int lastKey = NEGINF;

            for (int i = 0; i < nkeys; i++) {

                final int key = a[i];

                assert key > lastKey; // verify increasing and minimum.

                // write out only the difference.
                LongPacker.packLong(os, prefix - key);

                lastKey = key;

            }
            break;
        }
        default:
            throw new AssertionError();
        }

    }
    
    public void getKeys(DataInputStream is, Object keys, int nkeys)
        throws IOException
    {

        final byte version = is.readByte();
        
        switch (version) {
        case VERSION0: {

            final int[] a = (int[]) keys;

            int lastKey = NEGINF;

            for (int i = 0; i < nkeys; i++) {

                final int key = is.readInt();

                assert key > lastKey; // verify keys are in ascending order.

                a[i] = lastKey = key;

            }

            break;
        }
        case VERSION1: {

            final int[] a = (int[]) keys;

            int lastKey = NEGINF;

            for (int i = 0; i < nkeys; i++) {

                final int key = (int) LongPacker.unpackLong(is);
                
                assert key > lastKey; // verify keys are in ascending order.

                a[i] = lastKey = key;

            }

            break;
        }
        case VERSION2: {

            final int[] a = (int[]) keys;

            // the prefixed should be equal to the largest key.
            final int prefix = (int) LongPacker.unpackLong(is);
            assert prefix > 0;
            
            int lastKey = NEGINF;

            for (int i = 0; i < nkeys; i++) {

                final int key = prefix - (int)LongPacker.unpackLong(is);

                assert key > lastKey; // verify keys are in ascending order.

                a[i] = lastKey = key;

            }
            
            assert a[nkeys-1] == prefix;
            
            break;
        }
        default:
            throw new IOException("Unknown version: " + version);

        }

    }

}
