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
package com.bigdata.btree;

import java.io.DataInput;
import java.io.IOException;

import org.CognitiveWeb.extser.LongPacker;
import org.CognitiveWeb.extser.ShortPacker;

import com.bigdata.io.DataOutputBuffer;

/**
 * Compact serialization for the key buffer.
 * 
 * @todo this uses the {@link LongPacker} but many of the serialized values
 *       could be restricted to the range of a signed short so there may be
 *       an efficiency possible there.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class KeyBufferSerializer implements IKeySerializer {

    /**
     * 
     */
    private static final long serialVersionUID = 7361581167520945586L;

    public static final transient short VERSION0 = 0x0;
    
    public static final transient IKeySerializer INSTANCE = new KeyBufferSerializer();
    
    public IKeyBuffer getKeys(DataInput is) throws IOException {

        final short version = ShortPacker.unpackShort(is);
        
        if (version != VERSION0)
            throw new IOException("Unknown version=" + version);
        
        // #of keys in the node or leaf.
        final int nkeys = (int) LongPacker.unpackLong(is);
        
        // maximum #of keys allowed in the node or leaf.
        final int maxKeys = (int) LongPacker.unpackLong(is);
        
        // length of the byte[] containing the prefix and the remainder for each key.
        final int bufferLength = (int) LongPacker.unpackLong(is);
        
        /*
         * Read in deltas for each key and re-created the offsets.
         */
        int[] offsets = new int[nkeys];
        
        int lastOffset = 0; // prefixLength;

        for( int i=0; i<nkeys; i++ ) {
            
            int delta = (int) LongPacker.unpackLong(is);
            
            int offset = lastOffset + delta;
            
            offsets[i] = offset;
            
            lastOffset = offset;
            
        }
        
        byte[] buf = new byte[bufferLength];
        
        is.readFully(buf);
        
        return new ImmutableKeyBuffer(nkeys, maxKeys, offsets, buf);
        
    }

    public void putKeys(DataOutputBuffer os, IKeyBuffer keys) throws IOException {

        os.packShort(VERSION0);
        
        if(keys instanceof ImmutableKeyBuffer ) {

            putKeys2(os,(ImmutableKeyBuffer)keys);

        } else {
            
            putKeys2(os,(MutableKeyBuffer)keys);
            
        }
        
    }
    
    protected void putKeys2(DataOutputBuffer os, ImmutableKeyBuffer keys) throws IOException {
        
        final int nkeys = keys.nkeys;

        final int bufferLength = keys.buf.length;

        // #of keys in the node or leaf.
//        LongPacker.packLong(os, nkeys);
        os.packLong(nkeys);

        // maximum #of keys allowed in the node or leaf.
//        LongPacker.packLong(os, keys.maxKeys);
        os.packLong(keys.maxKeys);       

        // length of the byte[] buffer containing the prefix and remainder for each key.
//        LongPacker.packLong(os, bufferLength);
        os.packLong(bufferLength);
        
        /*
         * Write out deltas between offsets.
         */
        int lastOffset = 0; // keys.prefixLength;
        
        for( int i=0; i<nkeys; i++) {
            
            int offset = keys.offsets[i];
            
            int delta = offset - lastOffset;
            
//            LongPacker.packLong(os, delta);
            os.packLong(delta);
            
            lastOffset = offset;
            
        }
        
//        os.write(keys.buf);
        os.write(keys.buf, 0, bufferLength);
        
    }

    /**
     * Serializes a mutable key buffer using the same format as an immutable
     * key buffer. Key buffers are always read back in as immutable key
     * buffers and then converted to mutable key buffers when a mutation
     * operation is required by the btree.
     * 
     * @param os
     * @param keys
     * @throws IOException
     */
    protected void putKeys2(DataOutputBuffer os, MutableKeyBuffer keys) throws IOException {
        
        final int nkeys = keys.nkeys;
        
        final int prefixLength = keys.getPrefixLength();
        
        // offsets into the serialized key buffer.
        final int[] offsets = new int[nkeys];
        
        // compute the total length of the key buffer.
        int bufferLength = prefixLength;
        
        for(int i=0; i<nkeys; i++) {
            
            // offset to the remainder of the ith key in the buffer.
            offsets[i] = bufferLength;
            
            int remainder = keys.keys[i].length - prefixLength;
            
            assert remainder >= 0;
            
            bufferLength += remainder;
            
        }
        
        // #of keys in the node or leaf.
//        LongPacker.packLong(os, nkeys);
        os.packLong(nkeys);

        // maximum #of keys allowed in the node or leaf.
//        LongPacker.packLong(os, keys.getMaxKeys());
        os.packLong(keys.getMaxKeys());

        // length of the byte[] buffer containing the prefix and remainder for each key.
//        LongPacker.packLong(os, bufferLength);
        os.packLong(bufferLength);
        
        /*
         * Write out deltas between offsets.
         * 
         * Note: this is 60% of the cost of this method. This is not pack long
         * so much as doing individual byte put operations on the output stream
         * (which is over a ByteBuffer).  Just using a BAOS here doubles the 
         * index segment build throughput.
         */
        {
//            ByteArrayOutputStream baos = new ByteArrayOutputStream(nkeys*8);
//            DataOutputStream dbaos = new DataOutputStream(baos);

            int lastOffset = 0;
        
        for( int i=0; i<nkeys; i++) {
            
            int offset = offsets[i];
            
            int delta = offset - lastOffset;
            
//            LongPacker.packLong(dbaos, delta);
            os.packLong(delta);
            
            lastOffset = offset;
            
        }
        
//        dbaos.flush();
//        
//        os.write(baos.toByteArray());
        }
        
        /*
         * write out the prefix followed by the remainder of each key in
         * turn.
         */

        if (nkeys > 0) {

            os.write(keys.keys[0], 0, prefixLength);

            for (int i = 0; i < nkeys; i++) {

                int remainder = keys.keys[i].length - prefixLength;

                os.write(keys.keys[i], prefixLength, remainder);

            }
            
        }

    }

}