package com.bigdata.objndx;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.CognitiveWeb.extser.LongPacker;

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

    public static final transient int VERSION0 = 0x0;
    
    public static final transient IKeySerializer INSTANCE = new KeyBufferSerializer();
    
    public IKeyBuffer getKeys(DataInputStream is) throws IOException {

        final int version = is.readInt();
        
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

    public void putKeys(DataOutputStream os, IKeyBuffer keys) throws IOException {

        os.writeInt(VERSION0);
        
        if(keys instanceof ImmutableKeyBuffer ) {

            putKeys2(os,(ImmutableKeyBuffer)keys);

        } else {
            
            putKeys2(os,(MutableKeyBuffer)keys);
            
        }
        
    }
    
    protected void putKeys2(DataOutputStream os, ImmutableKeyBuffer keys) throws IOException {
        
        final int nkeys = keys.nkeys;

        final int bufferLength = keys.buf.length;

        // #of keys in the node or leaf.
        LongPacker.packLong(os, nkeys);

        // maximum #of keys allowed in the node or leaf.
        LongPacker.packLong(os, keys.maxKeys);

        // length of the byte[] buffer containing the prefix and remainder for each key.
        LongPacker.packLong(os, bufferLength);
        
        /*
         * Write out deltas between offsets.
         */
        int lastOffset = 0; // keys.prefixLength;
        
        for( int i=0; i<nkeys; i++) {
            
            int offset = keys.offsets[i];
            
            int delta = offset - lastOffset;
            
            LongPacker.packLong(os, delta);
            
            lastOffset = offset;
            
        }
        
        os.write(keys.buf);
        
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
    protected void putKeys2(DataOutputStream os, MutableKeyBuffer keys) throws IOException {
        
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
        LongPacker.packLong(os, nkeys);

        // maximum #of keys allowed in the node or leaf.
        LongPacker.packLong(os, keys.getMaxKeys());

        // length of the byte[] buffer containing the prefix and remainder for each key.
        LongPacker.packLong(os, bufferLength);
        
        /*
         * Write out deltas between offsets.
         */
        int lastOffset = 0;
        
        for( int i=0; i<nkeys; i++) {
            
            int offset = offsets[i];
            
            int delta = offset - lastOffset;
            
            LongPacker.packLong(os, delta);
            
            lastOffset = offset;
            
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