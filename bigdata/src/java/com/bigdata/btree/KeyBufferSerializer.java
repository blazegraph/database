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
package com.bigdata.btree;

import java.io.DataInput;
import java.io.IOException;

import org.CognitiveWeb.extser.LongPacker;
import org.CognitiveWeb.extser.ShortPacker;

import com.bigdata.io.DataOutputBuffer;

/**
 * Compact serialization for the key buffer.
 * 
 * @todo discard changes or get huffman compression working for the keys. note
 *       that key compression and value compression schemes could be different
 *       and that there is already a compression option for an entire leaf/node.
 * 
 * @todo try to get this running with DataInputBuffer as the declared input type
 *       so that we can have direct access to the byte[] and avoid lots of
 *       copying.
 * 
 * @todo this uses the {@link LongPacker} but many of the serialized values
 *       could be restricted to the range of a signed short so there may be an
 *       efficiency possible there.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class KeyBufferSerializer implements IKeySerializer {

    /**
     * 
     */
    private static final long serialVersionUID = 7361581167520945586L;

    /**
     * Original serialization.
     */
    public static final transient short VERSION0 = 0x0;
    
//    /**
//     * Serialization variant where the data after the version identifier are
//     * compressed using a {@link Deflater} (ZLIB).
//     */
//    public static final transient short VERSION1 = 0x1;
    
    public static final transient IKeySerializer INSTANCE = new KeyBufferSerializer();

//    /**
//     * Encoder provides just the huffman encoding aspect of ZLIB, but writes 
//     * data that are compatible with ZLIB decompression.
//     */
//    private HuffmanEncoder encoder = new HuffmanEncoder();
//    
//    /**
//     * Decoder just applies {@link Inflater} to decompress ZLIB format data.
//     */
//    private HuffmanDecoder decoder = new HuffmanDecoder();

    /**
     * The minimum aggregate length of the serialized keys before compression
     * will be contemplated. Short runs are not worth Huffman encoding since the
     * dictionary can cause the net result to be larger. Therefore a minimum
     * threshold on the the total length of the keys is appropriate for
     * comtemplating compression.
     */
    public final int minKeyLengthToCompress = 1024;
    
    /**
     * The minimum compression ratio that will result in the compressed form
     * being serialized. Since decompression requires CPU cycles, we require
     * compression to have some savings over the uncompressed form in order to
     * justify those additional CPU cycles. When this threshold is not reached
     * the uncompressed form is written out instead.
     */
    public final float minCompressionRatio = .9f;
    
    public static class Counters {
        
        /**
         * #of records serialized.
         */
        public int nserialized = 0;
        
        /**
         * #of records deserialized.
         */
        public int ndeserialized = 0;
        
        /**
         * #of records serialized where the record was compressed.
         */
        public int ncompressed = 0;
        
        /**
         * #of records deserialized where the record was decompressed.
         */
        public int ndecompressed = 0;
        
        /**
         * #of bytes written _before_ compression (this only counts the
         * serialized key buffer bytes and not the record header).
         */
        public long bytesWritten= 0L;
        
        /**
         * #of bytes compressed.
         */
        public long bytesCompressed = 0L;
        
        /**
         * #of bytes read whether or not compressed (this only counts the
         * serialized key buffer bytes and not the record header).
         */
        public long bytesRead= 0L;
        
        /**
         * #of bytes decompressed.
         */
        public long bytesDecompressed = 0L;

        /**
         * compute the compression ratio (compressed to uncompressed).
         */
        public float compressionRatio() {
            
            if(bytesWritten == 0L) return 0L;
            
            float ratio = ((float) bytesCompressed) / bytesWritten;
            
            return ratio;
            
        }
        
        public String toString() {
            
            return "serialized::" +

            "read( #read=" + ndeserialized + ", #decompressed=" + ndecompressed
                    + ", #bytesRead=" + bytesRead + ", #bytesDecompressed="
                    + bytesDecompressed + "), "

                    + "written( #written=" + nserialized + ", #compressed="
                    + ncompressed + ", #bytesWritten=" + bytesWritten
                    + ", #bytesCompressed=" + bytesCompressed + "), " +

                    "compressionRatio=" + compressionRatio();
            
        }
        
    }
    
    public final Counters counters = new Counters();
    
    /**
     * Buffer is reused to minimize memory allocation.
     */
    private DataOutputBuffer temp_baos = new DataOutputBuffer();

    public IKeyBuffer getKeys(DataInput is) throws IOException {

        counters.ndeserialized++;
        
        final short version = ShortPacker.unpackShort(is);
        
//        if (version != VERSION0 && version != VERSION1) {
        if (version != VERSION0) {

            throw new IOException("Unknown version=" + version);
            
        }

//        if(version == VERSION1) {
//
//            /*
//             * Decompress the record before deserializing it.
//             */
//            
//            counters.ndecompressed++;
//            
//            int nbytes = (int)LongPacker.unpackLong(is);
//            
//            counters.bytesDecompressed += nbytes;
//            
//            temp_baos.reset();
//            
//            temp_baos.write(is,nbytes);
//            
//            byte[] uncompressed = decoder.decompress(temp_baos.buf, 0,
//                    temp_baos.len);
//            
//            is = new DataInputBuffer(uncompressed);
//            
//        }
        
        /*
         * Deserialize the record.
         */
        
        // #of keys in the node or leaf.
        final int nkeys = (int) LongPacker.unpackLong(is);
        
        // maximum #of keys allowed in the node or leaf.
        final int maxKeys = (int) LongPacker.unpackLong(is);
        
        // length of the byte[] containing the prefix and the remainder for each key.
        final int bufferLength = (int) LongPacker.unpackLong(is);
        
        /*
         * Read in deltas for each key and re-create the offsets.
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

        // Note: does not track the offset/delta[]s.
        counters.bytesRead += buf.length;

        return new ImmutableKeyBuffer(nkeys, maxKeys, offsets, buf);
        
    }

    public void putKeys(DataOutputBuffer os, IKeyBuffer keys)
            throws IOException {
        
        /*
         * FIXME Compression is never producing a smaller representation. Look
         * into this further. Are the keys or the serialization format making
         * compression impossible or is there a problem with how I am invoking
         * the compression library???
         * 
         * Note: This was originally intended as an optimization -- If the #of
         * keys is very small then we never attempt to compress the record. This
         * reduces copying of data into the temp_baos and from there onto the
         * [os] in cases where compression is highly unlikely to succeed.
         */
        if(true) { //keys.getKeyCount()<16) {
            
            counters.nserialized++;

            // offset into buffer before writing the data.
            final int pos = os.position();
            
            os.packShort(VERSION0);
            
            if (keys instanceof ImmutableKeyBuffer) {

                // putKeys2(os,(ImmutableKeyBuffer)keys);
                putKeys2(os, (ImmutableKeyBuffer) keys);

            } else {

                // putKeys2(os,(MutableKeyBuffer)keys);
                putKeys2(os, (MutableKeyBuffer) keys);

            }

            // #of bytes written.
            counters.bytesWritten += (os.position() - pos);
            
            return;
            
        }

        /*
         * Serialize the key buffer.
         */
        
        counters.nserialized++;
        
        temp_baos.reset();

        if (keys instanceof ImmutableKeyBuffer) {

            // putKeys2(os,(ImmutableKeyBuffer)keys);
            putKeys2(temp_baos, (ImmutableKeyBuffer) keys);

        } else {

            // putKeys2(os,(MutableKeyBuffer)keys);
            putKeys2(temp_baos, (MutableKeyBuffer) keys);
            
        }

        counters.bytesWritten += temp_baos.position();
        
//        /*
//         * If the key buffer is long enough, then apply compression.
//         */
//        if (temp_baos.len >= minKeyLengthToCompress) {
//
//            /*
//             * Compress the serialized keys.
//             */
//            final byte[] compressed = encoder.compress(temp_baos.buf, 0, temp_baos.len);
//
//            // compute the compression ratio (compressed to uncompressed).
//            float ratio = ((float) compressed.length) / temp_baos.len;
//            
//            /* If the compression ratio is decent, then write as compressed.
//             * 
//             * Note: it is not worth a minor space savings to have the additional
//             * effort of decompressing the data, so unless the compression ratio
//             * is decent we will write out the uncompressed form of the record. 
//             */
//            if (ratio < minCompressionRatio) {
//                
//                /*
//                 * write compressed version.
//                 */
//                
//                counters.ncompressed++;
//                
//                counters.bytesCompressed += compressed.length;
//                
//                os.packShort(VERSION1);
//
//                // #of compressed bytes to follow.
//                os.packLong(compressed.length);
//
//                // the compressed data.
//                os.write(compressed);
//             
//                return;
//                
//            }
//
//        }

        /*
         * Write the uncompressed form of the serialized keys.
         */

        os.packShort(VERSION0);
        
        os.write(temp_baos.array(),0,temp_baos.position());

    }
    
    /**
     * Serialize an {@link ImmutableKeyBuffer}.
     * 
     * @param os
     * @param keys
     * @throws IOException
     */
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
