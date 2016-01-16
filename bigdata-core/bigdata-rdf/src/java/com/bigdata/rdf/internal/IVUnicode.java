/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
/*
 * Created on Oct 5, 2011
 */

package com.bigdata.rdf.internal;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.io.compression.IUnicodeCompressor;
import com.bigdata.io.compression.NoCompressor;
import com.bigdata.util.Bytes;

/**
 * Utility class supporting {@link IV}s having inline Unicode data.
 * <p>
 * IVs must be able to report their correct mutual order. This means that the
 * Java {@link String} must be given the same order as the encoded Unicode
 * representation. Since we must include the #of bytes in the {@link IV}
 * representation, this means that we wind up with a length prefix followed by
 * some representation of the character data. This can not be consistent with
 * the code point ordering imposed by {@link String#compareTo(String)}.
 * Therefore, the {@link IVUnicodeComparator} is used to make the ordering over
 * the {@link String} data consistent with the encoded representation of that
 * data.
 * <p>
 * Note: This is not the only way to solve the problem. We could also have
 * generated the encoded representation from any {@link IV} having inline
 * Unicode data each time we need to compare two {@link IV}s, but that could
 * turn into a lot of overhead.
 * <p>
 * Note: This does not attempt to make the Unicode representation "tight" and is
 * not intended to handle very large Unicode strings. Large Unicode data in the
 * statement indices causes them to bloat and has a negative impact on the
 * overall system performance. The use case for inline Unicode data is when the
 * data are small enough that they are worth inserting into the statement
 * indices rather than indirecting through the TERM2ID/ID2TERM indices. Large
 * RDF Values should always be inserted into the BLOBS index which is designed
 * for that purpose.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * TODO This is directly persisting char[] data.  Is that portable?
 */
public class IVUnicode {

    /**
     * Helper instance for compression/decompression of Unicode string data.
     */
//    private static IUnicodeCompressor uc = new BOCU1Compressor();
//    private static IUnicodeCompressor uc = new SCSUCompressor();
    private static IUnicodeCompressor uc = new NoCompressor();

//    /**
//     * Helper instance for compression/decompression of Unicode string data.
//     */
//    private static UnicodeHelper un = new UnicodeHelper(uc);

    /**
     * Encode a Unicode string.
     * 
     * @param s
     *            The string.
     *            
     * @return The encoded byte[].
     */
    static public byte[] encode1(final String s) {
//        return un.encode1(s);
//        /*
//         * Inlined code from UnicodeHelper.encode1().
//         */
//        final ByteArrayBuffer tmp = new ByteArrayBuffer(s.length());
//        
//        final int nwritten = uc.encode(s, tmp);
//        
//        final int npack = LongPacker.getByteLength(nwritten);
//        
//        final byte[] a = new byte[npack+nwritten];
//
//        final DataOutputBuffer dob = new DataOutputBuffer(0/* len */, a/* buf */);
//        try {
//            // write the length of the (prefix+compressed) data.
//            dob.packLong(nwritten);
//
//            // copy the compressed data.
//            dob.append(tmp.array(), 0/* off */, tmp.pos());
//
//            // return the array, including the encode length prefix.
//            return a;
//        } finally {
//            try {
//                dob.close();
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        }
        /*
         * Dead simple encode.
         * 
         * Note: This places a 32k byte length limit on inline Unicode. That's
         * probably quite Ok as large Unicode values have an extreme negative
         * impact on the statement indices are really belong in the BLOBS index.
         */
        final int slen = s.length();
        if (slen > Short.MAX_VALUE)
            throw new UnsupportedOperationException();
        final int capacity = Bytes.SIZEOF_SHORT + (slen << 1);
        final IKeyBuilder k = new KeyBuilder(capacity);
        k.append((short) slen);
        for (int i = 0; i < slen; i++) {
            final char ch = s.charAt(i);
            final short sh = (short)ch;
            k.append(sh);
        }
        assert k.array().length == capacity;
        return k.array();
    }

    /**
     * Decode a {@link String} from the input stream. The result is appended
     * into the caller's buffer. The caller is responsible for resetting the
     * buffer as necessary.
     * 
     * @param in
     *            The input stream.
     * @param out
     *            The output buffer.
     *            
     * @throws IOException
     */
    static int decode(final InputStream in, final StringBuilder out)
            throws IOException {
        /*
         * Dead simple decode.
         * 
         * FIXME If this works, then change the API to pass in a byte[]. That
         * API will be better aligned for the IV decode use case since we always
         * have the byte[].
         */
        final DataInputStream dis = new DataInputStream(in);
        final int slen = decodeShort(dis.readShort());
        assert slen <= Short.MAX_VALUE : slen;
        assert slen >= 0 : slen;
        out.ensureCapacity(slen);
//        final char[] a = new char[slen];
        for (int i = 0; i < slen; i++) {
            final short unsignedShort = decodeShort(dis.readShort());
            final char ch = (char)unsignedShort;
            out.append(ch);
        }
//        final String s = new String(a);
//        out.append(s);
        return Bytes.SIZEOF_SHORT + (slen<<1);
        
//        return un.decode(in, out);
        /*
         * Inlined code from UnicodeHelper.decode().
         */
//        // read in the byte length of the encoded character data.
//        final long n = LongPacker.unpackLong(in);
//
//        if (n > Integer.MAX_VALUE) {
//            // Java does not support strings longer than int32 characters.
//            throw new IOException();
//        }
//        
//        // The byte length of the backed long value.
//        int ndecoded = LongPacker.getByteLength(n);
//
//        // ensure sufficient capacity for (at least) that many chars.
//        out.ensureCapacity((int) n + out.length());
//
//        // decode into the temporary buffer.
//        ndecoded += uc.decode(new SliceInputStream(in, (int) n), out);
//
//        return ndecoded;
    }

    private static short decodeShort(final short signedShort) {
        final short slen;
        { // Per XSDUnsignedShortIV
            
            int v = signedShort;
            
            if (v < 0) {
                
                v = v + 0x8000;

            } else {
                
                v = v - 0x8000;
                
            }

            slen = (short) v;//(v & 0xffff);

        }
        return slen;
    }
    
    /**
     * Return the byte length of the serialized representation of a unicode
     * string.
     * 
     * @param s
     *            The string.
     * 
     * @return Its byte length.
     */
    public static int byteLengthUnicode(final String s) {
        return Bytes.SIZEOF_INT + s.length();
        /*
         * Note: This does all the work to compress something and then returns
         * you the length. Caller's need to be smart about how and when they
         * find the byte length of an IV representing some inlined Unicode data
         * since we basically have to serialize the String to find the length.
         */
//        try {
//            return un.encode(s, new NullOutputStream(), new ByteArrayBuffer(s
//                    .length()));
//        } catch (IOException ex) {
//            throw new RuntimeException(ex);
//        }
    }

    /**
     * Class imposes the natural ordering of the encoded Unicode representation
     * for an {@link IV} having inline Unicode data on Java {@link String}s.
     * This is used by such {@link IV}s in order to impose on themselves the
     * correct natural order.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public static class IVUnicodeComparator implements Comparator<String> {

        public static final IVUnicodeComparator INSTANCE = new IVUnicodeComparator();
        
        private IVUnicodeComparator() {
            
        }
        
        @Override
        public int compare(final String o1, final String o2) {
            final int len1 = o1.length();
            final int len2 = o2.length();
            int ret = len1 < len2 ? -1 : len1 > len2 ? 1 : 0;
            if (ret == 0) {
                // Only compare strings which have the same length.
                ret = o1.compareTo(o2);
            }
            return ret;
        }
        
    }
    
}
