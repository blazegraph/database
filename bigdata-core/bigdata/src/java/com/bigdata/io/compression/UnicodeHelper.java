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
 * Created on May 26, 2011
 */

package com.bigdata.io.compression;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.io.LongPacker;
import com.bigdata.io.SliceInputStream;

/**
 * Utility class for compressed unicode encode/decode.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class UnicodeHelper {

    /**
     * Used to compress Unicode strings.
     */
    private final IUnicodeCompressor uc;

    public UnicodeHelper(final IUnicodeCompressor uc) {
        
        if(uc == null)
            throw new IllegalArgumentException();

        this.uc = uc;
        
    }
    
    /**
     * Encode the {@link String} onto the {@link OutputStream}. The temporary
     * buffer is used to perform the encoding. The byte length of the encoding
     * is written out using a packed long integer format followed by the coded
     * bytes. This method is capable of writing out very long strings.
     * 
     * @param s
     *            The character data.
     * @param out
     *            The output stream.
     * @param tmp
     *            The temporary buffer.
     * @throws IOException
     */
    public int encode(final String s, final OutputStream out,
            final ByteArrayBuffer tmp) throws IOException {
     
        // reset the temporary buffer
        tmp.reset();
        
        // encode the data onto the temporary buffer.
        int nencoded = uc.encode(s, tmp);
        
        // the #of bytes written onto the temporary buffer.
        final int len = tmp.pos();
        
        // write out the packed byte length of the temporary buffer.
        nencoded += LongPacker.packLong(out, len);

        // write out the data in the temporary buffer.
        out.write(tmp.array(), 0/* off */, len);
        
        return nencoded;
        
    }

    /**
     * Encode a Unicode string.
     * 
     * @param s
     *            The string.
     *            
     * @return The encoded byte[].
     */
    public byte[] encode1(final String s) {

        final ByteArrayBuffer tmp = new ByteArrayBuffer(s.length());
        
        final int nwritten = uc.encode(s, tmp);
        
        final int npack = LongPacker.getByteLength(nwritten);
        
        final byte[] a = new byte[npack+nwritten];

        final DataOutputBuffer dob = new DataOutputBuffer(0/* len */, a/* buf */);
        try {
            // write the length of the (prefix+compressed) data.
            dob.packLong(nwritten);

            // copy the compressed data.
            dob.append(tmp.array(), 0/* off */, tmp.pos());

            // return the array, including the encode length prefix.
            return a;
        } finally {
            try {
                dob.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        
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
    public int decode(final InputStream in, final StringBuilder out)
            throws IOException {

        // read in the byte length of the encoded character data.
        final long n = LongPacker.unpackLong(in);

        if (n > Integer.MAX_VALUE) {
            // Java does not support strings longer than int32 characters.
            throw new IOException();
        }
        
        // The byte length of the backed long value.
        int ndecoded = LongPacker.getByteLength(n);

        // ensure sufficient capacity for (at least) that many chars.
        out.ensureCapacity((int) n + out.length());

        // decode into the temporary buffer.
        ndecoded += uc.decode(new SliceInputStream(in, (int) n), out);

        return ndecoded;
        
    }

    /**
     * Decode a {@link String} from the input stream.
     * 
     * @param in
     *            The input stream.
     * @param tmp
     *            The temporary buffer used to decode the data. The buffer will
     *            be reset automatically by this method.
     * @return The decoded data.
     * 
     * @throws IOException
     */
    public String decode1(final DataInputBuffer in, final StringBuilder tmp)
            throws IOException {

        // reset the temporary buffer
        tmp.setLength(0);

        // decode into the temporary buffer.
        decode(in, tmp);

        // extract and return the decoded String.
        return tmp.toString();

    }

}
