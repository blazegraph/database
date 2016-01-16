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
 * Created on May 25, 2011
 */

package com.bigdata.io.compression;

import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.charset.Charset;

import org.apache.log4j.Logger;

import com.bigdata.io.ByteCountInputStream;
import com.bigdata.io.ByteCountOutputStream;

/**
 * BOCU-1 version.
 * 
 * @see http://userguide.icu-project.org/conversion/compression
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BOCU1Compressor implements IUnicodeCompressor, Serializable {

    private static final long serialVersionUID = 1L;

    private static final transient Logger log = Logger
            .getLogger(BOCU1Compressor.class);

    // @todo factor out lookup? (static)
    private final transient Charset cs = Charset.forName("BOCU-1");

//    public void encode(final CharSequence s, final IManagedByteArray out) {
//
//        // Wrap caller's buffer as OutputStream
//        final ManagedByteArrayOutputStream mbaos = new ManagedByteArrayOutputStream(
//                out);
//        
//        encode(s, mbaos);
//        
//    }

    public int encode(final CharSequence s, final OutputStream os) {

        final ByteCountOutputStream bcos = new ByteCountOutputStream(os); 
        
        // Wrap with Writer using encoder
        final OutputStreamWriter w = new OutputStreamWriter(bcos, cs);

        try {

            if (s instanceof MutableString) {
                // Efficient: tunnels to the backing char[].
                final MutableString t = (MutableString) s;
                w.write(t.array(), 0, t.length());
            } else if (s instanceof String) {
                w.write((String) s);
            } else {
                // TODO optimize for CharBuffer, StringBuilder
                w.write(s.toString());
            }

            w.flush();

            w.close();
            
            return bcos.getNWritten();

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

//    public void decode(final IByteArraySlice in, final Appendable sb) {
//
//        final DataInputBuffer dib = new DataInputBuffer(in.array(), in.off(),
//                in.len());
//
//        decode(dib, sb);
//
//    }

    public int decode(final InputStream in, final Appendable sb) {

        final ByteCountInputStream bcis = new ByteCountInputStream(in);
        
        // Wrap with decoder
        final InputStreamReader r = new InputStreamReader(bcis, cs);

        try {

            // decode
            int ch;
            while ((ch = r.read()) != -1) {

                sb.append((char) ch);

            }

            return bcis.getNRead();
            
        } catch (IOException ex) {

            throw new RuntimeException(ex);

        } finally {

            try {
                r.close();
            } catch (IOException e) {
                log.error(e, e);
            }

        }

    }

}
