/*

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

import java.io.IOException;

import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;

/**
 * The serialized record has a version# and indicates what kind of
 * compression technique was applied.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo write test case, move to com.bigdata.btree package.
 * 
 * @todo support dictinary compression.
 * 
 * @todo support hamming code compression.
 */
public class ValueBufferSerializer {

    public static final transient ValueBufferSerializer INSTANCE = new ValueBufferSerializer();
    
    private static final short VERSION0 = 0x0;
    
    private static final short COMPRESSION_NONE = 0x0;
    private static final short COMPRESSION_DICT = 0x1;
    private static final short COMPRESSION_HAMM = 0x2;
    private static final short COMPRESSION_RUNL = 0x3;
    
    /**
     * Serialize the {@link IValueBuffer}.
     * 
     * @param out
     *            Used to write the record (NOT reset by this method).
     * @param buf
     *            The data.
     *            
     * @return The serialized data.
     */
    public byte[] serialize(DataOutputBuffer out, IValueBuffer buf) {

        assert out != null;
        assert buf != null;
        
        try {
            
            final short version = VERSION0;

            out.packShort(version);

            // @todo support other compression strategies.
            final short compression = COMPRESSION_NONE;

            out.packShort(compression);

            switch (compression) {

            case COMPRESSION_NONE:
                serializeNoCompression(version, out, buf);
                break;
                
            default:
                throw new UnsupportedOperationException();
            
            }
            
        } catch(IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
        return out.toByteArray();
        
    }

    private void serializeNoCompression(short version,
            DataOutputBuffer out, IValueBuffer buf) throws IOException {
        
        final int n = buf.getValueCount();
        
        // #of values.
        out.packLong( n );
        
        // vector of value lengths.
        for(int i=0; i<n; i++) {
            
            byte[] val = buf.getValue(i);

            /*
             * Note: adds one so that we can differentiate between NULL
             * and byte[0].
             */
            out.packLong(val == null ? 0L : val.length + 1);
            
        }

        for(int i=0; i<n; i++) {
            
            byte[] val = buf.getValue(i);
        
            if(val==null) continue;
            
            out.write(val);
            
        }
        
    }

    /**
     * 
     * @param version
     * @param in
     * @return
     * @throws IOException
     * 
     * @todo Deserialization could choose a format in which the values were
     *       a byte[] and the data were only extracted as necessary rather
     *       unpacked into a byte[][] at once. This could improve
     *       performance for some cases. Since the performance depends on
     *       the use, this might be a choice that the caller could indicate.
     */
    private IValueBuffer deserializeNoCompression(short version,DataInputBuffer in) throws IOException {
        
        final int n = (int)in.unpackLong();
        
        final byte[][] vals = new byte[n][];

        for (int i = 0; i < n; i++) {

            /*
             * Note: length is encoded as len + 1 so that we can identify
             * nulls.
             */
            int len = (int) in.unpackLong();

            // allocate the byte[] for this value.
            vals[i] = len == 0 ? null : new byte[len - 1];
            
        }
        
        for( int i=0; i<n; i++) {
            
            if(vals[i] == null) continue;
            
            in.readFully(vals[i]);
            
        }
        
        return new MutableValueBuffer(n,vals);
        
    }
    
    /**
     * 
     * @param in
     * @return
     */
    public IValueBuffer deserialize(DataInputBuffer in) {

        try {
        
            final short version = in.unpackShort();
            
            if (version != VERSION0) {

                throw new RuntimeException("Unknown version: " + version);
                
            }
            
            final short compression = in.unpackShort();
        
            switch (compression) {

            case COMPRESSION_NONE:
                
                return deserializeNoCompression(version, in);
                
            default:
                throw new UnsupportedOperationException();
            
            }

        } catch(IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
    }
    
}