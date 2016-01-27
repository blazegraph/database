/*

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
 * Created on May 2, 2009
 */

package com.bigdata.io.compression;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;


/**
 * A compressor that copies bytes without compression them.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NOPRecordCompressor implements IRecordCompressor, Externalizable {
	
    protected static final Logger log = Logger.getLogger(CompressorRegistry.class);

    /**
     * 
     */
    private static final long serialVersionUID = 7525025093457384099L;
    
    public static final transient NOPRecordCompressor INSTANCE = new NOPRecordCompressor();

    /**
     * (De-)serialization ctor.
     */
    public NOPRecordCompressor() {
        
    }
    
	public void compress(ByteBuffer bin, ByteBuffer out) {
		out.put(bin);
	}

	public ByteBuffer compress(ByteBuffer bin) {
		
		if (log.isTraceEnabled())
			log.trace("NOP compression " + bin.limit());
		
		return bin;
	}

    /**
     * Writes the buffer on the output stream.
     */
    public void compress(final ByteBuffer buf, final OutputStream os) {

        if (true && buf.hasArray()) {

            final int off = buf.arrayOffset() + buf.position();

            final int len = buf.remaining();

            compress(buf.array(), off, len, os);
            
            buf.position(buf.limit());

            return;
            
        }
        
        // FIXME handle direct buffer and read-only buffer cases.
        throw new UnsupportedOperationException();
        
    }

    /**
     * Writes the bytes on the output stream.
     */
    public void compress(byte[] bytes, OutputStream os) {

        compress(bytes, os);
        
    }

    /**
     * Writes the bytes on the output stream.
     */
    public void compress(byte[] bytes, int off, int len, OutputStream os) {

        try {

            os.write(bytes, off, len);
           
        } catch (IOException e) {

            throw new RuntimeException(e);

        }

    }

    /**
     * Returns the argument.
     */
    public ByteBuffer decompress(ByteBuffer bin) {
        
        return bin;
        
    }

    /**
     * Returns the argument wrapped as a {@link ByteBuffer}.
     */
    public ByteBuffer decompress(byte[] bin) {

        return ByteBuffer.wrap(bin);

    }

    /** NOP */
    public void readExternal(ObjectInput arg0) throws IOException,
            ClassNotFoundException {
        
    }

    /** NOP */
    public void writeExternal(ObjectOutput arg0) throws IOException {
        
    }

}
