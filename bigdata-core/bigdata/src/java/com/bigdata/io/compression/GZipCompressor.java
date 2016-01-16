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

package com.bigdata.io.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.bigdata.io.ByteBufferInputStream;
import com.bigdata.io.ByteBufferOutputStream;

public class GZipCompressor implements IRecordCompressor {

	@Override
    public void compress(final ByteBuffer bin, final ByteBuffer out) {
        
	    compress(bin, new ByteBufferOutputStream(out));
	    
    }

	@Override
	public ByteBuffer compress(final ByteBuffer bin) {
	    
		final ByteArrayOutputStream os = new ByteArrayOutputStream();
		
		compress(bin, os);
		
		return ByteBuffer.wrap(os.toByteArray());
	}

	@Override
	public void compress(final ByteBuffer bin, final OutputStream os) {
		try {
			final GZIPOutputStream gzout = new GZIPOutputStream(os);
			final DataOutputStream dout = new DataOutputStream(gzout);
			
			// First write the length of the expanded data
			dout.writeInt(bin.limit());
			if (bin.hasArray()) {
				dout.write(bin.array());
			} else {
				final byte[] tbuf = new byte[bin.limit()];
				bin.get(tbuf);
				dout.write(tbuf);
			}
			dout.flush();
			gzout.flush();
			
			dout.close();
			gzout.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void compress(final byte[] bytes, final OutputStream os) {
		compress(bytes, 0, bytes.length, os);
	}

	@Override
    public void compress(final byte[] bytes, final int off, final int len,
            final OutputStream os) {
        try {
			final GZIPOutputStream gzout = new GZIPOutputStream(os);
			final DataOutputStream dout = new DataOutputStream(gzout);
			
			// First write the length of the expanded data
			dout.writeInt(len);
			dout.write(bytes, off, len);

			dout.flush();
			gzout.flush();
			
			dout.close();
			gzout.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public ByteBuffer decompress(final ByteBuffer bin) {
		final InputStream instr;
		if (bin.hasArray()) {
			instr = new ByteArrayInputStream(bin.array());
		} else {
			instr = new ByteBufferInputStream(bin);
		}
		
		return decompress(instr);
	}

	@Override
	public ByteBuffer decompress(final byte[] bin) {
		return decompress(new ByteArrayInputStream(bin));
	}

	public ByteBuffer decompress(final InputStream instr) {
		try {
			final GZIPInputStream gzin = new GZIPInputStream(instr);
			final DataInputStream din = new DataInputStream(gzin);
			
			final int length = din.readInt();
			final byte[] xbuf = new byte[length];
			for (int cursor = 0; cursor < length;) {
				final int rdlen = din.read(xbuf, cursor, (length - cursor));
				
				cursor += rdlen;
				
			}
			
			return ByteBuffer.wrap(xbuf);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
