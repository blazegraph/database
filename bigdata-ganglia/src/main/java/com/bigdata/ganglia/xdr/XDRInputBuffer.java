/*
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.ganglia.xdr;

/**
 * A class for reading XDR data from an internal buffer.
 * 
 * @see <a href="http://www.ietf.org/rfc/rfc4506.txt">RDF 4506</a>
 */
public class XDRInputBuffer {

	/** The backing buffer. */
	private final byte[] buf;

	/** The offset of the next byte to be read. */
	private int off;
	
	/** The offset of the first byte which may not be read. */
	private final int limit;

	/**
	 * 
	 * @param buffer
	 *            The buffer containing the data to be read.
	 * @param off
	 *            The offset of the first byte with data to be read.
	 * @param len
	 *            The #of bytes with data to be read starting at that offset.
	 */
	public XDRInputBuffer(final byte[] buffer, final int off, final int len) {

		if (buffer == null)
			throw new IllegalArgumentException();

		if (off < 0)
			throw new IllegalArgumentException();
		if (len < 0)
			throw new IllegalArgumentException();
		if (off + len > buffer.length)
			throw new IllegalArgumentException();

		this.buf = buffer;
		this.off = off;
		this.limit = off + len;

	}

	public long readLong() {
        
		long v = 0;
        
        // big-endian.
        v += (0xffL & buf[off++]) << 56;
        v += (0xffL & buf[off++]) << 48;
        v += (0xffL & buf[off++]) << 40;
        v += (0xffL & buf[off++]) << 32;
        v += (0xffL & buf[off++]) << 24;
        v += (0xffL & buf[off++]) << 16;
        v += (0xffL & buf[off++]) <<  8;
        v += (0xffL & buf[off++]) <<  0;
        
		return v;

	}

	public int readInt() {
        
		int v = 0;
        
        // big-endian.
        v += (0xffL & buf[off++]) << 24;
        v += (0xffL & buf[off++]) << 16;
        v += (0xffL & buf[off++]) <<  8;
        v += (0xffL & buf[off++]) <<  0;
        
		return v;

	}

	public long readUInt() {
		
        long v = 0;
        
        // big-endian.
        v += (0xffL & buf[off++]) << 24;
        v += (0xffL & buf[off++]) << 16;
        v += (0xffL & buf[off++]) <<  8;
        v += (0xffL & buf[off++]) <<  0;

        return v;
        
	}
	
	public short readShort() {

		int v = 0;

		// big-endian.
		off+=2; // skip 2 bytes - should be zeros.
		v += (0xffL & buf[off++]) << 8;
		v += (0xffL & buf[off++]) << 0;
//		off += 2;

		return (short) v;

	}

	public int readUShort() {

		int v = 0;

		// big-endian.
		off+=2;// skip 2 bytes - should be zeros.
		v += (0xffL & buf[off++]) << 8;
		v += (0xffL & buf[off++]) << 0;
//		off += 2;
		
//		if (v < 0) {
//
//			v = v + 0x8000;
//
//		} else {
//
//			v = v - 0x8000;
//
//		}

		return v;

	}

//	public byte readByte() {
//
//		int v = 0;
//
//		// big-endian.
//		v += (0xffL & buf[off++]) << 0;
//		off += 3;
//
//		return (byte) v;
//
//	}

	public float readFloat() {

		final int v = readInt();
        
        return Float.intBitsToFloat(v);
        
	}
	
	public double readDouble() {

		final long v = readLong();
        
        return Double.longBitsToDouble(v);
        
	}

	public String readString() {
		
		final int n = readInt();
		
		final String s = new String(buf, off, n);

		off += n;
		
		pad();
		
		return s;
		
	}

	/**
	 * Skips pad bytes in the buffer up to the nearest multiple of 4 (all XDR
	 * fields must be padded out to the nearest multiple of four bytes if they
	 * do not fall on a 4 byte boundary).
	 * 
	 * @see http://www.ietf.org/rfc/rfc4506.txt
	 */
	private void pad() {
		final int newOffset = ((off + 3) / 4) * 4;
		while (off < newOffset) {
			off++;
		}
	}

}
