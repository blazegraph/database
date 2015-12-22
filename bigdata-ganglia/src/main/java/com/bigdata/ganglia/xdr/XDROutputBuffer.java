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
 * A class for writing XDR data onto an internal buffer.
 * 
 * @see <a href="http://www.ietf.org/rfc/rfc4506.txt">RDF 4506</a>
 */
public class XDROutputBuffer {

	/** The backing buffer. */
	private final byte[] buffer;
	
	/** The offset of the next byte to be written. */
	private int offset;

	/**
	 * 
	 * @param BUFFER_SIZE The size of the fixed capacity buffer.
	 */
	public XDROutputBuffer(final int BUFFER_SIZE) {
		
		if (BUFFER_SIZE <= 0)
			throw new IllegalArgumentException();
		
		buffer = new byte[BUFFER_SIZE];
		
	}

	/**
	 * Reset the buffer (clears the offset of the next byte to be written to
	 * zero).
	 */
	public void reset() {
		
		offset = 0;
		
	}

	/**
	 * Return the backing byte[] buffer.
	 */
	public byte[] getBuffer() {
		
		return buffer;
		
	}
	
	/** Return the #of bytes written onto the buffer. */
	public int getLength() {
		
		return offset;
		
	}
	
	/**
	 * Puts a string into the buffer by first writing the size of the string as
	 * an int, followed by the bytes of the string, padded if necessary to a
	 * multiple of 4.
	 */
	public void writeString(final String s) {
		final byte[] bytes = s.getBytes();
		final int len = bytes.length;
		writeInt(len);
		System.arraycopy(bytes, 0, buffer, offset, len);
		offset += len;
		pad();
	}

	/**
	 * Pads the buffer with zero bytes up to the nearest multiple of 4 (all XDR
	 * fields must be padded out to the nearest multiple of four bytes if they
	 * do not fall on a 4 byte boundary).
	 * 
	 * @see http://www.ietf.org/rfc/rfc4506.txt
	 */
	private void pad() {
		final int newOffset = ((offset + 3) / 4) * 4;
		while (offset < newOffset) {
			buffer[offset++] = 0;
		}
	}

//	/**
//	 * Puts a byte into the buffer.
//	 */
//	public void writeByte(final byte i) {
//		buffer[offset++] = (byte) (i & 0xff);
//		pad();
//	}

	/**
	 * Puts a short integer into the buffer as 2 bytes, big-endian but w/
	 * leading zeros (e.g., as if an int32 value). This is based on looking at
	 * ganglia data as received on the wire. For example, <code>cpu_num</code>
	 * is reported as ushort.
	 */
	public void writeShort(final short i) {
		buffer[offset++] = (byte) 0;
		buffer[offset++] = (byte) 0;
		buffer[offset++] = (byte) ((i >> 8) & 0xff);
		buffer[offset++] = (byte) (i & 0xff);
//		pad();
	}

	/**
	 * Puts an integer into the buffer as 4 bytes, big-endian.
	 */
	public void writeInt(final int i) {
		buffer[offset++] = (byte) ((i >> 24) & 0xff);
		buffer[offset++] = (byte) ((i >> 16) & 0xff);
		buffer[offset++] = (byte) ((i >> 8) & 0xff);
		buffer[offset++] = (byte) (i & 0xff);
	}

	/**
	 * Puts a long into the buffer as 8 bytes, big-endian.
	 */
	public void writeLong(final long i) {
		buffer[offset++] = (byte) ((i >> 56) & 0xff);
		buffer[offset++] = (byte) ((i >> 48) & 0xff);
		buffer[offset++] = (byte) ((i >> 40) & 0xff);
		buffer[offset++] = (byte) ((i >> 32) & 0xff);
		buffer[offset++] = (byte) ((i >> 24) & 0xff);
		buffer[offset++] = (byte) ((i >> 16) & 0xff);
		buffer[offset++] = (byte) ((i >> 8) & 0xff);
		buffer[offset++] = (byte) (i & 0xff);
	}
	/**
	 * Puts a float into the buffer as 4 bytes, big-endian.
	 */
	public void writeFloat(final float f) {
		writeInt(Float.floatToIntBits(f));
	}

	/**
	 * Puts a double into the buffer as 8 bytes, big-endian.
	 */
	public void writeDouble(final double d) {
		writeLong(Double.doubleToLongBits(d));
	}

}
