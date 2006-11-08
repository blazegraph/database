/**
 * JDBM LICENSE v1.00
 *
 * Redistribution and use of this software and associated documentation
 * ("Software"), with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * 1. Redistributions of source code must retain copyright
 *    statements and notices.  Redistributions must also contain a
 *    copy of this document.
 *
 * 2. Redistributions in binary form must reproduce the
 *    above copyright notice, this list of conditions and the
 *    following disclaimer in the documentation and/or other
 *    materials provided with the distribution.
 *
 * 3. The name "JDBM" must not be used to endorse or promote
 *    products derived from this Software without prior written
 *    permission of Cees de Groot.  For written permission,
 *    please contact cg@cdegroot.com.
 *
 * 4. Products derived from this Software may not be called "JDBM"
 *    nor may "JDBM" appear in their names without prior written
 *    permission of Cees de Groot.
 *
 * 5. Due credit should be given to the JDBM Project
 *    (http://jdbm.sourceforge.net/).
 *
 * THIS SOFTWARE IS PROVIDED BY THE JDBM PROJECT AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * CEES DE GROOT OR ANY CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Copyright 2000 (C) Cees de Groot. All Rights Reserved.
 * Contributions are Copyright (C) 2000 by their associated contributors.
 *
 */
package com.bigdata.btree.compression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.CognitiveWeb.extser.Stateless;


/**
 * Provider for implementations {@link ByteArrayCompressor}suitable for storing
 * binary data, {@link BinaryCompressionProvider}, or {@link String} data,
 * {@link StringCompressionProvider}.
 * 
 * @author kevin
 */

public class LeadingValueCompressionProvider implements CompressionProvider, Externalizable {
	private static final long serialVersionUID = 1L;
	/*final*/ private int ignoreLeadingCount;

	/**
	 * Used for binary data.
	 */
	public static class BinaryCompressionProvider
	    extends LeadingValueCompressionProvider
	    implements Stateless
	{
        private static final long serialVersionUID = 1L;

        public BinaryCompressionProvider() {
	        super();
	    }
	}
	
	/**
	 * Use for {@link String} data as serialized by {@link java.io.DataOutput#writeUTF( String s )}
	 * which writes the length of the string as an unsigned short integer before the string value.
	 */
	public static class StringCompressionProvider
	    extends LeadingValueCompressionProvider
	    implements Stateless
	{
        private static final long serialVersionUID = 1L;

        public StringCompressionProvider() {
	        super( 2 );
	    }
	}
	
	/**
	 * Use for binary data.
	 */
	public static transient final LeadingValueCompressionProvider BINARY = new BinaryCompressionProvider();

	/**
	 * Use for {@link String} data as serialized by {@link java.io.DataOutput#writeUTF( String s )}
	 * which writes the length of the string as an unsigned short integer before the string value.
	 */
	public static transient final LeadingValueCompressionProvider STRING = new StringCompressionProvider();
	
	public LeadingValueCompressionProvider() {
		this(0);
	}

	public LeadingValueCompressionProvider(int ignoreLeadingCount) {
		this.ignoreLeadingCount = ignoreLeadingCount;
	}

	public ByteArrayCompressor getCompressor(DataOutput out) {
		Compressor comp = new Compressor();
		comp.out = out;
		return comp;
	}
	public ByteArrayDecompressor getDecompressor(DataInput in) {
		Decompressor decomp = new Decompressor();
		decomp.in = in;
		return decomp;
	}
	
	
    static byte[] readByteArray( DataInput in, byte[] previous, int ignoreLeadingCount ) throws IOException
	{
	    int len = in.readInt();
	    if (len == -1)
	    	return null;
	    
	    short actualCommon = 0;
	    
	    if ( len < 0 ) {
	    	len = -len;
	    	actualCommon = in.readShort();
	    }
	    
	    byte[] buf = new byte[ len ];

	    if (previous == null){
	    	actualCommon = 0;
	    }
	    
	    
    	if (actualCommon > 0){
    		in.readFully( buf, 0, ignoreLeadingCount);
    		System.arraycopy(previous, ignoreLeadingCount, buf, ignoreLeadingCount, actualCommon - ignoreLeadingCount);
    	}
	    in.readFully( buf, actualCommon, len - actualCommon );
	    return buf;
	}
	
    /**
     * Writes the contents of buf to the DataOutput out, with special encoding if
     * there are common leading bytes in the previous group stored by this compressor.
     * We play a little trick with the array length stored parameter that makes this
     * class completely backwards compatible with standard uncompressed jdbm byte arrays:
     * Namely, if the length is negative, it indicates that the value is compressed.  If it
     * is positive, then the value is not compressed.  Because we are storing the common
     * leading length in a short (2 bytes), there must be at least 2 leading bytes in common
     * before we compress the data.
     * Using the negative length trick also allows us to store uncompressed data without
     * any overhead from the compression algorithm. 
     */
	static void writeByteArray( DataOutput out, byte[] buf, byte[] previous, int ignoreLeadingCount ) throws IOException
	{
	    if ( buf == null ) {
	        out.writeInt( -1 );
	        return;
	    }
	    
    	short actualCommon = (short)ignoreLeadingCount;

    	if (previous != null){
	    	int maxCommon = buf.length > previous.length ? previous.length : buf.length;
	    	// TODO: Consider storing common byte count as a simple byte instead of a short...  If keys are longer than 255 (and have more than 255 common bytes), then there's probably a problem with the design...
	    	if (maxCommon > Short.MAX_VALUE) maxCommon = Short.MAX_VALUE;
	    	
	    	for (; actualCommon < maxCommon; actualCommon++) {
				if (buf[actualCommon] != previous[actualCommon])
					break;
			}
    	}
	    
    	// TODO: Replace writeInt and writeShort with algorithms that use the first bit (or 2 bits) to determine the number of bytes actually used 
        if (actualCommon > 2){
        	// there are enough common bytes to justify compression
        	out.writeInt( -buf.length ); // store as a negative to indicate compression
        	out.writeShort( actualCommon );
        	out.write( buf, 0, ignoreLeadingCount);
        	out.write( buf, actualCommon, buf.length - actualCommon );
        } else {
        	out.writeInt( buf.length ); // store as a positive to indicate no compression
        	out.write( buf );
        }
	    
	}	
	
	private class Compressor implements ByteArrayCompressor{
		private DataOutput out;
		private byte[] previous = null;
		
		public void compressNextGroup(byte[] in) throws IOException {
			writeByteArray(out, in, previous, ignoreLeadingCount);
			previous = in != null ? (byte[])in.clone() : null;
		}

		public void finishCompression() {
			// not needed in this implementation
		}
		
	}
	
	private class Decompressor implements ByteArrayDecompressor{
		private DataInput in;
		private byte[] previous = null;
		
		public void reset(DataInput in) {
			this.in = in;
		}

		public byte[] decompressNextGroup() throws IOException {
			previous = readByteArray(in, previous, ignoreLeadingCount);
			return previous;
		}
		
	}

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt( ignoreLeadingCount );
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ignoreLeadingCount = in.readInt();
    }

}
