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
import java.io.IOException;

import org.CognitiveWeb.extser.Stateless;


/**
 * Provider for default implementation of the ByteArrayGroupCompressor and ByteArrayGroupDecompressor that just
 * store the byte array groups as is.  These class is
 * designed to be compatible with earlier (1.0) storage implementations of
 * keys in org.jdbm.btree.BPage's serialize and deserialize methods.
 * 
 * @author kevin
 *
 */

public class DefaultCompressionProvider implements CompressionProvider, Stateless {
	private static final long serialVersionUID = 1L;

	public DefaultCompressionProvider() {
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
	
	
    static byte[] readByteArray( DataInput in ) throws IOException
	{
	    int len = in.readInt();
	    if ( len < 0 ) {
	        return null;
	    }
	    byte[] buf = new byte[ len ];
	    in.readFully( buf );
	    return buf;
	}
	
	
	static private class Compressor implements ByteArrayCompressor{
		private DataOutput out;

		public void compressNextGroup(byte[] in) throws IOException {
			writeByteArray(out, in);
		}

		public void finishCompression() {
			// not needed in this implementation
		}
		
	}
	
	static private class Decompressor implements ByteArrayDecompressor{
		private DataInput in;

		public void reset(DataInput in) {
			this.in = in;
		}

		public byte[] decompressNextGroup() throws IOException {
			return readByteArray(in);
		}
		
	}
    
	static void writeByteArray( DataOutput out, byte[] buf ) throws IOException
	{
	    if ( buf == null ) {
	        out.writeInt( -1 );
	    } else {
	        out.writeInt( buf.length );
	        out.write( buf );
	    }
	}

}
