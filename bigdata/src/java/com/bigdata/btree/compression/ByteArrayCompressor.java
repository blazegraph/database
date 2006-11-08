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

import java.io.IOException;

/**
 * Interface that provides for compressing of groups of byte arrays.
 * Implementors could use leading common value or prior leading common 
 * value strategies for compressing the results.
 * 
 * Note that implementors of this interface will probably need to store
 * state from one call to the next, so it is NOT safe to use a single instance
 * of these objects for multiple purposes without calling reset().
 * 
 * 
 * 
 * @author kevin
 *
 */

public interface ByteArrayCompressor {

	/**
	 * Adds the specified input bytes to the compressor.  There is no guarantee that
	 * all (or any) data will be written to the output (some compressors will need to
	 * cache data over multiple groups in order to perform effective compression).
	 * 
	 * If null is passed in for @param in, the compressor must store sufficient information
	 * for the decompressor to return null when decompressing that group.
	 * 
	 * @param out
	 * @param in
	 * @throws IOException
	 */
	void compressNextGroup(byte[] in) throws IOException;
	
	/**
	 * Causes the compressor to finish writing it's data to the output.  After this call, all
	 * data will be written to the output, and the compressor can be reused by calling reset().
	 * @param out
	 */
	void finishCompression();
	
	
}
