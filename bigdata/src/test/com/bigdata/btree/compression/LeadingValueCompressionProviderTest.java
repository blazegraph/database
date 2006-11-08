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
 * Copyright 2001 (C) Alex Boisvert. All Rights Reserved.
 * Contributions are Copyright (C) 2001 by their associated contributors.
 *
 * $Id$
 */
package com.bigdata.btree.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import com.bigdata.btree.compression.ByteArrayCompressor;
import com.bigdata.btree.compression.ByteArrayDecompressor;
import com.bigdata.btree.compression.LeadingValueCompressionProvider;


import junit.framework.TestCase;

public class LeadingValueCompressionProviderTest extends TestCase {

	LeadingValueCompressionProvider provider;
	protected void setUp() throws Exception {
		provider = new LeadingValueCompressionProvider(0);
	}

	private void doCompressUncompressTestFor(byte[][] groups) throws IOException{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);

		ByteArrayCompressor compressor = provider.getCompressor(dos);
		
		for (int i = 0; i < groups.length; i++) {
			compressor.compressNextGroup(groups[i]);
		}
		compressor.finishCompression();
		
		byte[] results = baos.toByteArray();
		
		ByteArrayInputStream bais = new ByteArrayInputStream(results);
		DataInputStream dis = new DataInputStream(bais);
		ByteArrayDecompressor decompressor = provider.getDecompressor(dis);
		
		for(int i = 0;i < groups.length; i++){
			byte[] data_out = decompressor.decompressNextGroup();
			assertTrue(Arrays.equals(groups[i], data_out));
		}
		
	}
	
	private byte[][] getIncrementingGroups(int groupCount, long seed, int lenInit, int comInit, int lenIncr, int comIncr){
		ByteArraySource bap = new ByteArraySource(seed);
		byte[][] groups = new byte[groupCount][];
		for(int i = 0; i < groupCount; i++){
			groups[i] = bap.getBytesWithCommonPrefix(lenInit, comInit);
			lenInit += lenIncr;
			comInit += comIncr;
		}
		return groups;
	}
	
	public void testCompDecompEqualLenEqualCommon() throws IOException{
		byte[][] groups = getIncrementingGroups(
				5, // number of groups 
				1000, // seed
				50, // starting byte array length
				5, // starting common bytes
				0, // length increment
				0 // common bytes increment
				);
		
		doCompressUncompressTestFor(groups);
	}
	
	public void testCompDecompEqualLenIncrCommon() throws IOException{
		byte[][] groups = getIncrementingGroups(
				5, // number of groups 
				1000, // seed
				50, // starting byte array length
				5, // starting common bytes
				0, // length increment
				2 // common bytes increment
				);
		
		doCompressUncompressTestFor(groups);
	}
	
	public void testCompDecompEqualLenDecrCommon() throws IOException{
		byte[][] groups = getIncrementingGroups(
				5, // number of groups 
				1000, // seed
				50, // starting byte array length
				40, // starting common bytes
				0, // length increment
				-2 // common bytes increment
				);
		
		doCompressUncompressTestFor(groups);
	}

	public void testCompDecompIncrLenEqualCommon() throws IOException{
		byte[][] groups = getIncrementingGroups(
				5, // number of groups 
				1000, // seed
				30, // starting byte array length
				25, // starting common bytes
				1, // length increment
				0 // common bytes increment
				);
		
		doCompressUncompressTestFor(groups);
	}
	
	public void testCompDecompDecrLenEqualCommon() throws IOException{
		byte[][] groups = getIncrementingGroups(
				5, // number of groups 
				1000, // seed
				50, // starting byte array length
				25, // starting common bytes
				-1, // length increment
				0 // common bytes increment
				);
		
		doCompressUncompressTestFor(groups);
	}
	
	public void testCompDecompNoCommon() throws IOException{
		byte[][] groups = getIncrementingGroups(
				5, // number of groups 
				1000, // seed
				50, // starting byte array length
				0, // starting common bytes
				-1, // length increment
				0 // common bytes increment
				);
		
		doCompressUncompressTestFor(groups);
	}

	public void testCompDecompNullGroups() throws IOException{
		byte[][] groups = getIncrementingGroups(
				5, // number of groups 
				1000, // seed
				50, // starting byte array length
				25, // starting common bytes
				-1, // length increment
				0 // common bytes increment
				);
		
		groups[2] = null;
		groups[4] = null;
		
		doCompressUncompressTestFor(groups);
	}

}
