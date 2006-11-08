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

package com.bigdata.btree;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import jdbm.helper.compression.ByteArraySource;
import jdbm.helper.compression.DefaultCompressionProvider;
import jdbm.helper.compression.LeadingValueCompressionProvider;
import junit.framework.TestCase;

import org.CognitiveWeb.extser.Stateless;

import com.bigdata.journal.ProxyTestCase;

public class BTreeKeyCompressionTest extends ProxyTestCase {
	RecordManager recman;
	
	private void reopenRecordManager() throws IOException{
		if (recman != null) recman.close();
		Properties props = new Properties();
		props.setProperty("jdbm.disableTransactions", "true");
		recman = RecordManagerFactory.createRecordManager(TestRecordFile.testFileName);
	}
	
	private void closeRecordManager() throws IOException{
		if (recman != null) recman.close();
		recman = null;
	}
	
	protected void setUp() throws Exception {
		TestRecordFile.deleteTestFile();
		reopenRecordManager();
	}

	protected void tearDown() throws Exception {
		if (recman != null) recman.close();
		TestRecordFile.deleteTestFile();
	}

	private BTree getNewTree(boolean keyCompression) throws IOException{
		BTree tree = BTree.createInstance(recman,  new ByteArrayComparator(), new ByteArraySerializer(), new ByteArraySerializer());
		if (keyCompression)
			tree.setKeyCompressionProvider(new LeadingValueCompressionProvider());
		
		return tree;
	}
	
	private BTree getNewFileSystemBTree(boolean keyCompression) throws IOException{
		BTree tree = BTree.createInstance(recman,  new StringComparator(), new StringSerializer(), new LongSerializer());
		if (keyCompression)
			tree.setKeyCompressionProvider(new LeadingValueCompressionProvider());
		
		return tree;
	}

	/*
	 * Test method for 'com.bigdata.journal.ndx.BTree.setKeyCompressionProvider(ByteArrayGroupCompressionProvider)'
	 */
	public void testSetKeyCompressionProvider() throws Exception {
		BTree tree = getNewTree(false);
		
		tree.setKeyCompressionProvider(new LeadingValueCompressionProvider());
		assertTrue(true);
		tree.insert(new byte[]{0, 1, 2, 3}, new byte[]{0, 1, 2, 3}, true);
		try{
			tree.setKeyCompressionProvider(new DefaultCompressionProvider());
			assertTrue(false);
		} catch (IllegalArgumentException e){}
		
	}

	public void testSimpleInsertion() throws Exception{
		// we are going to insert some keys with common leading bytes
		// then close the recman
		// then reload the tree and make sure we can find each key
		
		BTree tree = getNewTree(true);
		long recid = tree.getRecid();
		
		ByteArraySource provider = new ByteArraySource(42);
		
		int keycount = 500;
		byte[][] testKeys = new byte[keycount][];

		int keyLen = 9;
		int commonLen = 5;
		
		for (int i = 0; i < testKeys.length; i++) {
			testKeys[i] = provider.getBytesWithCommonPrefix(keyLen, commonLen);
			if (i % 7 == 0) keyLen += 3;
			if (i % 13 == 0) commonLen += 2;
			if (i % 11 == 0) keyLen -= 4;
			if (i % 15 == 0) commonLen -= 3;
			if (keyLen < 1) keyLen = (keyLen % 12) + 1;
			if (commonLen < 0 || commonLen > keyLen) commonLen = (commonLen % keyLen) + 1;
		}

		
		for (int i = 0; i < testKeys.length; i++) {
			tree.insert(testKeys[i], testKeys[i], true);	
		}
		
		recman.commit();
		
		for (int i = 0; i < testKeys.length; i++) {
			byte[] val = (byte[])tree.find(testKeys[i]);
			assertTrue(val != null);
			assertTrue(Arrays.equals(testKeys[i], val));
		}
		
		
		tree = null;
		reopenRecordManager();
		
		tree = BTree.load(recman, recid);
		for (int i = 0; i < testKeys.length; i++) {
			byte[] val = (byte[])tree.find(testKeys[i]);
			assertTrue(val != null);
			assertTrue(Arrays.equals(testKeys[i], val));
		}
	}
	
	public void testWithoutCompression() throws Exception{
		doPerformanceTest(false);
	}
	
	public void testWithCompression() throws Exception{
		doPerformanceTest(true);
	}
	
	public void testFileSystemCaptureWithoutCompression() throws IOException{
		doFileSystemCapturePerformanceTest(false);
	}

	public void testFileSystemCaptureWithCompression() throws IOException{
		doFileSystemCapturePerformanceTest(true);
	}
	
	private void doFileSystemCapturePerformanceTest(boolean compress) throws IOException{
		BTree tree = getNewFileSystemBTree(compress);
		
		long start = System.currentTimeMillis();

		File root = new File(".");
		addFilesBelow(tree, root);
		
		closeRecordManager();
		
		long stop = System.currentTimeMillis();
		
		File dbFile = new File(TestRecordFile.testFileName + ".db");
		long fileLen = dbFile.length();

		System.out.println("File System Capture - With" + (compress ? " compression" : "out compression") + " : Added " + tree._entries + " records in " + (stop - start)/1000 + " secs --> " + tree._entries * 1000 / (stop -start) + " inserts / sec  : File size is " + fileLen + " bytes" );
		
	}
	
	private void addFilesBelow(BTree tree, File root) throws IOException{
		File[] children = root.listFiles();
		if (children == null){
			// we probably don't have permissions to read the contents of this directory
			return;
		}
		
		for (int i = 0; i < children.length; i++) {
			File f = children[i];
			tree.insert(f.getCanonicalPath(), new Long(i), true);
			if (f.isDirectory())
				addFilesBelow(tree, f);
		}
	}
	
	public void doPerformanceTest(boolean compress) throws Exception{
		BTree tree = getNewTree(compress);
		
		long start = System.currentTimeMillis();
		
		ByteArraySource provider = new ByteArraySource(42);
		
		int keycount = 5000;

		int keyLen = 9;
		int commonLen = 5;
		byte[] val = {1, 2, 3, 4, 5};
		long totalKeyBytes = 0;
		long totalValBytes = 0;
		
		for (int i = 0; i < keycount; i++) {
			byte[] testKey = provider.getBytesWithCommonPrefix(keyLen, commonLen);
			tree.insert(testKey, val, true);
		
			totalKeyBytes += testKey.length;
			totalValBytes += val.length;
			
			if (i % 7 == 0) keyLen += 3;
			if (i % 13 == 0) commonLen += 2;
			if (i % 11 == 0) keyLen -= 4;
			if (i % 15 == 0) commonLen -= 3;
			if (keyLen < 1) keyLen = (keyLen % 12) + 1;
			if (keyLen > 30) keyLen = (keyLen % 12) + 1;
			if (commonLen > keyLen) commonLen = (commonLen % keyLen) + 1;
			if (commonLen < 5) commonLen = 5;
			
			if (i % 500 == 0) // comment this line out to determine the actual insertion that caused the error - it will be SLOW, though
				try{
					recman.commit();
				} catch (ArrayIndexOutOfBoundsException e){
					System.err.println("Error at commit on insertion " + i);
					throw e;
				}
		}

		closeRecordManager();
		
		long stop = System.currentTimeMillis();
		
		File dbFile = new File(TestRecordFile.testFileName + ".db");
		long fileLen = dbFile.length();
		
		System.out.println("With" + (compress ? " compression" : "out compression") + " : Added " + keycount + " records in " + (stop - start)/1000 + " secs --> " + keycount * 1000 / (stop -start) + " inserts / sec  : File size is " + fileLen + " bytes" );
		System.out.println("Total key bytes: " + totalKeyBytes + ", total val bytes: " + totalValBytes);
	}
	
	static public class StringSerializer implements Serializer, Stateless {
		private static final long serialVersionUID = 1L;

		public StringSerializer() {
			super();
		}

		public byte[] serialize(Object obj) throws IOException {
			String str = (String)obj;
			return str.getBytes();
		}

		public Object deserialize(byte[] serialized) throws IOException {
			return new String(serialized);
		}

	}
}
