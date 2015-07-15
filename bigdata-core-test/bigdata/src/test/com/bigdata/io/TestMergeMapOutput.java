package com.bigdata.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import junit.framework.TestCase2;

import org.junit.Test;

import com.bigdata.io.MergeStreamWithSnapshotData;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.AbstractJournal.ISnapshotData;

/**
 * Tests the utility to merge an input stream with a sorted set of <Long, ByteBuffer>
 * @author Martyn Cutcher
 *
 */
public class TestMergeMapOutput extends TestCase2 {

	/*
	 * Test populates a buffer with N1 characters of 'X', and merges N2 characters of 'Y'.
	 * 
	 * Then the output buffer is checked for total number of 'X' and 'Y'
	 */
	@Test
	public void testMerge() throws IOException {
		final Random r = new Random();
		
		final byte[] src = new byte[2048 * 1024]; // 2M
		for (int i = 0; i < src.length; i++) {
			src[i] = 'X';
		}
		
		final ISnapshotData tm = new AbstractJournal.SnapshotData();
		long pos = r.nextInt(4096);
		
		int ychars = 0;
		while (pos < src.length) {
			final int blen = r.nextInt(2048);
			final byte[] buf = new byte[(int) ((pos + blen) < src.length ? blen : (src.length - pos))];
			for (int i = 0; i < buf.length; i++) {
				buf[i] = 'Y';
			}
			tm.put(pos, buf);
			
			ychars += buf.length;
			pos += buf.length;
			
			pos += r.nextInt(4096);
		}
		
		final ByteArrayOutputStream out = new ByteArrayOutputStream();
				
		MergeStreamWithSnapshotData.process(new ByteArrayInputStream(src), tm, out);
		
		final byte[] outbuf = out.toByteArray();
		
		int outYcount = 0;
		for (int i = 0; i < outbuf.length; i++) {
			if (outbuf[i] == 'Y')
				outYcount++;
		}
		
		log.info("src.length: " + src.length + ", outbuf.length: " + outbuf.length);
		
		assertTrue(outbuf.length == src.length);
		
		assertTrue(ychars == outYcount);
	}

}
