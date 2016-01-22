package com.bigdata.util;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import junit.framework.TestCase;

public class TestHeapVsDirectByteBuffer extends TestCase {
	
	public void testSimplePerformance() {
		final ByteBuffer hbb = ByteBuffer.allocate(1024 * 1024);
		final ByteBuffer dbb = ByteBuffer.allocateDirect(1024 * 1024);
		
		final IntBuffer ihbb = hbb.asIntBuffer();
		final IntBuffer idbb = dbb.asIntBuffer();
		
		final int nints = 256 * 1024;
		
		final long t1 = System.nanoTime();
		for (int i = 0; i < nints; i++) {
			ihbb.put(i, i);
		}
		for (int t = 0; t < 10000; t++) {
			for (int i = 0; i < nints; i++) {
				ihbb.get(i);
			}
		}
		final long t2 = System.nanoTime();
		for (int i = 0; i < nints; i++) {
			idbb.put(i, i);
		}
		for (int t = 0; t < 10000; t++) {
			for (int i = 0; i < nints; i++) {
				idbb.get(i);
			}
		}
		final long t3 = System.nanoTime();
		
		System.out.println("  Heap Buffer: " + (t2 - t1) + "ns");
		System.out.println("Direct Buffer: " + (t3 - t2) + "ns");
	}

}
