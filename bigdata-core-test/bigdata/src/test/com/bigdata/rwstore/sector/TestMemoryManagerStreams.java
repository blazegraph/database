package com.bigdata.rwstore.sector;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import com.bigdata.io.DirectBufferPool;
import com.bigdata.rawstore.IPSOutputStream;
import com.bigdata.rwstore.PSInputStream;
import com.bigdata.rwstore.PSOutputStream;

import junit.framework.TestCase;

public class TestMemoryManagerStreams extends TestCase {

	final int sectorSize = 10 * 1024 * 1024; // 10M
	
	private MemoryManager manager;
	
	private String c_testData;
	
	private Random r;

	protected void setUp() throws Exception {
		r = new Random();
		manager = new MemoryManager(DirectBufferPool.INSTANCE, 10);
		c_testData = genTestData();
	}

	protected void tearDown() throws Exception {
		manager.clear();
		r = null;
		manager = null;
		c_testData = null;
		super.tearDown();
	}
	
	private String genTestData() {
		String src = "The quick brown fox jumped over the lazy dog";
		
		StringBuffer buf = new StringBuffer();
		while (buf.length() < (20 * 1024))
			buf.append(src);
		
		return buf.toString();
	}
	
	public void testSimpleAllocations() {
		
		String helloWorld = "Hello World";
		
		final long saddr = allocate(manager, helloWorld);
		
		String retstr = getString(saddr);
		
		assertTrue(helloWorld.equals(retstr));
		
		// confirm that the stream address can be freed
		manager.free(saddr);
		
		assert manager.getSlotBytes() == 0;
	}

	private long allocate(final IMemoryManager mm, String val) {
		
		final ByteBuffer bb = ByteBuffer.wrap(val.getBytes());
		
		return mm.allocate(bb, false/* blocks */);
		
	}

	
	private String getString(final long saddr) {
		
		final StringBuffer sb = new StringBuffer();
		
		final ByteBuffer[] bufs = manager.get(saddr);
		
		for (int i = 0; i < bufs.length; i++) {
			final byte[] data;
			if (bufs[i].isDirect()) {
				final ByteBuffer indbuf = ByteBuffer.allocate(bufs[i].remaining());
				data = indbuf.array();
				indbuf.put(bufs[i]);
				indbuf.flip();
			} else {
				data = bufs[i].array();
			}
			
			sb.append(new String(data));
		}
		
		return sb.toString();
	}

	public void testSimpleStreams() throws IOException, ClassNotFoundException {
		IPSOutputStream out = manager.getOutputStream();
		
		ObjectOutputStream outdat = new ObjectOutputStream(out);
		
		final String hw = "Hello World";
		
		outdat.writeObject(hw);
		outdat.flush();
		
		long addr = out.getAddr();
		
		InputStream instr = manager.getInputStream(addr);
		
		ObjectInputStream inobj = new ObjectInputStream(instr);
		final String tst = (String) inobj.readObject();
		
		assertTrue(hw.equals(tst));
		
		// confirm that the stream address can be freed
		manager.free(addr);
		
		assert manager.getSlotBytes() == 0;
	}

	public void testBlobStreams() throws IOException, ClassNotFoundException {
		IPSOutputStream out = manager.getOutputStream();
		
		ObjectOutputStream outdat = new ObjectOutputStream(out);
		final String blobBit = "A bit of a blob...";
		
		for (int i = 0; i < 40000; i++)
			outdat.writeObject(blobBit);
		outdat.close();
		
		long addr = out.getAddr(); // save and retrieve the address
		
		InputStream instr = manager.getInputStream(addr);
		
		ObjectInputStream inobj = new ObjectInputStream(instr);
		for (int i = 0; i < 40000; i++) {
			try {
				final String tst = (String) inobj.readObject();
			
				assertTrue(blobBit.equals(tst));
			} catch (IOException ioe) {
				System.err.println("Problem at " + i);
				throw ioe;
			}
		}
		
		try {
			inobj.readObject();
			fail("Expected EOFException");
		} catch (EOFException eof) {
			// expected
		} catch (Exception ue) {
			fail("Expected EOFException not " + ue.getMessage());
		}
		
		// confirm that the stream address can be freed
		manager.free(addr);
		
		assert manager.getSlotBytes() == 0;
	}

	public void testZipStreams() throws IOException, ClassNotFoundException {
		IPSOutputStream out = manager.getOutputStream();
		ObjectOutputStream outdat = new ObjectOutputStream(new GZIPOutputStream(out));
		final String blobBit = "A bit of a blob...";
		
		for (int i = 0; i < 40000; i++)
			outdat.writeObject(blobBit);
		outdat.close();
		
		long addr = out.getAddr(); // save and retrieve the address
			
		InputStream instr = manager.getInputStream(addr);
		
		ObjectInputStream inobj = new ObjectInputStream(new GZIPInputStream(instr));
		for (int i = 0; i < 40000; i++) {
			try {
				final String tst = (String) inobj.readObject();
			
				assertTrue(blobBit.equals(tst));
			} catch (IOException ioe) {
				System.err.println("Problem at " + i);
				throw ioe;
			}
		}
		
		manager.free(addr);
		
		assert manager.getSlotBytes() == 0;
	}

}
