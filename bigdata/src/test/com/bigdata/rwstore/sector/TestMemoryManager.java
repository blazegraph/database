package com.bigdata.rwstore.sector;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.bigdata.rwstore.sector.MemoryManager.MemoryManagerResourceError;
import com.bigdata.util.concurrent.DaemonThreadFactory;

import junit.framework.TestCase;

public class TestMemoryManager extends TestCase {

	final int sectorSize = 10 * 1024 * 1024; // 10M
	MemoryManager manager =  null;
	
	final char[] c_testData = genTestData();
	
	char[] genTestData() {
		String src = "The quick brown fox jumped over the lazy dog";
		
		StringBuffer buf = new StringBuffer();
		while (buf.length() < (20 * 1024))
			buf.append(src);
		
		return buf.toString().toCharArray();
	}

	Random r = new Random();
	private String genString(int min, int i) {
		return new String(c_testData, 0, min + r.nextInt(i-min));
		// return new String(c_testData, 0, i - 20);
	}
	
	public void testSimpleAllocations() {
		installMemoryManager();
		
		String helloWorld = "Hello World";
		
		final long saddr = allocate(manager, helloWorld);
		
		String retstr = getString(saddr);
		
		assertTrue(helloWorld.equals(retstr));
		
		System.out.println(helloWorld + " allocated address: " + saddr + " returned: " + retstr);
	}
	
	private void installMemoryManager() {
		manager =  new MemoryManager(5 * sectorSize, sectorSize);
	}

	/**
	 * The address mappings are between the integer allocation address and
	 * the sector index (16 signed bits) and offset (16 unsigned bits).
	 */
	public void testAddressMappings() {
		int i = 10000000;
		while (--i > 0) {
			// final int sector = r.nextInt(12);
			final int sector = r.nextInt(32 * 1024);
			final int bit = r.nextInt(64 * 1024);
			final int rwaddr = SectorAllocator.makeAddr(sector, bit);
			final int rsector = SectorAllocator.getSectorIndex(rwaddr);
			final int rbit = SectorAllocator.getSectorOffset(rwaddr);
			assertTrue("Address Error " + i + " , sector: " + sector + " != " + rsector + " or " + bit + " != " + rbit, (sector == rsector) && (bit == rbit));
		}
	}
	
	public void testStressAllocations() {
		installMemoryManager();
		
		for (int i = 0; i < 20; i++) {
			doStressAllocations(manager, true, 80000, 5 + r.nextInt(200));	
		}
	}
	
	public void testAllocationContexts() {
		installMemoryManager();
		
		AllocationContext context = manager.createAllocationContext();
		for (int i = 0; i < 500; i++) {
			doStressAllocations(context, false, 5000, 5 + r.nextInt(3000));			
			context.clear();
		}
	}
	
	public void testStressConcurrent() throws InterruptedException {
		installMemoryManager();
		
		final int nclients = 20;
		
        final ExecutorService executorService = Executors.newFixedThreadPool(
        		nclients, DaemonThreadFactory.defaultThreadFactory());

        final Collection<Callable<Long>> tasks = new HashSet<Callable<Long>>();
        for (int i = 0; i < nclients; i++) {
        	tasks.add(new Callable<Long>() {
				public Long call() throws Exception {
					try {
						doStressAllocations(manager, false, 50000, 5 + r.nextInt(600));
					} catch (Throwable t) {
						t.printStackTrace();
					}
					return null;
				}
        	});
		}
        
        executorService.invokeAll(tasks);
        executorService.awaitTermination(5, TimeUnit.SECONDS);
	}
	
	public void doStressAllocations(final IMemoryManager mm, final boolean clear, final int tests, final int maxString) {
		if (clear)
			mm.clear();
		
		int allocs = 0;
		int frees = 0;
		ArrayList<Long> addrs = new ArrayList<Long>();
		try {
			for (int i = 0; i < tests; i++) {
				long addr1 = allocate(mm, genString(1, maxString));
				allocs++;
				if ((i % 2) == 0) {
					addrs.add(Long.valueOf(addr1));
				} else if (i > 1000) {
					int f = r.nextInt(addrs.size());
					long faddr = ((Long) addrs.remove(f)).longValue();
					mm.free(faddr);
					// System.out.println("freeing: " + faddr);
					frees++;
				}
			}
		} catch (MemoryManagerResourceError err) {
			// all okay
		}
		
		System.out.println("Committed " + allocs + " allocations, and " + frees + " frees");
	}
	
	private String getString(long saddr) {
		final ByteBuffer ret = manager.get(saddr)[0];
		final byte[] data;
		if (ret.isDirect()) {
			ByteBuffer indbuf = ByteBuffer.allocate(ret.remaining());
			data = indbuf.array();
			indbuf.put(ret);
			indbuf.flip();
		} else {
			data = ret.array();
		}
		
		return new String(data);
	}
	
	private long allocate(final IMemoryManager mm, String val) {
		ByteBuffer bb = ByteBuffer.wrap(val.getBytes());
		
		return mm.allocate(bb);
	}
}
