/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/*
 * Created on Oct 14, 2006
 */

package com.bigdata.rwstore;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Properties;
import java.util.TreeMap;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.AbstractInterruptsTestCase;
import com.bigdata.journal.AbstractJournalTestCase;
import com.bigdata.journal.AbstractMRMWTestCase;
import com.bigdata.journal.AbstractMROWTestCase;
import com.bigdata.journal.AbstractRestartSafeTestCase;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.CommitRecordIndex;
import com.bigdata.journal.CommitRecordSerializer;
import com.bigdata.journal.DiskOnlyStrategy;
import com.bigdata.journal.ICommitRecord;
import com.bigdata.journal.Journal;
import com.bigdata.journal.RWStrategy;
import com.bigdata.journal.TestJournalBasics;
import com.bigdata.journal.Journal.Options;
import com.bigdata.rawstore.AbstractRawStoreTestCase;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.util.InnerCause;

/**
 * Test suite for {@link BufferMode#DiskRW} journals.
 * 
 * TODO: must modify RWStore to use DirectBufferPool to allocate and release
 * buffers, Once done then ensure the write cache is enabled when running test
 * suite
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRWJournal extends AbstractJournalTestCase {

	public TestRWJournal() {
		super();
	}

	public TestRWJournal(String name) {
		super(name);
	}

	public static Test suite() {

		final TestRWJournal delegate = new TestRWJournal(); // !!!! THIS CLASS
															// !!!!

		/*
		 * Use a proxy test suite and specify the delegate.
		 */

		final ProxyTestSuite suite = new ProxyTestSuite(delegate, "Disk RW Journal Test Suite");

		/*
		 * List any non-proxied tests (typically bootstrapping tests).
		 */

		// tests defined by this class.
		suite.addTestSuite(TestRWJournal.class);

		// test suite for the IRawStore api.
		suite.addTestSuite(TestRawStore.class);

		// test suite for handling asynchronous close of the file channel.
		suite.addTestSuite(TestInterrupts.class);

		// test suite for MROW correctness.
		suite.addTestSuite(TestMROW.class);

		// test suite for MRMW correctness.
		suite.addTestSuite(TestMRMW.class);

		/*
		 * Pickup the basic journal test suite. This is a proxied test suite, so
		 * all the tests will run with the configuration specified in this test
		 * class and its optional .properties file.
		 */
		suite.addTest(TestJournalBasics.suite());

		return suite;

	}

	public Properties getProperties() {

		final Properties properties = super.getProperties();

		properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW.toString());
		// properties.setProperty(Options.BUFFER_MODE,
		// BufferMode.TemporaryRW.toString());

		// properties.setProperty(Options.CREATE_TEMP_FILE, "true");

		// properties.setProperty(Options.FILE,
		// "/Volumes/SSDData/TestRW/tmp.rw");

		properties.setProperty(Options.DELETE_ON_EXIT, "true");

		properties.setProperty(Options.WRITE_CACHE_ENABLED, "" + writeCacheEnabled);

		// number of bits in FixedAllocators
		properties.setProperty(com.bigdata.rwstore.RWStore.Options.DEFAULT_FREE_BITS_THRESHOLD, "1000");

		// Size of META_BITS_BLOCKS
		properties.setProperty(com.bigdata.rwstore.RWStore.Options.DEFAULT_META_BITS_SIZE, "9");

		// properties.setProperty(RWStore.Options.ALLOCATION_SIZES,
		// "1,2,3,5,8,12,16,32"); // 2K max
		properties.setProperty(RWStore.Options.ALLOCATION_SIZES, "1,2,3,5,8,12,16"); // 1K

		// ensure history retention to force deferredFrees
		// properties.setProperty(AbstractTransactionService.Options.MIN_RELEASE_AGE,
		// "1"); // Non-zero

		return properties;

	}

	/**
	 * Verify normal operation and basic assumptions when creating a new journal
	 * using {@link BufferMode#DiskRW}.
	 * 
	 * @throws IOException
	 */
	public void test_create_disk01() throws IOException {

		File file = null;

		final Properties properties = getProperties();

		final Journal journal = new Journal(properties);

		try {

			final RWStrategy bufferStrategy = (RWStrategy) journal.getBufferStrategy();

			assertTrue("isStable", bufferStrategy.isStable());
			assertFalse("isFullyBuffered", bufferStrategy.isFullyBuffered());
			// assertEquals(Options.FILE, properties.getProperty(Options.FILE),
			// bufferStrategy.file.toString());
			assertEquals(Options.INITIAL_EXTENT, Long.parseLong(Options.DEFAULT_INITIAL_EXTENT), bufferStrategy
					.getInitialExtent());
			assertEquals(Options.MAXIMUM_EXTENT, 0L/* soft limit for disk mode */, bufferStrategy.getMaximumExtent());
			// assertNotNull("raf", bufferStrategy.getRandomAccessFile());
			assertEquals(Options.BUFFER_MODE, BufferMode.DiskRW, bufferStrategy.getBufferMode());

			file = journal.getFile();

		} finally {

			journal.destroy();

		}

		if (file != null && file.exists())
			fail("Did not delete the backing file: " + file);

	}

	/**
	 * Unit test verifies that {@link Options#CREATE} may be used to initialize
	 * a journal on a newly created empty file.
	 * 
	 * @throws IOException
	 */
	public void test_create_emptyFile() throws IOException {

		final File file = File.createTempFile(getName(), Options.JNL);

		final Properties properties = new Properties();

		properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW.toString());

		properties.setProperty(Options.FILE, file.toString());

		properties.setProperty(Options.WRITE_CACHE_ENABLED, "" + writeCacheEnabled);

		final Journal journal = new Journal(properties);

		try {

			assertEquals(file, journal.getFile());

		} finally {

			journal.destroy();

		}

	}

	/**
	 * Test suite integration for {@link AbstractRestartSafeTestCase}.
	 * 
	 * @todo there are several unit tests in this class that deal with
	 *       {@link DiskOnlyStrategy#allocate(int)} and
	 *       {@link DiskOnlyStrategy#update(long, int, ByteBuffer)}. If those
	 *       methods are added to the {@link IRawStore} API then move these unit
	 *       tests into {@link AbstractRawStoreTestCase}.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 * @version $Id: TestRWJournal.java 4010 2010-12-16 12:44:43Z martyncutcher
	 *          $
	 */
	public static class TestRawStore extends AbstractRestartSafeTestCase {

		public TestRawStore() {
			super();
		}

		public TestRawStore(String name) {
			super(name);
		}

		protected BufferMode getBufferMode() {

			return BufferMode.DiskRW;
			// return BufferMode.TemporaryRW;

		}

		public Properties getProperties() {

			System.out.println("TestRWJournal:getProperties");

			final Properties properties = super.getProperties();

			properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW.toString());
			// properties.setProperty(Options.BUFFER_MODE,
			// BufferMode.TemporaryRW.toString());

			properties.setProperty(Options.CREATE_TEMP_FILE, "true");

			// properties.setProperty(Options.FILE,
			// "/Volumes/SSDData/TestRW/tmp.rw");
			// properties.setProperty(RWStore.Options.ALLOCATION_SIZES,
			// "1,2,3,5,8,12,16,32,48,64,128,192,320,512,832,1344,2176,3520");
			// properties.setProperty(Options.RW_ALLOCATIONS,
			// "1,2,3,5,8,12,16,32,48,64");

			properties.setProperty(Options.DELETE_ON_EXIT, "true");

			properties.setProperty(Options.WRITE_CACHE_ENABLED, "" + writeCacheEnabled);

			// number of bits in FixedAllocators
			properties.setProperty(RWStore.Options.FREE_BITS_THRESHOLD, "50");

			// Size of META_BITS_BLOCKS
			properties.setProperty(RWStore.Options.META_BITS_SIZE, "9");

			// properties.setProperty(RWStore.Options.ALLOCATION_SIZES,
			// "1,2,3,5,8,12,16,32,48,64,128"); // 8K - max blob = 2K * 8K = 16M
			// properties.setProperty(RWStore.Options.ALLOCATION_SIZES,
			// "1,2,3,5,8,12,16,32,48,64,128"); // 2K max
			properties.setProperty(RWStore.Options.ALLOCATION_SIZES, "1,2,3,5,8,12,16"); // 2K
																							// max

			// ensure history retention to force deferredFrees
			// properties.setProperty(AbstractTransactionService.Options.MIN_RELEASE_AGE,
			// "1"); // Non-zero

			return properties;

		}

		protected IRawStore getStore() {

			return new Journal(getProperties());

		}

		// /**
		// * Test that allocate() pre-extends the store when a record is
		// allocated
		// * which would overflow the current user extent.
		// */
		// public void test_allocPreExtendsStore() {
		//       
		// final Journal store = (Journal) getStore();
		//
		// try {
		//
		// final DiskOnlyStrategy bufferStrategy = (DiskOnlyStrategy) store
		// .getBufferStrategy();
		//
		// final long nextOffset = store.getRootBlockView()
		// .getNextOffset();
		//
		// final long length = store.size();
		//
		// final long headerSize = FileMetadata.headerSize0;
		//
		// // #of bytes remaining in the user extent before overflow.
		// final long nfree = length - (headerSize + nextOffset);
		//
		// if (nfree >= Integer.MAX_VALUE) {
		//
		// /*
		// * The test is trying to allocate a single record that will
		// * force the store to be extended. This will not work if the
		// * store file already has a huge user extent with nothing
		// * allocated on it.
		// */
		//                    
		// fail("Can't allocate a record with: " + nfree + " bytes");
		//
		// }
		//
		// final int nbytes = (int) nfree;
		//                
		// final long addr = bufferStrategy.allocate(nbytes);
		//
		// assertNotSame(0L, addr);
		//
		// assertEquals(nbytes, store.getByteCount(addr));
		//                
		// // store file was extended.
		// assertTrue(store.size() > length);
		//                
		// } finally {
		//
		// store.destroy();
		//            
		// }
		//            
		// }

		/**
		 * Test allocate()+read() where the record was never written (the data
		 * are undefined unless written so there is nothing really to test here
		 * except for exceptions which might be through for this condition).
		 */
		public void test_allocate_then_read() {
		}

		/**
		 * Reallocates the same object several times, then commits and tests
		 * read back.
		 * 
		 * 
		 */
		public void test_reallocate() {
			final Journal store = (Journal) getStore();

			try {

				byte[] buf = new byte[1024]; // 2Mb buffer of random data
				r.nextBytes(buf);

				ByteBuffer bb = ByteBuffer.wrap(buf);

				RWStrategy bs = (RWStrategy) store.getBufferStrategy();

				RWStore rw = bs.getRWStore();

				long faddr1 = bs.write(bb);
				bb.position(0);
				// bs.delete(faddr);

				long faddr2 = bs.write(bb);
				bb.position(0);

				store.commit();

				rw.reset();

				ByteBuffer inbb1 = bs.read(faddr1);
				ByteBuffer inbb2 = bs.read(faddr2);

				assertEquals(bb, inbb1);
				assertEquals(bb, inbb2);

			} finally {
				store.destroy();
			}

		}

		/**
		 * Test write of a record and then update of a slice of that record.
		 * <p>
		 * Note: Since the record was written but not flushed it will be found
		 * in the write cache by update().
		 */
		public void test_write_plus_update() {
		}

		/**
		 * Ensures the allocation of unique addresses by mapping allocated
		 * address with uniqueness assertion against physical address.
		 */
		public void test_addressing() {

			final Journal store = (Journal) getStore();

			try {

				RWStrategy bufferStrategy = (RWStrategy) store.getBufferStrategy();

				RWStore rw = bufferStrategy.getRWStore();
				ArrayList<Integer> sizes = new ArrayList<Integer>();
				TreeMap<Long, Integer> paddrs = new TreeMap<Long, Integer>();
				for (int i = 0; i < 100000; i++) {
					int s = r.nextInt(250) + 1;
					sizes.add(s);
					int a = rw.alloc(s, null);
					long pa = rw.physicalAddress(a);
					assertTrue(paddrs.get(pa) == null);
					paddrs.put(pa, a);
				}

				for (int i = 0; i < 50; i++) {
					int s = r.nextInt(500) + 1;
					sizes.add(s);
					int a = rw.alloc(s, null);
					long pa = rw.physicalAddress(a);
					paddrs.put(pa, a);
				}

			} finally {

				store.destroy();

			}

		}

		/**
		 * Basic allocation test to ensure the FixedAllocators are operating
		 * efficiently.
		 * 
		 * A 90 byte allocation is expected to fit in a 128byte block. If we
		 * only allocate this fixed block size, then we would expect the
		 * physical address to increase by 128 bytes for each allocation.
		 */
		public void test_allocations() {

			Journal store = (Journal) getStore();

			try {

				RWStrategy bufferStrategy = (RWStrategy) store.getBufferStrategy();

				RWStore rw = bufferStrategy.getRWStore();
				long numAllocs = rw.getTotalAllocations();
				long startAllocations = rw.getTotalAllocationsSize();
				long faddr = allocBatch(rw, 1000, 275, 320);
				faddr = allocBatch(rw, 10000, 90, 128);
				faddr = allocBatch(rw, 20000, 45, 64);

				System.out.println("Final allocation: " + faddr + ", allocations: "
						+ (rw.getTotalAllocations() - numAllocs) + ", allocated bytes: "
						+ (rw.getTotalAllocationsSize() - startAllocations));

				store.commit();

				// Confirm that we can re-open the journal after commit
				store = (Journal) reopenStore(store);

			} finally {

				store.destroy();

			}

		}

		/**
		 * Not so much a test as a code coverage exercise.
		 * 
		 * The output from showAllocReserve confirms the relative merits of
		 * optimising for space vs density. The DirectFixedAllocators will
		 * allocate from DirectBuffers, where locality of reference is less
		 * important than efficient management of the memory, which is optimised
		 * by allocations in smaller amounts that match the demands at a finer
		 * granularity.
		 */
		public void testAllocationReserves() {
			final int cReserve16K = 16 * 1024;
			final int cReserve128K = 32 * 1024;

			showAllocReserve(false, 64, cReserve16K, cReserve16K);
			showAllocReserve(false, 128, cReserve16K, cReserve16K);
			showAllocReserve(false, 1024, cReserve16K, cReserve16K);
			showAllocReserve(false, 2048, cReserve16K, cReserve16K);
			showAllocReserve(false, 3072, cReserve16K, cReserve16K);
			showAllocReserve(false, 4096, cReserve16K, cReserve16K);
			showAllocReserve(false, 8192, cReserve16K, cReserve16K);

			showAllocReserve(true, 64, cReserve128K, cReserve16K);
			showAllocReserve(true, 128, cReserve128K, cReserve16K);
			showAllocReserve(true, 1024, cReserve128K, cReserve16K);
			showAllocReserve(true, 2048, cReserve128K, cReserve16K);
			showAllocReserve(true, 3072, cReserve128K, cReserve16K);
			showAllocReserve(true, 4096, cReserve128K, cReserve16K);
			showAllocReserve(true, 8192, cReserve128K, cReserve16K);
		}

		private void showAllocReserve(final boolean optDensity, final int slotSize, final int reserve, final int mod) {
			final int ints = FixedAllocator.calcBitSize(optDensity, slotSize, reserve, mod);
			// there are max 254 ints available to a FixedAllocator
			final int maxuse = (254 / (ints + 1)) * ints;
			System.out.println("Allocate " + ints + ":" + (32 * ints * slotSize) + " for " + slotSize + " in "
					+ reserve + " using " + maxuse + " of 254 possible");
		}

		long allocBatch(RWStore rw, int bsize, int asze, int ainc) {
			long curAddress = rw.physicalAddress(rw.alloc(asze, null));
			for (int i = 1; i < bsize; i++) {
				int a = rw.alloc(asze, null);
				long nxt = rw.physicalAddress(a);
				assertTrue("Problem with index: " + i, diff(curAddress, nxt) == ainc || (nxt % 8192 == 0));
				curAddress = nxt;
			}

			return curAddress;
		}

		int diff(final long cur, final long nxt) {
			int ret = (int) (nxt - cur);
			return ret < 0 ? -ret : ret;
		}

		int[] allocBatchBuffer(RWStore rw, int bsize, int base, int scope) {
			int[] retaddrs = new int[bsize];

			byte[] batchBuffer = new byte[base + scope];
			r.nextBytes(batchBuffer);
			for (int i = 0; i < bsize; i++) {
				int as = base + r.nextInt(scope);
				retaddrs[i] = (int) rw.alloc(batchBuffer, as, null);
			}

			return retaddrs;
		}

		/**
		 * Reallocation tests the freeing of allocated address and the re-use
		 * within a transaction.
		 * 
		 * The repeated runs with full reopening of the store check the
		 * initialization of the allocators on reload.
		 * 
		 * @throws IOException
		 */
		public void test_reallocation() throws IOException {
			final Properties properties = getProperties();
			File tmpfile = File.createTempFile("TestRW", "rw");
			properties.setProperty(Options.FILE, tmpfile.getAbsolutePath());
			properties.remove(Options.CREATE_TEMP_FILE);
			Journal store = new Journal(properties);

			try {

				RWStrategy bufferStrategy = (RWStrategy) store.getBufferStrategy();

				RWStore rw = bufferStrategy.getRWStore();
				long numAllocs = rw.getTotalAllocations();
				long startAllocations = rw.getTotalAllocationsSize();

				reallocBatch(rw, 1000, 275, 1000);

				store.commit();
				store.close();
				store = new Journal(properties);
				bufferStrategy = (RWStrategy) store.getBufferStrategy();
				rw = bufferStrategy.getRWStore();

				reallocBatch(rw, 1000, 100, 10000);

				store.commit();
				store.close();
				store = new Journal(properties);
				bufferStrategy = (RWStrategy) store.getBufferStrategy();
				rw = bufferStrategy.getRWStore();

				reallocBatch(rw, 1000, 100, 10000);

				store.commit();
				store.close();
				store = new Journal(properties);
				bufferStrategy = (RWStrategy) store.getBufferStrategy();
				rw = bufferStrategy.getRWStore();

				System.out.println("Final allocations: " + (rw.getTotalAllocations() - numAllocs)
						+ ", allocated bytes: " + (rw.getTotalAllocationsSize() - startAllocations) + ", file length: "
						+ rw.getStoreFile().length());
			} finally {

				store.destroy();

			}

		}

		private long reallocBatch(RWStore rw, int tsts, int sze, int grp) {
			long[] addr = new long[grp];
			for (int i = 0; i < grp; i++) {
				addr[i] = rw.alloc(2 + r.nextInt(sze), null);
			}
			for (int t = 0; t < tsts; t++) {
				for (int i = 0; i < grp; i++) {
					long old = addr[i];
					int asze = 2 + r.nextInt(sze);
					addr[i] = rw.alloc(asze, null);

					if (i % 2 == 0)
						rw.free(old, 1); // dunno what the real size is
				}
			}

			return 0L;
		}

		public void test_reallocationWithReadAndReopen() {

			Journal store = (Journal) getStore();

			try {

				RWStrategy bufferStrategy = (RWStrategy) store.getBufferStrategy();

				RWStore rw = bufferStrategy.getRWStore();

				final int tcount = 2000; // increase to ramp up stress levels

				long numAllocs = rw.getTotalAllocations();
				long startAllocations = rw.getTotalAllocationsSize();
				// reallocBatchWithRead(bufferStrategy, 100000, 275, 5);
				reallocBatchWithRead(store, 1, 100, 250, tcount, true, true);
				store.close();

				// added to try and foce bug
				System.out.println("Re-open Journal");
				store = (Journal) getStore();
				reallocBatchWithRead(store, 1, 800, 1500, tcount, true, true);
				reallocBatchWithRead(store, 1, 50, 250, tcount, true, true);
				reallocBatchWithRead(store, 1, 50, 250, tcount, true, true);
				store.close();
				// .. end add to force bug

				System.out.println("Re-open Journal");
				store = (Journal) getStore();
				reallocBatchWithRead(store, 1, 2000, 10000, tcount, true, true);
				reallocBatchWithRead(store, 1, 200, 500, tcount, true, true);
				store.close();
				System.out.println("Re-open Journal");
				store = (Journal) getStore();
				reallocBatchWithRead(store, 1, 800, 1256, tcount, true, true);
				reallocBatchWithRead(store, 1, 50, 250, tcount, true, true);
				reallocBatchWithRead(store, 1, 50, 250, tcount, true, true);
				showStore(store);
				store.close();
				System.out.println("Re-open Journal");
				store = (Journal) getStore();
				showStore(store);
				reallocBatchWithRead(store, 1, 400, 1000, tcount, true, true);
				reallocBatchWithRead(store, 1, 1000, 2000, tcount, true, true);
				reallocBatchWithRead(store, 1, 400, 1000, tcount, true, true);
				store.close();
				System.out.println("Re-open Journal");
				store = (Journal) getStore();

				bufferStrategy = (RWStrategy) store.getBufferStrategy();

				rw = bufferStrategy.getRWStore();

				System.out.println("Final allocations: " + (rw.getTotalAllocations() - numAllocs)
						+ ", allocated bytes: " + (rw.getTotalAllocationsSize() - startAllocations) + ", file length: "
						+ rw.getStoreFile().length());
			} finally {

				store.destroy();

			}

		}

		void showStore(Journal store) {
			RWStrategy bufferStrategy = (RWStrategy) store.getBufferStrategy();

			RWStore rw = bufferStrategy.getRWStore();

			System.out.println("Fixed Allocators: " + rw.getFixedAllocatorCount() + ", heap allocated: "
					+ rw.getFileStorage() + ", utilised bytes: " + rw.getAllocatedSlots() + ", file length: "
					+ rw.getStoreFile().length());

		}

		// Only realloc 1/5
		byte allocChar = 0;

		private long reallocBatchWithRead(Journal store, int tsts, int min, int sze, int grp, boolean commit,
				boolean reopen) {
			allocChar = (byte) (allocChar + 1);

			RWStrategy bs = (RWStrategy) store.getBufferStrategy();

			byte[] buf = new byte[sze + 4]; // extra for checksum
			// r.nextBytes(buf);
			for (int i = 0; i < buf.length; i++) {
				buf[i] = allocChar;
			}

			RWStore rw = bs.getRWStore();

			long[] addr = new long[grp / 5];
			int[] szes = new int[grp];
			for (int i = 0; i < grp; i++) {
				szes[i] = min + r.nextInt(sze - min);
				ByteBuffer bb = ByteBuffer.wrap(buf, 0, szes[i]);
				if (i % 5 == 0)
					addr[i / 5] = bs.write(bb);
			}

			if (commit) {
				store.commit();
			}

			for (int t = 0; t < tsts; t++) {
				for (int i = 0; i < (grp / 5); i++) {
					long old = addr[i];
					try {
						bs.read(old);
					} catch (Exception e) {
						throw new RuntimeException("problem handling read: " + i + " in test: " + t + " from address: "
								+ old, e);
					}
					ByteBuffer bb = ByteBuffer.wrap(buf, 0, szes[i]);
					addr[i] = bs.write(bb);
					bb.flip();
					bs.delete(old);
				}
			}

			if (commit) {
				store.commit();

				if (reopen)
					rw.reset();

			}
			return 0L;
		}

		/**
		 * Adjust tcount to increase stress levels
		 */
		public void test_stressReallocationWithRead() {

			Journal store = (Journal) getStore();

			try {

				final int tcount = 2000; // increase to ramp up stress levels

				RWStrategy bufferStrategy = (RWStrategy) store.getBufferStrategy();

				RWStore rw = bufferStrategy.getRWStore();

				long numAllocs = rw.getTotalAllocations();
				long startAllocations = rw.getTotalAllocationsSize();
				// reallocBatchWithRead(bufferStrategy, 100000, 275, 5);
				reallocBatchWithRead(store, 1, 50, 250, tcount, false, false);

				reallocBatchWithRead(store, 1, 50, 250, tcount, false, false);
				reallocBatchWithRead(store, 1, 50, 250, tcount, false, false);

				reallocBatchWithRead(store, 1, 5000, 10000, tcount, false, false);

				reallocBatchWithRead(store, 1, 800, 1500, tcount, false, false);
				reallocBatchWithRead(store, 1, 50, 250, tcount, false, false);
				reallocBatchWithRead(store, 1, 50, 250, tcount, false, false);

				// Extend file with sizeable allocations
				reallocBatchWithRead(store, 1, 5000, 10000, tcount, false, false);
				reallocBatchWithRead(store, 1, 5000, 10000, tcount, false, false);
				reallocBatchWithRead(store, 1, 5000, 10000, tcount, false, false);
				reallocBatchWithRead(store, 1, 5000, 10000, tcount, false, false);
				reallocBatchWithRead(store, 1, 5000, 10000, tcount, false, false);

				reallocBatchWithRead(store, 1, 250, 500, tcount, false, false);

				reallocBatchWithRead(store, 1, 5000, 10000, tcount, false, false);

				reallocBatchWithRead(store, 1, 800, 1256, tcount, false, false);
				reallocBatchWithRead(store, 1, 50, 250, tcount, false, false);
				reallocBatchWithRead(store, 1, 50, 250, tcount, false, false);

				reallocBatchWithRead(store, 1, 5000, 10000, tcount, false, false);

				// Extend file with more sizeable allocations
				reallocBatchWithRead(store, 1, 5000, 10000, tcount, false, false);
				reallocBatchWithRead(store, 1, 5000, 10000, tcount, false, false);
				reallocBatchWithRead(store, 1, 5000, 10000, tcount, false, false);
				reallocBatchWithRead(store, 1, 5000, 10000, tcount, false, false);
				reallocBatchWithRead(store, 1, 5000, 10000, tcount, false, false);
				reallocBatchWithRead(store, 1, 5000, 10000, tcount, false, false);
				reallocBatchWithRead(store, 1, 5000, 10000, tcount, false, false);
				reallocBatchWithRead(store, 1, 5000, 10000, tcount, false, false);
				reallocBatchWithRead(store, 1, 5000, 10000, tcount, false, false);
				reallocBatchWithRead(store, 1, 5000, 10000, tcount, false, false);

				reallocBatchWithRead(store, 1, 500, 1000, tcount, false, false);
				reallocBatchWithRead(store, 1, 1000, 2000, tcount, false, false);
				reallocBatchWithRead(store, 1, 500, 1000, tcount, false, false);

				store.commit();

				showStore(store);

				store.close();

				store = (Journal) getStore();
				showStore(store);
			} finally {

				store.destroy();

			}

		}

		/**
		 * Test of blob allocation, does not check on read back, just the
		 * allocation
		 */
		public void test_blob_allocs() {
			if (false) {
				return;
			}

			final Journal store = (Journal) getStore();

			try {

				RWStrategy bufferStrategy = (RWStrategy) store.getBufferStrategy();

				RWStore rw = bufferStrategy.getRWStore();
				long numAllocs = rw.getTotalAllocations();
				long startAllocations = rw.getTotalAllocationsSize();
				int startBlob = 1024 * 256;
				int endBlob = 1024 * 1256;
				int[] faddrs = allocBatchBuffer(rw, 100, startBlob, endBlob);

				final StringBuilder str = new StringBuilder();
				rw.getStorageStats().showStats(str);
				System.out.println(str);
			} finally {

				store.destroy();

			}

		}

		/**
		 * Test of blob allocation and read-back, firstly from cache and then
		 * from disk.
		 */
		public void test_blob_readBack() {

			final Journal store = (Journal) getStore();

			try {
				final RWStrategy bs = (RWStrategy) store.getBufferStrategy();

				final RWStore rw = bs.getRWStore();

				byte[] buf = new byte[2 * 1024 * 1024]; // 5Mb buffer of random
														// data
				r.nextBytes(buf);

				ByteBuffer bb = ByteBuffer.wrap(buf);

				long faddr = bs.write(bb); // rw.alloc(buf, buf.length);

				log.info("Blob Allocation at " + rw.convertFromAddr(faddr));

				bb.position(0);

				ByteBuffer rdBuf = bs.read(faddr);

				assertEquals(bb, rdBuf);

				System.out.println("Now commit to disk");

				store.commit();

				// Now reset - clears writeCache and reinits from disk
				rw.reset();

				rdBuf = bs.read(faddr);
				assertEquals(bb, rdBuf);

			} finally {

				store.destroy();

			}

		}

		/**
		 * Test of blob allocation and read-back, firstly from cache and then
		 * from disk.
		 * 
		 * @throws InterruptedException
		 */
		public void test_blob_realloc() throws InterruptedException {

			final Journal store = (Journal) getStore();

			try {

				final byte[] buf = new byte[1024 * 2048]; // 2Mb buffer of
															// random data
				r.nextBytes(buf);

				final ByteBuffer bb = ByteBuffer.wrap(buf);

				final RWStrategy bs = (RWStrategy) store.getBufferStrategy();

				final RWStore rw = bs.getRWStore();

				long faddr = bs.write(bb); // rw.alloc(buf, buf.length);

				bb.position(0);

				ByteBuffer rdBuf = bs.read(faddr);

				assertEquals(bb, rdBuf);

				// now delete the memory
				bs.delete(faddr);

				// verify immediateFree!
				assertEquals(0L, bs.getPhysicalAddress(faddr));

				// allocate another address, might (or might not) be the same.
				faddr = bs.write(bb); // rw.alloc(buf, buf.length);
				final long pa = bs.getPhysicalAddress(faddr);
				bb.position(0);

				System.out.println("Now commit to disk (1)");

				store.commit();

				// Now reset - clears writeCache and reinits from disk
				rw.reset();

				rdBuf = bs.read(faddr);
				assertEquals(bb, rdBuf);

				// now delete the memory
				bs.delete(faddr);

				// Must not have been immediately freed if history is retained.
				if (rw.getHistoryRetention() != 0) {
					assertEquals(pa, bs.getPhysicalAddress(faddr));

					/*
					 * Commit before testing for deferred frees. Since there is a
					 * prior commit point, we are not allowed to immediately free
					 * any record from that commit point in order to preserve the
					 * consistency of the last commit point, so we have to commit
					 * first then test for deferred frees.
					 */
					System.out.println("Now commit to disk (2)");

					store.commit();

					Thread.currentThread().sleep(10); // to force deferredFrees

					// Request release of deferred frees.
					rw.checkDeferredFrees(true/* freeNow */, store);
					
					// Now commit() to ensure the deferrals can be recycled
					store.commit();
				} else {
					// The address is deleted, but will still return a valid
					//	address since it is committed
					assertEquals(pa, bs.getPhysicalAddress(faddr));
					
					// Now commit() to ensure the deferrals can be recycled
					store.commit();
				}

				assertEquals(0L, bs.getPhysicalAddress(faddr));

                try {
                    // should fail with PhysicalAddressResolutionException
                    rdBuf = bs.read(faddr);
                    fail("Expecting: "
                            + PhysicalAddressResolutionException.class);
                } catch (Throwable t) {
                    if (InnerCause.isInnerCause(t,
                            PhysicalAddressResolutionException.class)) {
                        if (log.isInfoEnabled()) {
                            log.info("Ignoring expected exception: " + t);
                        }
                    } else {
                        fail("Expected: "
                                + PhysicalAddressResolutionException.class
                                        .getName() + " reading from "
                                + (faddr >> 32) + ", instead got: " + t, t);
                    }
                }

			} finally {

				store.destroy();

			}

		}

		/**
		 * Ttest write() + flush() + update() - for this case the data have been
		 * flushed from the write cache so the update will be a random write on
		 * the file rather than being buffered by the write cache.
		 */
		public void test_write_flush_update() {

			final Journal store = (Journal) getStore();

			try {

				RWStrategy bufferStrategy = (RWStrategy) store.getBufferStrategy();

				final int nbytes = 60;

				// random data.
				byte[] a = new byte[nbytes];
				r.nextBytes(a);

				// write a new record.
				final long addr = bufferStrategy.write(ByteBuffer.wrap(a));

				assertEquals(nbytes, store.getByteCount(addr));

				// Note: This will result flush the write cache.
				store.commit();

				/*
				 * Read back the record and verify the update is visible.
				 */
				{

					final ByteBuffer b = bufferStrategy.read(addr);

					assertNotNull(b);

					for (int i = 20; i < 40; i++) {

						assertEquals("data differs at offset=" + i, a[i], b.get(i));

					}

				}

			} finally {

				store.destroy();

			}

		}

		public void test_metaAlloc() {

			Journal store = (Journal) getStore();

			RWStrategy bs = (RWStrategy) store.getBufferStrategy();

			RWStore rw = bs.getRWStore();
			long realAddr = 0;
			try {
				for (int i = 0; i < 100000; i++) {
					int allocAddr = rw.metaAlloc();

					realAddr = rw.metaBit2Addr(allocAddr);
				}
				System.out.println("metaAlloc lastAddr: " + realAddr);
			} finally {
				store.destroy();
			}
		}

		static class DummyAllocationContext implements IAllocationContext {
		}

		/**
		 * From a RWStore, creates multiple AllocationContexts to isolate
		 * updates, re-allocate storage and protect against by concurrent
		 * Contexts. This is the core functionality required to support
		 * Transactions.
		 * 
		 * If an allocation is made for an AllocationContext then this will
		 * result in a ContextAllocation object being created in the RWStore
		 * within which "shadow" allocations can be made. If such a shadow
		 * allocation is deleted, within the AllocationContext, then this can be
		 * removed immediately.
		 * 
		 * @throws IOException
		 */
		public void test_allocationContexts() throws IOException {
			Journal store = (Journal) getStore();
			try {
				RWStrategy bs = (RWStrategy) store.getBufferStrategy();

				RWStore rw = bs.getRWStore();

				// JournalShadow shadow = new JournalShadow(store);

				// Create a couple of contexts
				IAllocationContext allocContext1 = new DummyAllocationContext();
				IAllocationContext allocContext2 = new DummyAllocationContext();

				int sze = 650;
				byte[] buf = new byte[sze + 4]; // extra for checksum
				r.nextBytes(buf);

				long addr1a = bs.write(ByteBuffer.wrap(buf), allocContext1);
				long addr1b = bs.write(ByteBuffer.wrap(buf), allocContext1);
				rw.detachContext(allocContext1);

				long addr2a = bs.write(ByteBuffer.wrap(buf), allocContext2);
				long addr2b = bs.write(ByteBuffer.wrap(buf), allocContext2);
				rw.detachContext(allocContext2);

				// Re-establish context
				long addr1c = bs.write(ByteBuffer.wrap(buf), allocContext1);

				// By detaching contexts we end up using the same allocator
				assertTrue("allocator re-use", bs.getPhysicalAddress(addr1c) > bs.getPhysicalAddress(addr2b));

				// Now, prior to commit, try deleting an uncommitted allocation
				bs.delete(addr1c, allocContext1);
				// and re-allocating it from the same context
				long addr1d = bs.write(ByteBuffer.wrap(buf), allocContext1);

				assertTrue("re-allocation", addr1c == addr1d);

				rw.detachContext(allocContext1);

				// Now commit
				store.commit();

				// now try deleting and re-allocating again, but in a global
				// context
				bs.delete(addr1d); // this should call deferFree
				long addr1e = bs.write(ByteBuffer.wrap(buf), allocContext1);

				assertTrue("deferred-delete", addr1e != addr1d);

				// Now commit
				store.commit();
			} finally {
				store.destroy();
			}

		}

		public void test_stressCommitIndex() {
			Journal journal = (Journal) getStore();
			try {
				final int cRuns = 1000;
				for (int i = 0; i < cRuns; i++)
					commitSomeData(journal);
				
		        final ITupleIterator<CommitRecordIndex.Entry> commitRecords;
			    {
		            /*
		             * Commit can be called prior to Journal initialisation, in which
		             * case the commitRecordIndex will not be set.
		             */
		            final IIndex commitRecordIndex = journal.getReadOnlyCommitRecordIndex();
		    
		            final IndexMetadata metadata = commitRecordIndex
		                    .getIndexMetadata();
	
		            commitRecords = commitRecordIndex.rangeIterator();
		            
		            int iters = 0;
		            int records = 0;
		            while (commitRecords.hasNext()) {
		                final ITuple<CommitRecordIndex.Entry> tuple = commitRecords.next();
		                	
		                iters++;
		                final CommitRecordIndex.Entry entry = tuple.getObject();

		                final ICommitRecord record = CommitRecordSerializer.INSTANCE
		                        .deserialize(journal.read(entry.addr));
		                
		                if (record != null) {
		                	records++;
		                }
		            }
		            
		            assertTrue("Should be " + cRuns + " == " + records, cRuns == records);
		        }
			} finally {
				journal.destroy();
			}

		}
		
		private void commitSomeData(Journal store) {
			RWStrategy bs = (RWStrategy) store.getBufferStrategy();

			RWStore rw = bs.getRWStore();

			// byte[] buf = new byte[r.nextInt(1024)]; // extra for checksum
			byte[] buf = new byte[863]; // extra for checksum
			r.nextBytes(buf);

			bs.write(ByteBuffer.wrap(buf));

			store.commit();
		}

		public void test_stressAlloc() {

			Journal store = (Journal) getStore();

			final RWStrategy bs = (RWStrategy) store.getBufferStrategy();

			final RWStore rw = bs.getRWStore();
			// long realAddr = 0;
			try {
				// allocBatch(store, 1, 32, 650, 100000000);
				allocBatch(store, 1, 32, 650, 50000);
				store.commit();
				final StringBuilder str = new StringBuilder();
				rw.showAllocators(str);
				System.out.println(str);
				store.close();
				System.out.println("Re-open Journal");
				store = (Journal) getStore();

				showStore(store);
			} finally {
				store.destroy();
			}
		}

		/**
		 * Tests whether tasks are able to access and modify data safely by
		 * emulating transactions by calling activateTx and deactivateTx
		 * directly.
		 */
		public void test_sessionProtection() {
			// Sequential logic

			final Journal store = (Journal) getStore();
			final RWStrategy bs = (RWStrategy) store.getBufferStrategy();
			final RWStore rw = bs.getRWStore();

			byte[] buf = new byte[300]; // Just some data
			r.nextBytes(buf);

			ByteBuffer bb = ByteBuffer.wrap(buf);

			long faddr = bs.write(bb); // rw.alloc(buf, buf.length);

			rw.activateTx();

			bs.delete(faddr); // delettion protected by session

			bb.position(0);

			ByteBuffer rdBuf = bs.read(faddr);

			// should be able to successfully read from freed address
			assertEquals(bb, rdBuf);

			rw.deactivateTx();

			store.commit();
		}

		/**
		 * To stress teh sesion protection, we will allocate a batch of
		 * addresses, then free half with protection.  Then reallocate half
		 * again after releasing the session.
		 * This should result in all the original batch being allocated,
		 * exercising both session protection and write cache clearing
		 */
		public void test_stressSessionProtection() {
			// Sequential logic

			final Journal store = (Journal) getStore();
			final RWStrategy bs = (RWStrategy) store.getBufferStrategy();
			final RWStore rw = bs.getRWStore();

			byte[] buf = new byte[300]; // Just some data
			r.nextBytes(buf);

			ByteBuffer bb = ByteBuffer.wrap(buf);
			
			rw.activateTx();
			ArrayList<Long> addrs = new ArrayList<Long>();
			
			// We just want to stress a single allocator, so make 5000
			// allocations to force multiple allocBlocks.
			
			for (int i = 0; i < 5000; i++) {
				addrs.add(bs.write(bb));
				bb.flip();
			}

			for (int i = 0; i < 5000; i+=2) {
				bs.delete(addrs.get(i));
			}

			// now release session to make addrs reavailable
			rw.deactivateTx();


			for (int i = 0; i < 5000; i+=2) {
				bs.write(bb);
				bb.flip();
			}

			store.commit();

			bb.position(0);

			for (int i = 0; i < 3000; i++) {
				bb.position(0);
				
				ByteBuffer rdBuf = bs.read(addrs.get(i));

				// should be able to
				assertEquals(bb, rdBuf);
			}


			store.commit();
		}

		/**
		 * The pureAlloc test is to test the allocation aspect of the memory
		 * management rather than worrying about writing the data
		 */
		public void test_pureAlloc() {

			Journal store = (Journal) getStore();

			RWStrategy bs = (RWStrategy) store.getBufferStrategy();

			RWStore rw = bs.getRWStore();
			long realAddr = 0;
			try {
				// allocBatch(store, 1, 32, 650, 100000000);
				pureAllocBatch(store, 1, 32, rw.m_maxFixedAlloc - 4, 300000); // cover
																				// wider
																				// range
																				// of
																				// blocks
				store.commit();
				System.out.println("Final allocations: " + rw.getTotalAllocations() + ", allocated bytes: "
						+ rw.getTotalAllocationsSize() + ", file length: " + rw.getStoreFile().length());
				store.close();
				System.out.println("Re-open Journal");
				store = (Journal) getStore();

				showStore(store);
			} finally {
				store.destroy();
			}
		}

		private long allocBatch(Journal store, int tsts, int min, int sze, int grp) {

			RWStrategy bs = (RWStrategy) store.getBufferStrategy();

			byte[] buf = new byte[sze + 4]; // extra for checksum
			r.nextBytes(buf);

			for (int i = 0; i < grp; i++) {
				int alloc = min + r.nextInt(sze - min);
				ByteBuffer bb = ByteBuffer.wrap(buf, 0, alloc);
				bs.write(bb);
			}

			return 0L;
		}

		/*
		 * Allocate tests but save 50% to re-alloc
		 */
		private long pureAllocBatch(Journal store, int tsts, int min, int sze, int grp) {

			RWStrategy bs = (RWStrategy) store.getBufferStrategy();

			RWStore rw = bs.getRWStore();
			int freeAddr[] = new int[512];
			int freeCurs = 0;
			for (int i = 0; i < grp; i++) {
				int alloc = min + r.nextInt(sze - min);
				int addr = rw.alloc(alloc, null);

				if (i % 3 != 0) { // make avail 2 out of 3 for realloc
					freeAddr[freeCurs++] = addr;
					if (freeCurs == freeAddr.length) {
						for (int f = 0; f < freeAddr.length; f++) {
							rw.free(freeAddr[f], 0);
						}
						freeCurs = 0;
					}
				}
			}

			return 0L;
		}
	}

	/**
	 * Test suite integration for {@link AbstractInterruptsTestCase}.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 * @version $Id: TestRWJournal.java 4010 2010-12-16 12:44:43Z martyncutcher
	 *          $
	 */
	public static class TestInterrupts extends AbstractInterruptsTestCase {

		public TestInterrupts() {
			super();
		}

		public TestInterrupts(String name) {
			super(name);
		}

		protected IRawStore getStore() {

			final Properties properties = getProperties();

			properties.setProperty(Options.DELETE_ON_EXIT, "true");

			properties.setProperty(Options.CREATE_TEMP_FILE, "true");

			properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW.toString());

			properties.setProperty(Options.WRITE_CACHE_ENABLED, "" + writeCacheEnabled);

			return new Journal(properties).getBufferStrategy();
			// return new Journal(properties);

		}

	}

	/**
	 * Test suite integration for {@link AbstractMROWTestCase}.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 * @version $Id: TestRWJournal.java 4010 2010-12-16 12:44:43Z martyncutcher
	 *          $
	 */
	public static class TestMROW extends AbstractMROWTestCase {

		public TestMROW() {
			super();
		}

		public TestMROW(String name) {
			super(name);
		}

		protected IRawStore getStore() {

			final Properties properties = getProperties();

			properties.setProperty(Options.CREATE_TEMP_FILE, "true");

			properties.setProperty(Options.DELETE_ON_EXIT, "true");

			properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW.toString());

			properties.setProperty(Options.WRITE_CACHE_ENABLED, "" + writeCacheEnabled);

			return new Journal(properties).getBufferStrategy();

		}

	}

	/**
	 * Test suite integration for {@link AbstractMRMWTestCase}.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 * @version $Id: TestRWJournal.java 4010 2010-12-16 12:44:43Z martyncutcher
	 *          $
	 */
	public static class TestMRMW extends AbstractMRMWTestCase {

		public TestMRMW() {
			super();
		}

		public TestMRMW(String name) {
			super(name);
		}

		protected IRawStore getStore() {

			final Properties properties = getProperties();

			properties.setProperty(Options.CREATE_TEMP_FILE, "true");

			properties.setProperty(Options.DELETE_ON_EXIT, "true");

			properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW.toString());

			properties.setProperty(Options.WRITE_CACHE_ENABLED, "" + writeCacheEnabled);

			return new Journal(properties).getBufferStrategy();

		}

	}

	/**
	 * Note: Since the write cache is a direct ByteBuffer we have to make it
	 * very small (or disable it entirely) when running the test suite or the
	 * JVM will run out of memory - this is exactly the same (Sun) bug which
	 * motivates us to reuse the same ByteBuffer when we overflow a journal
	 * using a write cache. Since small write caches are disallowed, we wind up
	 * testing with the write cache disabled!
	 */
	private static final boolean writeCacheEnabled = false; // 512;

}
