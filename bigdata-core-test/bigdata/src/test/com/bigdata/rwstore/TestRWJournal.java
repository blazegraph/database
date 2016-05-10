/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BloomFilter;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.SimpleEntry;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.journal.AbstractInterruptsTestCase;
import com.bigdata.journal.AbstractJournal.ISnapshotEntry;
import com.bigdata.journal.AbstractJournalTestCase;
import com.bigdata.journal.AbstractMRMWTestCase;
import com.bigdata.journal.AbstractMROWTestCase;
import com.bigdata.journal.AbstractRestartSafeTestCase;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.CommitRecordIndex;
import com.bigdata.journal.CommitRecordSerializer;
import com.bigdata.journal.ICommitRecord;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Journal.Options;
import com.bigdata.journal.RWStrategy;
import com.bigdata.journal.TestJournalAbort;
import com.bigdata.journal.TestJournalBasics;
import com.bigdata.journal.VerifyCommitRecordIndex;
import com.bigdata.util.Bytes;
import com.bigdata.rawstore.IAllocationContext;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rwstore.StorageStats.Bucket;
import com.bigdata.service.AbstractTransactionService;
import com.bigdata.util.InnerCause;
import com.bigdata.util.PseudoRandom;

/**
 * Test suite for {@link BufferMode#DiskRW} journals.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
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

		// ..and add TestAllocBits
		suite.addTestSuite(TestAllocBits.class);

		/*
		 * Pickup the basic journal test suite. This is a proxied test suite, so
		 * all the tests will run with the configuration specified in this test
		 * class and its optional .properties file.
		 */
		suite.addTest(TestJournalBasics.suite());

		/*
		 * TODO This should be a proxied test suite. It is RWStore specific
		 * right now.
		 * 
		 * @see #1021 (Add critical section protection to
		 * AbstractJournal.abort() and BigdataSailConnection.rollback())
		 */
		suite.addTestSuite(TestJournalAbort.class);
		
		return suite;

	}

	@Override
	public Properties getProperties() {

        final Properties properties = super.getProperties();

        properties.setProperty(Journal.Options.COLLECT_PLATFORM_STATISTICS,
                "false");

        properties.setProperty(Journal.Options.COLLECT_QUEUE_STATISTICS,
                "false");

        properties.setProperty(Journal.Options.HTTPD_PORT, "-1"/* none */);

        properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW
                .toString());
		// properties.setProperty(Options.BUFFER_MODE,
		// BufferMode.TemporaryRW.toString());

		// properties.setProperty(Options.CREATE_TEMP_FILE, "true");

		// properties.setProperty(Options.FILE,
		// "/Volumes/SSDData/TestRW/tmp.rw");

		properties.setProperty(Options.DELETE_ON_EXIT, "true");

		properties.setProperty(Options.WRITE_CACHE_ENABLED, "" + writeCacheEnabled);

	    properties.setProperty(Options.WRITE_CACHE_BUFFER_COUNT, "10");
	       
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
		
		// Set OVERWRITE_DELETE
		// properties.setProperty(RWStore.Options.OVERWRITE_DELETE, "true");

		return properties;

	}
	
//    private int fibslug(int n) {
//    	if (n < 2) 
//    		return 1;
//    	else
//    		return fibslug(n-1) + fibslug(n-2);
//    }
//    public void testFibSlug() {
//    	final long t1 = System.currentTimeMillis();
//    	fibslug(37);
//    	final long t2 = System.currentTimeMillis();
//    	fibslug(40);
//    	final long t3 = System.currentTimeMillis();
//    	
//    	log.info("fib 37: " + (t2-t1) + "ms, fib 40: " + (t3-t2) + "ms");
//    }
	
	/**
	 * The RWStore relies on several bit manipulation methods to manage both FixedAllocators
	 * and meta allocations.
	 * <p>
	 * This test stresses these methods.
	 */
	public void testRWBits() {
		final int bSize = 32 << 1; // a smaller array stresses more than a larger - say 8192
		final int[] bits = new int[bSize];
		final int nbits = bSize * 32;
		
		// Set all the bits one at a time
		for (int i = 0; i < nbits; i++) {
			final int b = RWStore.fndBit(bits, bSize);
			assertTrue(b != -1);
			assertFalse(RWStore.tstBit(bits,  b));
			RWStore.setBit(bits,  b);
			assertTrue(RWStore.tstBit(bits,  b));
		}
		
		// check that all are set
		assertTrue(-1 == RWStore.fndBit(bits, bSize));
		
		// now loop around clearing a random bit, then searching and setting it
		for (int i = 0; i < 30 * 1024 * 1024; i++) {
			final int b = r.nextInt(nbits);
			assertTrue(RWStore.tstBit(bits,  b));
			RWStore.clrBit(bits, b);
			assertFalse(RWStore.tstBit(bits,  b));
			
			assertTrue(b == RWStore.fndBit(bits,  bSize));
			RWStore.setBit(bits, b);
			assertTrue(RWStore.tstBit(bits,  b));
		}
		
		assertTrue(-1 == RWStore.fndBit(bits, bSize));
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
     * Unit tests for optimization when using the {@link RWStore} but not using
     * delete markers. In this case, a post-order traversal is used to
     * efficiently delete the nodes and leaves and the root leaf is then
     * replaced.
     */
    public void test_removeAllRWStore() {

        final Properties properties = getProperties();
        
//        properties.setProperty(Journal.Options.WRITE_CACHE_BUFFER_COUNT,"20");
        
        final Journal store = new Journal(properties);

        try {
        
            final BTree btree;
            {

                final IndexMetadata metadata = new IndexMetadata(UUID
                        .randomUUID());

                metadata.setBranchingFactor(3);

                metadata.setDeleteMarkers(false);

                btree = BTree.create(store, metadata);

            }

            final KeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_INT);

            final int NTRIALS = 100;

            final int NINSERTS = 1000;

            final double removeAllRate = 0.001;

            final double checkpointRate = 0.001;

            for (int i = 0; i < NTRIALS; i++) {

                for (int j = 0; j < NINSERTS; j++) {

                    if (r.nextDouble() < checkpointRate) {

                        // flush to the backing store.
                        if (log.isInfoEnabled())
                            log.info("checkpoint: " + btree.getStatistics());

                        btree.writeCheckpoint();

                    }

                    if (r.nextDouble() < removeAllRate) {

                        if (log.isInfoEnabled())
                            log.info("removeAll: " + btree.getStatistics());

                        btree.removeAll();

                    }

                    final int tmp = r.nextInt(10000);

                    final byte[] key = keyBuilder.reset().append(tmp).getKey();

                    btree.insert(key, new SimpleEntry(tmp));

                }

            }

            if (log.isInfoEnabled())
                log.info("will removeAll: "+btree.getStatistics());

            btree.removeAll();

            if (log.isInfoEnabled())
                log.info("will checkpoint: " + btree.getStatistics());

            btree.writeCheckpoint();

            if (log.isInfoEnabled())
                log.info(" did checkpoint: " + btree.getStatistics());

        } finally {

            store.destroy();

        }

    }

    /**
     * Unit test for an issue where the {@link RWStore} did not discard the
     * logged delete blocks in {@link RWStore#reset()}.
     * <p>
     * This test writes some records and commits. It then deletes those records
     * and invokes {@link RWStore#reset()}. It then re-deletes those records and
     * commits. If we have failed to discard the logged deletes, then we will
     * now have double-deletes in the delete blocks for that commit point. The
     * commit time is noted.
     * <p>
     * Next, the test creates several more commit points in order to trigger
     * recycling. We verify that the commit record for the commit time noted
     * above is no longer accessible. If the test reaches this point, then we
     * know that double-deletes were not logged.
     * <p>
     * Note: This test was verified as correctly detecting the bug described in
     * the ticket.
     * <p>
     * Note: The recycler MUST be used for this test.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/602">
     *      RWStore does not discard logged deletes on reset()</a>
     */
    public void test_deleteBlocksDiscardedOnReset() {

        final Properties p = getProperties();
        
        // The recycler MUST be used so we log delete blocks.
        p.setProperty(AbstractTransactionService.Options.MIN_RELEASE_AGE, "1");

        final Journal store = new Journal(p);

        try {
        
            /*
             * Create a named index and write a bunch of data.
             */
            final String name = "name";
            final long commitTime0;
            {

                final BTree btree;
                final IndexMetadata metadata = new IndexMetadata(
                        UUID.randomUUID());

                metadata.setBranchingFactor(3);

                metadata.setDeleteMarkers(false);

                btree = (BTree) store.register(name, metadata);

                final KeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_INT);

                final int NTRIALS = 1;

                final int NINSERTS = 100;

                final double removeAllRate = 0.001;

                final double checkpointRate = 0.001;

                for (int i = 0; i < NTRIALS; i++) {

                    for (int j = 0; j < NINSERTS; j++) {

                        if (r.nextDouble() < checkpointRate) {

                            // flush to the backing store.
                            if (log.isInfoEnabled())
                                log.info("checkpoint: " + btree.getStatistics());

                            btree.writeCheckpoint();

                        }

                        if (r.nextDouble() < removeAllRate) {

                            if (log.isInfoEnabled())
                                log.info("removeAll: " + btree.getStatistics());

                            btree.removeAll();

                        }

                        final int tmp = r.nextInt(10000);

                        final byte[] key = keyBuilder.reset().append(tmp)
                                .getKey();

                        btree.insert(key, new SimpleEntry(tmp));

                    }

                }

                // We've written a bunch of data, now commit.
                commitTime0 = store.commit();

            }
            
            /*
             * Now, removeAll(). This will cause a bunch of deletes, but we will
             * abort rather than committing.
             */
            {

                final BTree btree = store.getIndex(name);
                
                assertNotNull(btree);

                if (log.isInfoEnabled())
                    log.info("will removeAll: " + btree.getStatistics());

                btree.removeAll();

                if (log.isInfoEnabled())
                    log.info("will checkpoint: " + btree.getStatistics());

                btree.writeCheckpoint();

                if (log.isInfoEnabled())
                    log.info(" did checkpoint: " + btree.getStatistics());
                
                // discard the write set (and hopefully the delete blocks)
                store.abort();

            }
            
            /*
             * Now do it again, but this time we will do the commit.
             */
            final long commitTime1;
            {
                
                final BTree btree = store.getIndex(name);
                
                assertNotNull(btree);

                if (log.isInfoEnabled())
                    log.info("will removeAll: " + btree.getStatistics());

                btree.removeAll();

                if (log.isInfoEnabled())
                    log.info("will checkpoint: " + btree.getStatistics());

                btree.writeCheckpoint();

                if (log.isInfoEnabled())
                    log.info(" did checkpoint: " + btree.getStatistics());
                
                commitTime1 = store.commit();
                
            }

            /*
             * Write ahead, forcing recycling of commit points.
             */
            for(int i=0; i<10; i++) {
                
                store.write(getRandomData());
                
                store.commit();
                
            }
            
        } finally {

            store.destroy();

        }
        
    }

    /**
	 * Test suite integration for {@link AbstractRestartSafeTestCase}.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 */
	public static class TestRawStore extends AbstractRestartSafeTestCase {

		public TestRawStore() {
			super();
		}

		public TestRawStore(String name) {
			super(name);
		}

		@Override
		protected BufferMode getBufferMode() {

			return BufferMode.DiskRW;
			// return BufferMode.TemporaryRW;

		}

      @Override
		public Properties getProperties() {

            if (log.isInfoEnabled())
                log.info("TestRWJournal:getProperties");

//			final Properties properties = PropertyUtil.flatCopy(super.getProperties());
            final Properties properties = new Properties(super.getProperties());

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

			properties.setProperty(Options.READ_CACHE_BUFFER_COUNT, "20");

			properties.setProperty(RWStore.Options.READ_BLOBS_ASYNC, "true");
			
			// Size of META_BITS_BLOCKS
			properties.setProperty(RWStore.Options.META_BITS_SIZE, "9");

			// properties.setProperty(RWStore.Options.ALLOCATION_SIZES,
			// "1,2,3,5,8,12,16,32,48,64,128"); // 8K - max blob = 2K * 8K = 16M
			// properties.setProperty(RWStore.Options.ALLOCATION_SIZES,
//			properties.setProperty(RWStore.Options.ALLOCATION_SIZES, "1,2,3,5,8,12,16"); // 2K
			
			// ensure history retention to force deferredFrees
			// properties.setProperty(AbstractTransactionService.Options.MIN_RELEASE_AGE,
			// "1"); // Non-zero

			return properties;

		}

      @Override
		protected IRawStore getStore() {

			return getStore(0);

		}

		protected IRawStore getSmallSlotStore() {

			return getSmallSlotStore(0);

		}

		protected IRawStore getSmallSlotStore(final int slotSize) {

			return getSmallSlotStore(slotSize, 100 /*nallocators*/, 0.2f /*candiateWasteThreshold*/);

		}

		protected IRawStore getSmallSlotStore(final int slotSize, final int nWasteAllocators, final float maxWaste) {

            final Properties properties = new Properties(getProperties());

            properties.setProperty(
                    AbstractTransactionService.Options.MIN_RELEASE_AGE, "0");

            properties.setProperty(
                    RWStore.Options.SMALL_SLOT_TYPE, "" + slotSize);

            properties.setProperty(
                    RWStore.Options.SMALL_SLOT_WASTE_CHECK_ALLOCATORS, "" + nWasteAllocators);

            properties.setProperty(
                    RWStore.Options.SMALL_SLOT_HIGH_WASTE, "" + maxWaste);

            return getStore(properties);

		}

        protected Journal getStore(final long retentionMillis) {

            final Properties properties = new Properties(getProperties());

            properties.setProperty(
                    AbstractTransactionService.Options.MIN_RELEASE_AGE, ""
                            + retentionMillis);

            return getStore(properties);

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
		 * Has been amended to exercise different cache read paths.
		 */
		public void test_reallocate() {
			final Journal store = (Journal) getStore();

			try {

				final byte[] buf = new byte[1024]; // 2Mb buffer of random data
				r.nextBytes(buf);

				final ByteBuffer bb = ByteBuffer.wrap(buf);

				final RWStrategy bs = (RWStrategy) store.getBufferStrategy();

				final RWStore rw = bs.getStore();

				long faddr1 = bs.write(bb);
				bb.position(0);
				// bs.delete(faddr);

				long faddr2 = bs.write(bb);
				bb.position(0);

				// test read from writeCache
				ByteBuffer inbb1 = bs.read(faddr1);
				ByteBuffer inbb2 = bs.read(faddr2);

				assertEquals(bb, inbb1);
				assertEquals(bb, inbb2);
				
				store.commit();

				// test direct read from readCache / postCommit
				inbb1 = bs.read(faddr1);
				inbb2 = bs.read(faddr2);

				assertEquals(bb, inbb1);
				assertEquals(bb, inbb2);
				
				rw.reset();

				// test read from store (via readcache)
				inbb1 = bs.read(faddr1);
				inbb2 = bs.read(faddr2);

				assertEquals(bb, inbb1);
				assertEquals(bb, inbb2);

				// test read direct from readCache
				inbb1 = bs.read(faddr1);
				inbb2 = bs.read(faddr2);

				assertEquals(bb, inbb1);
				assertEquals(bb, inbb2);

			} finally {
				store.destroy();
			}

		}

//		/**
//		 * Test write of a record and then update of a slice of that record.
//		 * <p>
//		 * Note: Since the record was written but not flushed it will be found
//		 * in the write cache by update().
//		 */
//		public void test_write_plus_update() {
//		}

		/**
		 * Ensures the allocation of unique addresses by mapping allocated
		 * address with uniqueness assertion against physical address.
		 */
		public void test_addressing() {

			final Journal store = (Journal) getStore();

			try {

			    final RWStrategy bufferStrategy = (RWStrategy) store.getBufferStrategy();

				final RWStore rw = bufferStrategy.getStore();
				final ArrayList<Integer> sizes = new ArrayList<Integer>();
				final TreeMap<Long, Integer> paddrs = new TreeMap<Long, Integer>();
				for (int i = 0; i < 100000; i++) {
					final int s = r.nextInt(250) + 1;
					sizes.add(s);
					final int a = rw.alloc(s, null);
					final long pa = rw.physicalAddress(a);
					assertTrue(paddrs.get(pa) == null);
					paddrs.put(pa, a);
				}

				for (int i = 0; i < 50; i++) {
					final int s = r.nextInt(500) + 1;
					sizes.add(s);
					final int a = rw.alloc(s, null);
					final long pa = rw.physicalAddress(a);
					paddrs.put(pa, a);
				}

			} finally {

				store.destroy();

			}

		}

		/**
		 * Ensures the allocation of unique addresses by mapping allocated
		 * address with uniqueness assertion against physical address.
		 */
		public void test_addressingContiguous() {

			final Journal store = (Journal) getStore();

			try {

			    final RWStrategy bufferStrategy = (RWStrategy) store.getBufferStrategy();

				final RWStore rw = bufferStrategy.getStore();
				final int cSlotSize = 128;
				final int cAllocSize = 99;
				
				long pap = rw.physicalAddress(rw.alloc(cAllocSize, null));
				for (int i = 0; i < 500000; i++) {
					final int a = rw.alloc(cAllocSize, null);
					final long pa = rw.physicalAddress(a);
					
					if (pa != (pap+cSlotSize)) {
						// for debug
						rw.physicalAddress(a);
						fail("Non-Contiguous slots: " + i + ", " + pa + "!=" + (pap+cSlotSize));
					}
					
					pap = pa;
					
				}
				
				store.commit();
				
				final StringBuilder sb = new StringBuilder();
				rw.showAllocators(sb);
				
				log.warn(sb.toString());

			} finally {

				store.destroy();

			}

		}

		/**
		 * Tests the recycling of small slot alloctors and outputs statistics related
		 * to contiguous allocations indicative of reduced IOPS.
		 */
		public void test_smallSlotRecycling() {

			final Journal store = (Journal) getSmallSlotStore(1024);

			try {

			    final RWStrategy bufferStrategy = (RWStrategy) store.getBufferStrategy();

				final RWStore rw = bufferStrategy.getStore();
				final int cSlotSize = 128;
				final int cAllocSize = 99;
				
				int breaks = 0;
				int contiguous = 0;
				
				ArrayList<Integer> recycle = new ArrayList<Integer>();
				
				long pap = rw.physicalAddress(rw.alloc(cAllocSize, null));
				for (int i = 0; i < 500000; i++) {
					final int a = rw.alloc(cSlotSize, null);
					final long pa = rw.physicalAddress(a);
					
					if (r.nextInt(7) < 5) { // more than 50% recycle
						recycle.add(a);
					}
					
					if (pa == (pap+cSlotSize)) {
						contiguous++;
					} else {
						breaks++;
					}
					
					pap = pa;
					
					if (recycle.size() > 5000) {
						log.warn("Transient Frees for immediate recyling");
						for (int e : recycle) {
							rw.free(e, cAllocSize);
						}
						recycle.clear();
					}
				}
				
				store.commit();
				
				final StringBuilder sb = new StringBuilder();
				rw.showAllocators(sb);
				
				log.warn("Contiguous: " + contiguous + ", breaks: " + breaks + "\n" + sb.toString());

			} finally {

				store.destroy();

			}

		}
		
		/**
         * At scale the small slot handling can lead to large amounts of store
         * waste, tending to the small slot allocation thresholds of 50%,
         * dependent on use case.
         * <p>
         * To mitigate this problem, when more than some minimum number of
         * allocators are in use the storage stats are used to check on overall
         * usage. If the waste is above some specified amount, then an attempt
         * is made to locate a "reasonable" candidate allocator to be used.
         * <p>
         * To test below sets thresholds to quickly trigger this behaviour, with
         * a low minimum number of allocators, and low "candidate" re-use
         * threshold.
         * 
         * @see BLZG-1278 (Small slot optimization to minimize waste).
         */
		public void test_smallSlotWasteRecylcing() {
			// Note that the Waste parameter effectively controls the recycling in high waste scenarios
			final Journal store = (Journal) getSmallSlotStore(1024, 10 /*min allocators to check*/, 20.0f/* % waste of store total slot waste before checking for candidates*/);
			try {

			    final RWStrategy bufferStrategy = (RWStrategy) store.getBufferStrategy();

				final RWStore rw = bufferStrategy.getStore();
				final int cSlotSize = 128;
				final int cAllocSize = 99;
				
				int breaks = 0;
				int contiguous = 0;
				
				ArrayList<Integer> recycle = new ArrayList<Integer>();
				
				long pap = rw.physicalAddress(rw.alloc(cAllocSize, null));
				for (int i = 0; i < 500000; i++) {
					final int a = rw.alloc(cSlotSize, null);
					final long pa = rw.physicalAddress(a);
					
					if (r.nextInt(7) < 3) { // less than 50% recycle, so will not be added automatically to free list
						recycle.add(a);
					}
					
					if (pa == (pap+cSlotSize)) {
						contiguous++;
					} else {
						breaks++;
					}
					
					if ((i+1) % 20000 == 0) {
						// add intermediate commit to stress correct recycling
						store.commit();
					}
					
					pap = pa;
					
					if (recycle.size() > 5000) {
						log.warn("Transient Frees for immediate recyling");
						for (int e : recycle) {
							rw.free(e, cAllocSize);
						}
						recycle.clear();
					}
				}
				
				store.commit();
				
				final StringBuilder sb = new StringBuilder();
				rw.showAllocators(sb);
				
				log.warn("Contiguous: " + contiguous + ", breaks: " + breaks + "\n" + sb.toString());

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

				final RWStrategy bufferStrategy = (RWStrategy) store.getBufferStrategy();

				final RWStore rw = bufferStrategy.getStore();
				final long numAllocs = rw.getTotalAllocations();
				final long startAllocations = rw.getTotalAllocationsSize();
				long faddr = allocBatch(rw, 1000, 275, 320);
				faddr = allocBatch(rw, 10000, 90, 128);
				faddr = allocBatch(rw, 20000, 45, 64);

				if(log.isInfoEnabled())
				    log.info("Final allocation: " + faddr + ", allocations: "
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
		 * optimizing for space vs density. The DirectFixedAllocators will
		 * allocate from DirectBuffers, where locality of reference is less
		 * important than efficient management of the memory, which is optimized
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
			if(log.isInfoEnabled())
			    log.info("Allocate " + ints + ":" + (32 * ints * slotSize) + " for " + slotSize + " in "
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
			final Properties properties = new Properties(getProperties());
			final File tmpfile = File.createTempFile("TestRW", ".rw");
			properties.setProperty(Options.FILE, tmpfile.getAbsolutePath());
			properties.setProperty(Options.CREATE_TEMP_FILE,"false");
//			Journal store = new Journal(properties);
			Journal store = getStore(properties);

			try {

				RWStrategy bufferStrategy = (RWStrategy) store.getBufferStrategy();

				RWStore rw = bufferStrategy.getStore();
				long numAllocs = rw.getTotalAllocations();
				long startAllocations = rw.getTotalAllocationsSize();

				reallocBatch(rw, 1000, 275, 1000);

				store.commit();
				store.close();
				store = new Journal(properties);
				bufferStrategy = (RWStrategy) store.getBufferStrategy();
				rw = bufferStrategy.getStore();

				reallocBatch(rw, 1000, 100, 1000);

				store.commit();
				store.close();
				store = new Journal(properties);
				bufferStrategy = (RWStrategy) store.getBufferStrategy();
				rw = bufferStrategy.getStore();

				reallocBatch(rw, 1000, 100, 1000);

				store.commit();
				store.close();
				store = new Journal(properties);
				bufferStrategy = (RWStrategy) store.getBufferStrategy();
				rw = bufferStrategy.getStore();

				if(log.isInfoEnabled())
				    log.info("Final allocations: " + (rw.getTotalAllocations() - numAllocs)
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

				RWStore rw = bufferStrategy.getStore();

				final int tcount = 5000; // increase to ramp up stress levels

				long numAllocs = rw.getTotalAllocations();
				long startAllocations = rw.getTotalAllocationsSize();
				// reallocBatchWithRead(bufferStrategy, 100000, 275, 5);
				reallocBatchWithRead(store, 1, 100, 250, tcount, true, true);
				store.close();

				// added to try and foce bug
				if (log.isInfoEnabled())log.info("Re-open Journal");
				store = (Journal) getStore();
				reallocBatchWithRead(store, 1, 800, 1500, tcount, true, true);
				reallocBatchWithRead(store, 1, 50, 250, tcount, true, true);
				reallocBatchWithRead(store, 1, 50, 250, tcount, true, true);
				store.close();
				// .. end add to force bug

				if (log.isInfoEnabled()) log.info("Re-open Journal");
				store = (Journal) getStore();
				reallocBatchWithRead(store, 1, 2000, 10000, tcount, true, true);
				reallocBatchWithRead(store, 1, 200, 500, tcount, true, true);
				store.close();
				if (log.isInfoEnabled()) log.info("Re-open Journal");
				store = (Journal) getStore();
				reallocBatchWithRead(store, 1, 800, 1256, tcount, true, true);
				reallocBatchWithRead(store, 1, 50, 250, tcount, true, true);
				reallocBatchWithRead(store, 1, 50, 250, tcount, true, true);
				showStore(store);
				store.close();
				if (log.isInfoEnabled()) log.info("Re-open Journal");
				store = (Journal) getStore();
				showStore(store);
				reallocBatchWithRead(store, 1, 400, 1000, tcount, true, true);
				reallocBatchWithRead(store, 1, 1000, 2000, tcount, true, true);
				reallocBatchWithRead(store, 1, 400, 1000, tcount, true, true);
				store.close();
				if (log.isInfoEnabled()) log.info("Re-open Journal");
				store = (Journal) getStore();

				bufferStrategy = (RWStrategy) store.getBufferStrategy();

				rw = bufferStrategy.getStore();

				if (log.isInfoEnabled()) log.info("Final allocations: " + (rw.getTotalAllocations() - numAllocs)
						+ ", allocated bytes: " + (rw.getTotalAllocationsSize() - startAllocations) + ", file length: "
						+ rw.getStoreFile().length());
			} finally {

				store.destroy();

			}

		}

		public void notest_stressReallocationWithReadAndReopen() {
			for (int i = 0; i < 20; i++) {
				test_reallocationWithReadAndReopen();
			}
		}
		
		public void testAllocationContexts() {
			
			final Journal store = (Journal) getStore();

			try {

				final RWStrategy bufferStrategy = (RWStrategy) store.getBufferStrategy();

				final RWStore rw = bufferStrategy.getStore();
				
				final IAllocationContext cntxt1 = rw.newAllocationContext(true/*isolated*/);
				
				final IAllocationContext cntxt2 = rw.newAllocationContext(true/*isolated*/);
				
				// allocate a global address
				final int gaddr = rw.alloc(412, null);
				
				store.commit();
				
				// allocate a context address grabbing previous global allocation
				int c1addr = rw.alloc(412, cntxt1);
				// imagine we are re-allocating the original address
				rw.free(gaddr, 412, cntxt1);				
				// now abort context
				rw.abortContext(cntxt1);
				
				store.commit();

				final long paddr = rw.physicalAddress(gaddr);
				
				assertTrue("Global allocation must be protected", paddr != 0);

			} finally {
				store.destroy();
			}
		}
		
		/**
		 * This tests whether AllocationContexts efficiently recycle transient
		 * allocations.
		 * 
		 * To do this it will allocate and free 1 million 50 byte regions, in batches
		 * of 10K.
		 */
		public void testStressAllocationContextRecycling() {
			
			final Journal store = (Journal) getStore(1);

			try {

				final RWStrategy bufferStrategy = (RWStrategy) store.getBufferStrategy();

				final RWStore rw = bufferStrategy.getStore();
				
				final IAllocationContext cntxt = rw.newAllocationContext(true/*isolated*/);
				
				// allocate a global address
				final int allocs = 100000;
				final Random ran = new Random();
				
				for (int r = 0; r < 20; r++) {
					ArrayList<Integer> addrs = new ArrayList<Integer>();
					for (int a = 0; a < allocs; a++) {
						addrs.add(rw.alloc(50, cntxt));
					}
					final PseudoRandom ps = new PseudoRandom(allocs, ran.nextInt(allocs));
					for (int a : addrs) {
						rw.free(addrs.get(ps.next()), 50, cntxt);
					}
				}
				
				assertTrue(rw.getFixedAllocatorCount() < 20);
				
				if (log.isInfoEnabled()) {
					final StringBuilder str = new StringBuilder();
					rw.showAllocators(str);
					
					log.info(str);
				}
				
				store.commit();
				
			} finally {
				store.destroy();
			}
		}
		
		/**
		 * This tests whether AllocationContexts efficiently recycle transient
		 * allocations with an Unisolated AllocationContext.
		 * 
		 * To do this it will allocate and free 1 million 50 byte regions, in batches
		 * of 10K.
		 */
		public void testStressUnisolatedAllocationContextRecycling() {
			
			final Journal store = (Journal) getStore(1);

			try {

				final RWStrategy bufferStrategy = (RWStrategy) store.getBufferStrategy();

				final RWStore rw = bufferStrategy.getStore();
				
				final IAllocationContext cntxt = rw.newAllocationContext(false /*Isolated*/);
				
				// allocate a global address
				final int allocs = 100000;
				final Random ran = new Random();
				
				for (int r = 0; r < 20; r++) {
					final ArrayList<Integer> addrs = new ArrayList<Integer>();
					for (int a = 0; a < allocs; a++) {
						addrs.add(rw.alloc(50, cntxt));
					}
					final PseudoRandom ps = new PseudoRandom(allocs, ran.nextInt(allocs));
					for (int a : addrs) {
						rw.free(addrs.get(ps.next()), 50, cntxt);
					}
				}
				
				assertTrue(rw.getFixedAllocatorCount() < 20);
				
				if (log.isInfoEnabled()) {
					final StringBuilder str = new StringBuilder();
					rw.showAllocators(str);
					
					log.info(str);
				}
				
				store.commit();
				
			} finally {
				store.destroy();
			}
		}
		
		int getIndex(int rwaddr) {
			return (-rwaddr) >>> 13;
		}
		
		/**
		 * Need to test the handling of aborted Unisolated connections and specifically the
		 * logic behind freed addresses.
		 * <p>
		 * The process is to: 
		 *	Allocate a range of data (to establish a number of Allocators)
		 *  Create an Isolated context and allocate a handful more
		 *  Create an Unisolated context
		 *  Free some committed addreses
		 *  Abort the Unisolated context - calling abort on the Journal
		 *  Commit the Isolated context
		 *  Observe if any of the previously committed data is freed!
		 */
		public void testUnisolatedAllocationContextRecycling() {
			
			final Journal store = (Journal) getStore(0);

			try {

				final RWStrategy bufferStrategy = (RWStrategy) store.getBufferStrategy();

				final RWStore rw = bufferStrategy.getStore();
				
				final IAllocationContext cntxt = rw.newAllocationContext(false /*Isolated*/); // Unisolated context
				
				// allocate a global address
				final int allocs = 1000;
				final PseudoRandom ran = new PseudoRandom(20000);
				final ArrayList<Integer> addrs = new ArrayList<Integer>();
				final ArrayList<Integer> sizes = new ArrayList<Integer>();
				
				for (int a = 0; a < allocs; a++) {
					final int sze = 32 + ran.nextInt(1200);
					addrs.add(rw.alloc(sze, cntxt));
					sizes.add(sze);
				}
				
				// Commit the addresses
				store.commit();
				
				// Now create some allocation contexts
				IAllocationContext iso_cntxt = rw.newAllocationContext(true /*Isolated*/); // Isolated context
				for (int a = 0; a < 3; a++) {
					rw.alloc(32 + ran.nextInt(1200), iso_cntxt); // don't need to remember these
				}
				
				if (log.isInfoEnabled()) {
					final StringBuilder str = new StringBuilder();
					rw.showAllocators(str);
					
					log.info(str);
				}
				
				// Now free a few of committed addresses from the unisolated connection
				for (int a = 0; a < 36; a++) {
					rw.free(addrs.get(a), sizes.get(a), cntxt);
				}
				
				// now call abort on the unisolated connection
				store.abort();
				
				// Freed addresses current?
				
				for (int a = 0; a < 36; a++) {
					final int rwaddr = addrs.get(a);
					
					if (log.isInfoEnabled())
						log.info("Address: " + rw.physicalAddress(rwaddr)
								+ ", Committed: " + rw.isCommitted(rwaddr));
				}
				
				// Intermediate commit?
				// store.commit();
				log.info("INTERMEDIATE COMMIT");
				
				
				for (int a = 0; a < 36; a++) {
					final int rwaddr = addrs.get(a);
					
					if (log.isInfoEnabled())
						log.info("Address: " + rw.physicalAddress(rwaddr)
								+ ", Committed: " + rw.isCommitted(rwaddr)
								+ ", Index: " + getIndex(rwaddr));
				}
				
				log.info("DETACH CONTEXT");
				
				// This is the point that the AllocationContext ends up setting the free bits, iff we have committed after the abort.				
				rw.detachContext(iso_cntxt);
				
				for (int a = 0; a < 36; a++) {
					final int rwaddr = addrs.get(a);
					
					if (log.isInfoEnabled())
						log.info("Address: " + rw.physicalAddress(rwaddr)
								+ ", Committed: " + rw.isCommitted(rwaddr)
								+ ", Index: " + getIndex(rwaddr));
				}
				
				// ...and now those free bits are committed!				
				store.commit();
				
				log.info("COMMIT");
				
				for (int a = 0; a < 36; a++) {
					final int rwaddr = addrs.get(a);
					
					if (log.isInfoEnabled())
						log.info("Address: " + rw.physicalAddress(rwaddr)
								+ ", Committed: " + rw.isCommitted(rwaddr)
								+ ", Index: " + getIndex(rwaddr));
				}
				
				// ...and now those free bits are committed!				
				store.commit();
				
				log.info("SECONDARY");
				
				for (int a = 0; a < 36; a++) {
					final int rwaddr = addrs.get(a);
					
					if (log.isInfoEnabled())
						log.info("Address: " + rw.physicalAddress(rwaddr)
								+ ", Committed: " + rw.isCommitted(rwaddr)
								+ ", Index: " + getIndex(rwaddr));
				}
				
			} finally {
				store.destroy();
			}
		}
		
		void showAddress(final RWStore rw, final int rwaddr) {
			if (log.isInfoEnabled())
				log.info("Address: " + rw.physicalAddress(rwaddr)
						+ ", Committed: " + rw.isCommitted(rwaddr)
						+ ", Index: " + getIndex(rwaddr));
		}
		
		public void testSimpleUnisolatedAllocationContextRecycling() {
			
			final Journal store = (Journal) getStore(0);

			try {

				final RWStrategy bufferStrategy = (RWStrategy) store.getBufferStrategy();

				final RWStore rw = bufferStrategy.getStore();
				
				final IAllocationContext cntxt = rw.newAllocationContext(false /*Isolated*/); // Unisolated context
				
				// allocate three global addresses of different sizes
				final int sze1 = 48; // 64 bytes allocator
				final int sze2 = 72; // 128	
				final int sze3 = 135; // 256
				
				final int addr1 = rw.alloc(sze1, cntxt);
				final int addr2 = rw.alloc(sze2, cntxt);
				final int addr3 = rw.alloc(sze3, cntxt);
				
				// Commit the addresses
				store.commit();
				
				showAddress(rw, addr1);
				showAddress(rw, addr2);
				showAddress(rw, addr3);
				
				// Now create some allocation contexts
				final IAllocationContext iso_cntxt = rw.newAllocationContext(true /*Isolated*/); // Isolated context
				
				// now allocate a new unisolated address
				rw.alloc(sze1, cntxt);
				// free the originall
				rw.free(addr1,  sze1, cntxt);
				// and ensure that allocator is 'owned' by an iso
				rw.alloc(sze1, iso_cntxt);
				
				// now grab a pristine allocator
				rw.alloc(sze2,  iso_cntxt);
				// and free the original address (with the unisolated context)
				rw.free(addr2, sze2, cntxt);				
				
				if (log.isInfoEnabled()) {
					final StringBuilder str = new StringBuilder();
					rw.showAllocators(str);
					
					log.info(str);
				}	
				
				store.abort();
				
				log.info("ABORT");
				showAddress(rw, addr1);
				showAddress(rw, addr2);
				showAddress(rw, addr3);
				
				// This is the point that the AllocationContext ends up setting the free bits, iff we have committed after the abort.				
				rw.detachContext(iso_cntxt);
				
				log.info("DETACH");
				showAddress(rw, addr1);
				showAddress(rw, addr2);
				showAddress(rw, addr3);
				
				store.commit();
				
				log.info("COMMIT");
				showAddress(rw, addr1);
				showAddress(rw, addr2);
				showAddress(rw, addr3);
				
			} finally {
				store.destroy();
			}
		}
		
		void showStore(final Journal store) {
			
		    final RWStrategy bufferStrategy = (RWStrategy) store.getBufferStrategy();

			final RWStore rw = bufferStrategy.getStore();

			if (log.isInfoEnabled())
				log.info("Fixed Allocators: " + rw.getFixedAllocatorCount()
						+ ", heap allocated: " + rw.getFileStorage()
						+ ", utilised bytes: " + rw.getAllocatedSlots()
						+ ", file length: " + rw.getStoreFile().length());

		}

		// Only realloc 1/5
		byte allocChar = 0;

		private long reallocBatchWithRead(Journal store, int tsts, int min, int sze, int grp, boolean commit,
				boolean reopen) {
			allocChar = (byte) (allocChar + 1);

			final RWStrategy bs = (RWStrategy) store.getBufferStrategy();

			final byte[] buf = new byte[sze + 4]; // extra for checksum
			// r.nextBytes(buf);
			for (int i = 0; i < buf.length; i++) {
				buf[i] = allocChar;
			}

			final RWStore rw = bs.getStore();

			final long[] addr = new long[grp / 5];
			final int[] szes = new int[grp];
			for (int i = 0; i < grp; i++) {
				szes[i] = min + r.nextInt(sze - min);
				final ByteBuffer bb = ByteBuffer.wrap(buf, 0, szes[i]);
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
					final ByteBuffer bb = ByteBuffer.wrap(buf, 0, szes[i]);
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

				final int tcount = 1000; // increase to ramp up stress levels

				final RWStrategy bufferStrategy = (RWStrategy) store.getBufferStrategy();

				final RWStore rw = bufferStrategy.getStore();

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
		 * Test to determine that allocation performance does not degrade at scale.
		 * 
		 * The idea is to first create 128K  allocations, and measure recycling
		 * performance.
		 * 
		 * Then to add an additional 128K allocations and re-measure recycling to
		 * observe if performance degrades.
		 * 
		 * The test is "observational" and console output should be monitored.
		 * 
		 * Should be disabled for CI.
		 */
		public void notest_stress_alloc_performance() {
            final Properties properties = new Properties(getProperties());

            // we want lots of allocators, but avoid a large file
            properties.setProperty(RWStore.Options.ALLOCATION_SIZES, "1,2,3,4,5,6,7,8"); // 512 max

			final Journal store = (Journal) getStore(properties);

			try {

			    final RWStrategy bufferStrategy = (RWStrategy) store.getBufferStrategy();

				final RWStore rw = bufferStrategy.getStore();
				
				System.out.println("File: " + rw.getStoreFile().getAbsolutePath());

				final ArrayList<Integer> addrs = new ArrayList<Integer>();
				
				final Random r = new Random();
				
				for (int i = 0; i < 2048*64; i++) {
					addrs.add(rw.alloc(1+r.nextInt(500), null));
				}
				
				final long s1 = System.nanoTime();
				for (int t = 0; t < 1024 * 1024; t++) {
					final int i = r.nextInt(addrs.size());
				}
				final long s2 = System.nanoTime();
				System.out.println("Random iter: " + (s2-s1) + "ns");

				System.out.println("File size: " + rw.getStoreFile().length());
				
				for (int run = 0; run < 20; run++) {
					for (int i = 0; i < 2048*64; i++) {
						addrs.add(rw.alloc(1+r.nextInt(500), null));
					}
					final long s3 = System.nanoTime();
					for (int t = 0; t < 1024 * 1024; t++) {
						final int i = r.nextInt(addrs.size());
						rw.free(addrs.get(i), 1);
						addrs.set(i, rw.alloc(1+r.nextInt(500), null));
					}
					final long s4 = System.nanoTime();
					System.out.println("Test1 Alloc: " + (s4-s3) + "ns");
					System.out.println("File size: " + rw.getStoreFile().length());
				}
				

			} finally {

				store.destroy();

			}

		}

		/**
		 * Test of blob allocation, does not check on read back, just the
		 * allocation
		 */
		public void test_blob_allocs() {
//			if (false) {
//				return;
//			}

			final Journal store = (Journal) getStore();

			try {

			    final RWStrategy bufferStrategy = (RWStrategy) store.getBufferStrategy();

				final RWStore rw = bufferStrategy.getStore();
				final long numAllocs = rw.getTotalAllocations();
				final long startAllocations = rw.getTotalAllocationsSize();
				final int startBlob = 1024 * 256;
				final int endBlob = 1024 * 1256;
				final int[] faddrs = allocBatchBuffer(rw, 100, startBlob, endBlob);

                if (log.isInfoEnabled()) {
                    final StringBuilder str = new StringBuilder();
                    rw.showAllocators(str);
                    log.info(str);
				}
			} finally {

				store.destroy();

			}

		}

		/**
		 * Test of blob allocation and read-back, firstly from cache and then
		 * from disk.
		 * @throws InterruptedException 
		 */
		public void test_blob_readBack() {

			final Journal store = (Journal) getStore();

			try {
				final RWStrategy bs = (RWStrategy) store.getBufferStrategy();

				final RWStore rw = bs.getStore();

				final byte[] buf = new byte[2 * 1024 * 1024]; // 5Mb buffer of random
														// data
				r.nextBytes(buf);

				final ByteBuffer bb = ByteBuffer.wrap(buf);

				final long faddr = bs.write(bb); // rw.alloc(buf, buf.length);

				if(log.isInfoEnabled())log.info("Blob Allocation at " + rw.convertFromAddr(faddr));

				bb.position(0);

				ByteBuffer rdBuf = bs.read(faddr);

				assertEquals(bb, rdBuf);

				if(log.isInfoEnabled())log.info("Now commit to disk");

				store.commit();

				// Now reset - clears writeCache and reinits from disk
				rw.reset();
				
				// RWStore.reset() no longer resets the write cache, we need to do that explicitly!
				rw.getWriteCacheService().resetAndClear();

				rdBuf = bs.read(faddr);
				assertEquals(bb, rdBuf);

			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			} finally {

				store.destroy();

			}

		}
		
		public void test_stressBlobReadBack() {
			for (int i = 0; i < 100; i++) {
				test_blob_readBack();
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

				final RWStore rw = bs.getStore();

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

				if(log.isInfoEnabled())log.info("Now commit to disk (1)");

				store.commit();

				// Now reset - clears writeCache and reinits from disk
				rw.reset();

				// RWStore.reset() no longer resets the write cache, we need to do that explicitly!
				rw.getWriteCacheService().resetAndClear();
				rw.getWriteCacheService().setExtent(rw.getStoreFile().length());

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
					if(log.isInfoEnabled())log.info("Now commit to disk (2)");

					store.commit();

					Thread.currentThread().sleep(10); // to force deferredFrees

					// Request release of deferred frees.
					rw.checkDeferredFrees(/*true freeNow */ store);
					
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
            try {

			final RWStrategy bs = (RWStrategy) store.getBufferStrategy();

			final RWStore rw = bs.getStore();
			long realAddr = 0;
				for (int i = 0; i < 100000; i++) {
					int allocAddr = rw.metaAlloc();

					realAddr = rw.metaBit2Addr(allocAddr);
				}
				if(log.isInfoEnabled())log.info("metaAlloc lastAddr: " + realAddr);
			} finally {
				store.destroy();
			}
		}

		public void test_multiVoidCommit() {

			final Journal store = (Journal) getStore();
            try {

			final RWStrategy bs = (RWStrategy) store.getBufferStrategy();

			final RWStore rw = bs.getStore();
			
			final boolean initRequire = bs.requiresCommit(store.getRootBlockView());
			assertTrue(initRequire);
			for (int n = 0; n < 20; n++) {
				store.commit();
				final IRootBlockView rbv = store.getRootBlockView();
				assertTrue(1 == rbv.getCommitCounter());
				assertFalse(bs.requiresCommit(store.getRootBlockView()));
				assertFalse(rw.requiresCommit());
			}
			} finally {
				store.destroy();
			}
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
			final Journal store = (Journal) getStore();
			try {
			    final RWStrategy bs = (RWStrategy) store.getBufferStrategy();

				final RWStore rw = bs.getStore();

				// JournalShadow shadow = new JournalShadow(store);

				// Create a couple of contexts
				final IAllocationContext allocContext1 = rw.newAllocationContext(true);
				final IAllocationContext allocContext2 = rw.newAllocationContext(true);

				final int sze = 650;
				final byte[] buf = new byte[sze + 4]; // extra for checksum
				r.nextBytes(buf);

				final long addr1a = bs.write(ByteBuffer.wrap(buf), allocContext1);
				final long addr1b = bs.write(ByteBuffer.wrap(buf), allocContext1);
				rw.detachContext(allocContext1);

				final long addr2a = bs.write(ByteBuffer.wrap(buf), allocContext2);
				final long addr2b = bs.write(ByteBuffer.wrap(buf), allocContext2);
				rw.detachContext(allocContext2);

				// Attempt to re-establish context
				try {
					bs.write(ByteBuffer.wrap(buf), allocContext1);
					fail("Should have failed to re-use detached context");
				} catch (IllegalStateException ise) {
					// Expected
				}
				
				final IAllocationContext allocContext3 = rw.newAllocationContext(true);
				final long addr1c = bs.write(ByteBuffer.wrap(buf), allocContext3);

				// By detaching contexts we end up using the same allocator
				assertTrue("allocator re-use", bs.getPhysicalAddress(addr1c) > bs.getPhysicalAddress(addr2b));

				// Now, prior to commit, try deleting an uncommitted allocation
				bs.delete(addr1c, allocContext3);
				// and re-allocating it from the same context
				final long addr1d = bs.write(ByteBuffer.wrap(buf), allocContext3);

				assertTrue("re-allocation", addr1c == addr1d);

				rw.detachContext(allocContext3);

				// Now commit
				store.commit();

				// now try deleting and re-allocating again, but in a global
				// context
				bs.delete(addr1d); // this should call deferFree
				final long addr1e = bs.write(ByteBuffer.wrap(buf));

				assertTrue("deferred-delete", addr1e != addr1d);

				// Now commit
				store.commit();
			} finally {
				store.destroy();
			}

		}

		public void test_stressCommitIndexWithRetention() {

            assertEquals(1000,doStressCommitIndex(40000L /* 40 secs history */, 1000));


		}
		
		public void test_stressCommitIndexNoRetention() {
			
			assertEquals(2,doStressCommitIndex(0L /* no history */, 1000));

		}
		
		public void test_stressCommit() {
			Journal journal = (Journal) getStore(0); // remember no history!
			
			for (int i = 0; i < 1000; i++)
				commitSomeData(journal);

		}
		
		public int doStressCommitIndex(final long retention, final int runs) {
			final Journal journal = (Journal) getStore(retention); // remember no history!
			try {
				final int cRuns = runs;
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
		            
		            return records;
		        }
			} finally {
				journal.destroy();
			}

		}
		
		private void commitSomeData(final Journal store) {
			
		    final IRWStrategy bs = (IRWStrategy) store.getBufferStrategy();

//			final IStore rw = bs.getStore();

			// byte[] buf = new byte[r.nextInt(1024)]; // extra for checksum
			final byte[] buf = new byte[863]; // extra for checksum
			
			r.nextBytes(buf);

			bs.write(ByteBuffer.wrap(buf));

			store.commit();
		}

		public void test_stressAlloc() {

			Journal store = (Journal) getStore();
            try {

			final RWStrategy bs = (RWStrategy) store.getBufferStrategy();

			final RWStore rw = bs.getStore();
			// long realAddr = 0;
				// allocBatch(store, 1, 32, 650, 100000000);
				allocBatch(store, 1, 32, 650, 50000);
				store.commit();
				final StringBuilder str = new StringBuilder();
				rw.showAllocators(str);
				if(log.isInfoEnabled())log.info(str);
				store.close();
				if(log.isInfoEnabled())log.info("Re-open Journal");
				store = (Journal) getStore();

				showStore(store);
			} finally {
				store.destroy();
			}
		}

		public void test_snapshotData() throws IOException {
			final Journal journal = (Journal) getStore(0); // remember no history!

			try {
				for (int i = 0; i < 100; i++)
					commitSomeData(journal);

				final AtomicReference<IRootBlockView> rbv = new AtomicReference<IRootBlockView>();
				final Iterator<ISnapshotEntry> data = journal.snapshotAllocationData(rbv).entries();

				while (data.hasNext()) {
					final ISnapshotEntry e = data.next();
					
					log.info("Position: " + e.getAddress() + ", data size: "
							+ e.getData().length);
				}
			} finally {
				journal.destroy();
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
			try {
			final RWStrategy bs = (RWStrategy) store.getBufferStrategy();
			final RWStore rw = bs.getStore();

			final byte[] buf = new byte[300]; // Just some data
			r.nextBytes(buf);

			final ByteBuffer bb = ByteBuffer.wrap(buf);

			long faddr = bs.write(bb); // rw.alloc(buf, buf.length);

			IRawTx tx = rw.newTx();

			bs.delete(faddr); // delettion protected by session

			bb.position(0);

			final ByteBuffer rdBuf = bs.read(faddr);

			// should be able to successfully read from freed address
			assertEquals(bb, rdBuf);

			tx.close();

			store.commit();
			} finally {
			    store.destroy();
			}
			
		}

		/**
		 * To stress the session protection, we will allocate a batch of
		 * addresses, then free half with protection.  Then reallocate half
		 * again after releasing the session.
		 * This should result in all the original batch being allocated,
		 * exercising both session protection and write cache clearing
		 */
		public void test_stressSessionProtection() {
			stressSessionProtection(0); // no small slots
			stressSessionProtection(1024); // standard small slots
		}

		void stressSessionProtection(final int smallSlotSize) {
			// ensure smallslots WILL recycle "high-waste" immediately for even single allocator
			final Journal store = (Journal) getSmallSlotStore(smallSlotSize, 1, 0.2f);
			try {
			final RWStrategy bs = (RWStrategy) store.getBufferStrategy();
			final RWStore rw = bs.getStore();

			final byte[] buf = new byte[300]; // Just some data
			r.nextBytes(buf);

			final ByteBuffer bb = ByteBuffer.wrap(buf);
			
			final IRawTx tx = rw.newTx();
			final ArrayList<Long> addrs = new ArrayList<Long>();
			
			// We just want to stress a single allocator, so make 5000
			// allocations to force multiple allocBlocks.
			
			for (int i = 0; i < 10000; i++) {
				addrs.add(bs.write(bb));
				bb.flip();
			}
			
			final Bucket bucket =  findBucket(rw, buf.length+4); // allow for checksum
			
			store.commit();

			for (int i = 0; i < addrs.size(); i+=2) {
				bs.delete(addrs.get(i));
			}

			store.commit();

			for (int i = 0; i < addrs.size(); i++) {
				bs.write(bb);
				bb.flip();
			}

			
			{
				long used = bucket.usedSlots();
				long reserved = bucket.reservedSlots();
	
				assertTrue(reserved > used);
				assertTrue(reserved-used > (addrs.size()/2)); // must not have reclaimed recycled slots
			}

			// now release session to make addrs reavailable
			tx.close();
			
			store.commit();
			
			for (int i = 0; i < addrs.size(); i++) {
				bs.write(bb);
				bb.flip();
			}

			store.commit();
			
			{
				long used = bucket.usedSlots();
				long reserved = bucket.reservedSlots();
	
				assertTrue(reserved > used);

				// check should be okay for small slots if high waste check is always triggered!
				assertTrue(reserved-used < (addrs.size()/2)); // must have reclaimed recycled slots
			}

			} finally {
			    store.destroy();
			}
		}
		
		private Bucket findBucket(RWStore rws, final int dataSize) {
			final StorageStats stats = rws.getStorageStats();
			
			final Iterator<Bucket> buckets = stats.getBuckets();
			while (buckets.hasNext()) {
				final Bucket bucket = buckets.next();
				
				if (bucket.m_size > dataSize) {
					return bucket;
				}
			}
			
			return null;
		}
		
		private int getTotalAllocatedSlots(RWStore rws) {
			final StorageStats stats = rws.getStorageStats();
			
			final Iterator<Bucket> buckets = stats.getBuckets();
			
			int allocated = 0;
			while (buckets.hasNext()) {
				allocated += buckets.next().usedSlots();
			}
			
			return allocated;
		}
		
		private int getTotalReservedSlots(RWStore rws) {
			final StorageStats stats = rws.getStorageStats();
			
			final Iterator<Bucket> buckets = stats.getBuckets();
			
			int reserved = 0;
			while (buckets.hasNext()) {
				reserved += buckets.next().reservedSlots();
			}
			
			return reserved;
		}
		
		/**
		 * Test low level RWStore add/removeAddress methods as used in HA
		 * WriteCache replication to ensure Allocation consistency
		 * 
		 * @throws IOException
		 */
		public void testSimpleReplication() throws IOException {

			// Create a couple of stores with temp files
			Journal store1 = (Journal) getStore();
			Journal store2 = (Journal) getStore();
			
			final RWStore rw1 = ((RWStrategy) store1.getBufferStrategy()).getStore();
			final RWStore rw2 = ((RWStrategy) store2.getBufferStrategy()).getStore();

			assertTrue(rw1 != rw2);
			
			final int addr1 = rw1.alloc(123, null);
			rw2.addAddress(addr1, 123);
			assertTrue(rw1.physicalAddress(addr1) == rw2.physicalAddress(addr1));
			
			rw1.free(addr1, 123);
			rw2.removeAddress(addr1);
			
			// address will still be valid
			assertTrue(rw1.physicalAddress(addr1) == rw2.physicalAddress(addr1));
			
			// confirm address re-cycled
			assert(addr1 == rw1.alloc(123, null));
			
			// and can be "re-added"
			rw2.addAddress(addr1, 123);
		}

		/**
		 * Test low level RWStore add/removeAddress methods as used in HA
		 * WriteCache replication to ensure Allocation consistency
		 * 
		 * @throws IOException
		 */
		public void testStressReplication() throws IOException {

			// Create a couple of stores with temp files
			final Journal store1 = (Journal) getStore();
			final Journal store2 = (Journal) getStore();
			try {		
				final RWStore rw1 = ((RWStrategy) store1.getBufferStrategy()).getStore();
				final RWStore rw2 = ((RWStrategy) store2.getBufferStrategy()).getStore();
	
				assertTrue(rw1 != rw2);
				
				final Random r = new Random();
				
				for (int i = 0; i < 100000; i++) {
					final int sze = 1 + r.nextInt(2000);
					final int addr = rw1.alloc(sze, null);
					rw2.addAddress(addr, sze);
				}
			} finally {
				store1.destroy();
				store2.destroy();
			}
		}

		/**
		 * The pureAlloc test is to test the allocation aspect of the memory
		 * management rather than worrying about writing the data
		 */
		public void test_pureAlloc() {

			Journal store = (Journal) getStore();
            try {

			final RWStrategy bs = (RWStrategy) store.getBufferStrategy();

			final RWStore rw = bs.getStore();
			long realAddr = 0;
				// allocBatch(store, 1, 32, 650, 100000000);
				pureAllocBatch(store, 1, 32, rw.m_maxFixedAlloc - 4, 30000); // cover
																				// wider
																				// range
																				// of
																				// blocks
				store.commit();
				if(log.isInfoEnabled())log.info("Final allocations: " + rw.getTotalAllocations() + ", allocated bytes: "
						+ rw.getTotalAllocationsSize() + ", file length: " + rw.getStoreFile().length());
				store.close();
				if(log.isInfoEnabled())log.info("Re-open Journal");
				store = (Journal) getStore();

				showStore(store);
			} finally {
				store.destroy();
			}
		}

		private long allocBatch(Journal store, int tsts, int min, int sze, int grp) {

			final RWStrategy bs = (RWStrategy) store.getBufferStrategy();

			final byte[] buf = new byte[sze + 4]; // extra for checksum
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
		private long pureAllocBatch(final Journal store, final int tsts, int min, int sze, int grp) {

		    final RWStrategy bs = (RWStrategy) store.getBufferStrategy();

			final RWStore rw = bs.getStore();
			final int freeAddr[] = new int[512];
			int freeCurs = 0;
			for (int i = 0; i < grp; i++) {
				final int alloc = min + r.nextInt(sze - min);
				final int addr = rw.alloc(alloc, null);

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
		
		/**
		 * Simple state tests.
		 * 
		 * These are written to confirm the allocation bit states for a number of scenarios.
		 */
		
		/**
		 * State1
		 * 
		 * Allocate - Commit - Free
		 * 
		 * assert that allocation remains committed
		 */
		public void test_allocCommitFree() {
			Journal store = (Journal) getStore();
            try {

            	RWStrategy bs = (RWStrategy) store.getBufferStrategy();
            	
            	final long addr = bs.write(randomData(78));
            	
            	store.commit();
            	
            	bs.delete(addr);
            	
            	assertTrue(bs.isCommitted(addr));
            } finally {
            	store.destroy();
            }
		}

        /**
         * Verify that we correctly restore the RWStore commit state if
         * {@link RWStore#commit()} is followed by {@link RWStore#reset()}
         * rather than {@link RWStore#postCommit()}.
         * 
         * @see <a href="http://trac.blazegraph.com/ticket/973" >RWStore commit is
         *      not robust to internal failure.</a>
         */
		public void test_commitState() {
			Journal store = (Journal) getStore();
            try {

            	final RWStrategy bs = (RWStrategy) store.getBufferStrategy();
            	
            	final RWStore rws = bs.getStore();
            	
            	final long addr = bs.write(randomData(78));

            	// do 1st half of the RWStore commit protocol.
            	rws.commit();
            	            	
            	// then discard write set.
            	store.abort();
            	
            	assertFalse(bs.isCommitted(addr)); // rolled back
            	
            	// now cycle standard commit to confirm correct reset
            	for (int c = 0; c < 50; c++) {
            		bs.write(randomData(78));
            		store.commit();
            	}
           	
            	
            } finally {
            	store.destroy();
            }
		}
		
        /**
         * Test verifies that a failure to retain the commit state in
         * {@link RWStore#commit()} will cause problems if the write set is
         * discarded by {@link RWStore#reset()} such that subsequent write sets
         * run into persistent addressing errors.
         * 
         * @see <a href="http://trac.blazegraph.com/ticket/973" >RWStore commit is
         *      not robust to internal failure.</a>
         */
		public void test_commitStateError() {
			final Journal store = (Journal) getStore();
            try {

            	final RWStrategy bs = (RWStrategy) store.getBufferStrategy();
            	
            	final RWStore rws = bs.getStore();
            	
            	final long addr = bs.write(randomData(78));
            	
            	// do first half of the RWStore protocol.
            	rws.commit();

            /*
             * remove the commit state such that subsequent abort()/reset()
             * will fail to correctly restore the pre-commit state.
             */
            rws.clearCommitStateRef();

            // abort() succeeds because it is allowed even if commit() was
            // not called.
            	store.abort();
            	
            	assertFalse(bs.isCommitted(addr)); // rolled back
            	
            	try {
	            	// now cycle standard commit to force an error from bad reset
	            	for (int c = 0; c < 50; c++) {
	            		bs.write(randomData(78));
	            		store.commit();
	            	}
	            	fail("Expected failure");
            	} catch (Exception e) {
            		// expected
            		log.info("Expected!");
            	}
           	
            } finally {
            	store.destroy();
            }
		}
		
        /**
         * Verify that a double-commit causes an illegal state exception.
         * Further verify that an {@link RWStore#reset()} allwos us to then
         * apply and commit new write sets.
         * 
         * @see <a href="http://trac.blazegraph.com/ticket/973" >RWStore commit is
         *      not robust to internal failure.</a>
         */
		public void test_commitStateIllegal() {
			final Journal store = (Journal) getStore();
            try {

            final RWStrategy bs = (RWStrategy) store.getBufferStrategy();
            	
            	final RWStore rws = bs.getStore();
            	
            	bs.write(randomData(78));
            	
            	rws.commit();
            	
            	try {
            		store.commit();
            		
            		fail("Expected failure");
            	} catch (Exception ise) {
            		if (InnerCause.isInnerCause(ise, IllegalStateException.class)) {
	            		store.abort();
	            		
	            		store.commit();
            		} else {
            			fail("Unexpected Exception");
            		}
            	}
           	
            	
            } finally {
            	store.destroy();
            }
		}
		
		public void test_allocCommitFreeWithHistory() {
			final Journal store = (Journal) getStore(4);
            try {

            	final RWStrategy bs = (RWStrategy) store.getBufferStrategy();
            	
            	final long addr = bs.write(randomData(78));
            	
            	store.commit();
            	
            	bs.delete(addr);
            	
            	assertTrue(bs.isCommitted(addr));
            } finally {
            	store.destroy();
            }
		}
		
		/**
		 * This test releases over a blobs worth of deferred frees
		 */
		public void test_blobDeferredFrees() {
			doBlobDeferredFrees(4000); // standard
		}
		
		/**
		 * This is the test that was able to reproduce the recycler failure for
		 * BLZG-1236 when run with 10M deferred frees.
		 * 
		 * @see BLZG-1236 (recycler error)
		 */
		public void test_stressBlobDeferredFrees() {
			doBlobDeferredFrees(10000000); // 10M (40M data)
		}
		
		void doBlobDeferredFrees(final int cAddrs) {

			final Properties properties = new Properties(getProperties());

            properties.setProperty(
                    AbstractTransactionService.Options.MIN_RELEASE_AGE, "4000");

            properties.setProperty(RWStore.Options.ALLOCATION_SIZES,
                    "1,2,3,5,8,12,16"); // 1K

			final Journal store = (Journal) getStore(properties);
            try {

            	final RWStrategy bs = (RWStrategy) store.getBufferStrategy();
            	
            	final ArrayList<Long> addrs = new ArrayList<Long>();
            	for (int i = 0; i < cAddrs; i++) {
            		addrs.add(bs.write(randomData(45)));
            	}
            	store.commit();

            	for (long addr : addrs) {
            		bs.delete(addr);
            	}
                for (int i = 0; i < cAddrs; i++) {
                    if(!bs.isCommitted(addrs.get(i))) {
                        fail("i="+i+", addr="+addrs.get(i));
                    }
                }

               	store.commit();
               	
            	// Age the history (of the deletes!)
            	Thread.currentThread().sleep(6000);
            	
            	// modify store but do not allocate similar size block
            	// as that we want to see has been removed
               	final long addr2 = bs.write(randomData(220)); // modify store
            	
            	store.commit();
            	bs.delete(addr2); // modify store
               	store.commit();
            	
               	// delete is actioned
            	for (int i = 0; i < 4000; i++) {
                   	assertFalse(bs.isCommitted(addrs.get(i)));
            	}
              } catch (InterruptedException e) {
			} finally {
            	store.destroy();
            }
		}
		
		/**
		 * This test releases over a blobs worth of deferred frees where the
		 * blob requires a blob header.
		 * 
		 * To test this we use a max fixed alloc of 192 bytes, so 24 8 bytes addresses can be stored
		 * in each "leaf" and 48 leaves can be addressed in a standard blob header.
		 * 
		 * This should set the allocation threshold at 48 * 24 = 1152 deferred addresses
		 * 
		 * Anything greater than that requires that the blob header itself is a blob and this is
		 * what we need to test.
		 */
		public void test_stressBlobBlobHeaderDeferredFrees() {

            final Properties properties = new Properties(getProperties());

            properties.setProperty(
                    AbstractTransactionService.Options.MIN_RELEASE_AGE, "4000");

            final int maxFixed = 3; // 192 bytes (3 * 64)
            properties.setProperty(RWStore.Options.ALLOCATION_SIZES,
            		"1,2," + maxFixed);
            
            final int maxAlloc = maxFixed * 64;
            final int leafEntries = maxAlloc / 8;
            final int headerEntries = maxAlloc / 4;
            final int threshold = headerEntries * leafEntries;
            
            final int nallocs = threshold << 3; // 8 times greater than allocation threshold

			Journal store = (Journal) getStore(properties);
            try {

            	RWStrategy bs = (RWStrategy) store.getBufferStrategy();
            	
            	ArrayList<Long> addrs = new ArrayList<Long>();
            	for (int i = 0; i < nallocs; i++) {
            		addrs.add(bs.write(randomData(45)));
            	}
            	store.commit();

            	for (long addr : addrs) {
            		bs.delete(addr);
            	}
                for (int i = 0; i < nallocs; i++) {
                    if(!bs.isCommitted(addrs.get(i))) {
                        fail("i="+i+", addr="+addrs.get(i));
                    }
                }

               	store.commit();
               	
            	// Age the history (of the deletes!)
            	Thread.currentThread().sleep(6000);
            	
            	// modify store but do not allocate similar size block
            	// as that we want to see has been removed
               	final long addr2 = bs.write(randomData(220)); // modify store
            	
            	store.commit();
            	bs.delete(addr2); // modify store
               	store.commit();
            	
               	// delete is actioned
            	for (int i = 0; i < nallocs; i++) {
                   	assertFalse(bs.isCommitted(addrs.get(i)));
            	}
              } catch (InterruptedException e) {
			} finally {
            	store.destroy();
            }
		}
		
		/**
		 * Repeats the BlobBlobHeader of deferred frees, but also with blob data.
		 */
		public void test_stressBlobBlobHeaderBlobDataDeferredFrees() {

            final Properties properties = new Properties(getProperties());

            properties.setProperty(
                    AbstractTransactionService.Options.MIN_RELEASE_AGE, "4000");

            final int maxFixed = 3; // 192 bytes (3 * 64)
            properties.setProperty(RWStore.Options.ALLOCATION_SIZES,
            		"1,2," + maxFixed);
            
            final int maxAlloc = maxFixed * 64;
            final int leafEntries = maxAlloc / 8;
            final int headerEntries = maxAlloc / 4;
            final int threshold = headerEntries * leafEntries;
            
            final int nallocs = threshold << 3; // 8 times greater than allocation threshold

			Journal store = (Journal) getStore(properties);
            try {

            	RWStrategy bs = (RWStrategy) store.getBufferStrategy();
            	
            	ArrayList<Long> addrs = new ArrayList<Long>();
            	for (int i = 0; i < nallocs; i++) {
            		addrs.add(bs.write(randomData(1024))); // mostly blob data
            	}
            	store.commit();

            	for (long addr : addrs) {
            		bs.delete(addr);
            	}
                for (int i = 0; i < nallocs; i++) {
                    if(!bs.isCommitted(addrs.get(i))) {
                        fail("i="+i+", addr="+addrs.get(i));
                    }
                }

               	store.commit();
               	
            	// Age the history (of the deletes!)
            	Thread.currentThread().sleep(6000);
            	
            	// modify store but do not allocate similar size block
            	// as that we want to see has been removed
               	final long addr2 = bs.write(randomData(1024)); // modify store
            	
            	store.commit();
            	bs.delete(addr2); // modify store
               	store.commit();
            	
               	// delete is actioned
            	for (int i = 0; i < nallocs; i++) {
                   	assertFalse(bs.isCommitted(addrs.get(i)));
            	}
              } catch (InterruptedException e) {
			} finally {
            	store.destroy();
            }
		}
		
		/**
		 * Can be tested by removing RWStore call to journal.removeCommitRecordEntries
		 * in freeDeferrals.
		 * 
		 * final int commitPointsRemoved = journal.removeCommitRecordEntries(fromKey, toKey);
		 * 
		 * replaced with
		 * 
		 * final int commitPointsRemoved = commitPointsRecycled;
		 * 
		 */
		public void testVerifyCommitRecordIndex() {
            final Properties properties = new Properties(getProperties());

            properties.setProperty(
                    AbstractTransactionService.Options.MIN_RELEASE_AGE, "100");

			final Journal store = (Journal) getStore(properties);
            try {

            	RWStrategy bs = (RWStrategy) store.getBufferStrategy();
            	      	
	        	for (int r = 0; r < 10; r++) {
		        	ArrayList<Long> addrs = new ArrayList<Long>();
		        	for (int i = 0; i < 100; i++) {
		        		addrs.add(bs.write(randomData(45)));
		        	}
		        	store.commit();
		
		        	for (long addr : addrs) {
		        		bs.delete(addr);
		        	}
		
		           	store.commit();
		           	
		        	// Age the history (of the deletes!)
		        	Thread.currentThread().sleep(200);
	        	}
	        	
				final String fname = bs.getStore().getStoreFile().getAbsolutePath();
				
	        	store.close();
	        	
	        	VerifyCommitRecordIndex.main(new String[]{fname});
        	
            } catch (InterruptedException e) {
			} finally {
            	store.destroy();
            }
		}
		
		public void testResetHARootBlock() {
            final Properties properties = new Properties(getProperties());

 			final Journal store = (Journal) getStore(properties);
            try {

            	final RWStrategy bs = (RWStrategy) store.getBufferStrategy();
            	final RWStore rw = bs.getStore();
            	
 	        	for (int r = 0; r < 10; r++) {
		        	ArrayList<Long> addrs = new ArrayList<Long>();
		        	for (int i = 0; i < 1000; i++) {
		        		addrs.add(bs.write(randomData(2048)));
		        	}
		        	store.commit();
		        	
		        	final StorageStats stats1 = rw.getStorageStats();
		        	
		        	rw.resetFromHARootBlock(store.getRootBlockView());
		        	
		        	final StorageStats stats2 = rw.getStorageStats();

		        	// Now check that we can read all allocations after reset
		        	for (long addr : addrs) {
		        		store.read(addr);
		        	}
		
	        	}
	        	
				final String fname = bs.getStore().getStoreFile().getAbsolutePath();
				
	        	store.close();
	        	
 			} finally {
            	store.destroy();
            }
		}
		
		public void testSimpleReset() {
            final Properties properties = new Properties(getProperties());

 			final Journal store = (Journal) getStore(properties);
            try {

            	final RWStrategy bs = (RWStrategy) store.getBufferStrategy();
            	final RWStore rw = bs.getStore();
            	
 	        	for (int r = 0; r < 10; r++) {
		        	ArrayList<Long> addrs = new ArrayList<Long>();
		        	for (int i = 0; i < 1000; i++) {
		        		addrs.add(bs.write(randomData(2048)));
		        	}
		        	
		        	rw.reset();
		        	
	        	}
	        	
				final String fname = bs.getStore().getStoreFile().getAbsolutePath();
				
	        	store.close();
	        	
 			} finally {
            	store.destroy();
            }
		}
		
		

		private Journal getStore(Properties props) {
			return new Journal(props);
		}

		/**
		 * State2
		 * 
		 * Allocate - Commit - Free - Commit
		 * 
		 * assert that allocation is no longer committed
		 */
		public void test_allocCommitFreeCommit() {
			Journal store = (Journal) getStore();
            try {

            	RWStrategy bs = (RWStrategy) store.getBufferStrategy();
            	
            	final long addr = bs.write(randomData(78));
            	
            	store.commit();
            	
            	bs.delete(addr);
            	
            	assertTrue(bs.isCommitted(addr));

            	store.commit();

            	assertFalse(bs.isCommitted(addr));
            } finally {
            	store.destroy();
            }
		}
		
		/**
		 * In order to see deferred recycling we need to make two
		 * commits (with modifications) after the retention period
		 * has expired.
		 * 
		 * To be able to check for the address release, we must ensure that
		 * the same address cannot be re-allocated, so a different size
		 * allocation is requested.
		 */
		public void test_allocCommitFreeCommitWithHistory() {
			Journal store = (Journal) getStore(4);
            try {

            	RWStrategy bs = (RWStrategy) store.getBufferStrategy();
            	
            	final long addr = bs.write(randomData(78));
            	
            	store.commit();
            	
            	bs.delete(addr);
            	
            	assertTrue(bs.isCommitted(addr));

               	store.commit();

            	// delete is deferred
            	assertTrue(bs.isCommitted(addr));
            	
            	Thread.currentThread().sleep(5000);
            	
            	// modify store but do not allocate similar size block
            	// as that we want to see has been removed
               	final long addr2 = bs.write(randomData(220)); // modify store
            	
            	store.commit();
            	bs.delete(addr2); // modify store
               	store.commit();
            	
               	// delete is actioned
            	assertFalse(bs.isCommitted(addr));
            	
            } catch (InterruptedException e) {

			} finally {
            	store.destroy();
            }
		}
		
		public void test_allocBlobCommitFreeCommitWithHistory() {
			Journal store = (Journal) getStore(4000);
            try {

            	final RWStrategy bs = (RWStrategy) store.getBufferStrategy();
            	
            	final long addr = bs.write(randomData(253728)); // BLOB
            	
            	store.commit();
            	
            	bs.delete(addr);
            	
            	assertTrue(bs.isCommitted(addr));

               	store.commit();

            	// delete is deferred
            	assertTrue(bs.isCommitted(addr));
            	
            	Thread.currentThread().sleep(5000);
            	
            	// modify store but do not allocate similar size block
            	// as that we want to see has been removed
               	final long addr2 = bs.write(randomData(220)); // modify store
            	
            	store.commit();
            	bs.delete(addr2); // modify store
               	store.commit();
            	
               	// delete is actioned
            	assertFalse(bs.isCommitted(addr));
            	
            } catch (InterruptedException e) {

			} finally {
            	store.destroy();
            }
		}
		
		public void test_allocBlobBoundariesCommitFreeCommitWithHistory() {
			final Journal store = (Journal) getStore(5);
            try {

            	final RWStrategy bs = (RWStrategy) store.getBufferStrategy();
            	
            	if (false) {
	                final int n = 1000000;
	                final BloomFilter filter = new BloomFilter(n, 0.02d, n);
	                filter.add(randomBytes(12));
	                final long addrb = filter.write(store);
	                
					if (log.isInfoEnabled())
						log.info("Bloomfilter: " + ((int) addrb));
            	}
            	
               	final long addr = bs.write(randomData(4088)); // BLOB 2 * 2044
               	final long addr1 = bs.write(randomData(4089)); // BLOB 2 * 2044
               	final long addr3 = bs.write(randomData(4087)); // BLOB 2 * 2044
            	
            	store.commit();
            	
               	bs.delete(addr);
               	bs.delete(addr1);
               	bs.delete(addr3);
            	
            	assertTrue(bs.isCommitted(addr));

               	store.commit();

            	// delete is deferred
            	assertTrue(bs.isCommitted(addr));
            	
            	Thread.currentThread().sleep(5000);
            	
            	// modify store but do not allocate similar size block
            	// as that we want to see has been removed
               	final long addr2 = bs.write(randomData(220)); // modify store
            	
            	store.commit();
            	bs.delete(addr2); // modify store
               	store.commit();
            	
               	// delete is actioned
            	assertFalse(bs.isCommitted(addr));
            	
            } catch (InterruptedException e) {

			} finally {
            	store.destroy();
            }
		}
		
		/**
		 * State3
		 * 
		 * Allocate - Commit - Free - Commit
		 * 
		 * Tracks writeCache state through allocation
		 */
		public void test_allocCommitFreeCommitWriteCache() {
			Journal store = (Journal) getStore();
            try {
            	RWStrategy bs = (RWStrategy) store.getBufferStrategy();
            	
            	final long addr = bs.write(randomData(78));
            	
            	// Has just been written so must be in cache
            	assertTrue(bs.inWriteCache(addr));
            	
            	store.commit();
            	
            	bs.delete(addr);
            	
            	// since data is committed, should be accessible from any new
            	// readCommitted transaction
            	assertTrue(bs.inWriteCache(addr));

            	store.commit();
            	
            	// If no transactions are around (which they are not) then must
            	//	be released as may be re-allocated
            	assertFalse(bs.inWriteCache(addr));
            	
            } finally {
            	store.destroy();
            }
		}
		
		public void test_allocCommitFreeCommitWriteCacheWithHistory() {
			final Journal store = (Journal) getStore(5);
            try {
            	final RWStrategy bs = (RWStrategy) store.getBufferStrategy();
            	
            	final long addr = bs.write(randomData(78));
            	
            	// Has just been written so must be in cache
            	assertTrue(bs.inWriteCache(addr));
            	
            	store.commit();
            	
            	bs.delete(addr);
            	
            	// since data is committed, should be accessible from any new
            	// readCommitted transaction
            	assertTrue(bs.inWriteCache(addr));

            	store.commit();
            	
            	// Since state is retained, the delete is deferred
            	assertTrue(bs.inWriteCache(addr));
            	
            } finally {
            	store.destroy();
            }
		}
		
		/**
		 * State4
		 * 
		 * Allocate - Commit - Free - Commit
		 * 
		 * ..but with session protection using a RawTx
		 */
		public void test_allocCommitFreeCommitSessionWriteCache() {
			final Journal store = (Journal) getStore();
            try {
            	final RWStrategy bs = (RWStrategy) store.getBufferStrategy();
            	
            	final long addr = bs.write(randomData(78));
            	
            	// Has just been written so must be in cache
            	assertTrue(bs.inWriteCache(addr));
            	
            	store.commit();
            	
            	IRawTx tx = bs.newTx();
            	
            	bs.delete(addr);
            	
            	// since data is committed, should be accessible from any new
            	// readCommitted transaction
            	assertTrue(bs.inWriteCache(addr));

            	store.commit();
            	
            	// Since transactions are around the cache will not have been
            	//	cleared (and cannot be re-allocated)
            	assertTrue(bs.inWriteCache(addr));
            	
            	tx.close(); // Release Session
            	
            	// Cache must have been cleared
            	assertFalse(bs.inWriteCache(addr));
            	
            } finally {
            	store.destroy();
            }
		}
		
		/**
		 * State5
		 * 
		 * Allocate - Commit - Free - Commit
		 * 
		 * Tracks writeCache state through allocation
		 */
		public void test_allocCommitFreeCommitAllocSessionWriteCache() {
			final Journal store = (Journal) getStore();
            try {
            	final RWStrategy bs = (RWStrategy) store.getBufferStrategy();
            	
            	final long addr = bs.write(randomData(78));
            	
            	// Has just been written so must be in cache
            	assertTrue(bs.inWriteCache(addr));
            	
            	store.commit();
            	
            	IRawTx tx = bs.newTx();
            	
            	bs.delete(addr);
            	
            	// since data is committed, should be accessible from any new
            	// readCommitted transaction
            	assertTrue(bs.inWriteCache(addr));

            	store.commit();
            	
            	// Since transactions are around the cache will not have been
            	//	cleared (and cannot be re-allocated)
            	assertTrue(bs.inWriteCache(addr));
            	
            	tx.close(); // Release Session
            	
            	// Cache must have been cleared
            	assertFalse(bs.inWriteCache(addr));
            	
            	final long addr2 = bs.write(randomData(78));
            	
            	assertTrue(addr2 == addr);
            	
            	assertTrue(bs.inWriteCache(addr));
            	
            	store.abort(); // removes uncommitted data
            	
            	assertFalse(bs.inWriteCache(addr));

            } finally {
            	store.destroy();
            }
		}
		
		/**
		 * Tests semantics of a simple reset
		 * 
		 * Commit some data
		 * Delete committed and allocate new data
		 * Reset
		 * Test that deletion and new allocation are void
		 */
		public void test_simpleReset() {
			final Journal store = (Journal) getStore();
	        try {
	        	final RWStrategy bs = (RWStrategy) store.getBufferStrategy();
	        	
	        	final long addr = bs.write(randomData(78));
	        	
	        	// Has just been written so must be in cache
	        	assertTrue(bs.inWriteCache(addr));
	        	
	        	store.commit();
	        	
	        	bs.delete(addr);
	        	
	        	final long addr2 = bs.write(randomData(78));
	           	assertTrue(bs.inWriteCache(addr2));
	        	
	        	bs.abort();
	        	
	        	assertTrue(bs.inWriteCache(addr)); // not removed in reset
	           	assertFalse(bs.inWriteCache(addr2));
	        	try {
	        		bs.read(addr2);
	        		fail("Exception expected");
	        	} catch (IllegalArgumentException e) {
	        		// expected
	        	}
	        	store.commit();

	        	assertTrue(bs.isCommitted(addr));
	        } finally {
	        	store.destroy();
	        }
			
		}
		
		/**
		 * Tests semantics of a simple reset after reopen to emulate
		 * an HAJournal reopen
		 * 
		 * As for simple reset but also re-open, then write and abort.
		 * 
		 * @throws IOException 
		 */
		public void test_reopenReset() throws IOException {
			final Properties properties = new Properties(getProperties());
			final File tmpfile = File.createTempFile("TestRW", ".rw");
			properties.setProperty(Options.FILE, tmpfile.getAbsolutePath());
			properties.setProperty(Options.CREATE_TEMP_FILE,"false");
//			Journal store = new Journal(properties);
			Journal store = getStore(properties);
	        try {
	        	RWStrategy bs = (RWStrategy) store.getBufferStrategy();
	        	
	        	final long addr = bs.write(randomData(78));
	        	
	        	// Has just been written so must be in cache
	        	assertTrue(bs.inWriteCache(addr));
	        	
	        	store.commit();
	        	
	        	bs.delete(addr);
	        	
	        	final long addr2 = bs.write(randomData(78));
	           	assertTrue(bs.inWriteCache(addr2));
	        	
	        	bs.abort();
	        	
	        	assertTrue(bs.inWriteCache(addr)); // not removed in reset
	           	assertFalse(bs.inWriteCache(addr2));
	        	try {
	        		bs.read(addr2);
	        		fail("Exception expected");
	        	} catch (IllegalArgumentException e) {
	        		// expected
	        	}
	        	store.commit();

	        	assertTrue(bs.isCommitted(addr));
	        	
	        	store.close();
				store = new Journal(properties);
				bs = (RWStrategy) store.getBufferStrategy();
				
	        	final long addr3 = bs.write(randomData(78));
	           	assertTrue(bs.inWriteCache(addr3));
	        	
	        	bs.abort();
	        	
	        	assertFalse(bs.inWriteCache(addr3)); // not removed in reset

	        	
	        } finally {
	        	store.destroy();
	        }
			
		}
		
		/**
		 * Tests semantics of a simple isolated reset
		 * 
		 * Commit some data
		 * UnIsolated: Delete committed and allocate new data
		 * Isolated: Delete committed and allocate new data
		 * Reset
		 * Test that deletion and new allocation are void for
		 * unisolated actions but not isolated
		 */
		public void test_isolatedReset() {
			Journal store = (Journal) getStore();
	        try {
	        	RWStrategy bs = (RWStrategy) store.getBufferStrategy();
	        	final IAllocationContext isolated = bs.newAllocationContext(true);
	        	
	        	final long addr = bs.write(randomData(78));
	        	final long addr2 = bs.write(randomData(78));
	        	
	        	// Has just been written so must be in cache
	        	assertTrue(bs.inWriteCache(addr));
	        	
	        	store.commit();
	        	
	        	bs.delete(addr);
	        	
	        	final long addr3 = bs.write(randomData(78));
	           	assertTrue(bs.inWriteCache(addr3));
	           	
	        	bs.delete(addr2, isolated);
	        	final long addr4 = bs.write(randomData(78), isolated);
	           	assertTrue(bs.inWriteCache(addr4));
	           		        	
	        	bs.abort();
	        	
	        	assertTrue(bs.inWriteCache(addr)); // not removed in reset
	           	assertFalse(bs.inWriteCache(addr3)); // unisolated removed
	           	assertTrue(bs.inWriteCache(addr4)); // isolated remains
	        	try {
	        		bs.read(addr3);
	        		fail("Exception expected");
	        	} catch (IllegalArgumentException e) {
	        		// expected
	        	}
	        	
	        	bs.detachContext(isolated);
	        	
	        	store.commit();

	        	assertTrue(bs.isCommitted(addr));
	        	assertTrue(bs.isCommitted(addr4));
	        } finally {
	        	store.destroy();
	        }
			
		}
		
		/**
		 * Tests semantics of a more complex isolated reset
		 * 
		 * Primarily the same as the simple isolated but ensuring
		 * more unisolated interactions after isolation is
		 * established.
		 * 
		 * Commit some data
		 * UnIsolated: Delete committed and allocate new data
		 * Isolated: Delete committed and allocate new data
		 * UnIsolated: Delete committed and allocate new data
		 * Reset
		 * Test that deletion and new allocation are void for
		 * unisolated actions but not isolated
		 */
		public void test_notSoSimpleIsolatedReset() {
			Journal store = (Journal) getStore();
	        try {
	        	RWStrategy bs = (RWStrategy) store.getBufferStrategy();
	        	final IAllocationContext isolated = bs.newAllocationContext(true);
	        	
	        	final long addr = bs.write(randomData(78));
	        	final long addr2 = bs.write(randomData(78));
	        	final long addr5 = bs.write(randomData(78));
	        	
	        	// Has just been written so must be in cache
	        	assertTrue(bs.inWriteCache(addr));
	        	
	        	store.commit();
	        	
	        	// Unisolated actions
	        	bs.delete(addr);
	        	
	        	final long addr3 = bs.write(randomData(78));
	           	assertTrue(bs.inWriteCache(addr3));
	           	
	           	// Isolated actions
	        	bs.delete(addr2, isolated);
	        	final long addr4 = bs.write(randomData(78), isolated);
	           	assertTrue(bs.inWriteCache(addr4));
	           		        	
	        	// Further Unisolated actions
	        	bs.delete(addr5);
	        	final long addr6 = bs.write(randomData(78));
	           	assertTrue(bs.inWriteCache(addr6));
	           	
	        	bs.abort();
	        	
	        	assertTrue(bs.inWriteCache(addr)); // not removed in reset
	           	assertFalse(bs.inWriteCache(addr3)); // unisolated removed
	           	assertFalse(bs.inWriteCache(addr6)); // unisolated removed
	           	assertTrue(bs.inWriteCache(addr4)); // isolated remains
	        	try {
	        		bs.read(addr3);
	        		fail("Exception expected");
	        	} catch (IllegalArgumentException e) {
	        		// expected
	        	}
	        	try {
	        		bs.read(addr6);
	        		fail("Exception expected");
	        	} catch (IllegalArgumentException e) {
	        		// expected
	        	}
	        	
	        	// Detach isolated context prior to commit
	        	bs.detachContext(isolated);
	        	
	        	store.commit();

	        	assertTrue(bs.isCommitted(addr));
	        	assertTrue(bs.isCommitted(addr4));
	        	assertTrue(bs.isCommitted(addr5));
	        	assertFalse(bs.isCommitted(addr2));
	        } finally {
	        	store.destroy();
	        }
			
		}
		
		/**
		 * Concurrent readers should no longer be an issue now that
		 * reset() is not re-initializing from the root block.
		 * 
		 * This test should confirm that.
		 * 
		 * Establishes some committed data and runs two readers
		 * concurrent with a single writer intermittently aborting
		 * 
		 * @throws InterruptedException 
		 */
		public void test_simpleConcurrentReadersWithResets() throws InterruptedException {
			final Journal store = (Journal) getStore();
			// use executor service to enable exception trapping
			final ExecutorService es = store.getExecutorService();
	        try {
	        	final RWStrategy bs = (RWStrategy) store.getBufferStrategy();
	        	
	        	final long[] addrs = new long[50];
	        	for (int w = 0; w < addrs.length; w++) {
	        		addrs[w] = bs.write(randomData(r.nextInt(150)+10));
	        	}
	        	
	        	store.commit();
	        	
	        	assertTrue(bs.isCommitted(addrs[0]));
	        	
				Runnable writer = new Runnable() {
               @Override
	        		public void run() {
	        			for (int i = 0; i < 2000; i++) {
	        				bs.delete(addrs[r.nextInt(addrs.length)]);
	        				for (int w = 0; w < 1000; w++)
	        					bs.write(randomData(r.nextInt(500)+1));
	        				bs.abort();
	        			}
	        		}
	        	};
				final Future<?> wfuture = es.submit(writer);
				Runnable reader1 = new Runnable() {
               @Override
	        		public void run() {
	        			for (int i = 0; i < 5000; i++) {
	        				for (int rdr = 0; rdr < addrs.length; rdr++) {
	        					bs.read(addrs[r.nextInt(addrs.length)]);
	        				}
	        				try {
								Thread.sleep(1);
							} catch (InterruptedException e) {
								throw new RuntimeException(e);
							}
	        			}
	        		}
	        	};
	        	final Future<?> r1future = es.submit(reader1);
	        	Runnable reader2 = new Runnable() {
	        	   @Override
	        		public void run() {
	        			for (int i = 0; i < 5000; i++) {
	        				for (int rdr = 0; rdr < addrs.length; rdr++) {
	        					bs.read(addrs[r.nextInt(addrs.length)]);
	        				}
	        				try {
								Thread.sleep(1);
							} catch (InterruptedException e) {
								throw new RuntimeException(e);
							}
	        			}
	        		}
	        	};
	        	final Future<?> r2future = es.submit(reader2);
	        	
	        	try {
		        	wfuture.get();
		        	r1future.get();
		        	r2future.get();
	        	} catch (Exception e) {
	        		fail(e.getMessage(), e);
	        	}
	        	for (int i = 0; i < addrs.length; i++) {
	        		assertTrue(bs.isCommitted(addrs[i]));
	        	}
	        } finally {
	        	es.shutdownNow();
	        	store.destroy();
	        }			
		}
		
		ByteBuffer randomData(final int sze) {
			byte[] buf = new byte[sze + 4]; // extra for checksum
			r.nextBytes(buf);
			
			return ByteBuffer.wrap(buf, 0, sze);
		}
		
		byte[] randomBytes(final int sze) {
			byte[] buf = new byte[sze + 4]; // extra for checksum
			r.nextBytes(buf);
			
			return buf;
		}

	}
	
	/**
	 * Test suite integration for {@link AbstractMROWTestCase}.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 */
	public static class TestMROW extends AbstractMROWTestCase {

		public TestMROW() {
			super();
		}

		public TestMROW(String name) {
			super(name);
		}

      @Override
		protected IRawStore getStore() {

			final Properties properties = getProperties();

			properties.setProperty(Options.CREATE_TEMP_FILE, "true");

			properties.setProperty(Options.DELETE_ON_EXIT, "true");

			properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW.toString());

			properties.setProperty(Options.WRITE_CACHE_ENABLED, "" + writeCacheEnabled);

			return new Journal(properties);//.getBufferStrategy();

		}

	}

	/**
	 * Test suite integration for {@link AbstractMRMWTestCase}.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 */
	public static class TestMRMW extends AbstractMRMWTestCase {

		public TestMRMW() {
			super();
		}

		public TestMRMW(String name) {
			super(name);
		}

      @Override
		protected IRawStore getStore() {

			final Properties properties = getProperties();

			properties.setProperty(Options.CREATE_TEMP_FILE, "true");

			properties.setProperty(Options.DELETE_ON_EXIT, "true");

			properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW.toString());

			properties.setProperty(Options.WRITE_CACHE_ENABLED, "" + writeCacheEnabled);

			return new Journal(properties);//.getBufferStrategy();

		}

	}

	/**
	 * Test suite integration for {@link AbstractInterruptsTestCase}.
	 * 
	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
	 *         Thompson</a>
	 */
	public static class TestInterrupts extends AbstractInterruptsTestCase {
	
		public TestInterrupts() {
			super();
		}
	
		public TestInterrupts(String name) {
			super(name);
		}
	
      @Override
		protected IRawStore getStore() {
	
			final Properties properties = getProperties();
	
			properties.setProperty(Options.DELETE_ON_EXIT, "true");
	
			properties.setProperty(Options.CREATE_TEMP_FILE, "true");
	
			properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW.toString());
	
			properties.setProperty(Options.WRITE_CACHE_ENABLED, "" + writeCacheEnabled);
	
			return new Journal(properties);//.getBufferStrategy();
			// return new Journal(properties);
	
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
	private static final boolean writeCacheEnabled = true; // 512;

}
