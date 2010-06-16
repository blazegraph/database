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

import com.bigdata.journal.AbstractInterruptsTestCase;
import com.bigdata.journal.AbstractJournalTestCase;
import com.bigdata.journal.AbstractMRMWTestCase;
import com.bigdata.journal.AbstractMROWTestCase;
import com.bigdata.journal.AbstractRestartSafeTestCase;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.DiskOnlyStrategy;
import com.bigdata.journal.Journal;
import com.bigdata.journal.RWStrategy;
import com.bigdata.journal.TestJournalBasics;
import com.bigdata.journal.Journal.Options;
import com.bigdata.rawstore.AbstractRawStoreTestCase;
import com.bigdata.rawstore.IRawStore;

/**
 * Test suite for {@link BufferMode#DiskRW} journals.
 * 
 * TODO: must modify RWStore to use DirectBufferPool to allocate and release buffers, 
 * Once done then ensure the write cache is enabled when running test suite
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

        final TestRWJournal delegate = new TestRWJournal(); // !!!! THIS CLASS !!!!

        /*
         * Use a proxy test suite and specify the delegate.
         */

        final ProxyTestSuite suite = new ProxyTestSuite(delegate,
                "Disk RW Journal Test Suite");

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

        properties.setProperty(Options.CREATE_TEMP_FILE, "true");

        properties.setProperty(Options.DELETE_ON_EXIT, "true");

        properties.setProperty(Options.WRITE_CACHE_ENABLED, ""
                + writeCacheEnabled);

        return properties;

    }
    
    /**
     * Verify normal operation and basic assumptions when creating a new journal
     * using {@link BufferMode#DiskRW}.
     * 
     * @throws IOException
     */
    public void test_create_disk01() throws IOException {

        final Properties properties = getProperties();

        final Journal journal = new Journal(properties);

        try {

            final RWStrategy bufferStrategy = (RWStrategy) journal
                    .getBufferStrategy();

            assertTrue("isStable", bufferStrategy.isStable());
            assertFalse("isFullyBuffered", bufferStrategy.isFullyBuffered());
            // assertEquals(Options.FILE, properties.getProperty(Options.FILE),
            // bufferStrategy.file.toString());
            assertEquals(Options.INITIAL_EXTENT, Long
                    .parseLong(Options.DEFAULT_INITIAL_EXTENT), bufferStrategy
                    .getInitialExtent());
            assertEquals(Options.MAXIMUM_EXTENT,
                    0L/* soft limit for disk mode */, bufferStrategy
                            .getMaximumExtent());
            assertNotNull("raf", bufferStrategy.getRandomAccessFile());
            assertEquals(Options.BUFFER_MODE, BufferMode.DiskRW, bufferStrategy
                    .getBufferMode());

        } finally {

            journal.destroy();

        }

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

        properties.setProperty(Options.WRITE_CACHE_ENABLED, ""
                + writeCacheEnabled);

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
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
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
            
        }

//        /**
//         * Test that allocate() pre-extends the store when a record is allocated
//         * which would overflow the current user extent.
//         */
//        public void test_allocPreExtendsStore() {
//       
//            final Journal store = (Journal) getStore();
//
//            try {
//
//                final DiskOnlyStrategy bufferStrategy = (DiskOnlyStrategy) store
//                        .getBufferStrategy();
//
//                final long nextOffset = store.getRootBlockView()
//                        .getNextOffset();
//
//                final long length = store.size();
//
//                final long headerSize = FileMetadata.headerSize0;
//
//                // #of bytes remaining in the user extent before overflow.
//                final long nfree = length - (headerSize + nextOffset);
//
//                if (nfree >= Integer.MAX_VALUE) {
//
//                    /*
//                     * The test is trying to allocate a single record that will
//                     * force the store to be extended. This will not work if the
//                     * store file already has a huge user extent with nothing
//                     * allocated on it.
//                     */
//                    
//                    fail("Can't allocate a record with: " + nfree + " bytes");
//
//                }
//
//                final int nbytes = (int) nfree;
//                
//                final long addr = bufferStrategy.allocate(nbytes);
//
//                assertNotSame(0L, addr);
//
//                assertEquals(nbytes, store.getByteCount(addr));
//                
//                // store file was extended.
//                assertTrue(store.size() > length);
//                
//            } finally {
//
//                store.destroy();
//            
//            }
//            
//        }

        /**
         * Test allocate()+read() where the record was never written (the data
         * are undefined unless written so there is nothing really to test here
         * except for exceptions which might be through for this condition).
         */
        public void test_allocate_then_read() {}

        /**
         * Reallocates the same object several times, then commits and tests read back.
         * 
         * 
         */
        public void test_reallocate() {
            final Journal store = (Journal) getStore();

            try {

                byte[] buf = new byte[1024]; // 2Mb buffer of random data
                r.nextBytes(buf);
                
                ByteBuffer bb = ByteBuffer.wrap(buf);

                RWStrategy bs = (RWStrategy) store
                        .getBufferStrategy();

                RWStore rw = bs.getRWStore();
                
                long faddr1 = bs.write(bb);
                bb.position(0);
                //bs.delete(faddr);
                
                long faddr2 = bs.write(bb);
                bb.position(0);
                
                bs.commit();
                
                rw.reopen();
                
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
        public void test_write_plus_update() {}
        
        /**
         * Ensures the allocation of unique addresses by mapping allocated address with uniqueness 
         * assertion against physical address.
         */
        public void test_addressing() {
            
            final Journal store = (Journal) getStore();

            try {

                RWStrategy bufferStrategy = (RWStrategy) store
                        .getBufferStrategy();

                RWStore rw = bufferStrategy.getRWStore();
                ArrayList<Integer> sizes = new ArrayList<Integer>();
                TreeMap<Long, Integer> paddrs = new TreeMap<Long, Integer>();
                for (int i = 0; i < 1000000; i++) {
                	int s = r.nextInt(250);
                	sizes.add(s);
                	int a = rw.alloc(s);
                	long pa = rw.physicalAddress(a);
                	assertTrue(paddrs.get(pa) == null);
                	paddrs.put(pa, a);
                }
                
                for (int i = 0; i < 50; i++) {
                	int s = r.nextInt(500);
                	sizes.add(s);
                	int a = rw.alloc(s);
                	long pa = rw.physicalAddress(a);
                	paddrs.put(pa, a);
                	System.out.println("Physical Address: " + pa + ", size: " + s);
                }
                
            } finally {

                store.destroy();
            
            }

        	
        }
        
        /**
         * Basic allocation test to ensure the FixedAllocators are operating efficiently.
         * 
         * A 90 byte allocation is expected to fit in a 128byte block.  If we only allocate
         * this fixed block size, then we would expect the physical address to increase by 128 bytes
         * for each allocation.
         */
        public void test_allocations() {
            
            final Journal store = (Journal) getStore();

            try {

                RWStrategy bufferStrategy = (RWStrategy) store
                        .getBufferStrategy();

                RWStore rw = bufferStrategy.getRWStore();
                long numAllocs = rw.getTotalAllocations();
                long startAllocations = rw.getTotalAllocationsSize();
                long faddr = allocBatch(rw, 1000, 275, 320);
                faddr = allocBatch(rw, 10000, 90, 128);
                faddr = allocBatch(rw, 20000, 45, 64);
                
                System.out.println("Final allocation: " + faddr 
                		+ ", allocations: " + (rw.getTotalAllocations() - numAllocs)
                		+ ", allocated bytes: " + (rw.getTotalAllocationsSize() - startAllocations));
            } finally {

                store.destroy();
            
            }

        	
        }
        
        long allocBatch(RWStore rw, int bsize, int asze, int ainc) {
	        long curAddress = rw.physicalAddress(rw.alloc(asze));
	        for (int i = 1; i < bsize; i++) {
	        	int a = rw.alloc(asze);
	        	long nxt = rw.physicalAddress(a);
	        	assertTrue("Problem with index: " + i, (curAddress+ainc) == nxt || (nxt % 8192 == 0));
	        	curAddress = nxt;
	        }
	        
	        return curAddress;
        }

        int[] allocBatchBuffer(RWStore rw, int bsize, int base, int scope) {
        	int[] retaddrs = new int[bsize];
        	
            byte[] batchBuffer = new byte[base+scope];
            r.nextBytes(batchBuffer);
	        for (int i = 0; i < bsize; i++) {
	        	int as = base + r.nextInt(scope);
	        	System.out.println("Allocating " + i + " - " + as + " bytes");
	        	if (i == 400) {
		        	System.out.println("About to allocate the 401th");
	        	}
	        	retaddrs[i] = (int) rw.alloc(batchBuffer, as);
	        }
	        
	        return retaddrs;
        }

        
        /**
         * Reallocation tests the freeing of allocated address and the re-use within a transaction.
         */
        public void test_reallocation() {
            
            final Journal store = (Journal) getStore();

            try {

                RWStrategy bufferStrategy = (RWStrategy) store
                        .getBufferStrategy();

                RWStore rw = bufferStrategy.getRWStore();
                long numAllocs = rw.getTotalAllocations();
                long startAllocations = rw.getTotalAllocationsSize();
                reallocBatch(rw, 10000, 275, 10000);
                reallocBatch(rw, 10000, 860, 10000);
                System.out.println("Final allocations: " + (rw.getTotalAllocations() - numAllocs)
                		+ ", allocated bytes: " + (rw.getTotalAllocationsSize() - startAllocations)
                 + ", file length: " + rw.getStoreFile().length());
            } finally {

                store.destroy();
            
            }

        	
        }

        private long reallocBatch(RWStore rw, int tsts, int sze, int grp) {
        	long[] addr = new long[grp];
        	for (int i = 0; i < grp; i++) {
        		addr[i] = rw.alloc(sze);
        	}
        	for (int t = 0; t < tsts; t++) {
            	for (int i = 0; i < grp; i++) {
            		long old = addr[i];
            		addr[i] = rw.alloc(sze);
            		rw.free(old, sze);
            	}       		
        	}
	        
	        return 0L;
		}

        /**
         * Test of blob allocation, does not check on read back, just the allocation
         */
        public void test_blob_allocs() {
            
            final Journal store = (Journal) getStore();

            try {

                RWStrategy bufferStrategy = (RWStrategy) store
                        .getBufferStrategy();

                RWStore rw = bufferStrategy.getRWStore();
                long numAllocs = rw.getTotalAllocations();
                long startAllocations = rw.getTotalAllocationsSize();
                int startBlob = 1024 * 256;
                int endBlob = 1024 * 1256;
                int[] faddrs = allocBatchBuffer(rw, 500, startBlob, endBlob);
                
                System.out.println("Final allocation: " + rw.physicalAddress(faddrs[499])
                		+ ", allocations: " + (rw.getTotalAllocations() - numAllocs)
                		+ ", allocated bytes: " + (rw.getTotalAllocationsSize() - startAllocations));
            } finally {

                store.destroy();
            
            }
        	
        }
        /**
         * Test of blob allocation and read-back, firstly from cache and then from disk.
         */
        public void test_blob_readBack() {
            
            final Journal store = (Journal) getStore();

            try {

                byte[] buf = new byte[1024 * 2048]; // 2Mb buffer of random data
                r.nextBytes(buf);
                
                ByteBuffer bb = ByteBuffer.wrap(buf);

                RWStrategy bs = (RWStrategy) store
                        .getBufferStrategy();

                RWStore rw = bs.getRWStore();
                
                long faddr = bs.write(bb); // rw.alloc(buf, buf.length);
                
                bb.position(0);
                
                ByteBuffer rdBuf = bs.read(faddr);
                
                assertEquals(bb, rdBuf);
                
                System.out.println("Now commit to disk");
                
                bs.commit();
                
                // Now reset - clears writeCache and reinits from disk
                rw.reopen();
                
                rdBuf = bs.read(faddr);
                assertEquals(bb, rdBuf);

            } finally {

                store.destroy();
            
            }
        	
        }
        
        /**
         * Test of blob allocation and read-back, firstly from cache and then from disk.
         */
        public void test_blob_realloc() {
            
            final Journal store = (Journal) getStore();

            try {

                byte[] buf = new byte[1024 * 2048]; // 2Mb buffer of random data
                r.nextBytes(buf);
                
                ByteBuffer bb = ByteBuffer.wrap(buf);

                RWStrategy bs = (RWStrategy) store
                        .getBufferStrategy();

                RWStore rw = bs.getRWStore();
                
                long faddr = bs.write(bb); // rw.alloc(buf, buf.length);
                
                bb.position(0);
                
                ByteBuffer rdBuf = bs.read(faddr);
                
                assertEquals(bb, rdBuf);
                
                System.out.println("Now commit to disk");
                
                bs.commit();
                
                
                // Now reset - clears writeCache and reinits from disk
                rw.reopen();
                
                rdBuf = bs.read(faddr);
                assertEquals(bb, rdBuf);

                // now delete the memory
                bs.delete(faddr);
                
                try {
                	rdBuf = bs.read(faddr); // should fail with illegal state
                	throw new RuntimeException("Fail");
                } catch (Exception ise) {
                	assertTrue("Expected IllegalStateException", ise instanceof IllegalStateException);
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

                RWStrategy bufferStrategy = (RWStrategy) store
                        .getBufferStrategy();

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
                    
                    for(int i=20; i<40; i++) {
                        
                        assertEquals("data differs at offset=" + i, a[i], b
                                .get(i));
                        
                    }
                    
                }
                
            } finally {

                store.destroy();
            
            }

        }

    }
    
    /**
     * Test suite integration for {@link AbstractInterruptsTestCase}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
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

            properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW
                    .toString());

            properties.setProperty(Options.WRITE_CACHE_ENABLED, ""
                    + writeCacheEnabled);

            return new Journal(properties).getBufferStrategy();

        }

    }
    
    /**
     * Test suite integration for {@link AbstractMROWTestCase}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
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

            properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW
                    .toString());

            properties.setProperty(Options.WRITE_CACHE_ENABLED, ""
                    + writeCacheEnabled);

            return new Journal(properties).getBufferStrategy();

        }

    }

    /**
     * Test suite integration for {@link AbstractMRMWTestCase}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
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

            properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW
                    .toString());

            properties.setProperty(Options.WRITE_CACHE_ENABLED, ""
                    + writeCacheEnabled);

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
