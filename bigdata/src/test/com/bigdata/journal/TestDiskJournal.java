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

package com.bigdata.journal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;

import com.bigdata.rawstore.AbstractRawStoreTestCase;
import com.bigdata.rawstore.IRawStore;

/**
 * Test suite for {@link BufferMode#Disk} journals.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestDiskJournal extends AbstractJournalTestCase {

    public TestDiskJournal() {
        super();
    }

    public TestDiskJournal(String name) {
        super(name);
    }

    public static Test suite() {

        final TestDiskJournal delegate = new TestDiskJournal(); // !!!! THIS CLASS !!!!

        /*
         * Use a proxy test suite and specify the delegate.
         */

        final ProxyTestSuite suite = new ProxyTestSuite(delegate,
                "Disk Journal Test Suite");

        /*
         * List any non-proxied tests (typically bootstrapping tests).
         */
        
        // tests defined by this class.
        suite.addTestSuite(TestDiskJournal.class);

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

        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());

        properties.setProperty(Options.CREATE_TEMP_FILE, "true");

        properties.setProperty(Options.DELETE_ON_EXIT, "true");

        properties.setProperty(Options.WRITE_CACHE_CAPACITY, ""
                + writeCacheCapacity);

        return properties;

    }
    
    /**
     * Verify normal operation and basic assumptions when creating a new journal
     * using {@link BufferMode#Disk}.
     * 
     * @throws IOException
     */
    public void test_create_disk01() throws IOException {

        final Properties properties = getProperties();

        final Journal journal = new Journal(properties);

        try {

            final DiskOnlyStrategy bufferStrategy = (DiskOnlyStrategy) journal
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
            assertEquals(Options.BUFFER_MODE, BufferMode.Disk, bufferStrategy
                    .getBufferMode());

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
            
            return BufferMode.Disk;
            
        }

        /**
         * Test that allocate() pre-extends the store when a record is allocated
         * which would overflow the current user extent.
         */
        public void test_allocPreExtendsStore() {
       
            final Journal store = (Journal) getStore();

            try {

                final DiskOnlyStrategy bufferStrategy = (DiskOnlyStrategy) store
                        .getBufferStrategy();

                final long nextOffset = store.getRootBlockView()
                        .getNextOffset();

                final long length = store.size();

                final long headerSize = FileMetadata.headerSize0;

                // #of bytes remaining in the user extent before overflow.
                final long nfree = length - (headerSize + nextOffset);

                if (nfree >= Integer.MAX_VALUE) {

                    /*
                     * The test is trying to allocate a single record that will
                     * force the store to be extended. This will not work if the
                     * store file already has a huge user extent with nothing
                     * allocated on it.
                     */
                    
                    fail("Can't allocate a record with: " + nfree + " bytes");

                }

                final int nbytes = (int) nfree;
                
                final long addr = bufferStrategy.allocate(nbytes);

                assertNotSame(0L, addr);

                assertEquals(nbytes, store.getByteCount(addr));
                
                // store file was extended.
                assertTrue(store.size() > length);
                
            } finally {

                store.destroy();
            
            }
            
        }

        /**
         * Test allocate()+read() where the record was never written (the data
         * are undefined unless written so there is nothing really to test here
         * except for exceptions which might be through for this condition).
         */
        public void test_allocate_then_read() {

            final Journal store = (Journal) getStore();

            try {

                final DiskOnlyStrategy bufferStrategy = (DiskOnlyStrategy) store
                        .getBufferStrategy();

                final int nbytes = 100;
                
                final long addr = bufferStrategy.allocate(nbytes);

                assertEquals(nbytes, store.getByteCount(addr));
                
                final ByteBuffer b = bufferStrategy.read(addr);
                
                // read returns a buffer that was allocated but never written.
                assertNotNull(b);
                
                // position in the buffer is zero.
                assertEquals(0,b.position());
                
                // limit of the buffer is [nbytes].
                assertEquals(nbytes,b.limit());

                // capacity of the buffer is [nbytes].
                assertEquals(nbytes,b.capacity());
                
                /*
                 * Note: the actual bytes in the buffer ARE NOT DEFINED.
                 */
                
            } finally {

                store.destroy();
            
            }

        }

        /**
         * Test allocate and update of a record for a record where
         * {@link IRawStore#write(ByteBuffer)} was never invoked.
         * <p>
         * Note: Since the record was allocated but never written then it will
         * not be found in the write cache by update().
         */
        public void test_allocate_plus_update() {
            
            final Journal store = (Journal) getStore();

            try {

                final DiskOnlyStrategy bufferStrategy = (DiskOnlyStrategy) store
                        .getBufferStrategy();

                final int nbytes = 60;

                // allocate a new record.
                final long addr = bufferStrategy.allocate(nbytes);

                assertEquals(nbytes, store.getByteCount(addr));
                
                // random data.
                final byte[] a = new byte[nbytes];
                r.nextBytes(a);
                
                /*
                 * Update part of the record.
                 * 
                 * This updates bytes [20:40) from the array of random bytes
                 * onto the record starting at byte 20 in the record.
                 */
                {
                    
                    final ByteBuffer b = ByteBuffer.wrap(a);
                    
                    b.limit(40);
                    
                    b.position(20);
                    
                    bufferStrategy.update(addr, 20, b);
                    
                }
                
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
        
        /**
         * Test write of a record and then update of a slice of that record.
         * <p>
         * Note: Since the record was written but not flushed it will be found
         * in the write cache by update().
         */
        public void test_write_plus_update() {
            
            final Journal store = (Journal) getStore();

            try {

                DiskOnlyStrategy bufferStrategy = (DiskOnlyStrategy) store
                        .getBufferStrategy();

                final int nbytes = 60;

                // random data.
                byte[] a = new byte[nbytes];
                r.nextBytes(a);
                
                // write a new record.
                final long addr = bufferStrategy.write(ByteBuffer.wrap(a));

                assertEquals(nbytes, store.getByteCount(addr));
                
                /*
                 * Update part of the record.
                 * 
                 * This updates bytes [20:40) from the array of random bytes
                 * onto the record starting at byte 20 in the record.
                 */
                {

                    // increment the bytes in that region of the record.
                    for(int i=20; i<40; i++) a[i]++;
                    
                    ByteBuffer b = ByteBuffer.wrap(a);
                    
                    b.limit(40);
                    
                    b.position(20);
                    
                    bufferStrategy.update(addr, 20, b);
                    
                }
                
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
        
        /**
         * Ttest write() + flush() + update() - for this case the data have been
         * flushed from the write cache so the update will be a random write on
         * the file rather than being buffered by the write cache.
         */
        public void test_write_flush_update() {
            
            final Journal store = (Journal) getStore();

            try {

                DiskOnlyStrategy bufferStrategy = (DiskOnlyStrategy) store
                        .getBufferStrategy();

                final int nbytes = 60;

                // random data.
                byte[] a = new byte[nbytes];
                r.nextBytes(a);
                
                // write a new record.
                final long addr = bufferStrategy.write(ByteBuffer.wrap(a));

                assertEquals(nbytes, store.getByteCount(addr));
                
                // Note: This will flush the write cache.
                store.commit();
                
                /*
                 * Update part of the record which we just flushed from the
                 * write cache.
                 * 
                 * This updates bytes [20:40) from the array of random bytes
                 * onto the record starting at byte 20 in the record.
                 */
                {

                    // increment the bytes in that region of the record.
                    for(int i=20; i<40; i++) a[i]++;
                    
                    ByteBuffer b = ByteBuffer.wrap(a);
                    
                    b.limit(40);
                    
                    b.position(20);
                    
                    bufferStrategy.update(addr, 20, b);
                    
                }
                
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

            properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk
                    .toString());

            properties.setProperty(Options.WRITE_CACHE_CAPACITY, ""
                    + writeCacheCapacity);

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

            properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk
                    .toString());

            properties.setProperty(Options.WRITE_CACHE_CAPACITY, ""
                    + writeCacheCapacity);

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

            properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk
                    .toString());

            properties.setProperty(Options.WRITE_CACHE_CAPACITY, ""
                    + writeCacheCapacity);

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
    private static final int writeCacheCapacity = 0; // 512;
    
}
