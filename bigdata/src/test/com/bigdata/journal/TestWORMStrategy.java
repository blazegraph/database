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

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;

import com.bigdata.io.DirectBufferPool;
import com.bigdata.rawstore.IRawStore;

/**
 * Test suite for {@link BufferMode#DiskWORM} journals.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo we should also run this entire test suite w/o the write cache to verify
 *       that the write cache can be safely disabled.
 */
public class TestWORMStrategy extends AbstractJournalTestCase {

    public TestWORMStrategy() {
        super();
    }

    public TestWORMStrategy(String name) {
        super(name);
    }

    public static Test suite() {

        final TestWORMStrategy delegate = new TestWORMStrategy(); // !!!! THIS CLASS !!!!

        /*
         * Use a proxy test suite and specify the delegate.
         */

        final ProxyTestSuite suite = new ProxyTestSuite(delegate,
                "DiskWORM Journal Test Suite");

        /*
         * List any non-proxied tests (typically bootstrapping tests).
         */
        
        // tests defined by this class.
        suite.addTestSuite(TestWORMStrategy.class);

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

        properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskWORM.toString());

        properties.setProperty(Options.CREATE_TEMP_FILE, "true");

        properties.setProperty(Options.DELETE_ON_EXIT, "true");

        properties.setProperty(Options.WRITE_CACHE_ENABLED, ""
                + writeCacheEnabled);

        return properties;

    }
    
    /**
     * Verify normal operation and basic assumptions when creating a new journal
     * using {@link BufferMode#DiskWORM}.
     * 
     * @throws IOException
     */
    public void test_create_disk01() throws IOException {

        final Properties properties = getProperties();

        final Journal journal = new Journal(properties);

        try {

            final WORMStrategy bufferStrategy = (WORMStrategy) journal
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
            assertEquals(Options.BUFFER_MODE, BufferMode.DiskWORM, bufferStrategy
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

        properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskWORM.toString());

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
            
            return BufferMode.DiskWORM;
            
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

            properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskWORM
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

            properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskWORM
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

            properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskWORM
                    .toString());

            properties.setProperty(Options.WRITE_CACHE_ENABLED, ""
                    + writeCacheEnabled);

            /*
             * The following two properties are dialed way down in order to
             * raise the probability that we will observe the following error
             * during this test.
             * 
             * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6371642
             * 
             * FIXME We should make the MRMW test harder and focus on
             * interleaving concurrent extensions of the backing store for both
             * WORM and R/W stores.
             */
            
            // Note: Use a relatively small initial extent. 
            properties.setProperty(Options.INITIAL_EXTENT, ""
                    + DirectBufferPool.INSTANCE.getBufferCapacity() * 1);

            // Note: Use a relatively small extension each time.
            properties.setProperty(Options.MINIMUM_EXTENSION,
                    "" + (long) (DirectBufferPool.INSTANCE
                                    .getBufferCapacity() * 1.1));

            return new Journal(properties).getBufferStrategy();

        }

    }

    /**
     * Note: The write cache is allocated by the {@link WORMStrategy} from
     * the {@link DirectBufferPool} and should be released back to that pool as
     * well, so the size of the {@link DirectBufferPool} SHOULD NOT grow as we
     * run these tests. If it does, then there is probably a unit test which is
     * not tearing down its {@link Journal} correctly.
     */
//    /**
//     * Note: Since the write cache is a direct ByteBuffer we have to make it
//     * very small (or disable it entirely) when running the test suite or the
//     * JVM will run out of memory - this is exactly the same (Sun) bug which
//     * motivates us to reuse the same ByteBuffer when we overflow a journal
//     * using a write cache. Since small write caches are disallowed, we wind up
//     * testing with the write cache disabled!
//     */
    private static final boolean writeCacheEnabled = true;
    
}
    