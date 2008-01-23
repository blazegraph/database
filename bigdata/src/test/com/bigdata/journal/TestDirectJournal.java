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
import java.util.Properties;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;

import com.bigdata.rawstore.IRawStore;

/**
 * Test suite for {@link BufferMode#Direct} journals.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */

public class TestDirectJournal extends AbstractTestCase {

    public TestDirectJournal() {
        super();
    }

    public TestDirectJournal(String name) {
        super(name);
    }

    public static Test suite() {

        final TestDirectJournal delegate = new TestDirectJournal(); // !!!! THIS CLASS !!!!

        /*
         * Use a proxy test suite and specify the delegate.
         */

        ProxyTestSuite suite = new ProxyTestSuite(delegate,
                "Direct Journal Test Suite");

        /*
         * List any non-proxied tests (typically bootstrapping tests).
         */
        
        // tests defined by this class.
        suite.addTestSuite(TestDirectJournal.class);

        // test suite for the IRawStore api.
        suite.addTestSuite( TestRawStore.class );
        
        // test suite for handling asynchronous close of the file channel.
        suite.addTestSuite( TestInterrupts.class );

        // test suite for MROW correctness.
        suite.addTestSuite( TestMROW.class );

        // test suite for MRMW correctness.
        suite.addTestSuite( TestMRMW.class );

        /*
         * Pickup the basic journal test suite. This is a proxied test suite, so
         * all the tests will run with the configuration specified in this test
         * class and its optional .properties file.
         */
        suite.addTest(TestJournalBasics.suite());

        return suite;

    }

    public Properties getProperties() {

        Properties properties = super.getProperties();

        properties.setProperty(Options.DELETE_ON_EXIT,"true");

        properties.setProperty(Options.CREATE_TEMP_FILE,"true");

        properties.setProperty(Options.BUFFER_MODE, BufferMode.Direct.toString());

        return properties;

    }

    /**
     * Verify normal operation and basic assumptions when creating a new journal
     * using {@link BufferMode#Direct}.
     * 
     * @throws IOException
     */
    public void test_create_direct01() throws IOException {

        final Properties properties = getProperties();

        Journal journal = new Journal(properties);

        DirectBufferStrategy bufferStrategy = (DirectBufferStrategy) journal.getBufferStrategy();

        assertTrue("isStable", bufferStrategy.isStable());
        assertTrue("isFullyBuffered", bufferStrategy.isFullyBuffered());
//        assertEquals(Options.FILE, properties.getProperty(Options.FILE),
//                bufferStrategy.file.toString());
        assertEquals(Options.INITIAL_EXTENT, Options.DEFAULT_INITIAL_EXTENT,
                bufferStrategy.getInitialExtent());
        assertEquals(Options.MAXIMUM_EXTENT, 0L /*soft limit for Direct buffer*/,
                bufferStrategy.getMaximumExtent());
        assertNotNull("raf", bufferStrategy.raf);
        assertEquals("bufferMode", BufferMode.Direct, bufferStrategy
                .getBufferMode());
        assertNotNull("directBuffer", bufferStrategy.getBuffer());
        assertTrue("userExtent", bufferStrategy.getExtent() > bufferStrategy
                .getUserExtent());
        assertEquals("bufferCapacity", bufferStrategy.getUserExtent(),
                bufferStrategy.getBuffer().capacity());

        journal.closeAndDelete();

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
            
            return BufferMode.Direct;
            
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

            Properties properties = getProperties();
            
            properties.setProperty(Options.DELETE_ON_EXIT,"true");

            properties.setProperty(Options.CREATE_TEMP_FILE,"true");

            properties.setProperty(Options.BUFFER_MODE, BufferMode.Direct
                    .toString());
            
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

            Properties properties = getProperties();
            
            properties.setProperty(Options.DELETE_ON_EXIT,"true");

            properties.setProperty(Options.CREATE_TEMP_FILE,"true");

            properties.setProperty(Options.BUFFER_MODE, BufferMode.Direct
                    .toString());
            
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

            Properties properties = getProperties();
            
            properties.setProperty(Options.DELETE_ON_EXIT,"true");

            properties.setProperty(Options.CREATE_TEMP_FILE,"true");

            properties.setProperty(Options.BUFFER_MODE, BufferMode.Direct
                    .toString());
            
            return new Journal(properties).getBufferStrategy();
            
        }
        
    }
    
}
