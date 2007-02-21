/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Oct 14, 2006
 */

package com.bigdata.journal;

import java.io.IOException;
import java.util.Properties;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;

import com.bigdata.rawstore.AbstractRawStoreTestCase;

/**
 * Test suite for {@link BufferMode#Transient} journals.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */

public class TestTransientJournal extends AbstractTestCase {

    public TestTransientJournal() {
        super();
    }

    public TestTransientJournal(String name) {
        super(name);
    }

    public static Test suite() {

        final TestTransientJournal delegate = new TestTransientJournal(); // !!!! THIS CLASS !!!!

        /*
         * Use a proxy test suite and specify the delegate.
         */

        ProxyTestSuite suite = new ProxyTestSuite(delegate,
                "Transient Journal Test Suite");

        /*
         * List any non-proxied tests (typically bootstrapping tests).
         */
        
        // tests defined by this class.
        suite.addTestSuite(TestTransientJournal.class);

        // test suite for the IRawStore api.
        suite.addTestSuite( TestRawStore.class );

        // test suite for MROW correctness.
        suite.addTestSuite( TestMROW.class );

        // test suite for btree on the journal.
        suite.addTestSuite( TestBTree.class );

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

        properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient.toString());

        return properties;

    }

    /**
     * Verify normal operation and basic assumptions when creating a new journal
     * using {@link BufferMode#Transient}.
     * 
     * @throws IOException
     */
    public void test_create_transient01() throws IOException {

        final Properties properties = getProperties();

        Journal journal = new Journal(properties);

        TransientBufferStrategy bufferStrategy = (TransientBufferStrategy) journal._bufferStrategy;

        assertFalse("isStable",bufferStrategy.isStable());
        assertTrue("isFullyBuffered",bufferStrategy.isFullyBuffered());
        assertEquals(Options.INITIAL_EXTENT, Options.DEFAULT_INITIAL_EXTENT,
                bufferStrategy.getExtent());
        assertEquals(Options.MAXIMUM_EXTENT, Options.DEFAULT_MAXIMUM_EXTENT,
                bufferStrategy.getMaximumExtent());
        assertEquals(Options.BUFFER_MODE, BufferMode.Transient, bufferStrategy
                .getBufferMode());
        assertNotNull("directBuffer", bufferStrategy.directBuffer);
        assertEquals("userExtent", bufferStrategy.getExtent(), bufferStrategy
                .getUserExtent());
        assertEquals("bufferCapacity", bufferStrategy.getUserExtent(),
                bufferStrategy.directBuffer.capacity());
        
    }
            
    /**
     * Test suite integration for {@link AbstractRawStoreTestCase}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo While the transient store uses root blocks there is no means
     *       currently defined to re-open a journal based on a transient store
     *       and hence it is not possible to extend
     *       {@link AbstractRestartSafeTestCase}.
     */
    public static class TestRawStore extends AbstractBufferStrategyTestCase {
        
        public TestRawStore() {
            super();
        }

        public TestRawStore(String name) {
            super(name);
        }

        protected BufferMode getBufferMode() {
            
            return BufferMode.Transient;
            
        }

//        public Properties getProperties() {
//
//            Properties properties = super.getProperties();
//
//            properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient.toString());
//
//            return properties;
//
//        }
//
//        protected IRawStore getStore() {
//            
//            return new Journal(getProperties());
//            
//        }

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

        protected BufferMode getBufferMode() {
            
            return BufferMode.Transient;
            
        }
        
    }

    /**
     * Test suite integration for {@link AbstractBTreeWithJournalTestCase}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class TestBTree extends AbstractBTreeWithJournalTestCase {
        
        public TestBTree() {
            super();
        }

        public TestBTree(String name) {
            super(name);
        }
        
        public BufferMode getBufferMode() {
            
            return BufferMode.Transient;
            
        }
        
    }

}
