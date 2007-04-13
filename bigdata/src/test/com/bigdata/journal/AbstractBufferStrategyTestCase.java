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
 * Created on Feb 9, 2007
 */

package com.bigdata.journal;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.Random;

import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.rawstore.AbstractRawStoreTestCase;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;

/**
 * Base class for writing test cases for the different {@link IBufferStrategy}
 * implementations.
 * 
 * @todo write tests for
 *       {@link IBufferStrategy#transferTo(java.io.RandomAccessFile)}. This
 *       code is currently getting "checked" by the {@link IndexSegmentBuilder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractBufferStrategyTestCase extends AbstractRawStoreTestCase {

    public AbstractBufferStrategyTestCase() {
    }

    public AbstractBufferStrategyTestCase(String name) {
        super(name);
    }

    abstract protected BufferMode getBufferMode();
    
    public Properties getProperties() {

        if (properties == null) {

            properties = super.getProperties();

            properties.setProperty(Options.BUFFER_MODE, getBufferMode()
                    .toString());

            /*
             * Use a temporary file for the test. Such files are always deleted when
             * the journal is closed or the VM exits.
             */
            properties.setProperty(Options.CREATE_TEMP_FILE,"true");
//            properties.setProperty(Options.DELETE_ON_CLOSE,"true");
            properties.setProperty(Options.DELETE_ON_EXIT,"true");

//            // Note: also deletes the file before it is used.
//            properties.setProperty(Options.FILE, AbstractTestCase
//                    .getTestJournalFile(getName(), properties));
        }
        
        return properties;

    }
    
    private Properties properties;

    protected IRawStore getStore() {
        
        return new Journal(getProperties());
        
    }

    public void tearDown() throws Exception
    {

        super.tearDown();
        
        if(properties==null) return;
        
        String filename = properties.getProperty(Options.FILE);
        
        if(filename==null) return;
        
        File file = new File(filename);
        
        if(file.exists() && !file.delete()) {
            
            file.deleteOnExit();
            
        }
        
    }
     
    /**
     * Unit test for {@link AbstractBufferStrategy#overflow(int)}. The test
     * verifies that the extent and the user extent are correctly updated after
     * an overflow.
     */
    public void test_overflow() {
        
        Journal store = (Journal) getStore();
        
        AbstractBufferStrategy bufferStrategy = (AbstractBufferStrategy) store
                .getBufferStrategy();

        final long userExtent = bufferStrategy.getUserExtent();
        
        final long extent = bufferStrategy.getExtent();
        
        final long initialExtent = bufferStrategy.getInitialExtent();
        
        final int nextOffset = bufferStrategy.getNextOffset();
        
        assertEquals("extent",initialExtent, extent);
        
        final int needed = Bytes.kilobyte32;
        
        if(bufferStrategy.getBufferMode()==BufferMode.Mapped) {

            // operation is not supported for mapped files.
            try {
                bufferStrategy.overflow(needed);
                fail("Expecting: " + UnsupportedOperationException.class);
            } catch (UnsupportedOperationException ex) {
                System.err.println("Ignoring expected exception: " + ex);
            }
            
        } else {

            assertTrue("overflow()",bufferStrategy.overflow(needed));
            
            assertTrue("extent", extent + needed <= bufferStrategy
                    .getExtent());
            
            assertTrue("userExtent", userExtent + needed <= bufferStrategy
                    .getUserExtent());
            
            assertEquals(nextOffset,bufferStrategy.getNextOffset());

        }
        
        store.closeAndDelete();
        
    }

    /**
     * Test verifies that a write up to the remaining extent does not trigger
     * an overflow.
     */
    public void test_writeNoExtend() {

        Journal store = (Journal) getStore();
        
        AbstractBufferStrategy bufferStrategy = (AbstractBufferStrategy) store
                .getBufferStrategy();

        final long userExtent = bufferStrategy.getUserExtent();
        
        final long extent = bufferStrategy.getExtent();
        
        final long initialExtent = bufferStrategy.getInitialExtent();
        
        final int nextOffset = bufferStrategy.getNextOffset();
        
        assertEquals("extent",initialExtent, extent);

        long remaining = userExtent - nextOffset;
        
        assertTrue(remaining<Integer.MAX_VALUE);
        
        ByteBuffer tmp = ByteBuffer.allocate((int)remaining);
        
        bufferStrategy.write(tmp);

        // no change in extent.
        assertEquals("extent",extent, bufferStrategy.getExtent());
        
        // no change in user extent.
        assertEquals("userExtent",userExtent, bufferStrategy.getUserExtent());

        store.closeAndDelete();

    }
    
    /**
     * Test verifies that a write over the remaining extent triggers an
     * overflow. The test also makes sure that the existing data is recoverable
     * and that the new data is also recoverable (when the buffer is extended it
     * is typically copied while the length of a file is simply changed).
     */
    public void test_writeWithExtend() {

        Journal store = (Journal) getStore();
        
        AbstractBufferStrategy bufferStrategy = (AbstractBufferStrategy) store
                .getBufferStrategy();

        if(bufferStrategy.getBufferMode()==BufferMode.Mapped) {
            
            return;
            
        }
        
        final long userExtent = bufferStrategy.getUserExtent();
        
        final long extent = bufferStrategy.getExtent();
        
        final long initialExtent = bufferStrategy.getInitialExtent();
        
        final int nextOffset = bufferStrategy.getNextOffset();
        
        assertEquals("extent",initialExtent, extent);

        /*
         * now write random bytes that exactly fill the remaining space and
         * verify that write.
         */
        long remaining = userExtent - nextOffset;
        
        assertTrue(remaining<Integer.MAX_VALUE);
        
        final byte[] b = new byte[(int)remaining];
        
        Random r = new Random();
        
        r.nextBytes(b);
        
        ByteBuffer tmp = ByteBuffer.wrap(b);
        
        final long addr = bufferStrategy.write(tmp);

        // no change in extent.
        assertEquals("extent",extent, bufferStrategy.getExtent());
        
        // no change in user extent.
        assertEquals("userExtent",userExtent, bufferStrategy.getUserExtent());

        assertEquals(b, bufferStrategy.read(addr));
        
        /*
         * now write some more random bytes forcing an extension of the buffer.
         * we verify both the original write on the buffer and the new write.
         * this helps to ensure that data was copied correctly into the extended
         * buffer.
         */
        
        final byte[] b2 = new byte[Bytes.kilobyte32];
        
        r.nextBytes(b2);
        
        ByteBuffer tmp2 = ByteBuffer.wrap(b2);
        
        final long addr2 = bufferStrategy.write(tmp2);
        
        // verify extension of buffer.
        assertTrue("extent", extent + b2.length <= bufferStrategy.getExtent());

        // verify extension of buffer.
        assertTrue("userExtent", userExtent + b2.length <= bufferStrategy
                .getUserExtent());

        // verify data written before we overflowed the buffer.
        assertEquals(b, bufferStrategy.read(addr));

        // verify data written after we overflowed the buffer.
        assertEquals(b2, bufferStrategy.read(addr2));
    
        store.closeAndDelete();

    }
    
}
