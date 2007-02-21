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
 * Created on Feb 15, 2007
 */

package com.bigdata.journal;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Random;

import com.bigdata.rawstore.AbstractRawStoreTestCase;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;

/**
 * Test suite for {@link TemporaryRawStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTemporaryStore extends AbstractRawStoreTestCase {

    /**
     * 
     */
    public TestTemporaryStore() {
    }

    /**
     * @param name
     */
    public TestTemporaryStore(String name) {
        super(name);
    }

    protected IRawStore getStore() {

        return new TemporaryRawStore();
        
    }

    /**
     * Unit test for {@link AbstractBufferStrategy#overflow(int)}. The test
     * verifies that the extent and the user extent are correctly updated after
     * an overflow.
     */
    public void test_overflow() {
        
        TemporaryRawStore store = (TemporaryRawStore) getStore();
        
        AbstractBufferStrategy bufferStrategy = (AbstractBufferStrategy) store
                .getBufferStrategy();

        final long userExtent = bufferStrategy.getUserExtent();
        
        final long extent = bufferStrategy.getExtent();
        
        final long initialExtent = bufferStrategy.getInitialExtent();
        
        final int nextOffset = bufferStrategy.getNextOffset();
        
        assertEquals("extent",initialExtent, extent);
        
        final int needed = Bytes.kilobyte32;

        assertTrue("overflow()", bufferStrategy.overflow(needed));

        assertTrue("extent", extent + needed <= bufferStrategy.getExtent());

        assertTrue("userExtent", userExtent + needed <= bufferStrategy
                .getUserExtent());

        assertEquals(nextOffset, bufferStrategy.getNextOffset());

        store.close();
            
    }

    /**
     * Test verifies that a write up to the remaining extent does not trigger an
     * overflow.
     */
    public void test_writeNoExtend() {

        TemporaryRawStore store = (TemporaryRawStore) getStore();
        
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

        store.close();

    }
    
    /**
     * Test verifies that a write over the remaining extent triggers an
     * overflow. The test also makes sure that the existing data is recoverable
     * and that the new data is also recoverable (when the buffer is extended it
     * is typically copied while the length of a file is simply changed).
     */
    public void test_writeWithExtend() {

        TemporaryRawStore store = (TemporaryRawStore) getStore();
        
        AbstractBufferStrategy bufferStrategy = (AbstractBufferStrategy) store
                .getBufferStrategy();

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
    
        store.close();

    }
    
    /**
     * Test that the store transparently overflows onto disk when the maximum
     * in-memory limit has been exceeded. The test also makes sure that the
     * existing data is recoverable and that the new data is also recoverable
     * (when the buffer is extended it is typically copied while the length of a
     * file is simply changed).  Finally, the test makes sure that the temporary
     * file is deleted when the store is closed.
     */
    public void test_overflowToDisk() {
        
        Random r = new Random();

        /*
         * Note: We use a small store for this test to minimize the resource
         * requirements. This should have no impact on the ability to test
         * correctness.
         */
        TemporaryRawStore store = new TemporaryRawStore(Bytes.kilobyte*10,
                Bytes.kilobyte * 100, false);
        
        // verify that we are using an in-memory buffer.
        assertTrue(store.getBufferStrategy() instanceof TransientBufferStrategy); 
        
        {

            AbstractBufferStrategy bufferStrategy = (AbstractBufferStrategy) store
                    .getBufferStrategy();

            final long userExtent = bufferStrategy.getUserExtent();

            final long extent = bufferStrategy.getExtent();

            final long initialExtent = bufferStrategy.getInitialExtent();

            final int nextOffset = bufferStrategy.getNextOffset();

            // will be zero for a transient buffer.
            assertEquals("nextOffset",0,nextOffset);
            
            // check the initial extent.
            assertEquals("extent", store.initialInMemoryExtent, extent);

            // will be the same for a transient buffer.
            assertEquals("initialExtent", store.initialInMemoryExtent, initialExtent );

            // will be the same for a transient buffer.
            assertEquals("userExtent", store.initialInMemoryExtent, userExtent );

            /*
             * pre-extend the transient buffer to its maximum capacity.
             */
            bufferStrategy.truncate(store.maximumInMemoryExtent);

            // verify that we are using an in-memory buffer.
            assertTrue(store.getBufferStrategy() instanceof TransientBufferStrategy);

            /*
             * for the transient store, this gives us exactly that many bytes in
             * both the user extent and the overall extent (there is no reserved
             * header).
             */
            assertEquals("extent", store.maximumInMemoryExtent, bufferStrategy
                    .getExtent());
            assertEquals("userExtent", store.maximumInMemoryExtent,
                    bufferStrategy.getUserExtent());

        }

        final byte[] b;
        
        final long addr;
        
        {
            
            final long extent = store.getBufferStrategy().getExtent();

            final long userExtent = store.getBufferStrategy().getUserExtent();

            /*
             * now write random bytes that exactly fill the remaining space and
             * verify that write.
             */
            long remaining = userExtent
                    - store.getBufferStrategy().getNextOffset();

            assertTrue(remaining < Integer.MAX_VALUE);

            b = new byte[(int) remaining];

            r.nextBytes(b);

            ByteBuffer tmp = ByteBuffer.wrap(b);

            addr = store.write(tmp);

            // verify that we are using an in-memory buffer.
            assertTrue(store.getBufferStrategy() instanceof TransientBufferStrategy);

            // no change in extent.
            assertEquals("extent", extent, store.getBufferStrategy()
                    .getExtent());

            // no change in user extent.
            assertEquals("userExtent", userExtent, store.getBufferStrategy()
                    .getUserExtent());

            // verify the data.
            assertEquals(b, store.read(addr));

        }

        /*
         * Now write some more random bytes forcing an extension of the buffer.
         * 
         * Note that this will cause the buffer to overflow and convert to a
         * disk-based buffer.
         * 
         * We verify both the original write on the buffer and the new write.
         * this helps to ensure that data was copied correctly into the extended
         * buffer.
         */
        
        final byte[] b2 = new byte[Bytes.kilobyte32];
        
        r.nextBytes(b2);
        
        ByteBuffer tmp2 = ByteBuffer.wrap(b2);
        
        final long addr2 = store.write(tmp2);
        
        // verify that we are using an disk-based store.
        assertTrue(store.getBufferStrategy() instanceof DiskOnlyStrategy); 
        
        // verify extension of store.
        assertTrue("extent", store.maximumInMemoryExtent + b2.length <= store
                .getBufferStrategy().getExtent());

        // verify extension of store.
        assertTrue("userExtent",
                store.maximumInMemoryExtent + b2.length <= store
                        .getBufferStrategy().getUserExtent());

        // verify data written before we overflowed the buffer.
        assertEquals(b, store.read(addr));

        // verify data written after we overflowed the buffer.
        assertEquals(b2, store.read(addr2));
    
        // the name of the on-disk file.
        File file = ((DiskOnlyStrategy)store.getBufferStrategy()).getFile();
        
        // verify that it exists.
        assertTrue(file.exists());
        
        // close the store.
        store.close();

        // verify that the file is gone.
        assertFalse(file.exists());

    }
    
}
