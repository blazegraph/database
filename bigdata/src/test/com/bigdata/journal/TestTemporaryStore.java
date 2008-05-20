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
 * Created on Feb 15, 2007
 */

package com.bigdata.journal;

import java.nio.ByteBuffer;
import java.util.Random;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;

import com.bigdata.rawstore.AbstractRawStoreTestCase;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;

/**
 * Test suite for {@link TemporaryStore} (temporary store with named indices).
 * 
 * @todo add test to verify read back after we overflow the initial write cache.
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

    public static Test suite() {

        final TestTemporaryStore delegate = new TestTemporaryStore(); // !!!! THIS CLASS !!!!

        /*
         * Use a proxy test suite and specify the delegate.
         */

        ProxyTestSuite suite = new ProxyTestSuite(delegate,
                "Temporary Raw Store Test Suite");

        /*
         * List any non-proxied tests (typically bootstrapping tests).
         */
        
        // tests defined by this class.
        suite.addTestSuite(TestTemporaryStore.class);

        // test suite for the IRawStore api.
        suite.addTestSuite( TestRawStore.class );

        // test suite for handling asynchronous close of the file channel.
        suite.addTestSuite( TestInterrupts.class );
        
        // test suite for MROW correctness.
        suite.addTestSuite( TestMROW.class );

        // test suite for MRMW correctness.
        suite.addTestSuite( TestMRMW.class );

        return suite;
        
    }
    
    protected IRawStore getStore() {

        return new TemporaryRawStore();
        
    }

    /**
     * Test suite integration for {@link AbstractRawStoreTestCase}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * Note: You can not re-open a transient store, hence it is not possible to
     * extend {@link AbstractRestartSafeTestCase}.
     */
    public static class TestRawStore extends AbstractRawStoreTestCase {
        
        public TestRawStore() {
            super();
        }

        public TestRawStore(String name) {
            super(name);
        }

        protected IRawStore getStore() {
            return new TemporaryRawStore();
        }

    }
    
    /**
     * Test suite integration for {@link TestInterrupts}.
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

            return new TemporaryRawStore();
            
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

            return new TemporaryRawStore();
            
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

            return new TemporaryRawStore();
            
        }

    }
    
    /**
     * Unit test for {@link AbstractBufferStrategy#overflow(long)}. The test
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
        
        final long nextOffset = bufferStrategy.getNextOffset();
        
        assertEquals("extent",initialExtent, extent);
        
        final long needed = Bytes.kilobyte32;

        bufferStrategy.force(true);
        
        assertTrue("overflow()", bufferStrategy.overflow(needed));

        assertTrue("extent", extent + needed <= bufferStrategy.getExtent());

        assertTrue("userExtent", userExtent + needed <= bufferStrategy
                .getUserExtent());

        assertEquals(nextOffset, bufferStrategy.getNextOffset());

        store.close();
            
    }

    /**
     * Write random bytes on the store.
     * 
     * @param store
     *            The store.
     * 
     * @param nbytesToWrite
     *            The #of bytes to be written. If this is larger than the
     *            maximum record length then multiple records will be written.
     * 
     * @return The address of the last record written.
     */
    protected long writeRandomData(TemporaryRawStore store,final long nbytesToWrite) {

        final int maxRecordSize = store.getMaxRecordSize();
        
        assert nbytesToWrite > 0;
        
        long addr = 0L;
        
        AbstractBufferStrategy bufferStrategy = (AbstractBufferStrategy) store
                .getBufferStrategy();
        
        int n = 0;

        long leftover = nbytesToWrite;
        
        while (leftover > 0) {

            // this will be an int since maxRecordSize is an int.
            int nbytes = (int) Math.min(maxRecordSize, leftover);

            assert nbytes>0;
            
            final byte[] b = new byte[nbytes];

            Random r = new Random();

            r.nextBytes(b);

            ByteBuffer tmp = ByteBuffer.wrap(b);

            addr = bufferStrategy.write(tmp);

            n++;
            
            leftover -= nbytes;
            
            System.err.println("Wrote record#" + n + " with " + nbytes
                    + " bytes: addr=" + store.toString(addr) + ", #leftover="
                    + leftover);

        }

        System.err.println("Wrote " + nbytesToWrite + " bytes in " + n
                + " records: last addr=" + store.toString(addr));

        assert addr != 0L;
        
        return addr;

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
        
        final long nextOffset = bufferStrategy.getNextOffset();
        
        assertEquals("extent",initialExtent, extent);

        long remaining = userExtent - nextOffset;
        
        writeRandomData(store, remaining);

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
        
        final long nextOffset = bufferStrategy.getNextOffset();
        
        assertEquals("extent",initialExtent, extent);

        /*
         * now write random bytes that exactly fill the remaining space and
         * verify that write.
         */
        long remaining = userExtent - nextOffset;
        
//        assertTrue(remaining<Integer.MAX_VALUE);
//        
//        final byte[] b = new byte[(int)remaining];
//        
//        Random r = new Random();
//        
//        r.nextBytes(b);
//        
//        ByteBuffer tmp = ByteBuffer.wrap(b);
//        
//        final long addr = bufferStrategy.write(tmp);

        final long addr = writeRandomData(store, remaining);
        
        // no change in extent.
        assertEquals("extent",extent, bufferStrategy.getExtent());
        
        // no change in user extent.
        assertEquals("userExtent",userExtent, bufferStrategy.getUserExtent());

        ByteBuffer b = bufferStrategy.read(addr);
        
        /*
         * now write some more random bytes forcing an extension of the buffer.
         * we verify both the original write on the buffer and the new write.
         * this helps to ensure that data was copied correctly into the extended
         * buffer.
         */
        
        final byte[] b2 = new byte[Bytes.kilobyte32];
        
        new Random().nextBytes(b2);
        
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
    
//    /**
//     * Test that the store transparently overflows onto disk when the maximum
//     * in-memory limit has been exceeded. The test also makes sure that the
//     * existing data is recoverable and that the new data is also recoverable
//     * (when the buffer is extended it is typically copied while the length of a
//     * file is simply changed).  Finally, the test makes sure that the temporary
//     * file is deleted when the store is closed.
//     */
//    public void test_overflowToDisk() {
//        
//        Random r = new Random();
//
//        TemporaryRawStore store = new TemporaryRawStore();
//        
//        {
//
//            AbstractBufferStrategy bufferStrategy = (AbstractBufferStrategy) store
//                    .getBufferStrategy();
//
//            final long userExtent = bufferStrategy.getUserExtent();
//
//            final long extent = bufferStrategy.getExtent();
//
//            final long initialExtent = bufferStrategy.getInitialExtent();
//
//            final long nextOffset = bufferStrategy.getNextOffset();
//
//            // will be zero for a transient buffer.
//            assertEquals("nextOffset",0,nextOffset);
//            
//            // check the initial extent.
//            assertEquals("extent", store.initialInMemoryExtent, extent);
//
//            // will be the same for a transient buffer.
//            assertEquals("initialExtent", store.initialInMemoryExtent, initialExtent );
//
//            // will be the same for a transient buffer.
//            assertEquals("userExtent", store.initialInMemoryExtent, userExtent );
//
//            /*
//             * pre-extend the transient buffer to its maximum capacity.
//             */
//            bufferStrategy.truncate(store.maximumInMemoryExtent);
//
//            // verify that we are using an in-memory buffer.
//            assertTrue(store.getBufferStrategy() instanceof TransientBufferStrategy);
//
//            /*
//             * for the transient store, this gives us exactly that many bytes in
//             * both the user extent and the overall extent (there is no reserved
//             * header).
//             */
//            assertEquals("extent", store.maximumInMemoryExtent, bufferStrategy
//                    .getExtent());
//            assertEquals("userExtent", store.maximumInMemoryExtent,
//                    bufferStrategy.getUserExtent());
//
//        }
//
//        final byte[] b;
//        
//        final long addr;
//        
//        {
//            
//            final long extent = store.getBufferStrategy().getExtent();
//
//            final long userExtent = store.getBufferStrategy().getUserExtent();
//
//            /*
//             * now write random bytes that exactly fill the remaining space and
//             * verify that write.
//             */
//            long remaining = userExtent
//                    - store.getBufferStrategy().getNextOffset();
//
////            assertTrue(remaining < Integer.MAX_VALUE);
////
////            b = new byte[(int) remaining];
////
////            r.nextBytes(b);
////
////            ByteBuffer tmp = ByteBuffer.wrap(b);
////
////            addr = store.write(tmp);
//            
//            addr = writeRandomData(store, remaining);
//
//            // verify that we are using an in-memory buffer.
//            assertTrue(store.getBufferStrategy() instanceof TransientBufferStrategy);
//
//            // no change in extent.
//            assertEquals("extent", extent, store.getBufferStrategy()
//                    .getExtent());
//
//            // no change in user extent.
//            assertEquals("userExtent", userExtent, store.getBufferStrategy()
//                    .getUserExtent());
//
//            // read back the data.
//            ByteBuffer tmp = store.read(addr);
//            
//            b = new byte[tmp.remaining()];
//            
//            tmp.get(b);
//
//        }
//
//        /*
//         * Now write some more random bytes forcing an extension of the buffer.
//         * 
//         * Note that this will cause the buffer to overflow and convert to a
//         * disk-based buffer.
//         * 
//         * We verify both the original write on the buffer and the new write.
//         * this helps to ensure that data was copied correctly into the extended
//         * buffer.
//         */
//        
//        final byte[] b2 = new byte[Bytes.kilobyte32];
//        
//        r.nextBytes(b2);
//        
//        ByteBuffer tmp2 = ByteBuffer.wrap(b2);
//        
//        final long addr2 = store.write(tmp2);
//        
//        // verify that we are using an disk-based store.
//        assertTrue(store.getBufferStrategy() instanceof DiskOnlyStrategy); 
//        
//        // verify extension of store.
//        assertTrue("extent", store.maximumInMemoryExtent + b2.length <= store
//                .getBufferStrategy().getExtent());
//
//        // verify extension of store.
//        assertTrue("userExtent",
//                store.maximumInMemoryExtent + b2.length <= store
//                        .getBufferStrategy().getUserExtent());
//
//        // verify data written before we overflowed the buffer.
//        assertEquals(b, store.read(addr));
//
//        // verify data written after we overflowed the buffer.
//        assertEquals(b2, store.read(addr2));
//    
//        // the name of the on-disk file.
//        File file = ((DiskOnlyStrategy)store.getBufferStrategy()).getFile();
//        
//        // verify that it exists.
//        assertTrue(file.exists());
//        
//        // close the store.
//        store.close();
//
//        // verify that the file is gone.
//        assertFalse(file.exists());
//
//    }
    
}
