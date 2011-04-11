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
 * Created on Feb 4, 2007
 */

package com.bigdata.journal;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.Random;

import com.bigdata.LRUNexus;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rwstore.PhysicalAddressResolutionException;
import com.bigdata.util.InnerCause;

/**
 * Test suite for restart-safe (data survives commit and reopen of the store).
 * 
 * @todo verify {@link ICommitter} protocol.
 * 
 * @todo verify nextOffset after restart and other metadata preserved by the
 * root blocks.
 * 
 * @todo verify {@link IBufferStrategy#truncate(long)}.  note that you can not
 * extend a mapped file.  note that the Direct mode must extend both the file
 * and the buffer while the Disk mode only extends the file.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractRestartSafeTestCase extends AbstractBufferStrategyTestCase {

    public AbstractRestartSafeTestCase() {
    }

    public AbstractRestartSafeTestCase(String name) {
        super(name);
    }

//    /**
//     * override to disable deletion of the store on close.
//     */
//    public Properties getProperties() {
//        
//        Properties properties = super.getProperties();
//        
//        properties.setProperty(Options.DELETE_ON_CLOSE,"false");
//
//        return properties;
//        
//    }
    
    /**
     * Re-open the same backing store.
     * 
     * @param store
     *            the existing store.
     * 
     * @return A new store.
     * 
     * @exception Throwable
     *                if the existing store is not closed, e.g., from failure to
     *                obtain a file lock, etc.
     */
    protected IRawStore reopenStore(final IRawStore store) {

        boolean closedForWrites = false;
        
        if (store.isReadOnly() && store instanceof Journal
                && ((Journal) store).getRootBlockView().getCloseTime() != 0L) {

            closedForWrites = true;
            
        }

        // close the store.
        store.close();
        
        // Note: Clone to avoid modifying!!!
        final Properties properties = (Properties)getProperties().clone();
        
        // Turn this off now since we want to re-open the same store.
        properties.setProperty(Options.CREATE_TEMP_FILE,"false");
        
        // The backing file that we need to re-open.
        final File file = store.getFile();
        
        assertNotNull(file);
        
        // Set the file property explicitly.
        properties.setProperty(Options.FILE,file.toString());
        
        if(closedForWrites) {

            /*
             * This supports unit tests which use closeForWrites() and then
             * reopen the journal.
             */
            properties.setProperty(Options.READ_ONLY,"true");
            
        }
        
        return new Journal( properties );
        
    }

    /**
     * Writes a record, verifies the write but does NOT commit the store. Closes
     * and reopens the store and finally verifies the write was lost.
     */
    public void test_restartSafe_oneWriteNoCommit() {

        IAtomicStore store = (IAtomicStore) getStore();

        try {
            assertTrue(store.isStable());

            final Random r = new Random();

            final int len = 100;

            final byte[] expected = new byte[len];

            r.nextBytes(expected);

            final ByteBuffer tmp = ByteBuffer.wrap(expected);

            final long addr1 = store.write(tmp);

            // verify that the position is advanced to the limit.
            assertEquals(len, tmp.position());
            assertEquals(tmp.position(), tmp.limit());

            // read the data back.
            final ByteBuffer actual = store.read(addr1);

            assertEquals(expected, actual);

            /*
             * verify the position and limit after the read.
             */
            assertEquals(0, actual.position());
            assertEquals(expected.length, actual.limit());

            /*
             * DO NOT COMMIT.
             */

            // re-open the store.
            store = (IAtomicStore) reopenStore(store);

            assertTrue(store.isStable());

            /*
             * attempt read the data back. this should throw an exception since
             * the nextOffset in the root block will still be zero and the store
             * can therefore correctly reject this address as never written.
             */
            try {
                store.read(addr1);
                fail("Expecting: " + IllegalArgumentException.class);
            } catch (RuntimeException ex) {
                if (InnerCause.isInnerCause(ex,
                        IllegalArgumentException.class)) {
                    if (log.isInfoEnabled())
                        log.info("Ignoring expected exception: " + ex);
                } else {
                    fail("Expecting inner cause: "
                            + IllegalArgumentException.class
                            + ", not: " + ex, ex);
                }
            }

        } finally {

            store.destroy();

        }

    }

    /**
     * Writes a record, verifies the write then commits the store. Closes and
     * reopens the store and finally verifies the write on the reopened store.
     */
    public void test_restartSafe_oneWrite() {
        
        IAtomicStore store = (IAtomicStore)getStore();
        
        try {
        
        assertTrue(store.isStable());
        
        Random r = new Random();
        
        final int len = 100;
        
        byte[] expected = new byte[len];
        
        r.nextBytes(expected);
        
        ByteBuffer tmp = ByteBuffer.wrap(expected);
        
        long addr1 = store.write(tmp);

        // verify that the position is advanced to the limit.
        assertEquals(len,tmp.position());
        assertEquals(tmp.position(),tmp.limit());

        // read the data back.
        ByteBuffer actual = store.read(addr1);
        
        assertEquals(expected,actual);
        
        /*
         * verify the position and limit after the read.
         */
        assertEquals(0,actual.position());
        assertEquals(expected.length,actual.limit());

        /*
         * Commit the changes - if you do not commit the changes then the root
         * blocks are not updated and your data is lost on restart.
         */
        store.commit();
        
        // re-open the store.
        store = (IAtomicStore)reopenStore(store);
        
        assertTrue( store.isStable() );
        
        // read the data back.
        actual = store.read(addr1);
        
        assertEquals(expected,actual);

        } finally {

            store.destroy();
            
        }
        
    }
    
    /**
     * Test writes a bunch of records and verifies that each can be read after
     * it is written.  The test then performs a random order read and verifies
     * that each of the records can be read correctly.
     */
    public void test_restartSafe_multipleWrites() {

        IAtomicStore store = (IAtomicStore)getStore();
        
        try {
        
        assertTrue(store.isStable());

        Random r = new Random();

        /*
         * write a bunch of random records.
         */
        final int limit = 100;
        
        final long[] addrs = new long[limit];
        
        final byte[][] records = new byte[limit][];
        
        for(int i=0; i<limit; i++) {

            byte[] expected = new byte[r.nextInt(100) + 1];
        
            r.nextBytes(expected);
        
            ByteBuffer tmp = ByteBuffer.wrap(expected);
            
            long addr = store.write(tmp);

            // verify that the position is advanced to the limit.
            assertEquals(expected.length,tmp.position());
            assertEquals(tmp.position(),tmp.limit());

            assertEquals(expected,store.read(addr));
        
            addrs[i] = addr;
            
            records[i] = expected;
            
        }

        /*
         * now verify data with random reads.
         */

        int[] order = getRandomOrder(limit);
        
        for(int i=0; i<limit; i++) {
            
            long addr = addrs[order[i]];
            
            byte[] expected = records[order[i]];

            assertEquals(expected,store.read(addr));
            
        }
        
        /*
         * Commit the changes - if you do not commit the changes then the root
         * blocks are not updated and your data is lost on restart.
         */
        store.commit();
        
        // re-open the store.
        store = (IAtomicStore)reopenStore(store);
        
        assertTrue( store.isStable() );

        /*
         * now verify data with random reads.
         */

        order = getRandomOrder(limit);
        
        for(int i=0; i<limit; i++) {
            
            long addr = addrs[order[i]];
            
            byte[] expected = records[order[i]];

            assertEquals(expected,store.read(addr));
            
        }

        } finally {

            store.destroy();
            
        }
        
    }

    /**
     * Test of abort semantics.
     */
    public void test_abort() {

        class AbortException extends RuntimeException {
            private static final long serialVersionUID = 1L;
        }

        final IAtomicStore store = (IAtomicStore) getStore();

        try {

            // write some data onto the store.
            for (int i = 0; i < 100; i++) {
                
                store.write(getRandomData());
                
            }

            // trigger an abort.
            throw new AbortException();

        } catch (AbortException ex) {

            // discard the write set.
            store.abort();

            /*
             * write different data onto the store (just to verify that it is
             * still functional).
             */
            for (int i = 0; i < 100; i++) {
                store.write(getRandomData());
            }
            
        } catch (Throwable t) {

            // discard the write set.
            store.abort();

            fail("Unexpected exception: " + t, t);

        } finally {

            store.destroy();

        }

    }

    /**
     * Unit tests writes some data, commits, and closes the journal against
     * future writes. The {@link LRUNexus} is then cleared and we verify that we
     * can still read data back from the store. Finally, we close and then
     * reopen the store in a read-only mode and verify that we can still read on
     * the store.
     * <p>
     * This test was written to verify that closing the journal against future
     * writes does not leave the write cache in an unusable state (e.g., if it
     * is discarded, then we do not attempt to read against the write cache).
     * Since {@link AbstractJournal#closeForWrites(long)} does not interfere
     * with existing readers, care must be exercised if we are to release the
     * write cache atomically.
     * 
     * @todo test also with a concurrent reader since concurrent close of the
     *       write cache could be a problem.
     */
    public void test_closeForWrites() {
        
        Journal store = (Journal) getStore();
        
        if (store.getBufferStrategy() instanceof RWStrategy)
        	return; // void test

        try {

            final int nrecs = 1000;
            final ByteBuffer[] recs = new ByteBuffer[nrecs];
            final long addrs[] = new long[nrecs];

            // Write a bunch of data onto the store.
            for (int i = 0; i < nrecs; i++) {

                recs[i] = getRandomData();

                addrs[i] = store.write(recs[i]);

            }

            // commit
            final long lastCommitTime = store.commit();

            // close against further writes.
            store.closeForWrites(lastCommitTime/* closeTime */);

            if (LRUNexus.INSTANCE != null) {

                // discard the record level cache so we will read through.
                LRUNexus.INSTANCE.deleteCache(store.getUUID());

            }

            // Verify read back.
            for (int i = 0; i < nrecs; i++) {

                final long addr = addrs[i];

                final ByteBuffer expected = recs[i];

                // position := 0, limit := capacity;
                expected.clear();

                final ByteBuffer actual = store.read(addr);

                assertEquals(expected, actual);

            }
            
            if(store.isStable()) {
                
                store = (Journal) reopenStore(store);

                // Verify read back after re-open.
                for (int i = 0; i < nrecs; i++) {

                    final long addr = addrs[i];

                    final ByteBuffer expected = recs[i];

                    // position := 0, limit := capacity;
                    expected.clear();

                    final ByteBuffer actual = store.read(addr);

                    assertEquals(expected, actual);

                }
                
            }
            
        } finally {
            
            store.destroy();
            
        }
        
    }
    
}
