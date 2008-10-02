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

import com.bigdata.rawstore.IRawStore;

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
    protected IRawStore reopenStore(IRawStore store) {

        // close the store.
        store.close();
        
        // Note: Clone to avoid modifying!!!
        Properties properties = (Properties)getProperties().clone();
        
        // Turn this off now since we want to re-open the same store.
        properties.setProperty(Options.CREATE_TEMP_FILE,"false");
        
        // The backing file that we need to re-open.
        File file = store.getFile();
        
        assertNotNull(file);
        
        // Set the file property explictly.
        properties.setProperty(Options.FILE,file.toString());
        
        return new Journal( properties );
        
    }

    /**
     * Writes a record, verifies the write but does NOT commit the store. Closes
     * and reopens the store and finally verifies the write was lost.
     */
    public void test_restartSafe_oneWriteNoCommit() {
        
        IAtomicStore store = (IAtomicStore)getStore();
        
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

//        /*
//         * Commit the changes - if you do not commit the changes then the root
//         * blocks are not updated and your data is lost on restart.
//         */
//        store.commit();
        
        // re-open the store.
        store = (IAtomicStore)reopenStore(store);
        
        assertTrue( store.isStable() );
        
        /*
         * attempt read the data back. this should throw an exception since the
         * nextOffset in the root block will still be zero and the store can
         * therefore correctly reject this address as never written.
         */
        try {
            actual = store.read(addr1);
            fail("Expecting: "+IllegalArgumentException.class);
        }catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
//        // verify that the data are all zeros since we did not commit the store.
//        assertEquals(new byte[len],actual);

        store.destroy();
        
    }
    
    /**
     * Writes a record, verifies the write then commits the store. Closes and
     * reopens the store and finally verifies the write on the reopened store.
     */
    public void test_restartSafe_oneWrite() {
        
        IAtomicStore store = (IAtomicStore)getStore();
        
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

        store.destroy();
        
    }
    
    /**
     * Test writes a bunch of records and verifies that each can be read after
     * it is written.  The test then performs a random order read and verifies
     * that each of the records can be read correctly.
     */
    public void test_restartSafe_multipleWrites() {

        IAtomicStore store = (IAtomicStore)getStore();
        
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

        store.destroy();
        
    }
    
}
