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
 * Created on Feb 4, 2007
 */

package com.bigdata.journal;

import java.nio.ByteBuffer;
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
    
    /**
     * Re-open the same backing store.
     * 
     * @return A new store.
     * 
     * @exception Throwable
     *                if the existing store is not closed, e.g., from failure to
     *                obtain a file lock, etc.
     */
    protected IRawStore reopenStore() {
        
        return new Journal(getProperties());
        
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
        ByteBuffer actual = store.read(addr1,null);
        
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
        
        // close the store.
        store.close();
        
        // re-open the store.
        store = (IAtomicStore)reopenStore();
        
        assertTrue( store.isStable() );
        
        /*
         * attempt read the data back. this should throw an exception since the
         * nextOffset in the root block will still be zero and the store can
         * therefore correctly reject this address as never written.
         */
        try {
            actual = store.read(addr1,null);
            fail("Expecting: "+IllegalArgumentException.class);
        }catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
//        // verify that the data are all zeros since we did not commit the store.
//        assertEquals(new byte[len],actual);

        store.close();
        
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
        ByteBuffer actual = store.read(addr1,null);
        
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
        
        // close the store.
        store.close();
        
        // re-open the store.
        store = (IAtomicStore)reopenStore();
        
        assertTrue( store.isStable() );
        
        // read the data back.
        actual = store.read(addr1,null);
        
        assertEquals(expected,actual);

        store.close();
        
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

            assertEquals(expected,store.read(addr, null));
        
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

            assertEquals(expected,store.read(addr, null));
            
        }
        
        /*
         * Commit the changes - if you do not commit the changes then the root
         * blocks are not updated and your data is lost on restart.
         */
        store.commit();
        
        // close the store.
        store.close();
        
        // re-open the store.
        store = (IAtomicStore)reopenStore();
        
        assertTrue( store.isStable() );

        /*
         * now verify data with random reads.
         */

        order = getRandomOrder(limit);
        
        for(int i=0; i<limit; i++) {
            
            long addr = addrs[order[i]];
            
            byte[] expected = records[order[i]];

            assertEquals(expected,store.read(addr, null));
            
        }

        store.close();
        
    }
    
}
