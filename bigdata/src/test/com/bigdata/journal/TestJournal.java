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
 * Created on Oct 8, 2006
 */

package com.bigdata.journal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;


/**
 * Test suite for {@link Journal} initialization.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Refactor to use junit-ext and parameterize the test suite for various
 * configurations of the journal, especially the different buffering mechanisms.
 * Reuse the various helper method in place from TestCase2. We need to handle
 * (a) isolated tests, which run on a new journal; and possibly (b) tests that
 * share the same journal.  The GOM test suite is setup like (b), but journal
 * tests are probably better off like (a) -- fully isolated.
 * 
 * FIXME Work through basic operations for writing and committing a transaction
 * without concurrency support, then modify to add in concurrency.
 * 
 * @todo tests of creating a new journal, including with bad properties.
 * 
 * @todo tests of opening an existing journal, including with incomplete writes
 *       of a root block.

 * @todo tests when the journal is very large (NOT the normal use case for
 *       bigdata).
 * 
 * @todo tests of the exclusive lock mechanism during startup/shutdown (the
 *       advisory file locking mechanism). This is not used for the
 *       memory-mapped mode, but it is used for both "Direct" and "Disk" modes.
 * 
 * @todo test ability to extend the journal.
 * 
 * @todo test ability to compact and truncate the journal. Compaction moves
 *       slots from the end of the journal to fill holes earlier in the journal.
 *       Truncation chops off the tail. Compaction is done in order to
 *       facilitate truncation for a journal whose size requirements have
 *       decreased based on observed load characteristics.
 * 
 * @todo write tests for correct migration of committed records to a database.
 * 
 * @todo write tests for correct logical deletion of records no longer readable
 *       by any active transaction (they have been since updated or deleted by a
 *       committed transaction _and_ there is no transaction running that has a
 *       view of a historical consistent state in which that record version is
 *       visible).
 */

public class TestJournal extends ProxyTestCase {

    Random r = new Random();
        
    /**
     * 
     */
    public TestJournal() {
    }

    /**
     * @param arg0
     */
    public TestJournal(String arg0) {
        super(arg0);
    }
    // FIXME Test re-open of a journal in direct mode.
    public void test_open_direct() throws IOException {
        
    }
    
    // FIXME Test re-open of a journal in mapped mode.
    public void test_open_mapped() throws IOException {
        
    }
    
    // FIXME Test re-open of a journal in disk mode.
    public void test_open_disk() throws IOException {
        
    }

    /**
     * Test ability to release and consume slots on the journal.
     * 
     * @throws IOException
     * 
     * @todo Isolate and test this aspect of the API.
     */
    public void test_releaseSlots001() throws IOException {
       
    }
    
    //
    // Under one slot.
    //

    /**
     * Test of write with read back where the write fits in one slot.
     * 
     * @throws IOException
     * 
     * @todo test correct detection of corrupt states in slot chain. to do this
     *       right requires that we diddle the journal into a corrupt state and
     *       then verify that the corrupt condition is correctly detected by the
     *       various methods.
     *       
     * @todo test of write that wraps the journal.
     * @todo test of write that triggers reuse of slots already written on the
     *       journal by the same tx.
     * @todo test of write that correctly avoids reuse of slots last written on
     *       the journal by another tx.
     * @todo write test where there is not enough room to write the data in the
     *       journal (even after release).
     * @todo etc.
     */

    public void test_write_underOneSlot01() throws IOException {

        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);
        properties.setProperty("slotSize","128");

        try {
            
            Journal journal = new Journal(properties);
            
            assertEquals("slotSize",128,journal.slotSize);

            doWriteRoundTripTest(journal, new Tx(journal,0), 0, 10);

            /*
             * Verify that the #of allocated slots (this relies on the fact that
             * there is only one object in the journal).
             */
            assertEquals(1,journal.allocationIndex.cardinality());

            journal.close();
            
        } finally {

            deleteTestJournalFile(filename);
            
        }
        
    }

    /**
     * Writes an object that does not fill a slot.
     * 
     * @throws IOException
     */
    public void test_write_underOneSlot02() throws IOException {

        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);
        properties.setProperty("slotSize","128");

        try {
            
            Journal journal = new Journal(properties);
            
            assertEquals("slotSize",128,journal.slotSize);

            final Tx tx = new Tx(journal,0);
            final long id = 1;
            final int nbytes = 67;
            
            assertTrue("dataSize",journal.slotMath.dataSize>nbytes);
            
            doWriteRoundTripTest(journal, tx, id, nbytes);

            /*
             * Verify that the #of allocated slots (this relies on the fact that
             * there is only one object in the journal).
             */
            assertEquals(1,journal.allocationIndex.cardinality());

            journal.close();

        } finally {

            deleteTestJournalFile(filename);
            
        }
        
    }
    
    //
    // One slot.
    //

    /**
     * Test of write with read back that fills an entire slot exactly.
     * 
     * @throws IOException
     */
    public void test_write_fillsOneSlot() throws IOException {

        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);
        properties.setProperty("slotSize","128");

        try {
            
            Journal journal = new Journal(properties);
            
            assertEquals("slotSize",128,journal.slotSize);

            doWriteRoundTripTest(journal, new Tx(journal,0), 0, journal.slotMath.dataSize);

            /*
             * Verify that the #of allocated slots (this relies on the fact that
             * there is only one object in the journal).
             */
            assertEquals(1,journal.allocationIndex.cardinality());

            journal.close();

        } finally {

            deleteTestJournalFile(filename);
            
        }
        
    }

    /**
     * Test of write with read back that fills an entire slot less one byte.
     * 
     * @throws IOException
     */
    public void test_write_fillsOneSlotMinus1() throws IOException {

        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);
        properties.setProperty("slotSize","128");

        try {
            
            Journal journal = new Journal(properties);
            
            assertEquals("slotSize",128,journal.slotSize);

            doWriteRoundTripTest(journal, new Tx(journal,0), 0, journal.slotMath.dataSize-1);

            /*
             * Verify that the #of allocated slots (this relies on the fact that
             * there is only one object in the journal).
             */
            assertEquals(1,journal.allocationIndex.cardinality());

            journal.close();

        } finally {

            deleteTestJournalFile(filename);
            
        }
        
    }

    /**
     * Test of write with read back that fills an entire slot plus one byte.
     * 
     * @throws IOException
     */
    public void test_write_fillsOneSlotPlus1() throws IOException {

        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);
        properties.setProperty("slotSize","128");

        try {
            
            Journal journal = new Journal(properties);
            
            assertEquals("slotSize",128,journal.slotSize);

            doWriteRoundTripTest(journal, new Tx(journal,0), 0, journal.slotMath.dataSize+1);

            /*
             * Verify that the #of allocated slots (this relies on the fact that
             * there is only one object in the journal).
             */
            assertEquals(2,journal.allocationIndex.cardinality());

            journal.close();

        } finally {

            deleteTestJournalFile(filename);
            
        }
        
    }

    //
    // Two slots.
    //

    /**
     * Test of write with read back that fills two slots exactly.
     * 
     * @throws IOException
     */
    public void test_write_fillsTwoSlots() throws IOException {

        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);
        properties.setProperty("slotSize","128");

        try {
            
            Journal journal = new Journal(properties);
            
            assertEquals("slotSize",128,journal.slotSize);

            doWriteRoundTripTest(journal, new Tx(journal,0), 0, journal.slotMath.dataSize * 2);

            /*
             * Verify that the #of allocated slots (this relies on the fact that
             * there is only one object in the journal).
             */
            assertEquals(2,journal.allocationIndex.cardinality());

            journal.close();

        } finally {

            deleteTestJournalFile(filename);
            
        }
        
    }

    /**
     * Test of write with read back that fills two slots less one byte.
     * 
     * @throws IOException
     */
    public void test_write_fillsTwoSlotsMinus1() throws IOException {

        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);
        properties.setProperty("slotSize","128");

        try {
            
            Journal journal = new Journal(properties);
            
            assertEquals("slotSize",128,journal.slotSize);

            doWriteRoundTripTest(journal, new Tx(journal,0), 0,
                    (journal.slotMath.dataSize * 2) - 1);

            /*
             * Verify that the #of allocated slots (this relies on the fact that
             * there is only one object in the journal).
             */
            assertEquals(2,journal.allocationIndex.cardinality());

            journal.close();

        } finally {

            deleteTestJournalFile(filename);
            
        }
        
    }

    /**
     * Test of write with read back that fills two slots plus one byte.
     * 
     * @throws IOException
     */
    public void test_write_fillsTwoSlotsPlus1() throws IOException {

        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);
        properties.setProperty("slotSize","128");

        try {
            
            Journal journal = new Journal(properties);
            
            assertEquals("slotSize",128,journal.slotSize);

            doWriteRoundTripTest(journal, new Tx(journal,0), 0,
                    (journal.slotMath.dataSize * 2) + 1);

            /*
             * Verify that the #of allocated slots (this relies on the fact that
             * there is only one object in the journal).
             */
            assertEquals(3,journal.allocationIndex.cardinality());

            journal.close();

        } finally {

            deleteTestJournalFile(filename);
            
        }
        
    }

    //
    // Three slots.
    //

    /**
     * Test of write with read back that fills three slots exactly.
     * 
     * @throws IOException
     */
    public void test_write_fillsThreeSlots() throws IOException {

        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);
        properties.setProperty("slotSize","128");

        try {
            
            Journal journal = new Journal(properties);
            
            assertEquals("slotSize",128,journal.slotSize);

            doWriteRoundTripTest(journal, new Tx(journal,0), 0, journal.slotMath.dataSize * 3);
            
            /*
             * Verify that the #of allocated slots (this relies on the fact that
             * there is only one object in the journal).
             */
            assertEquals(3,journal.allocationIndex.cardinality());

            journal.close();

        } finally {

            deleteTestJournalFile(filename);
            
        }
        
    }

    /**
     * Test of write with read back that fills three slots less one byte.
     * 
     * @throws IOException
     */
    public void test_write_fillsThreeSlotsMinus1() throws IOException {

        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);
        properties.setProperty("slotSize","128");

        try {
            
            Journal journal = new Journal(properties);
            
            assertEquals("slotSize",128,journal.slotSize);

            doWriteRoundTripTest(journal, new Tx(journal,0), 0,
                    (journal.slotMath.dataSize * 3) - 1);
            
            /*
             * Verify that the #of allocated slots (this relies on the fact that
             * there is only one object in the journal).
             */
            assertEquals(3,journal.allocationIndex.cardinality());

            journal.close();

        } finally {

            deleteTestJournalFile(filename);
            
        }
        
    }

    /**
     * Test of write with read back that fills three slots plus one byte.
     * 
     * @throws IOException
     */
    public void test_write_fillsThreeSlotsPlus1() throws IOException {

        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);
        properties.setProperty("slotSize","128");

        try {
            
            Journal journal = new Journal(properties);
            
            assertEquals("slotSize",128,journal.slotSize);

            doWriteRoundTripTest(journal, new Tx(journal,0), 0,
                    (journal.slotMath.dataSize * 3) + 1);
            
            /*
             * Verify that the #of allocated slots (this relies on the fact that
             * there is only one object in the journal).
             */
            assertEquals(4,journal.allocationIndex.cardinality());

            journal.close();

        } finally {

            deleteTestJournalFile(filename);
            
        }
        
    }

    /**
     * Test of multiple objects write with interleaved and final read back. Each
     * object fills one or more slots.
     * 
     * @throws IOException
     * 
     * @todo Modify to use a distribution for data size that fits with
     *       expectations for the journal. E.g., most objects fit into one slot,
     *       some take several slots. Occasional outliners might take 100 or
     *       more slots (up to ~8k or 32k, but there is a limit on object size
     *       for the journal).
     * 
     * @todo Evolve this into a stress test.
     * 
     * @todo Evolve this into a stress test that also verifies restart.
     */
    public void test_write_multipleObjectWrites001() throws IOException {

        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);
        properties.setProperty("slotSize","128");

        // #of objects to write.
        long limit = 1000;

        // The data written on the store.
        Map<Long,byte[]> written = new HashMap<Long, byte[]>();
        
        try {
            
            Journal journal = new Journal(properties);
            
            assertEquals("slotSize",128,journal.slotSize);

            Tx tx = new Tx(journal,0);
            
            int maxSize = journal.slotMath.dataSize * 50;
            
            for (long id = 0; id < limit; id++) {

                int nbytes = r.nextInt(maxSize)+1; // +1 avoids zero length items.

                written.put(id, doWriteRoundTripTest(journal, tx, id, nbytes) );

            }
            
            /*
             * Verify that the written data can all be read back.
             */
            System.err.println("Re-reading data to re-verify writes.");

            Iterator<Map.Entry<Long,byte[]>> itr = written.entrySet().iterator();
            
            while( itr.hasNext() ) {
                
                Map.Entry<Long,byte[]> entry = itr.next();
                
                long id = entry.getKey();
                
                byte[] expected = entry.getValue();

                System.err.println("Verifying read: tx=" + tx + ", id=" + id
                        + ", size=" + expected.length);
                
                ByteBuffer actual = journal.read(tx, id);
                
                assertEquals("acutal.position()",0,actual.position());
                assertEquals("acutal.limit()",expected.length,actual.limit());
                assertEquals("acutal.capacity()",expected.length,actual.capacity());
                assertEquals(expected,actual);

            }

            journal.close();

        } finally {

            deleteTestJournalFile(filename);
            
        }
        
    }

    //
    // Delete object.
    //

    /**
     * Test verifies that an object written on the store may be deleted.
     * 
     * @todo Do some more simple tests where a few objects are written, read
     *       back, deleted one by one, and verify that they can no longer be
     *       read.
     * 
     * @todo Do stress test with writes, reads, and deletes.
     * 
     * @todo Verify that the slots are released once there is no active
     *       transaction that could read the deleted version (testing this is
     *       complex - it is really a concurrency control test and can't be
     *       written without building out the transaction model further).
     */
    
    public void test_delete001() throws IOException {

        final Properties properties = getProperties();

        final String filename = getTestJournalFile();

        properties.setProperty("file", filename);
        properties.setProperty("slotSize", "128");

        try {

            Journal journal = new Journal(properties);

            assertEquals("slotSize", 128, journal.slotSize);

            Tx tx = new Tx(journal,0);
            
            long id = 0;
            
            byte[] expected = doWriteRoundTripTest(journal, tx, id,
                    (journal.slotMath.dataSize * 3) + 1);

            /*
             * #of slots allocated to that object (this relies on the fact that
             * it is the only object in the journal).
             */
            final int nallocated = journal.allocationIndex.cardinality();
            System.err.println("Allocated "+nallocated+" slots to tx="+tx+", id="+id);

            ByteBuffer actual = journal.read(tx, id);

            assertEquals("acutal.position()",0,actual.position());
            assertEquals("acutal.limit()",expected.length,actual.limit());
            assertEquals("acutal.capacity()",expected.length,actual.capacity());
            assertEquals(expected,actual);

            // The firstSlot for the version that we are about to delete.
            final int firstSlot = journal.objectIndex.get(id).intValue();
            
            journal.delete(tx, id);

            // Verify the object is now correctly marked as deleted in the
            // object index.
            assertEquals(-(firstSlot+1),journal.objectIndex.get(id).intValue());

            // Verify that the #of allocated slots has not changed.
            assertEquals(nallocated,journal.allocationIndex.cardinality());

            /*
             * Test read after delete.
             */
            try {
                
                journal.read(tx, id);

                fail("Expecting " + IllegalArgumentException.class);
                
            } catch (IllegalArgumentException ex) {
                
                System.err.println("Ignoring expected exception: " + ex);
                
            }

            /*
             * Test delete after delete.
             */
            try {
                
                journal.delete(tx, id);

                fail("Expecting " + IllegalStateException.class);
                
            } catch (IllegalStateException ex) {
                
                System.err.println("Ignoring expected exception: " + ex);
                
            }

            // Verify that the #of allocated slots has not changed.
            assertEquals(nallocated,journal.allocationIndex.cardinality());

            /*
             * Deallocate the slots for that object.
             */

            journal.deallocateSlots(tx, id);
            
            // Verify the entry in the object index is gone.
            assertNull(journal.objectIndex.get(id));

            // Verify that there are no more allocated slots.
            assertEquals("nallocated", 0, journal.allocationIndex.cardinality());

            /*
             * Verify that read now reports "NOTFOUND" (vs DELETED).
             */
            assertNull("Read returns non-null", journal.read(tx, id));

            journal.close();

        } finally {

            deleteTestJournalFile(filename);

        }
    }
    
    //
    // Test helper.
    //

    /**
     * Write a data version consisting of N random bytes and verify that we can
     * read it back out again.
     * 
     * @param journal
     *            The journal.
     * @param tx
     *            The transaction.
     * @param id
     *            The persistent identifier.
     * @param nbytes
     *            The data version length.
     * 
     * @return The data written. This can be used to re-verify the write after
     *         intervening reads.
     */
    
    protected byte[] doWriteRoundTripTest(Journal journal,Tx tx, long id, int nbytes) {

        System.err.println("Test writing tx="+tx+", id="+id+", nbytes="+nbytes);
        
        byte[] expected = new byte[nbytes];
        
        r.nextBytes(expected);
        
        ByteBuffer data = ByteBuffer.wrap(expected);
        
        assertNull(journal.objectIndex.get(id));
        
        int firstSlot = journal.write(tx,id,data);

        assertNotNull(journal.objectIndex.get(id));
        
        assertEquals(firstSlot,journal.objectIndex.get(id).intValue());

        ByteBuffer actual = journal.read(tx, id);
        
        assertEquals("acutal.position()",0,actual.position());
        assertEquals("acutal.limit()",expected.length,actual.limit());
        assertEquals("acutal.capacity()",expected.length,actual.capacity());
        assertEquals(expected,actual);

        return expected;
        
    }
    
}
