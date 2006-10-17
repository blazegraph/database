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
 * Test suite for basic {@link Journal} operations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo tests of creating a new journal, including with bad properties.
 * 
 * @todo tests of opening an existing journal, including with incomplete writes
 *       of a root block.
 * 
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
     * FIXME Isolate and test this aspect of the API.
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
            
            assertEquals("slotSize",128,journal.slotMath.slotSize);

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
            
            assertEquals("slotSize",128,journal.slotMath.slotSize);

            final Tx tx = new Tx(journal,0);
            final int id = 1;
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

        try {
            
            Journal journal = new Journal(properties);

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

        try {
            
            Journal journal = new Journal(properties);

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

        try {
            
            Journal journal = new Journal(properties);
            
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

        try {
            
            Journal journal = new Journal(properties);

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

        try {
            
            Journal journal = new Journal(properties);
            
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

        try {
            
            Journal journal = new Journal(properties);

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

        try {
            
            Journal journal = new Journal(properties);

            doWriteRoundTripTest(journal, new Tx(journal, 0), 0,
                    journal.slotMath.dataSize * 3);
            
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

        try {
            
            Journal journal = new Journal(properties);

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

        try {
            
            Journal journal = new Journal(properties);
            
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
     * Test writes multiple versions and verifies that the correct version may
     * be read back at any time. The last written version is then deleted and we
     * verify that read, write and delete operations all correctly report that
     * the data is deleted.
     * 
     * @throws IOException
     * 
     * FIXME Do a version of this that operations in a transaction.
     * 
     * FIXME For both the isolated and the unisolated version, verify that we
     * are immediately deallocating slots for the historical versions. There are
     * some fenceposts here that need to be tested, including: when a version
     * exists on the journal before the transaction starts only the 2nd write
     * should cause the prior version to be immediately deallocated; when a
     * version is being overwritten without isolation, the prior version may be
     * immediately deallocated IFF there are no active transactions (since there
     * can then be no reads of the prior version).
     */
    public void test_writeMultipleVersions() throws IOException {

        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);

        try {
            
            Journal journal = new Journal(properties);

            // Two versions of id0.
            final int id0 = 0;
            final ByteBuffer expected0v0 = getRandomData(journal);
            final ByteBuffer expected0v1 = getRandomData(journal);
            
            // Three versions of id1.
            final int id1 = 1;
            final ByteBuffer expected1v0 = getRandomData(journal);
            final ByteBuffer expected1v1 = getRandomData(journal);
            final ByteBuffer expected1v2 = getRandomData(journal);
            
            // precondition tests, write id0 version0, postcondition tests.
            assertNotFound(journal.read(null,id0,null));
            
            assertNotFound(journal.read(null,id1,null));

            journal.write(null,id0,expected0v0);
            
            assertEquals(expected0v0.array(),journal.read(null, id0, null));
            
            assertNotFound(journal.read(null,id1,null));

            // write id1 version0, postcondition tests.
            journal.write(null,id1,expected1v0);
            
            assertEquals(expected0v0.array(),journal.read(null, id0, null));
            
            assertEquals(expected1v0.array(),journal.read(null, id1, null));
            
            // write id1 version1, postcondition tests.
            journal.write(null,id1,expected1v1);
            
            assertEquals(expected0v0.array(),journal.read(null, id0, null));
            
            assertEquals(expected1v1.array(),journal.read(null, id1, null));
            
            // write id1 version2, postcondition tests.
            journal.write(null,id1,expected1v2);
            
            assertEquals(expected0v0.array(),journal.read(null, id0, null));
            
            assertEquals(expected1v2.array(),journal.read(null, id1, null));

            // write id0 version1, postcondition tests.
            journal.write(null,id0,expected0v1);
            
            assertEquals(expected0v1.array(),journal.read(null, id0, null));
            
            assertEquals(expected1v2.array(),journal.read(null, id1, null));

            // delete id1, postcondition tests.

            journal.delete(null, id1);
            
            assertEquals(expected0v1.array(),journal.read(null, id0, null));
            
            assertDeleted(journal, id1);

            // delete id0, postcondition tests.

            journal.delete(null, id0);
            
            assertDeleted(journal, id0);
            
            assertDeleted(journal, id1);

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
     * @todo Evolve this into a stress test that also verifies restart. This
     *       currently does a lot of redundent reads in
     *       {@link #doWriteRoundTripTest(Journal, Tx, int, int)} that we would
     *       not want as part of a benchmark, but they are fine for a stress
     *       test.
     */
    public void test_write_multipleObjectWrites001() throws IOException {

        final Properties properties = getProperties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);

        // #of objects to write.
        long limit = 100;

        // The data written on the store.
        Map<Integer,byte[]> written = new HashMap<Integer, byte[]>();
        
        try {
            
            Journal journal = new Journal(properties);

            Tx tx = new Tx(journal,0);
            
            int maxSize = journal.slotMath.dataSize * 50;
            
            for (int id = 0; id < limit; id++) {

                int nbytes = r.nextInt(maxSize)+1; // +1 avoids zero length items.

                written.put(id, doWriteRoundTripTest(journal, tx, id, nbytes) );

            }
            
            /*
             * Verify that the written data can all be read back.
             */
            System.err.println("Re-reading data to re-verify writes.");

            Iterator<Map.Entry<Integer,byte[]>> itr = written.entrySet().iterator();
            
            while( itr.hasNext() ) {
                
                Map.Entry<Integer,byte[]> entry = itr.next();
                
                int id = entry.getKey();
                
                byte[] expected = entry.getValue();

                System.err.println("Verifying read: tx=" + tx + ", id=" + id
                        + ", size=" + expected.length);
                
                // FIXME Also try reading multiple times into a buffer to verify
                // that the buffer is used and that the contract for its use is
                // observed.
                ByteBuffer actual = journal.read(tx, id, null);
                
                assertEquals("acutal.position()",0,actual.position());
                assertEquals("acutal.limit()",expected.length,actual.limit());
                assertEquals("limit() - position()", expected.length,actual.limit() - actual.position());
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

        try {

            Journal journal = new Journal(properties);

            Tx tx = new Tx(journal,0);
            
            int id = 0;
            
            byte[] expected = doWriteRoundTripTest(journal, tx, id,
                    (journal.slotMath.dataSize * 3) + 1);

            /*
             * #of slots allocated to that object (this relies on the fact that
             * it is the only object in the journal).
             */
            final int nallocated = journal.allocationIndex.cardinality();
            System.err.println("Allocated "+nallocated+" slots to tx="+tx+", id="+id);

            ByteBuffer actual = journal.read(tx, id, null);

            assertEquals("acutal.position()",0,actual.position());
            assertEquals("acutal.limit()",expected.length,actual.limit());
            assertEquals("limit() - position()",expected.length,actual.limit() - actual.position());
            assertEquals(expected,actual);

            // The firstSlot for the version that we are about to delete.
            final int firstSlot = tx.objectIndex.getFirstSlot(id);
            
            assertEquals(firstSlot,journal.delete(tx, id));

            // Verify the object is now correctly marked as deleted in the
            // object index.
            try {
                tx.objectIndex.getFirstSlot(id);
                fail("Expecting: "+DataDeletedException.class);
            }
            catch(DataDeletedException ex) {
                System.err.println("Ignoring expected exception: "+ex);
            }

            // Verify that the #of allocated slots has not changed.
            assertEquals(nallocated,journal.allocationIndex.cardinality());

            /*
             * Test read after delete.
             */
            assertDeleted(tx,id);

            /*
             * Test delete after delete.
             */
            try {
                
                journal.delete(tx, id);

                fail("Expecting " + DataDeletedException.class);
                
            } catch (DataDeletedException ex) {
                
                System.err.println("Ignoring expected exception: " + ex);
                
            }

            /*
             * Test write after delete.
             */
            try {
                
                journal.write(tx, id, getRandomData(journal));

                fail("Expecting " + DataDeletedException.class);
                
            } catch (DataDeletedException ex) {
                
                System.err.println("Ignoring expected exception: " + ex);
                
            }

            // Verify that the #of allocated slots has not changed.
            assertEquals(nallocated,journal.allocationIndex.cardinality());

            /*
             * Deallocate the slots for that object.
             */

            journal.deallocateSlots(tx, firstSlot);

            // clean up the object index since the slots were deallocated (this
            // is just a wee-bit of a low-level hack).
            assertEquals(firstSlot, (tx == null ? journal.objectIndex
                    .removeDeleted(id) : tx.objectIndex.removeDeleted(id)));

            
            // Verify the entry in the object index is gone.
            assertEquals("Expecting 'NOTFOUND'", IObjectIndex.NOTFOUND,
                    tx.objectIndex.getFirstSlot(id));

            // Verify that there are no more allocated slots.
            assertEquals("nallocated", 0, journal.allocationIndex.cardinality());

            /*
             * Verify that read now reports "not found", indicating that the
             * caller MUST attempt to resolve the object against the database
             * (not that it will be found there either for this test case).
             */
            assertNull("Read returns non-null", journal.read(tx, id, null));

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
     *            The int32 within-segment persistent identifier.
     * @param nbytes
     *            The data version length.
     * 
     * @return The data written. This can be used to re-verify the write after
     *         intervening reads.
     */
    
    protected byte[] doWriteRoundTripTest(Journal journal,Tx tx, int id, int nbytes) {

        System.err.println("Test writing tx="+tx+", id="+id+", nbytes="+nbytes);
        
        byte[] expected = new byte[nbytes];
        
        r.nextBytes(expected);
        
        ByteBuffer data = ByteBuffer.wrap(expected);
        
        assertEquals(IObjectIndex.NOTFOUND,tx.objectIndex.getFirstSlot(id));
        
        int firstSlot = journal.write(tx,id,data);
        assertEquals("limit() != #bytes", expected.length, data.limit());
        assertEquals("position() != limit()",data.limit(),data.position());
        
        assertEquals(firstSlot,tx.objectIndex.getFirstSlot(id));
        
        /*
         * Read into a buffer allocated by the Journal.
         */
        ByteBuffer actual = journal.read(tx, id, null);

        assertEquals("acutal.position()",0,actual.position());
        assertEquals("acutal.limit()",expected.length,actual.limit());
        assertEquals("limit() - position() == #bytes",expected.length,actual.limit() - actual.position());
        assertEquals(expected,actual);

        /*
         * Read multiple copies into a buffer that we allocate ourselves.
         */
        final int ncopies = 7;
        int pos = 0;
        actual = ByteBuffer.allocate(expected.length * ncopies);
        for( int i=0; i<ncopies; i++ ) {

            /*
             * Setup to read into the next slice of our buffer.
             */
//            System.err.println("reading @ i="+i+" of "+ncopies);
            pos = i * expected.length;
            actual.limit( actual.capacity() );
            actual.position( pos );
            
            ByteBuffer tmp = journal.read(tx, id, actual);
            assertTrue("Did not read into the provided buffer", tmp == actual);
            assertEquals("position()", pos, actual.position() );
            assertEquals("limit() - position()", expected.length, actual.limit() - actual.position());
            assertEquals(expected,actual);

            /*
             * Attempt to read with insufficient remaining bytes in the buffer
             * and verify that the data are read into a new buffer.
             */
            actual.limit(pos+expected.length-1);
            tmp = journal.read(tx, id, actual);
            assertFalse("Read failed to allocate a new buffer", tmp == actual);
            assertEquals(expected,tmp);

        }
        
        return expected;
        
    }
    
}
