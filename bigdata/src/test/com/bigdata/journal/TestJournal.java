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
 * @todo tests when the journal is very large. This is NOT the normal use case
 *       for bigdata, but one option for an embedded database. However, even an
 *       embedded database would normally use a read-optimized database segment
 *       paired with the journal.
 * 
 * @todo Do stress test with writes, reads, and deletes.
 * 
 * @todo tests of the exclusive lock mechanism during startup/shutdown (the
 *       advisory file locking mechanism). This is not used for the
 *       memory-mapped mode, but it is used for both "Direct" and "Disk" modes.
 * 
 * @todo test for correct detection of a "full" journal.
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

    //
    //
    //
    
    /**
     * Test of low-level non-isolated read, write, update, delete operations
     * using the journal. These operations work directly with slot allocations
     * and do not use the object index. (In fact, the object index uses these
     * operations itself to store its nodes on the journal.) It is not possible
     * to test for a "not found" condition here since a READ just appends the
     * data from the slots into a buffer - you can read anything this way, but
     * the data may not be what you want. It also does not make sense to test
     * for the state of the old data following a write or delete since the data
     * will be unchanged until someone comes along to overwrite them.
     * 
     * @todo Note that deallocation may wind up being conditional so that it is
     *       restart safe (it probably needs to be).
     */
    public void test_lowLevelCrud() throws IOException {
        
        final Properties properties = getProperties();

        try {

            Journal journal = new Journal(properties);

            final ByteBuffer expected0 = getRandomData(journal);
            final ByteBuffer expected1 = getRandomData(journal);
            final ByteBuffer expected2 = getRandomData(journal);
            
            // write on the journal (aka insert).
            final ISlotAllocation slots0 = journal.write(expected0);

            assertSlotAllocationState(slots0, journal.allocationIndex, true);
            
            assertEquals(expected0.array(),journal.read(slots0, null));

            // update.
            final ISlotAllocation slots1 = journal.update(slots0, expected1);
            
            assertSlotAllocationState(slots0, journal.allocationIndex, false);

            assertSlotAllocationState(slots1, journal.allocationIndex, true);

            assertEquals(expected1.array(),journal.read(slots1, null));
            
            // update.
            final ISlotAllocation slots2 = journal.update(slots0, expected2);
            
            assertSlotAllocationState(slots0, journal.allocationIndex, false);

            assertSlotAllocationState(slots1, journal.allocationIndex, true);

            assertSlotAllocationState(slots2, journal.allocationIndex, true);

            assertEquals(expected2.array(),journal.read(slots2, null));
            
            // delete.
            journal.delete( slots2 );
            
            assertSlotAllocationState(slots2, journal.allocationIndex, false);
            
            journal.close();

        } finally {

            deleteTestJournalFile();

        }
        
    }
    
//    /**
//     * Test of low-level non-isolated read, write, update, delete operations on
//     * objects using the journal and the extSer package. These operations work
//     * directly with slot allocations and do not use the object index. (In fact,
//     * the object index uses these operations itself to store its nodes on the
//     * journal.) It is not possible to test for a "not found" condition here
//     * since a READ just appends the data from the slots into a buffer - you can
//     * read anything this way, but the data may not be what you want. It also
//     * does not make sense to test for the state of the old data following a
//     * write or delete since the data will be unchanged until someone comes
//     * along to overwrite them.
//     * 
//     * @todo Do variant tests of a persistence capable object index (actually,
//     *       this will need to be its own test suite).
//     */
//    public void test_lowLevelObjectCrud() throws IOException {
//        
//        final Properties properties = getProperties();
//
//        final String filename = getTestJournalFile();
//
//        properties.setProperty("file", filename);
//
//        try {
//
//            Journal journal = new Journal(properties);
//
//            final Object expected0 = "expected0";
//            final Object expected1 = "expected1";
//            final Object expected2 = "expected2";
//            
//            // write on the journal (aka insert).
//            final long id0 = journal._insertObject(expected0);
//
//            assertEquals(expected0,journal._readObject(id0));
//
//            // update.
//            final long id1 = journal._updateObject(id0, expected1);
//            
//            assertEquals(expected1,journal._readObject(id1));
//            
//            // update.
//            final long id2 = journal._updateObject(id1, expected2);
//
//            assertEquals(expected2,journal._readObject(id2));
//            
//            // delete.
//            journal._deleteObject(id2);
//            
//            journal.close();
//
//        } finally {
//
//            deleteTestJournalFile(filename);
//
//        }
//        
//    }

    /**
     * Correct rejection tests for persistent identifiers (must be positive
     * integers).
     */
    public void test_ids_correctRejection() throws IOException {
        
        final Properties properties = getProperties();
        
        properties.setProperty(Options.SLOT_SIZE,"128");

        try {
            
            Journal journal = new Journal(properties);

            try {
                journal.write(0,getRandomData(journal));
                fail("Expecting: "+IllegalArgumentException.class);
            } catch(IllegalArgumentException ex ) {
                System.err.println("Ignoring expected exception: "+ex);
            }

            try {
                journal.write(-1,getRandomData(journal));
                fail("Expecting: "+IllegalArgumentException.class);
            } catch(IllegalArgumentException ex ) {
                System.err.println("Ignoring expected exception: "+ex);
            }
            
            try {
                journal.read(0,null);
                fail("Expecting: "+IllegalArgumentException.class);
            } catch(IllegalArgumentException ex ) {
                System.err.println("Ignoring expected exception: "+ex);
            }
            
            try {
                journal.read(-1,null);
                fail("Expecting: "+IllegalArgumentException.class);
            } catch(IllegalArgumentException ex ) {
                System.err.println("Ignoring expected exception: "+ex);
            }
            
            try {
                journal.delete(0);
                fail("Expecting: "+IllegalArgumentException.class);
            } catch(IllegalArgumentException ex ) {
                System.err.println("Ignoring expected exception: "+ex);
            }
            
            try {
                journal.delete(-1);
                fail("Expecting: "+IllegalArgumentException.class);
            } catch(IllegalArgumentException ex ) {
                System.err.println("Ignoring expected exception: "+ex);
            }
            
            journal.close();
            
        } finally {

            deleteTestJournalFile();
            
        }

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
     * 
     * FIXME All of these tests are transactional the way that they are
     * currently written. Refactor the tests so that we can run them against
     * both the {@link Journal} and a writable {@link Tx} using their common
     * {@link IStore} interface.
     */

    public void test_write_underOneSlot01() throws IOException {

        final Properties properties = getProperties();
        
        properties.setProperty(Options.SLOT_SIZE,"128");

        try {
            
            Journal journal = new Journal(properties);
            
            assertEquals("slotSize",128,journal.slotMath.slotSize);

            Tx tx = new Tx(journal,0);
            
            doWriteRoundTripTest(tx, 1, 10);

//            /*
//             * Verify that the #of allocated slots (this relies on the fact that
//             * there is only one object in the journal).
//             */
//            assertEquals(1,journal.allocationIndex.getAllocatedSlotCount());

            tx.prepare();
            
            tx.commit();
            
            journal.close();
            
        } finally {

            deleteTestJournalFile();
            
        }
        
    }

    /**
     * Writes an object that does not fill a slot.
     * 
     * @throws IOException
     */
    public void test_write_underOneSlot02() throws IOException {

        final Properties properties = getProperties();
        
        properties.setProperty(Options.SLOT_SIZE,"128");

        try {
            
            Journal journal = new Journal(properties);
            
            assertEquals("slotSize",128,journal.slotMath.slotSize);

            final Tx tx = new Tx(journal,0);
            final int id = 1;
            final int nbytes = 67;
            
            assertTrue("dataSize",journal.slotMath.slotSize>nbytes);
            
            doWriteRoundTripTest(tx, id, nbytes);

//            /*
//             * Verify that the #of allocated slots (this relies on the fact that
//             * there is only one object in the journal).
//             */
//            assertEquals(1,journal.allocationIndex.getAllocatedSlotCount());

            tx.prepare();
            
            tx.commit();

            journal.close();

        } finally {

            deleteTestJournalFile();
            
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
        
        try {
            
            Journal journal = new Journal(properties);

            Tx tx = new Tx(journal,0);

            doWriteRoundTripTest(tx, 1, journal.slotMath.slotSize);

//            /*
//             * Verify that the #of allocated slots (this relies on the fact that
//             * there is only one object in the journal).
//             */
//            assertEquals(1,journal.allocationIndex.getAllocatedSlotCount());

            tx.prepare();
            
            tx.commit();

            journal.close();

        } finally {

            deleteTestJournalFile();
            
        }
        
    }

    /**
     * Test of write with read back that fills an entire slot less one byte.
     * 
     * @throws IOException
     */
    public void test_write_fillsOneSlotMinus1() throws IOException {

        final Properties properties = getProperties();
        
        try {
            
            Journal journal = new Journal(properties);

            Tx tx = new Tx(journal,0);
            
            doWriteRoundTripTest(tx, 1, journal.slotMath.slotSize-1);

//            /*
//             * Verify that the #of allocated slots (this relies on the fact that
//             * there is only one object in the journal).
//             */
//            assertEquals(1,journal.allocationIndex.getAllocatedSlotCount());

            tx.prepare();
            
            tx.commit();
            
            journal.close();

        } finally {

            deleteTestJournalFile();
            
        }
        
    }

    /**
     * Test of write with read back that fills an entire slot plus one byte.
     * 
     * @throws IOException
     */
    public void test_write_fillsOneSlotPlus1() throws IOException {

        final Properties properties = getProperties();
        
        try {
            
            Journal journal = new Journal(properties);
            
            Tx tx = new Tx(journal,0);
            
            doWriteRoundTripTest(tx, 1, journal.slotMath.slotSize+1);

//            /*
//             * Verify that the #of allocated slots (this relies on the fact that
//             * there is only one object in the journal).
//             */
//            assertEquals(2,journal.allocationIndex.getAllocatedSlotCount());

            tx.prepare();
            
            tx.commit();
            
            journal.close();

        } finally {

            deleteTestJournalFile();
            
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
        
        try {
            
            Journal journal = new Journal(properties);

            Tx tx = new Tx(journal,0);
            
            doWriteRoundTripTest(tx, 1, journal.slotMath.slotSize * 2);

//            /*
//             * Verify that the #of allocated slots (this relies on the fact that
//             * there is only one object in the journal).
//             */
//            assertEquals(2,journal.allocationIndex.getAllocatedSlotCount());

            tx.prepare();
            
            tx.commit();
            
            journal.close();

        } finally {

            deleteTestJournalFile();
            
        }
        
    }

    /**
     * Test of write with read back that fills two slots less one byte.
     * 
     * @throws IOException
     */
    public void test_write_fillsTwoSlotsMinus1() throws IOException {

        final Properties properties = getProperties();

        try {
            
            Journal journal = new Journal(properties);
            
            Tx tx = new Tx(journal,0);
            
            doWriteRoundTripTest(tx, 1,
                    (journal.slotMath.slotSize * 2) - 1);

//            /*
//             * Verify that the #of allocated slots (this relies on the fact that
//             * there is only one object in the journal).
//             */
//            assertEquals(2,journal.allocationIndex.getAllocatedSlotCount());

            tx.prepare();
            
            tx.commit();
            
            journal.close();

        } finally {

            deleteTestJournalFile();
            
        }
        
    }

    /**
     * Test of write with read back that fills two slots plus one byte.
     * 
     * @throws IOException
     */
    public void test_write_fillsTwoSlotsPlus1() throws IOException {

        final Properties properties = getProperties();
        
        try {
            
            Journal journal = new Journal(properties);

            Tx tx = new Tx(journal,0);
            
            doWriteRoundTripTest(tx, 1,
                    (journal.slotMath.slotSize * 2) + 1);

//            /*
//             * Verify that the #of allocated slots (this relies on the fact that
//             * there is only one object in the journal).
//             */
//            assertEquals(3,journal.allocationIndex.getAllocatedSlotCount());

            tx.prepare();
            
            tx.commit();
            
            journal.close();

        } finally {

            deleteTestJournalFile();
            
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

        try {
            
            Journal journal = new Journal(properties);

            Tx tx = new Tx(journal, 0);
            
            doWriteRoundTripTest(tx, 1,
                    journal.slotMath.slotSize * 3);
            
//            /*
//             * Verify that the #of allocated slots (this relies on the fact that
//             * there is only one object in the journal).
//             */
//            assertEquals(3,journal.allocationIndex.getAllocatedSlotCount());

            tx.prepare();
            
            tx.commit();
            
            journal.close();

        } finally {

            deleteTestJournalFile();
            
        }
        
    }

    /**
     * Test of write with read back that fills three slots less one byte.
     * 
     * @throws IOException
     */
    public void test_write_fillsThreeSlotsMinus1() throws IOException {

        final Properties properties = getProperties();
        
        try {
            
            Journal journal = new Journal(properties);

            Tx tx = new Tx(journal, 0);
            
            doWriteRoundTripTest(tx, 1,
                    (journal.slotMath.slotSize * 3) - 1);
            
//            /*
//             * Verify that the #of allocated slots (this relies on the fact that
//             * there is only one object in the journal).
//             */
//            assertEquals(3,journal.allocationIndex.getAllocatedSlotCount());

            tx.prepare();
            
            tx.commit();
            
            journal.close();

        } finally {

            deleteTestJournalFile();
            
        }
        
    }

    /**
     * Test of write with read back that fills three slots plus one byte.
     * 
     * @throws IOException
     */
    public void test_write_fillsThreeSlotsPlus1() throws IOException {

        final Properties properties = getProperties();
        
        try {
            
            Journal journal = new Journal(properties);

            Tx tx = new Tx(journal, 0);
            
            doWriteRoundTripTest(tx, 1,
                    (journal.slotMath.slotSize * 3) + 1);
            
//            /*
//             * Verify that the #of allocated slots (this relies on the fact that
//             * there is only one object in the journal).
//             */
//            assertEquals(4,journal.allocationIndex.getAllocatedSlotCount());

            tx.prepare();
            
            tx.commit();
            
            journal.close();

        } finally {

            deleteTestJournalFile();
            
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
     * @see TestTx#test_writeMultipleVersions()
     * 
     * FIXME Verify that we are immediately deallocating slots for the
     * historical versions.
     * 
     * FIXME When a version is being overwritten without isolation, verify that
     * the prior version is immediately deallocated IFF there are no active
     * transactions (since there can then be no reads of the prior version).
     */
    public void test_writeMultipleVersions() throws IOException {

        final Properties properties = getProperties();

        try {
            
            Journal journal = new Journal(properties);

            // Two versions of id0.
            final int id0 = 1;
            final ByteBuffer expected0v0 = getRandomData(journal);
            final ByteBuffer expected0v1 = getRandomData(journal);
            
            // Three versions of id1.
            final int id1 = 2;
            final ByteBuffer expected1v0 = getRandomData(journal);
            final ByteBuffer expected1v1 = getRandomData(journal);
            final ByteBuffer expected1v2 = getRandomData(journal);
            
            // precondition tests, write id0 version0, postcondition tests.
            assertNotFound(journal.read(id0,null));
            
            assertNotFound(journal.read(id1,null));

            journal.write(id0,expected0v0);
            
            assertEquals(expected0v0.array(),journal.read( id0, null));
            
            assertNotFound(journal.read(id1,null));

            // write id1 version0, postcondition tests.
            journal.write(id1,expected1v0);
            
            assertEquals(expected0v0.array(),journal.read( id0, null));
            
            assertEquals(expected1v0.array(),journal.read( id1, null));
            
            // write id1 version1, postcondition tests.
            journal.write(id1,expected1v1);
            
            assertEquals(expected0v0.array(),journal.read( id0, null));
            
            assertEquals(expected1v1.array(),journal.read( id1, null));
            
            // write id1 version2, postcondition tests.
            journal.write(id1,expected1v2);
            
            assertEquals(expected0v0.array(),journal.read( id0, null));
            
            assertEquals(expected1v2.array(),journal.read( id1, null));

            // write id0 version1, postcondition tests.
            journal.write(id0,expected0v1);
            
            assertEquals(expected0v1.array(),journal.read( id0, null));
            
            assertEquals(expected1v2.array(),journal.read( id1, null));

            // delete id1, postcondition tests.

            journal.delete(id1);
            
            assertEquals(expected0v1.array(),journal.read( id0, null));
            
            assertDeleted(journal, id1);

            // delete id0, postcondition tests.

            journal.delete(id0);
            
            assertDeleted(journal, id0);
            
            assertDeleted(journal, id1);

            journal.close();

        } finally {

            deleteTestJournalFile();
            
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
     *       {@link #doWriteRoundTripTest(IStore, int, int)} that we would not
     *       want as part of a benchmark, but they are fine for a stress test.
     * 
     * @todo This test is transactional. Either refactor into a transactional
     *       and a non-transactional test or just move it into {@link TestTx}.
     */
    public void test_write_multipleObjectWrites001() throws IOException {

        final Properties properties = getProperties();

        // #of objects to write.
        long limit = 100;

        // The data written on the store.
        Map<Integer,byte[]> written = new HashMap<Integer, byte[]>();
        
        try {
            
            Journal journal = new Journal(properties);

            Tx tx = new Tx(journal,0);
            
            int maxSize = journal.slotMath.slotSize * 50;
            
            for (int id = 1; id < limit; id++) {

                int nbytes = r.nextInt(maxSize)+1; // +1 avoids zero length items.

                written.put(id, doWriteRoundTripTest(tx, id, nbytes) );

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
                ByteBuffer actual = tx.read(id, null);
                
                assertEquals("acutal.position()",0,actual.position());
                assertEquals("acutal.limit()",expected.length,actual.limit());
                assertEquals("limit() - position()", expected.length,actual.limit() - actual.position());
                assertEquals(expected,actual);

            }

            tx.prepare();
            
            tx.commit();
            
            journal.close();

        } finally {

            deleteTestJournalFile();
            
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
     */
    
    public void test_delete001() throws IOException {

        try {

            Journal journal = new Journal(getProperties());
            
            final int id = 1;
            
            byte[] expected = doWriteRoundTripTest(journal, id,
                    (journal.slotMath.slotSize * 3) + 1);

//            /*
//             * #of slots allocated to that object (this relies on the fact that
//             * it is the only object in the journal).
//             */
//            final int nallocated = journal.allocationIndex.getAllocatedSlotCount();
//            System.err.println("Allocated "+nallocated+" slots for id="+id);

            ByteBuffer actual = journal.read(id, null);

            assertEquals("acutal.position()",0,actual.position());
            assertEquals("acutal.limit()",expected.length,actual.limit());
            assertEquals("limit() - position()",expected.length,actual.limit() - actual.position());
            assertEquals(expected,actual);

            // The slots for the version that we are about to delete.
            final ISlotAllocation slots = journal.objectIndex.get(id);

            // delete the version.
            journal.delete(id);

            /*
             * Since the version only existed within in the global scope, verify
             * that the slots were synchronously deallocated when the version
             * was deleted (this requires the journal to track whether or not
             * there are any active transactions).
             * 
             * @todo Do a variant test when there is an active transaction and
             * verify that the slots are NOT deallocated until that transaction
             * prepares or aborts. This requires some tricky work on the part of
             * the journal.
             */
            assertSlotAllocationState(slots, journal.allocationIndex,false);

//            // Verify that there are no more allocated slots.
//            assertEquals("nallocated", 0, journal.allocationIndex.getAllocatedSlotCount());

            // Verify the object is now correctly marked as deleted in the
            // object index.
            try {
                journal.objectIndex.get(id);
                fail("Expecting: "+DataDeletedException.class);
            }
            catch(DataDeletedException ex) {
                System.err.println("Ignoring expected exception: "+ex);
            }

            /*
             * Test read after delete.
             */
            assertDeleted(journal,id);

            /*
             * Test delete after delete.
             */
            try {
                
                journal.delete(id);

                fail("Expecting " + DataDeletedException.class);
                
            } catch (DataDeletedException ex) {
                
                System.err.println("Ignoring expected exception: " + ex);
                
            }

            /*
             * Test write after delete.
             */
            try {
                
                journal.write(id, getRandomData(journal));

                fail("Expecting " + DataDeletedException.class);
                
            } catch (DataDeletedException ex) {
                
                System.err.println("Ignoring expected exception: " + ex);
                
            }

            journal.close();

        } finally {

            deleteTestJournalFile();

        }

    }
    
    //
    // 
    //
    
}
