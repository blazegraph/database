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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import junit.framework.TestCase;

/**
 * Test suite for {@link Journal} initialization.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Work through basic operations for writing and committing a transaction
 * without concurrency support, then modify to add in concurrency.
 * 
 * @todo tests of creating a new journal, including with bad properties.
 * @todo tests of opening an existing journal, including with incomplete writes
 *       of a root block.
 * @todo tests when the journal is buffered by a direct buffer in memory and
 *       when it is not (buffered is the preferred case since all journal
 *       operations are then at memory speed except for actually appending data
 *       or flushing during a commit).
 * @todo tests when the journal is memory mapped.
 * @todo tests when the journal is using a non-mapped file channel.
 * @todo tests when the journal is very large (NOT the normal use case for
 *       bigdata).
 * @todo tests of the exclusive lock mechanism during startup/shutdown (the
 *       advisory file locking mechanism).
 * 
 * @todo test ability to extent the journal.
 * 
 * @todo test ability to compact and truncate the journal.
 * 
 * @todo write tests for correct migration of committed records to a database.
 * 
 * @todo write tests for correct logical deletion of records no longer readable
 *       by any active transaction (they have been since updated or deleted by a
 *       committed transaction _and_ there is no transaction running that has a
 *       view of a historical consistent state in which that record version is
 *       visible).
 */

public class TestJournal extends TestCase {

    Random r = new Random();
    
    /**
     * <p>
     * Return the name of a journal file to be used for a unit test. The file is
     * created using the temporary file creation mechanism, but it is then
     * deleted. Ideally the returned filename is unique for the scope of the
     * test and will not be reported by the journal as a "pre-existing" file.
     * </p>
     * <p>
     * Note: This method is not advised for performance tests in which the disk
     * allocation matters since the file is allocated in a directory choosen by
     * the OS.
     * </p>
     * 
     * @return The unique filename.
     * 
     * @throws IOException
     */
    
    String getTestJournalFile() throws IOException {

        File tmp = File.createTempFile("test-"+getName()+"-", ".jnl");
        
        if( ! tmp.delete() ) {
            
            throw new RuntimeException("Unable to remove empty test file: "
                    + tmp);
            
        }
        
        return tmp.toString();
        
    }

    /**
     * Delete the test file.
     */
    void deleteTestJournalFile(String filename) {
        try {
            if (!new File(filename).delete()) {
                System.err.println("Warning: could not delete: " + filename);
            }
        } catch (Throwable t) {
            System.err.println("Warning: " + t);
        }
    }

    /**
     * <p>
     * Compares byte[]s by value (not reference).
     * </p>
     * <p>
     * Note: This method will only be invoked if both arguments can be typed as
     * byte[] by the compiler. If either argument is not strongly typed, you
     * MUST case it to a byte[] or {@link #assertEquals(Object, Object)} will be
     * invoked instead.
     * </p>
     * 
     * @param expected
     * @param actual
     */
    public void assertEquals( byte[] expected, byte[] actual )
    {

        assertEquals( null, expected, actual );
        
    }

    /**
     * <p>
     * Compares byte[]s by value (not reference).
     * </p>
     * <p>
     * Note: This method will only be invoked if both arguments can be typed as
     * byte[] by the compiler. If either argument is not strongly typed, you
     * MUST case it to a byte[] or {@link #assertEquals(Object, Object)} will be
     * invoked instead.
     * </p>
     * 
     * @param msg
     * @param expected
     * @param actual
     */
    public void assertEquals( String msg, byte[] expected, byte[] actual )
    {

        if( msg == null ) {
            msg = "";
        } else {
            msg = msg + " : ";
        }
    
        if( expected == null && actual == null ) {
            
            return;
            
        }
        
        if( expected == null && actual != null ) {
            
            fail( msg+"Expected a null array." );
            
        }
        
        if( expected != null && actual == null ) {
            
            fail( msg+"Not expecting a null array." );
            
        }
        
        assertEquals
            ( msg+"length differs.",
              expected.length,
              actual.length
              );
        
        for( int i=0; i<expected.length; i++ ) {
            
            assertEquals
                ( msg+"values differ: index="+i,
                   expected[ i ],
                   actual[ i ]
                 );
            
        }
        
    }

    /**
     * Helper method verifies that the contents of <i>actual</i> from
     * position() to limit() are consistent with the expected byte[]. A
     * read-only view of <i>actual</i> is used to avoid side effects on the
     * position, mark or limit properties of the buffer.
     * 
     * @param expected Non-null byte[].
     * @param actual Buffer.
     */
    public void assertEquals(byte[] expected, ByteBuffer actual ) {

        if( expected == null ) throw new IllegalArgumentException();
        
        if( actual == null ) fail("actual is null");
        
        /* Create a read-only view on the buffer so that we do not mess with
         * its position, mark, or limit.
         */
        actual = actual.asReadOnlyBuffer();
        
        int len = actual.capacity();
        
        byte[] actual2 = new byte[len];
        
        actual.get(actual2);

        assertEquals(expected,actual2);
        
    }
    
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

    public void test_create_direct01() throws IOException {

        final Properties properties = new Properties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);

        try {
            
            Journal journal = new Journal(properties);

            assertEquals("slotSize", 128, journal.slotSize);
            assertNotNull("slotMath", journal.slotMath);
            assertEquals("file", filename, journal.file.toString());
            assertEquals("initialExtent", Journal.DEFAULT_INITIAL_EXTENT,
                    journal.initialExtent);
            assertNotNull("raf", journal.raf);
            assertEquals("bufferMode", BufferMode.Direct, journal.bufferMode);
            assertNotNull("directBuffer", journal.directBuffer);
            assertEquals("", journal.initialExtent, journal.directBuffer
                    .capacity());

        } finally {
            
            deleteTestJournalFile(filename);
            
        }
        
    }
    
    public void test_create_mapped01() throws IOException {
        
    }
    
    public void test_create_disk01() throws IOException {
        
    }
    
    public void test_open_direct() throws IOException {
        
    }
    
    public void test_open_mapped() throws IOException {
        
    }
    
    public void test_open_disk() throws IOException {
        
    }

    /**
     * Test ability to release and consume slots on the journal.
     * 
     * @throws IOException
     */
    public void test_releaseSlots001() throws IOException {
       
        throw new UnsupportedOperationException();
        
    }
    
    /**
     * Test of write with read back where the write fits in one slot.
     * 
     * @throws IOException
     * 
     * @todo test write that spans multiple slots (2, 10, 100).
     * @todo test correct detection of corrupt states in slot chain.
     * @todo test of multiple writes with interleaved and final read back. This
     *       should be tested both where we are overwritting new versions of the
     *       same object and where we are writing distinct objects.
     * @todo test of write that wraps the journal.
     * @todo test of write that triggers reuse of slots already written on the
     *       journal by the same tx.
     * @todo test of write that correctly avoids reuse of slots last written on
     *       the journal by another tx.
     * @todo write test where there is not enough room to write the data in the
     *       journal (even after release).
     * @todo etc.
     */

    public void test_write_001() throws IOException {

        final Properties properties = new Properties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);
        properties.setProperty("slotSize","128");

        try {
            
            Journal journal = new Journal(properties);
            
            assertEquals("slotSize",128,journal.slotSize);

            doWriteRoundTripTest(journal, 0, 0, 10);
            
        } finally {

            deleteTestJournalFile(filename);
            
        }
        
    }

    //
    // Under one slot.
    //

    /**
     * Writes an object that does not fill a slot.
     * 
     * @throws IOException
     */
    public void test_write_fitsInOneSlot01() throws IOException {

        final Properties properties = new Properties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);
        properties.setProperty("slotSize","128");

        try {
            
            Journal journal = new Journal(properties);
            
            assertEquals("slotSize",128,journal.slotSize);

            final long tx = 0;
            final long id = 1;
            final int nbytes = 67;
            
            assertTrue("dataSize",journal.slotMath.dataSize>nbytes);
            
            doWriteRoundTripTest(journal, tx, id, nbytes);
            
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

        final Properties properties = new Properties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);
        properties.setProperty("slotSize","128");

        try {
            
            Journal journal = new Journal(properties);
            
            assertEquals("slotSize",128,journal.slotSize);

            doWriteRoundTripTest(journal, 0, 0, journal.slotMath.dataSize);
            
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

        final Properties properties = new Properties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);
        properties.setProperty("slotSize","128");

        try {
            
            Journal journal = new Journal(properties);
            
            assertEquals("slotSize",128,journal.slotSize);

            doWriteRoundTripTest(journal, 0, 0, journal.slotMath.dataSize-1);
            
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

        final Properties properties = new Properties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);
        properties.setProperty("slotSize","128");

        try {
            
            Journal journal = new Journal(properties);
            
            assertEquals("slotSize",128,journal.slotSize);

            doWriteRoundTripTest(journal, 0, 0, journal.slotMath.dataSize+1);
            
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

        final Properties properties = new Properties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);
        properties.setProperty("slotSize","128");

        try {
            
            Journal journal = new Journal(properties);
            
            assertEquals("slotSize",128,journal.slotSize);

            doWriteRoundTripTest(journal, 0, 0, journal.slotMath.dataSize * 2);
            
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

        final Properties properties = new Properties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);
        properties.setProperty("slotSize","128");

        try {
            
            Journal journal = new Journal(properties);
            
            assertEquals("slotSize",128,journal.slotSize);

            doWriteRoundTripTest(journal, 0, 0,
                    (journal.slotMath.dataSize * 2) - 1);
            
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

        final Properties properties = new Properties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);
        properties.setProperty("slotSize","128");

        try {
            
            Journal journal = new Journal(properties);
            
            assertEquals("slotSize",128,journal.slotSize);

            doWriteRoundTripTest(journal, 0, 0,
                    (journal.slotMath.dataSize * 2) + 1);
            
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

        final Properties properties = new Properties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);
        properties.setProperty("slotSize","128");

        try {
            
            Journal journal = new Journal(properties);
            
            assertEquals("slotSize",128,journal.slotSize);

            doWriteRoundTripTest(journal, 0, 0, journal.slotMath.dataSize * 3);
            
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

        final Properties properties = new Properties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);
        properties.setProperty("slotSize","128");

        try {
            
            Journal journal = new Journal(properties);
            
            assertEquals("slotSize",128,journal.slotSize);

            doWriteRoundTripTest(journal, 0, 0,
                    (journal.slotMath.dataSize * 3) - 1);
            
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

        final Properties properties = new Properties();
        
        final String filename = getTestJournalFile();
        
        properties.setProperty("file",filename);
        properties.setProperty("slotSize","128");

        try {
            
            Journal journal = new Journal(properties);
            
            assertEquals("slotSize",128,journal.slotSize);

            doWriteRoundTripTest(journal, 0, 0,
                    (journal.slotMath.dataSize * 3) + 1);
            
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

        final Properties properties = new Properties();
        
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

            long tx = 0;
            
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
     *            The transaction identifier.
     * @param id
     *            The persistent identifier.
     * @param nbytes
     *            The data version length.
     * 
     * @return The data written. This can be used to re-verify the write after
     *         intervening reads.
     */
    
    protected byte[] doWriteRoundTripTest(Journal journal,long tx, long id, int nbytes) {

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
