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
 * Created on Nov 20, 2006
 */

package com.bigdata.objndx;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.bigdata.journal.ContiguousSlotAllocation;
import com.bigdata.journal.IObjectIndex;
import com.bigdata.journal.IRawStore;
import com.bigdata.journal.ISlotAllocation;
import com.bigdata.journal.SlotMath;
import com.bigdata.journal.SimpleObjectIndex.IObjectIndexEntry;

/**
 * @deprecated along with {@link IndexEntry} and {@link IndexEntrySerializer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIndexEntrySerializer extends AbstractObjectIndexTestCase {

    /**
     * 
     */
    public TestIndexEntrySerializer() {
    }

    /**
     * @param name
     */
    public TestIndexEntrySerializer(String name) {
        super(name);
    }

    IRawStore store = new SimpleStore();
    
    IndexEntrySerializer valueSer = new IndexEntrySerializer(store.getSlotMath());
    
//    public void test_byteToInt() {
//        
//        assertEquals((byte)0xff,(byte)-1);
//        
//    }
    
    /**
     * Generate a random entry for an {@link IObjectIndex}.
     */
    public IndexEntry getRandomEntry(SlotMath slotMath) {
        
        // when true, the entry marks a deleted version.
        boolean isDeleted = r.nextInt(100) < 10;

        // when true, a preExisting version is defined on the journal.
        boolean isPreExisting = r.nextInt(100) < 50;

        short versionCounter = nextVersionCounter();

        long currentVersion = isDeleted ? 0L : nextVersionRef();

        long preExistingVersion = isPreExisting ? nextVersionRef() : 0L;

        return new IndexEntry(slotMath, versionCounter, currentVersion,
                preExistingVersion);

    }

    /**
     * Return a random version counter.
     * 
     * @todo Shape the distribution to make version0 and other low-numbered
     *       versions much more likely.
     */
    private short nextVersionCounter() {

        return (short)r.nextInt((int)Short.MAX_VALUE);

    }

    /**
     * Reference to a random data object.
     * 
     * @return A reference to a random data object. The reference is only
     *         syntactically valid and MUST NOT be dereferenced
     */
    private long nextVersionRef() {

        int nbytes = r.nextInt(512)+1;
        
        int firstSlot = r.nextInt(Integer.MAX_VALUE - 1) + 1;

        return SlotMath.toLong(nbytes, firstSlot);

    }
    
    /**
     * Test with entry whose fields are all zeros.
     */
    public void test01() throws IOException {
        
        IndexEntry e1 = new IndexEntry(store.getSlotMath(),(short)0,0L,0L);
        
        ByteBuffer buf = ByteBuffer.allocate(valueSer.getSize(1));
        
        doRoundTripTest(buf, new IndexEntry[]{e1}, 1);
        
    }
    
    /**
     * Test with entry having known field values.
     */
    public void test02() throws IOException {
        
        IndexEntry e1 = new IndexEntry(store.getSlotMath(),(short)1,SlotMath.toLong(12, 90),0L);
        
        ByteBuffer buf = ByteBuffer.allocate(valueSer.getSize(1));
        
        doRoundTripTest(buf, new IndexEntry[]{e1}, 1);
        
    }
    
    /**
     * Test with entry having known field values.
     */
    public void test03() throws IOException {
        
        IndexEntry e1 = new IndexEntry(store.getSlotMath(),(short)2,0L,SlotMath.toLong(22, 80));
        
        ByteBuffer buf = ByteBuffer.allocate(valueSer.getSize(1));
        
        doRoundTripTest(buf, new IndexEntry[]{e1}, 1);
        
    }
    
    /**
     * Test with entry having known field values.
     */
    public void test04() throws IOException {
        
        IndexEntry e1 = new IndexEntry(store.getSlotMath(),(short)3,SlotMath.toLong(32, 92),SlotMath.toLong(2, 10));
        
        ByteBuffer buf = ByteBuffer.allocate(valueSer.getSize(1));
        
        doRoundTripTest(buf, new IndexEntry[]{e1}, 1);
        
    }
    
    /**
     * Test with entry having known field values corresponding to an observed
     * test failure.
     */
    public void test05() throws IOException {
        
        IndexEntry e1 = new IndexEntry(store.getSlotMath(),(short)28942,6851360340572110870L,2927585090617737519L);
        
        ByteBuffer buf = ByteBuffer.allocate(valueSer.getSize(1));
        
        doRoundTripTest(buf, new IndexEntry[]{e1}, 1);
        
    }
    
    /**
     * Stress test with entry whose fields are random.
     */
    public void testStress() throws IOException {
        
        final int LIMIT = 1000;

        final int maxBranchingFactor = 1024;

        final SlotMath slotMath = store.getSlotMath();
        
        for( int i=0; i<LIMIT; i++ ) {

            final int branchingFactor = r.nextInt(maxBranchingFactor
                    - BTree.MIN_BRANCHING_FACTOR)
                    + BTree.MIN_BRANCHING_FACTOR;
        
            final int nkeys = r.nextInt(branchingFactor);
            
            ByteBuffer buf = ByteBuffer.allocate(valueSer.getSize(nkeys));

            IndexEntry expected[] = new IndexEntry[branchingFactor];
            
            for( int j=0; j<nkeys; j++ ) {

                expected[j] = getRandomEntry(slotMath);
            
            }

            doRoundTripTest(buf, expected, nkeys);
        
        }
        
    }
    
    public void doRoundTripTest( ByteBuffer buf, IndexEntry[] expected, int n )
        throws IOException
    {
        
        // clear before writing.
        buf.clear();
        
        /*
         * Setup output stream over the buffer.
         */
        DataOutputStream os = new DataOutputStream(new ByteBufferOutputStream(
                buf));

        valueSer.putValues(os, expected, n);
        
        IndexEntry[] actual = new IndexEntry[expected.length];
        
        // flip for reading.
        buf.flip();
        
        try {

            /*
             * Setup input stream reading from the buffer.
             */
            final DataInputStream is = new DataInputStream(
                    new ByteBufferInputStream(buf));

            valueSer.getValues(is, actual, n);
            
        } catch(IOException ex) {
            
            for( int i=0; i<n; i++ ) {
                
                System.err.println("expected[i]="+expected[i]);
                
            }

            throw ex;

        }
        
        for( int i=0; i<n; i++ ) {
            
            assertEquals("values["+i+"]", expected[i], actual[i]);
            
        }
        
    }
 
    /**
     * Compare an array of {@link IObjectIndexEntry}s for consistent data.
     * 
     * @param expected
     * @param actual
     */
    public void assertEquals( IObjectIndexEntry[] expected, IObjectIndexEntry[] actual )
    {
        assertEquals( null, expected, actual );
    }

    /**
     * Compare an array of {@link IObjectIndexEntry}s for consistent data.
     * 
     * @param expected
     * @param actual
     */
    public void assertEquals( String msg, IObjectIndexEntry[] expected, IObjectIndexEntry[] actual )
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
     * Test two {@link IObjectIndexEntry entries} for consistent data.
     * 
     * @param expected
     * @param actual
     */
    public void assertEquals(IObjectIndexEntry expected,
            IObjectIndexEntry actual) {
        
        assertEquals(null,expected,actual);
        
    }
    
    /**
     * Test two {@link IObjectIndexEntry entries} for consistent data.
     * 
     * @param expected
     * @param actual
     */
    public void assertEquals(String msg, IObjectIndexEntry expected,
            IObjectIndexEntry actual) {
        
        if( msg == null ) {
            msg = "";
        } else {
            msg = msg + " : ";
        }

        if( expected == null) {
            
            assertNull(actual);
            
        } else {
        
            assertNotNull("actual",actual);
            
            assertEquals(msg+"versionCounter", expected.getVersionCounter(), actual
                    .getVersionCounter());

            assertEquals(msg+"isDeleted", expected.isDeleted(), actual.isDeleted());

            assertEquals(msg+"currentVersion", expected.getCurrentVersionSlots(),
                    actual.getCurrentVersionSlots());

            assertEquals(msg+"isPreExistingVersionOverwritten", expected
                    .isPreExistingVersionOverwritten(), actual
                    .isPreExistingVersionOverwritten());

            assertEquals(msg+"preExistingVersion", expected
                    .getPreExistingVersionSlots(), actual
                    .getPreExistingVersionSlots());
            
        }
        
    }
    
    /**
     * <p>
     * Verify that the {@link ISlotAllocation}s are consistent.
     * </p>
     * 
     * @param expected
     *            The expected slot allocation.
     * @param actual
     *            The actual slot allocation.
     */
    public void assertEquals(ISlotAllocation expected, ISlotAllocation actual) {

        assertEquals(null,expected,actual);

    }

    /**
     * <p>
     * Verify that the {@link ISlotAllocation}s are consistent.
     * </p>
     * <p>
     * Note: This test presumes that contiguous allocations are being used.
     * </p>
     * 
     * @param expected
     *            The expected slot allocation.
     * @param actual
     *            The actual slot allocation.
     */
    public void assertEquals(String msg, ISlotAllocation expected, ISlotAllocation actual) {

        if( msg == null ) {
            msg = "";
        } else {
            msg = msg + " : ";
        }

        if( expected == null ) {
            
            assertNull(actual);
            
        } else {

            if (!(expected instanceof ContiguousSlotAllocation)) {
                fail("Not expecting: " + expected.getClass());
            }

            if (!(actual instanceof ContiguousSlotAllocation)) {
                fail("Not expecting: " + actual.getClass());
            }

            assertEquals(msg + "firstSlot", expected.firstSlot(), actual
                    .firstSlot());

            assertEquals(msg + "byteCount", expected.getByteCount(), actual
                    .getByteCount());
        }

    }

}
