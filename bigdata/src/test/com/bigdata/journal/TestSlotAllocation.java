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
 * Created on Oct 22, 2006
 */

package com.bigdata.journal;

import junit.framework.TestCase;

/**
 * <p>
 * Test suite for {@link ISlotAllocation} - an interface that represents the
 * slots allocated to a single data version.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSlotAllocation extends TestCase {

    public TestSlotAllocation() {
    }

    public TestSlotAllocation(String arg0) {
        super(arg0);
    }
    
    /**
     * The assumed slot size for these tests.
     */
    final int slotSize = 128;
    
    /**
     * Used by some tests to compute the #of slots in which a given allocation
     * will fit.
     */
    final SlotMath slotMath = new SlotMath(slotSize);

    /**
     * Test constructors.
     * 
     * @todo test correct rejection for constructors.
     */
    public void test_ctor() {
        
        ISlotAllocation tmp;
        
        tmp = new UnboundedSlotAllocation();
        assertEquals("#bytes",0,tmp.getByteCount());
        ((UnboundedSlotAllocation)tmp).setByteCount(100);
        assertEquals("#bytes",100,tmp.getByteCount());
        try {
            tmp.capacity();
            fail("Expecting: "+UnsupportedOperationException.class);
        } catch( UnsupportedOperationException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        tmp = new CompactSlotAllocation(100,10);
        assertEquals("#bytes",100,tmp.getByteCount());
        assertEquals("capacity",10,tmp.capacity());
        
        tmp = new SingletonSlotAllocation(100);
        assertEquals("#bytes",100,tmp.getByteCount());
        assertEquals("capacity",1,tmp.capacity());
        
    }
    
    /**
     * Test the ability to add slots to an allocation, close the allocation and
     * then verify that the allocated slots are correctly reported by the
     * visitation interface.
     */
    public void test_basics() {

        int[] slots = new int[] {0,1,3,5,8,20,21};

        doTest(doTest(doWrite(new UnboundedSlotAllocation(),slots),slots),slots);
        
        doTest(doTest(doWrite(new CompactSlotAllocation(100,slots.length),slots),slots),slots);

        doTest(doTest(doWrite(new SingletonSlotAllocation(100),new int[]{1}),new int[]{1}),new int[]{1});
        
    }
    
    public void test_underflow() {

        try {
            
            doWrite(new CompactSlotAllocation(100,4),new int[]{1,2,3});
            
            fail("Expecting: "+IllegalStateException.class);
            
        } catch( IllegalStateException ex ) {
            
            System.err.println("Ignoring expected exception: "+ex);
            
        }
        
    }

    public void test_visitation_SingletonSlotAllocation() {
        
        ISlotAllocation slots = new SingletonSlotAllocation(100);
        
        assertFalse("closed", slots.isClosed() );

        assertEquals("capacity", 1, slots.capacity() );
        
        assertEquals("byteCount", 100, slots.getByteCount() );
        
        assertEquals("slotCount", 0, slots.getSlotCount());
        
        try {
            slots.firstSlot();
            fail("Expecting "+IllegalStateException.class);
        }
        catch(IllegalStateException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }

        slots.add(0);

        assertFalse("closed", slots.isClosed() );
        
        assertEquals("capacity", 1, slots.capacity() );
        
        assertEquals("byteCount", 100, slots.getByteCount() );
        
        assertEquals("slotCount", 1, slots.getSlotCount());
        
        assertEquals(0,slots.firstSlot());
        
        assertEquals(-1,slots.nextSlot());

        try {
            slots.nextSlot();
            fail("Expecting "+IllegalStateException.class);
        }
        catch(IllegalStateException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }

        slots.close();
        
        assertTrue("closed", slots.isClosed() );
        
        assertEquals("capacity", 1, slots.capacity() );
        
        assertEquals("byteCount", 100, slots.getByteCount() );
        
        assertEquals("slotCount", 1, slots.getSlotCount());
        
        assertEquals(0,slots.firstSlot());
        
        assertEquals(-1,slots.nextSlot());

        try {
            slots.nextSlot();
            fail("Expecting "+IllegalStateException.class);
        }
        catch(IllegalStateException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }

    }

    public void test_visitation_CompactSlotAllocation() {
        
        ISlotAllocation slots = new CompactSlotAllocation(100,2);
        
        assertFalse("closed", slots.isClosed() );

        assertEquals("capacity", 2, slots.capacity() );
        
        assertEquals("byteCount", 100, slots.getByteCount() );
        
        assertEquals("slotCount", 0, slots.getSlotCount());
        
        try {
            slots.firstSlot();
            fail("Expecting "+IllegalStateException.class);
        }
        catch(IllegalStateException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }

        //
        
        slots.add(0);

        assertFalse("closed", slots.isClosed() );
        
        assertEquals("capacity", 2, slots.capacity() );
        
        assertEquals("byteCount", 100, slots.getByteCount() );
        
        assertEquals("slotCount", 1, slots.getSlotCount());
        
        assertEquals(0,slots.firstSlot());
        
        assertEquals(-1,slots.nextSlot());

        try {
            slots.nextSlot();
            fail("Expecting "+IllegalStateException.class);
        }
        catch(IllegalStateException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }

        try {
            slots.close();
            fail("Expecting "+IllegalStateException.class);
        }
        catch(IllegalStateException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }

        //
        
        slots.add(2);

        assertFalse("closed", slots.isClosed() );
        
        assertEquals("capacity", 2, slots.capacity() );
        
        assertEquals("byteCount", 100, slots.getByteCount() );
        
        assertEquals("slotCount", 2, slots.getSlotCount());
        
        assertEquals(0,slots.firstSlot());
        
        assertEquals(2,slots.nextSlot());

        assertEquals(-1,slots.nextSlot());

        try {
            slots.nextSlot();
            fail("Expecting "+IllegalStateException.class);
        }
        catch(IllegalStateException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }

        slots.close();
        
        //
        
        assertTrue("closed", slots.isClosed() );
        
        assertEquals("capacity", 2, slots.capacity() );
        
        assertEquals("byteCount", 100, slots.getByteCount() );
        
        assertEquals("slotCount", 2, slots.getSlotCount());
        
        assertEquals(0,slots.firstSlot());
        
        assertEquals(2,slots.nextSlot());

        assertEquals(-1,slots.nextSlot());

        try {
            slots.nextSlot();
            fail("Expecting "+IllegalStateException.class);
        }
        catch(IllegalStateException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }

    }
    
    /**
     * Test verifies that we can write a sequence of slots into the allocation
     * and then close the allocation.
     * 
     * @param fixture
     * @param slots
     * @return
     */
    ISlotAllocation doWrite(ISlotAllocation fixture, int[] slots) {

        assertFalse( "closed", fixture.isClosed() );
        
        assertEquals( "#slots", 0,fixture.getSlotCount());

        try {
            fixture.add(-1);
            fail("Expecting: "+IllegalArgumentException.class);
        }
        catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        for( int i=0; i<slots.length; i++ ) {
            
            fixture.add(slots[i]);

            assertEquals(i+1,fixture.getSlotCount());
            
        }
        
        fixture.close();

        assertTrue( "closed", fixture.isClosed() );

        return fixture;
        
    }
    
    /**
     * Test visits the slots in their correct sequence.
     * 
     * @param fixture
     *            The fixture to be tested.
     * @param slots
     *            The ordered array of slot indices.
     */
    ISlotAllocation doTest(ISlotAllocation fixture, int[] slots) {
        
        assertEquals(slots[0],fixture.firstSlot());

        for( int i=1; i<slots.length; i++ ) {

            assertEquals(slots[i],fixture.nextSlot());
            
        }
        
        assertEquals(-1,fixture.nextSlot());

        try {
            fixture.nextSlot();
            fail("Expecting: "+IllegalStateException.class);
        }
        catch( IllegalStateException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }

        return fixture;
        
    }

    public void test_isContiguous_singleton() {

        ISlotAllocation tmp;
        
        tmp = new SingletonSlotAllocation(100);

        try {
            tmp.isContiguous();
            fail("Expecting: "+IllegalStateException.class);
        }
        catch( IllegalStateException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        tmp.add(10);

        tmp.close();
        
        assertTrue(tmp.isContiguous());
        
    }
    
    public void test_isContiguous_compact() {

        final int nbytes = 412;
        
        ISlotAllocation tmp = new CompactSlotAllocation(nbytes, slotMath
                .getSlotCount(nbytes));

        try {
            tmp.isContiguous();
            fail("Expecting: "+IllegalStateException.class);
        }
        catch( IllegalStateException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        tmp.add(10);
        tmp.add(11);
        tmp.add(12);
        tmp.add(13);

        tmp.close();
        
        assertTrue(tmp.isContiguous());
        
        // Try again.
        tmp = new CompactSlotAllocation(nbytes, slotMath.getSlotCount(nbytes));

        tmp.add(9);
        tmp.add(11);
        tmp.add(12);
        tmp.add(13);

        tmp.close();
        
        assertFalse(tmp.isContiguous());

        // Try again.
        tmp = new CompactSlotAllocation(nbytes, slotMath.getSlotCount(nbytes));

        tmp.add(10);
        tmp.add(12);
        tmp.add(13);
        tmp.add(14);

        tmp.close();
        
        assertFalse(tmp.isContiguous());

        // Try again.
        tmp = new CompactSlotAllocation(nbytes, slotMath.getSlotCount(nbytes));

        tmp.add(10);
        tmp.add(11);
        tmp.add(12);
        tmp.add(14);

        tmp.close();
        
        assertFalse(tmp.isContiguous());

    }

    public void test_isContiguous_unbounded() {

        ISlotAllocation tmp = new UnboundedSlotAllocation();

        try {
            tmp.isContiguous();
            fail("Expecting: "+IllegalStateException.class);
        }
        catch( IllegalStateException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        tmp.add(10);
        tmp.add(11);
        tmp.add(12);
        tmp.add(13);

        tmp.close();
        
        assertTrue(tmp.isContiguous());
        
        // Try again.
        tmp = new UnboundedSlotAllocation();

        tmp.add(9);
        tmp.add(11);
        tmp.add(12);
        tmp.add(13);

        tmp.close();
        
        assertFalse(tmp.isContiguous());

        // Try again.
        tmp = new UnboundedSlotAllocation();

        tmp.add(10);
        tmp.add(12);
        tmp.add(13);
        tmp.add(14);

        tmp.close();
        
        assertFalse(tmp.isContiguous());

        // Try again.
        tmp = new UnboundedSlotAllocation();

        tmp.add(10);
        tmp.add(11);
        tmp.add(12);
        tmp.add(14);

        tmp.close();
        
        assertFalse(tmp.isContiguous());

    }
    
    // @todo tests of toLong, but the core implementation is on SlotMath.
//    public void test_toLong() {
//
//        final int nbytes = 412;
//        
//        ISlotAllocation tmp;
//
//        // singleton.
//        tmp = new SingletonSlotAllocation(100);
//
//        tmp.add(10);
//        tmp.close();
//        tmp.toLong();
//        
//        // compact
//        tmp = new CompactSlotAllocation(nbytes, slotMath
//                .getSlotCount(nbytes));
//
//        
//        // @todo unbounded
//        
//    }
    
    public void test_toLong_correctRejection() {

        ISlotAllocation tmp;
        
        // empty singleton.
        tmp = new SingletonSlotAllocation(100);

        try {
            tmp.toLong();
            fail("Expecting: "+IllegalStateException.class);
        }
        catch( IllegalStateException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        // not contiguous.
        final int nbytes = 412;
        
        tmp = new CompactSlotAllocation(nbytes,slotMath.getSlotCount(nbytes));

        tmp.add(10);
        tmp.add(11);
        tmp.add(12);
        tmp.add(14);
        tmp.close();

        try {
            tmp.toLong();
            fail("Expecting: "+IllegalStateException.class);
        }
        catch( IllegalStateException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }

        // not contiguous.
        tmp = new UnboundedSlotAllocation();

        tmp.add(10);
        tmp.add(11);
        tmp.add(13);
        tmp.add(14);
        tmp.close();

        try {
            tmp.toLong();
            fail("Expecting: "+IllegalStateException.class);
        }
        catch( IllegalStateException ex ) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
    }
    
}
