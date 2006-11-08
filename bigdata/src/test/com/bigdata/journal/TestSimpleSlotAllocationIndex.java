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
 * Created on Oct 23, 2006
 */

package com.bigdata.journal;

/**
 * Test suite for {@link SimpleSlotAllocationIndex}.
 * 
 * @todo Refactor tests to run against {@link ISlotAllocationIndex} using the
 *       setUp() and create test case instances for both the simple and
 *       persistence capable implementations of that interface.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSimpleSlotAllocationIndex extends
        AbstractTestSlotAllocationIndex {

    public TestSimpleSlotAllocationIndex() {
    }

    public TestSimpleSlotAllocationIndex(String name) {
        super(name);
    }

    /**
     * Correct rejection tests for the constructor.
     */
    public void test_ctor_correctRejection() {
        
        try {
            new SimpleSlotAllocationIndex(null,10);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring excepted exception: "+ex);
        }

        try {
            new SimpleSlotAllocationIndex(new SlotMath(10),0);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring excepted exception: "+ex);
        }

        try {
            new SimpleSlotAllocationIndex(new SlotMath(10),-1);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring excepted exception: "+ex);
        }

    }

    /**
     * Correctness test for constructor post-conditions.
     */
    public void test_ctor() {
    
        final int slotSize = 100;
        final int slotLimit = 10;
        final SlotMath slotMath = new SlotMath(slotSize);
        
        final ISlotAllocationIndex alloc = new SimpleSlotAllocationIndex(
                slotMath, slotLimit);

        // basic constructor verification.
        assertEquals("slotSize",slotSize,alloc.getSlotSize());
        assertEquals("slotLimit",slotLimit,alloc.getSlotLimit());
        assertEquals("#allocated",0,alloc.getAllocatedSlotCount());
        try {
            // Note: slot0 is disallowed.
            alloc.isAllocated(0);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring excepted exception: "+ex);
        }
        for( int i=1; i<slotLimit; i++ ) {
            assertFalse("isAllocated("+i+")",alloc.isAllocated(i));
        }

    }

    /**
     * Test for allocation that fits within one slot.
     */
    public void test_alloc01() {
        
        final int slotSize = 100;
        final int slotLimit = 10;
        final SlotMath slotMath = new SlotMath(slotSize);
        
        ISlotAllocationIndex alloc = new SimpleSlotAllocationIndex(slotMath,slotLimit);

        // first allocation fits into the first slot (slot1).
        final int nbytes = 10;
        final int nslots = 1;
        final int firstSlot = 1;
        assertEquals(new ContiguousSlotAllocation(nbytes,nslots,firstSlot),alloc.alloc(nbytes));
        assertTrue(alloc.isAllocated(firstSlot));
        for( int i=firstSlot+nslots; i<slotLimit; i++ ) {
            assertFalse("isAllocated("+i+")",alloc.isAllocated(i));
        }
        
    }
   
    /**
     * Test for allocation that exactly fills one slot.
     */
    public void test_alloc02() {

        final int slotSize = 100;
        final int slotLimit = 10;
        final SlotMath slotMath = new SlotMath(slotSize);
        
        ISlotAllocationIndex alloc = new SimpleSlotAllocationIndex(slotMath,slotLimit);

        // first allocation fits into the first slot (slot1).
        final int nbytes = slotSize;
        final int nslots = 1;
        final int firstSlot = 1;
        assertEquals(new ContiguousSlotAllocation(nbytes,nslots,firstSlot),alloc.alloc(nbytes));
        assertTrue(alloc.isAllocated(firstSlot));
        for( int i=firstSlot+nslots; i<slotLimit; i++ ) {
            assertFalse("isAllocated("+i+")",alloc.isAllocated(i));
        }

    }

    /**
     * Test allocation of N single slots, where N is the #of slots available.
     */
    public void test_alloc03() {
        
        final int slotSize = 100;
        final int slotLimit = 10;
        final SlotMath slotMath = new SlotMath(slotSize);
        
        ISlotAllocationIndex alloc = new SimpleSlotAllocationIndex(slotMath,slotLimit);

        /*
         * Make one allocation for each slot.
         */
        for (int i = 1; i < slotLimit; i++) {
            // first allocation fits into the next slot.
            assertEquals(new ContiguousSlotAllocation(10, 1, i), alloc
                    .alloc(10));
            // verify the expected slot is marked as allocated.
            assertTrue(alloc.isAllocated(i));
            // verify what was already allocated.
            for( int j=1; j<=i; j++ ) {
                assertTrue("isAllocated(" + j + ")", alloc.isAllocated(j));
            }
            // verify what was not yet allocated.
            for (int j = i + 1; j < slotLimit; j++) {
                assertFalse("isAllocated(" + j + ")", alloc.isAllocated(j));
            }
        }
        
        // Verify that no more slots will be allocated.
        assertNull(alloc.alloc(10));
        
    }

    /**
     * Test for allocation that fits in two slots (over one slot by one byte).
     */
    public void test_alloc04() {

        final int slotSize = 100;
        final int slotLimit = 10;
        final SlotMath slotMath = new SlotMath(slotSize);
        
        ISlotAllocationIndex alloc = new SimpleSlotAllocationIndex(slotMath,slotLimit);

        final int nbytes = slotSize+1;
        final int nslots = 2;
        final int firstSlot = 1;
        assertEquals(new ContiguousSlotAllocation(nbytes,nslots,firstSlot),alloc.alloc(nbytes));
        assertTrue(alloc.isAllocated(firstSlot));
        assertTrue(alloc.isAllocated(firstSlot+1));
        for( int i=firstSlot+nslots; i<slotLimit; i++ ) {
            assertFalse("isAllocated("+i+")",alloc.isAllocated(i));
        }

    }
    
    /**
     * Test for allocation that fits in two slots exactly.
     */
    public void test_alloc05() {

        final int slotSize = 100;
        final int slotLimit = 10;
        final SlotMath slotMath = new SlotMath(slotSize);
        
        ISlotAllocationIndex alloc = new SimpleSlotAllocationIndex(slotMath,slotLimit);

        final int nbytes = slotSize+1;
        final int nslots = 2;
        final int firstSlot = 1;
        assertEquals(new ContiguousSlotAllocation(nbytes,nslots,firstSlot),alloc.alloc(nbytes));
        assertTrue(alloc.isAllocated(firstSlot));
        assertTrue(alloc.isAllocated(firstSlot+1));
        for( int i=firstSlot+nslots; i<slotLimit; i++ ) {
            assertFalse("isAllocated("+i+")",alloc.isAllocated(i));
        }

    }

    /**
     * Test for allocation that fits in two slots exactly when the last two
     * slots are still available - the correct behavior is that allocation
     * succeeds.
     */
    public void test_alloc06() {

        final int slotSize = 100;
        final int slotLimit = 10;
        final SlotMath slotMath = new SlotMath(slotSize);
        
        ISlotAllocationIndex alloc = new SimpleSlotAllocationIndex(slotMath,slotLimit);

        for(int i=1; i<slotLimit-2; i++ ) {
            alloc.setAllocated(i);
            assertTrue(alloc.isAllocated(i));
        }
        assertFalse(alloc.isAllocated(slotLimit-2));
        assertFalse(alloc.isAllocated(slotLimit-1));
        
        final int nbytes = slotSize*2;
        final int nslots = 2;
        final int firstSlot = slotLimit-2;
        assertEquals(new ContiguousSlotAllocation(nbytes,nslots,firstSlot),alloc.alloc(nbytes));
        assertTrue(alloc.isAllocated(firstSlot));
        assertTrue(alloc.isAllocated(firstSlot+1));

    }

    /**
     * Test for allocation that fits in two slots exactly when just the first
     * and the last slots are available - the correct behavior is that
     * allocation fails (allocations may not wrap around since they must be
     * contiguous).  In this version of the test, the allocator begins from
     * the first slot.
     */
    public void test_alloc07a() {

        final int slotSize = 100;
        final int slotLimit = 10;
        final SlotMath slotMath = new SlotMath(slotSize);
        
        ISlotAllocationIndex alloc = new SimpleSlotAllocationIndex(slotMath,slotLimit);

        for(int i=2; i<slotLimit-1; i++ ) {
            alloc.setAllocated(i);
            assertTrue(alloc.isAllocated(i));
        }
        assertFalse(alloc.isAllocated(1));
        assertFalse(alloc.isAllocated(slotLimit-1));
        
        final int nbytes = slotSize+2;
        assertEquals(null,alloc.alloc(nbytes)); // allocation fails (no space).
        assertFalse(alloc.isAllocated(1));
        assertFalse(alloc.isAllocated(slotLimit-1));

        /*
         * Verify that an allocation of a single slot will still succeeds. Note:
         * I am not sure whether the first or last slot would be allocated in
         * this case, and it does not matter, so I only verify that an
         * allocation was made.
         */
        assertNotNull(alloc.alloc(1));

    }

    /**
     * Test for allocation that fits in two slots exactly when just the first
     * and the last slots are available - the correct behavior is that
     * allocation fails (allocations may not wrap around since they must be
     * contiguous).  In this version of the test, the allocator begins from
     * the last slot.
     */
    public void test_alloc07b() {

        final int slotSize = 100;
        final int slotLimit = 10;
        final SlotMath slotMath = new SlotMath(slotSize);
        
        ISlotAllocationIndex alloc = new SimpleSlotAllocationIndex(slotMath,slotLimit);

        for(int i=1; i<slotLimit-2; i++ ) {
            alloc.setAllocated(i);
            assertTrue(alloc.isAllocated(i));
        }
        assertTrue(alloc.isAllocated(1));
        assertFalse(alloc.isAllocated(slotLimit-2));
        assertFalse(alloc.isAllocated(slotLimit-1));

        // force the position of the allocator.
        alloc.alloc(10);
        assertTrue(alloc.isAllocated(1));
        assertTrue(alloc.isAllocated(slotLimit-2));
        assertFalse(alloc.isAllocated(slotLimit-1));
        // mark the first slot as free.
        alloc.clear(1);
        assertFalse(alloc.isAllocated(1));
        assertTrue(alloc.isAllocated(slotLimit-2));
        assertFalse(alloc.isAllocated(slotLimit-1));

        /*
         * At this point the allocator should try to allocate start with the
         * last slot.
         */
        final int nbytes = slotSize+2;
        assertEquals(null,alloc.alloc(nbytes)); // allocation fails (no space).
        assertFalse(alloc.isAllocated(1));
        assertFalse(alloc.isAllocated(slotLimit-1));

        /*
         * Verify that an allocation of a single slot will still succeeds. Note:
         * I am not sure whether the first or last slot would be allocated in
         * this case, and it does not matter, so I only verify that an
         * allocation was made.
         */
        assertNotNull(alloc.alloc(1));

    }

    /**
     * Test for allocation that fits in two slots exactly when just the last
     * slot is still available - the correct behavior is that allocation fails.
     */
    public void test_alloc08() {

        final int slotSize = 100;
        final int slotLimit = 10;
        final SlotMath slotMath = new SlotMath(slotSize);
        
        ISlotAllocationIndex alloc = new SimpleSlotAllocationIndex(slotMath,slotLimit);

        for(int i=1; i<slotLimit-1; i++ ) {
            alloc.setAllocated(i);
            assertTrue(alloc.isAllocated(i));
        }
        assertFalse(alloc.isAllocated(slotLimit-1));
        
        final int nbytes = slotSize*2;
        assertEquals(null,alloc.alloc(nbytes)); // allocation fails (no space).
        assertFalse(alloc.isAllocated(slotLimit-1));

        // verify that an allocation of a single slot will still succeed.
        assertEquals(new ContiguousSlotAllocation(1,1,slotLimit-1),alloc.alloc(1));
        assertTrue(alloc.isAllocated(slotLimit-1));

    }

    /**
     * Verify that the {@link ISlotAllocation}s are consistent (the same slots
     * in the same order).
     * 
     * @param expected
     *            The expected slot allocation.
     * @param actual
     *            The actual slot allocation.
     * 
     * @see AbstractTestCase#assertEquals(ISlotAllocation, ISlotAllocation)
     */
    public void assertEquals(ISlotAllocation expected, ISlotAllocation actual) {

        /* Note: We MUST use a reference test here since the visitation pattern
         * does not support concurrent traversal.  Failure to test for the same
         * reference here will cause a seemingly suprious IllegalStateException
         * when trying to compare a SingletonSlotAllocation with itself.
         */ 
        if( expected == actual ) return; // same same.
        
        if( expected == null && actual != null ) fail("Expected null.");
        
        if( expected != null && actual == null ) fail("Expected non-null");
        
        assertEquals("capacity",expected.capacity(),actual.capacity());

        assertEquals("closed",expected.isClosed(),actual.isClosed());
        
        assertEquals("#bytes",expected.getByteCount(),actual.getByteCount());
        
        assertEquals("#slots",expected.getSlotCount(),actual.getSlotCount());

        // check the first slot.
        int expectedSlot = expected.firstSlot();
        int actualSlot = actual.firstSlot();
        assertEquals("firstSlot",expectedSlot,actualSlot);

        // check the remaining slots.
        int i = 1;
        do {
            
            expectedSlot = expected.nextSlot();
            
//            try {
                actualSlot = actual.nextSlot();
//            }
//            catch( IllegalStateException ex ) {
//                System.err.println("expected: "+expected.getClass());
//                System.err.println("actual  : "+actual.getClass());
//                throw ex;
//            }
            
            assertEquals("slot[" + i + "]", expectedSlot, actualSlot);

            i++;
            
        } while( expectedSlot != -1 );

    }
    
    /**
     * Verify that the slots are marked as indicated in the specified
     * {@link ISlotAllocationIndex}.
     * 
     * @param slots
     *            The slots.
     * @param allocationIndex
     *            The slot allocation index.
     * @param allocated
     *            true iff the slots must be marked as allocated in the index.
     *            
     * @see AbstractTestCase#assertSlotAllocationState(ISlotAllocation, ISlotAllocationIndex, boolean)
     */
//    * @param committed
//    *            true iff the slots must be marked as committed in the index.
    public void assertSlotAllocationState(ISlotAllocation slots,
            ISlotAllocationIndex allocationIndex, boolean allocated) {

        assert slots != null;
        
        assert allocationIndex != null;
        
        int i = 0;
        
        for (int slot = slots.firstSlot(); slot != -1; slot = slots.nextSlot()) {

            assertEquals(
                    "nslots=" + slots.getSlotCount() + " : slot[" + i + "]",
                    allocated, allocationIndex.isAllocated(slot));

            i++;
            
        }
        
    }

}
