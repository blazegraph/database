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
 * Created on Oct 18, 2006
 */

package com.bigdata.journal;

import java.util.BitSet;

/**
 * Non-persistence capable implementation of a slot allocation index.
 * 
 * FIXME Write test suite.
 * 
 * FIXME Implement policy for contiguous allocations.
 * 
 * FIXME Write persistence capable implementation.
 * 
 * @todo Tune implementation using performance test.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SimpleSlotAllocationIndex implements ISlotAllocationIndex {

    /**
     * Used to compute the #of slots required for an allocation.
     */
    final SlotMath slotMath;
    
    /**
     * The index of the first slot that MUST NOT be addressed ([0:slotLimit-1]
     * is the valid range of slot indices).
     */
    final int slotLimit;

    /**
     * Each bit corresponds to a slot in the journal. A bit set indicates that
     * the corresponding slot is allocated.  An allocated slot MAY or MAY NOT
     * be committed - that distinction is carried by another bit.
     */
    final BitSet allocated;

    /**
     * Each bit corresponds to a slot in the journal. A bit set indicates that
     * the corresponding slot is committed. It is an error if a bit is set in
     * this map when the corresponding bit in {@link #allocated} is clear.
     */
    final BitSet committed;
    
    final private int FIRST_SLOT = 1;
    
    /**
     * <p>
     * The next slot that is known to be available. Slot indices begin at 1 and
     * run up to {@link Integer#MAX_VALUE}. The value ZERO(0) is explicitly
     * excluded so that it may be treated as a [null] identifier.
     * </p>
     * 
     * @todo This MUST be initialized on startup. The default is valid only for
     *       a new journal.
     */
    private int _nextSlot = FIRST_SLOT;

    /**
     * Asserts that the slot index is in the legal range for the journal
     * <code>[0:slotLimit)</code>
     * 
     * @param slot
     *            The slot index.
     */
    void assertSlot(int slot) {

        if (slot >= FIRST_SLOT && slot < slotLimit)
            return;

        throw new AssertionError("slot=" + slot + " is not in [" + FIRST_SLOT
                + ":" + slotLimit + ")");

    }

    public SimpleSlotAllocationIndex(SlotMath slotMath, int slotLimit) {

        assert slotMath != null;
        
        assert slotLimit > 0;

        this.slotMath = slotMath;
        
        this.slotLimit = slotLimit;

        allocated = new BitSet( slotLimit );

        committed = new BitSet( slotLimit );
        
    }

    /**
     * Release slots required to store a new version.
     * 
     * @todo Track the next free slot, its run length (if we can), and the #of
     *       slots remaining in each slot allocation block so that this method
     *       can be ultra fast if there is no work to be done (this counter
     *       could even live in the root block, along with the index of the next
     *       free slot and the first free slot on the journal).
     * 
     * @todo This notices fragmentation but does not attempt to return an
     *       unfragmented allocation. We should probably set a policy in place
     *       for fragmentation on a per allocation basis (as a percent of the
     *       #of slots to be allocated) and on a trailing history basis (so that
     *       we can recognize when the journal is generating too many fragments,
     *       at least in the current allocation region).
     */
    public ISlotAllocation alloc(int nbytes) {
        
        assert nbytes > 0;
        
        // #of slots needed to write the data on the journal.
        final int nslots = slotMath.getSlotCount(nbytes);

        /* Create object to store the allocated slots indices. the case where
         * nslots == 1 is optimized.
         * 
         * @todo Use a singleton for nslots == 1?  Use a pool for larger slot
         * runs?  Reuse a single unbound slot allocation for larger runs and
         * resize it as required?  Reuse of the returned value requires that
         * we copy the data into the object index (vs copying a reference).
         */
        ISlotAllocation slots = (nslots == 1 ? new SingletonSlotAllocation(
                nbytes) : new CompactSlotAllocation(nbytes, nslots));
        
        int lastSlot = -1;
        
        int fragments = 0; 
        
        for( int i=0; i<nslots; i++ ) { 
            
            int slot = nextFreeSlot();

            if( i > 0 && lastSlot + 1 != slot ) {
            
                fragments++;
                
            }

            // add to the allocation that we will return to the caller.
            slots.add( slot );
            
            // mark as allocated.
            setAllocated(slot);
            
            lastSlot = slot;
            
        }

        slots.close();

        if( fragments > 0 ) {
            
            System.err.println("Allocation of " + nbytes + " bytes in "
                    + nslots + " slots has " + fragments + " fragments.");
            
        }
        
        return slots;
        
    }
    
    /**
     * Returns the next free slot. This updates {@link #_nextSlot} as a side
     * effect.  The slot is NOT marked as allocated.
     */
    private int nextFreeSlot()
    {

        // Required to prevent inadvertant extension of the BitSet.
        assertSlot(_nextSlot);

        // Determine whether [_nextSlot] has been consumed.
        if( ! allocated.get(_nextSlot) ) {
            
            /*
             * The current [_nextSlot] has not been allocated and is still
             * available.
             */
            
            return _nextSlot;
            
        }
        
        /*
         * Note: BitSet returns nbits when it can not find the next clear bit
         * but -1 when it can not find the next set bit. This is because the 
         * BitSet is logically infinite, so there is always another clear bit
         * beyond the current position.
         */
        _nextSlot = allocated.nextClearBit(_nextSlot);
        
        assert _nextSlot <= slotLimit;
        
        if( _nextSlot == slotLimit ) {
            
            /*
             * No more free slots, try wrapping and looking again.
             * 
             * @todo This scans the entire index -- there is definately some
             * wasted effort there. However, the allocation index structure
             * needs to evolve to support persistence and transactional
             * isolation so this is not worth tuning further at this time.
             */
            
            System.err.println("Journal is wrapping around.");
            
            _nextSlot = allocated.nextClearBit( FIRST_SLOT );
        
            assert _nextSlot <= slotLimit;
            
            if( _nextSlot == slotLimit ) {

                // The journal is full.
                throw new IllegalStateException("Journal is full");
                
            }
            
        }
        
        return _nextSlot;
        
    }
    
    public void setAllocated(int slot) {

        assertSlot(slot);
        
        if( allocated.get(slot) ) throw new IllegalStateException();
        
        allocated.set(slot);
        
    }
    
    public boolean isAllocated(int slot) {
        
        assertSlot( slot );
        
        return allocated.get(slot);
        
    }

    public void setCommitted(int slot) {
        
        if( ! allocated.get( slot ) ) throw new IllegalStateException();
        
        committed.set(slot);
        
    }

    public boolean isCommitted(int slot) {
        
        assertSlot( slot );
        
        return allocated.get(slot) && committed.get(slot);
        
    }

    public void clear(int slot) {
        
        assertSlot( slot );
        
        allocated.clear(slot);

        committed.clear(slot);
        
    }
    
    public void clear(ISlotAllocation slots) {
        
        assert slots != null;
        
        for( int slot = slots.firstSlot(); slot != -1; slot = slots.nextSlot() ) {
            
            clear( slot );
            
        }
        
    }
    
    public void setCommitted(ISlotAllocation slots) {
        
        assert slots != null;
        
        for( int slot = slots.firstSlot(); slot != -1; slot = slots.nextSlot() ) {
            
            setCommitted( slot );
            
        }
        
    }
    
    public int getAllocatedSlotCount() {

        return allocated.cardinality();
        
    }
    
}
