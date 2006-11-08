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
     * <code>(0:slotLimit)</code> (slot 0 is disallowed).
     * 
     * @param slot
     *            The slot index.
     * 
     * @throws IllegalArgumentException
     */
    void assertSlot(int slot) {

        if (slot >= FIRST_SLOT && slot < slotLimit)
            return;

        throw new IllegalArgumentException("slot=" + slot + " is not in ["
                + FIRST_SLOT + ":" + slotLimit + ")");

    }

    /**
     * Disallowed constructor.
     */
    private SimpleSlotAllocationIndex() {
    
        throw new UnsupportedOperationException();
        
    }
    
    public SimpleSlotAllocationIndex(SlotMath slotMath, int slotLimit) {

        if( slotMath == null ) throw new IllegalArgumentException();

        if( slotLimit <= 1 ) throw new IllegalArgumentException();
        
        this.slotMath = slotMath;
        
        this.slotLimit = slotLimit;

        allocated = new BitSet( slotLimit );

        committed = new BitSet( slotLimit );
        
    }

    /**
     * Release slots required to store a new version. This implementation always
     * creates a contiguous allocation.
     * 
     * @todo Track the next free slot, its run length (if we can), and the #of
     *       slots remaining in each slot allocation block so that this method
     *       can be ultra fast if there is no work to be done (this counter
     *       could even live in the root block, along with the index of the next
     *       free slot and the first free slot on the journal).
     * 
     * FIXME This code has grown somewhat organically and should be rewritten
     *       with a clear head.  The currently implementation is MUCH slower
     *       than the previous implementation.  The changes have to do with
     *       ensuring that allocations are contiguous, but we should be able
     *       to do that very cheaply.
     */
    public ISlotAllocation alloc(int nbytes) {
        
        if (nbytes <= 0)
            throw new IllegalArgumentException();
        
        // #of slots needed to write the data on the journal.
        final int nslots = slotMath.getSlotCount(nbytes);

        // used to avoid wrapping around more than once.
        final int fromSlot;
        
        try {

            fromSlot = nextFreeSlot();
            
        } catch (IllegalStateException ex) {
            
            System.err.println("Warning: " + ex);
            
            return null;
            
        }
        
        // set when we find a large enough contiguous extent.
        int firstSlot = -1; // the first slot in the allocation.
        int lastSlot = -1; // the last slot in the allocation.

        // #of times we rejected an extent that was too small for the allocation.
        int nattempts = 0;

        /*
         * true iff we have found an extent that is large enough, in which case
         * we also set firstSlot and lastSlot.
         */
        boolean haveSlots = false;
        
        /*
         * true iff we have wrapped around the end.
         */
        boolean wrapped = false;
        
        while (!haveSlots) {

            // try to find N contiguous slots.
            for (int i = 0; i < nslots; i++) {

                final int slot;

                try {

                    // find the next free slot.
                    slot = nextFreeSlot();
                    
                    if( wrapped && slot >= fromSlot ) {
                        /*
                         * The allocation has already wrapped around and the
                         * next slot that would be allocated has an index
                         * greater than or equal to the first slot that we
                         * tested. At this point there can not be any extent
                         * available that would satisify the allocation request.
                         */
                        return null;
                        
                    }
                    
                    /*
                     * advance [_nextSlot] since we need to skip the slot that
                     * we just tested and we do not mark it as allocated until
                     * we know that we have enough slots in hand.
                     */ 
                    _nextSlot++;
                    if( _nextSlot == slotLimit ) {
                        wrapped = true;
                        _nextSlot = 1;
                        if( i+1 < nslots ) {
                            /*
                             * We can not grow the extent further since we just
                             * wrapped around and the extent is not large enough
                             * to satisify the allocation so we restart the
                             * inner loop.
                             */
                            break;
                        }
                    }


                } catch (IllegalStateException ex) {

                    System.err.println("Warning: " + ex);

                    return null;

                }
                
                if (i == 0) {

                    firstSlot = slot;
                    
                } else if (i > 0 && lastSlot + 1 != slot) {

                    /*
                     * This occurs when the extent would not be contiguous. We
                     * break the inner loop and restart the scan in the outer
                     * loop.
                     */
                    break;

                }

                lastSlot = slot;

                if( i + 1 == nslots ) {
                    
                    haveSlots = true;
                    
                }
                
            }

        }

        // Note: allocations never wrap around the end of the journal so this
        // is always true.
        assert firstSlot <= lastSlot;
        
        // Note: firstSlot == lastSlot iff nslots == 1.
        assert lastSlot == firstSlot + nslots - 1;
        
        // mark as allocated.
        allocated.set(firstSlot,lastSlot+1);
        
        ISlotAllocation slots = new ContiguousSlotAllocation(nbytes, nslots, firstSlot);
        
        if( nattempts > 0 ) {
            
            System.err.println("Contiguous allocation of " + nbytes
                    + " bytes in " + nslots + " slots in " + nattempts
                    + " attempts.");
            
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

    public int getSlotLimit() {
        
        return slotLimit;
        
    }

    public int getSlotSize() {
        
        return slotMath.slotSize;
        
    }

}
