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
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME Modify Journal to use the distinction of allocated + committed (two bits of
 * state per slot).
 */
public class SimpleSlotAllocationIndex implements ISlotAllocationIndex {

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
    
    /**
     * <p>
     * The next slot that is known to be available. Slot indices begin at 0 and
     * run up to {@link Integer#MAX_VALUE}.
     * </p>
     * 
     * @todo This MUST be initialized on startup. The default is valid only for
     *       a new journal.
     */
    private int _nextSlot = 0;

    /**
     * Asserts that the slot index is in the legal range for the journal
     * <code>[0:slotLimit)</code>
     * 
     * @param slot
     *            The slot index.
     */
    void assertSlot(int slot) {

        if (slot >= 0 && slot < slotLimit)
            return;

        throw new AssertionError("slot=" + slot + " is not in [0:" + slotLimit
                + ")");

    }

    public SimpleSlotAllocationIndex(int slotLimit) {

        assert slotLimit > 0;

        this.slotLimit = slotLimit;

        allocated = new BitSet( slotLimit );

        committed = new BitSet( slotLimit );
        
    }

    public int release(int fromSlot, int minSlots ) {
       
        throw new UnsupportedOperationException();
        
    }
    
    /**
     * @todo Ensure that at least N slots are available for overwrite by this
     *       transaction. When necessary do extra work to migrate data to the
     *       database (perhaps handled by the journal/segment server), release
     *       slots that can be reused (part of concurrency control), or extend
     *       journal (handling transparent promotion of the journal to disk is a
     *       relatively low priority, but is required if the journal was not
     *       original opened in that mode).
     * 
     * @todo Allocation by {@link #releaseNextSlot(long)} should be optimized
     *       for fast consumption of the next N free slots.
     * 
     * FIXME Track the #of slots remaining in the global scope so that this
     * method can be ultra fast if there is no work to be done (this counter
     * could even live in the root block, along with the index of the next free
     * slot and the first free slot on the journal).
     */
    public int releaseSlots(int nslots) {

        _nextSlot = nextFreeSlot();
        
        return _nextSlot;
        
//        // first slot found.  this will be our return value.
//        int firstSlot = -1;
//        
//        // #of slots found.  we can not stop until nfree >= nslots.
//        int nfree = 0;
//        
//        /*
//         * True iff wrapped around the journal once already.
//         * 
//         * Note that the BitSet search methods only allow us to specify a
//         * fromIndex, not a fromIndex and a toIndex.  This means that we need
//         * to do a little more work to detect when we have scanned past our
//         * stopping point (and that BitSet will of necessity scan to the end
//         * of the bitmap).
//         */
//        boolean wrapped = false;
//        
//        // starting point for the search.
//        int fromSlot = this.nextSlot;
//        
//        // We do not search past this slot.
//        final int finalSlot = ( fromSlot == 0 ? slotLimit : fromSlot - 1 ); 
//        
//        while( nfree < nslots ) {
//            
//            if( _nextSlot == slotLimit ) {
//                
//                if( wrapped ) {
//                    
//                } else {
//                
//                    // restart search from front of journal.
//                    _nextSlot = 0;
//
//                    wrapped = true;
//                    
//                }
//                
//            }
//            
//            if( _nextSlot == -1 ) {
//                
//                int nrequired = nslots - nfree;
//                
//                _nextSlot = release(fromSlot, nrequired );
//                
//            }
//            
//            nfree++;
//            
//        }
//        
//        return _nextSlot;
        
    }

    /* Use to optimize release of the next N slots, and where N==1.
    private int[] releasedSlots;
    private int nextReleasedSlot;
    */
    
    public int releaseNextSlot() {

        // Required to prevent inadvertant extension of the BitSet.
        assertSlot(_nextSlot);
        
        // Mark this slot as in use.
        allocated.set(_nextSlot);

        return nextFreeSlot();
        
    }
    
    /**
     * Returns the next free slot.
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
            
            _nextSlot = allocated.nextClearBit( 0 );
            
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
        
        if( allocated.get( slot ) ) throw new IllegalStateException();
        
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
    
    public int getAllocatedSlotCount() {

        return allocated.cardinality();
        
    }
    
}
