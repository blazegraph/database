package com.bigdata.journal;

/**
 * A data structure used to get the header fields from a slot.
 * 
 * @see IBufferStrategy#readFirstSlot(int, boolean, SlotHeader, java.nio.ByteBuffer)
 */
class SlotHeader {
    
    /**
     * The prior slot# or -size iff this is the first slot in a chain of
     * slots for some data version.
     */
    int priorSlot;
    
    /**
     * The next slot# or {@link Journal#LAST_SLOT_MARKER} iff this is the
     * last slot in a chain of slots for some data version.
     */
    int nextSlot;
    
}