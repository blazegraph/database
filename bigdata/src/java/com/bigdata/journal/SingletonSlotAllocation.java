package com.bigdata.journal;

/**
 * An {@link ISlotAllocation} for a single slot.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SingletonSlotAllocation implements ISlotAllocation {

    transient final int nbytes;
    transient private int slot = -1;
    transient private boolean closed = false;
    transient private boolean haveNext = false; 
    
    /**
     * Create an allocation that must fit within a single slot.
     * 
     * @param nbytes The #of bytes in the allocation.
     */
    public SingletonSlotAllocation(int nbytes) {

        if (nbytes <= 0)
            throw new IllegalArgumentException("nbytes=" + nbytes
                    + " is non-positive.");

        this.nbytes = nbytes;
        
    }

    public int getByteCount() {
        
        return nbytes;
        
    }
    
    public int capacity() {
        
        return 1;
        
    }
    
    public void add(int slot) {

        if( slot < 0 ) throw new IllegalArgumentException();
        
        if( this.slot >= 0 ) {
            
            // We already have our slot.
            throw new IllegalStateException();
            
        }
        
        this.slot = slot;
        
    }

    public void close() {

        if( this.slot < 0 ) {
            
            throw new IllegalStateException("No slots added");
            
        }
        
        if( closed ) {
            
            throw new IllegalStateException("Already closed.");
            
        }

        closed = true;
        
    }

    public boolean isClosed() {
        
        return closed;
        
    }
    
    public int firstSlot() {

        if( slot == -1 ) throw new IllegalStateException();
        
        haveNext = true;
        
        return slot;
        
    }

    /*
     * Since there is only one slot, this returns -1 the first time and then
     * thrown an IllegalStateException.
     */
    public int nextSlot() {
        
        if( haveNext ) {
            
            haveNext = false;
            
            return -1;
            
        }
        
        throw new IllegalStateException();
        
    }

    public int getSlotCount() {

        return this.slot == -1 ? 0 : 1;
        
    }
    
}
