package com.bigdata.htree;

import com.bigdata.btree.ICounter;

/**
 * Mutable counter.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class Counter implements ICounter {

    private final HTree htree;
    
    public Counter(final HTree btree) {
        
        if (btree == null)
            throw new IllegalArgumentException();
        
        this.htree = btree;
        
    }
    
    public long get() {
        
        return htree.counter.get();
        
    }

    public long incrementAndGet() {
        
        final long counter = htree.counter.incrementAndGet();
        
        if (counter == htree.getCheckpoint().getCounter() + 1) {

            /*
             * The first time the counter is incremented beyond the value in
             * the checkpoint record we fire off a dirty event to put the
             * BTree on the commit list.
             */
            
        	htree.fireDirtyEvent();
        	
        }
            
        if (counter == 0L) {

            /*
             * The counter has wrapped back to ZERO.
             */
            
            throw new RuntimeException("Counter overflow");

        }
        
        return counter;
        
    }
    
}