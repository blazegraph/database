package com.bigdata.objndx;

import com.bigdata.cache.HardReferenceQueue;

/**
 * Wraps the basic implementation and exposes a protected method that we
 * need to write the tests in this suite.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <T>
 */
public class MyHardReferenceQueue<T> extends HardReferenceQueue<T> {

    public MyHardReferenceQueue(HardReferenceQueueEvictionListener<T> listener, int capacity) {
        super(listener, capacity);
    }
    
    public MyHardReferenceQueue(HardReferenceQueueEvictionListener<T> listener, int capacity, int nscan) {
        super(listener, capacity,nscan);
    }
    
    public T[] toArray() {
        
        return super.toArray();
        
    }
    
}