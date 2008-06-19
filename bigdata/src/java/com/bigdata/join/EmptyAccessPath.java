package com.bigdata.join;

import java.util.Collections;

import com.bigdata.btree.EmptyTupleIterator;
import com.bigdata.btree.ITupleIterator;

/**
 * An access path that is known to be empty.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class EmptyAccessPath<E> implements IAccessPath<E> {

    /**
     * Shared instance.
     */
    public static final transient IAccessPath INSTANCE = new EmptyAccessPath();
    
    /**
     * @throws UnsupportedOperationException
     */
    public IPredicate<E> getPredicate() {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Always returns <code>true</code>.
     */
    public boolean isEmpty() {

        return true;

    }

    /**
     * Always returns ZERO(0).
     */
    public long rangeCount() {

        return 0;

    }

    /**
     * Visits nothing.
     */
    @SuppressWarnings("unchecked")
    public ITupleIterator<E> rangeIterator() {

        return EmptyTupleIterator.INSTANCE;
        
    }

    /**
     * Visits nothing.
     */
    public IChunkedIterator<E> iterator() {
        
        return iterator(0,0);
        
    }

    /**
     * Visits nothing.
     */
    @SuppressWarnings("unchecked")
    public IChunkedIterator<E> iterator(int limit, int capacity) {
        
        return new ChunkedIterator<E>(Collections.EMPTY_LIST.iterator());
        
    }

    /**
     * Does nothing and always returns ZERO(0).
     */
    public long removeAll() {

        return 0L;

    }

}
