package com.bigdata.relation.accesspath;

import java.util.Collections;

import com.bigdata.btree.EmptyTupleIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.relation.rule.IPredicate;

/**
 * An access path that is known to be empty.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class EmptyAccessPath<E> implements IAccessPath<E> {

    private final IPredicate<E> predicate;

    private final IKeyOrder<E> keyOrder;

    /**
     * Ctor variant does not specify the {@link #getPredicate()} or the
     * {@link #getKeyOrder()} and those methods will throw an
     * {@link UnsupportedOperationException} if invoked.
     */
    public EmptyAccessPath() {

        this(null/* predicate */, null/* keyOrder */);
        
    }
    
    /**
     * Note: the {@link #getPredicate()} and {@link #getKeyOrder()} and methods
     * will throw an {@link UnsupportedOperationException} if the corresponding
     * argument is null.
     * 
     * @param predicate
     * @param keyOrder
     */
    public EmptyAccessPath(IPredicate<E> predicate, IKeyOrder<E> keyOrder) {

        if (predicate == null)
            throw new IllegalArgumentException();

        if (keyOrder == null)
            throw new IllegalArgumentException();

        this.predicate = predicate;
      
        this.keyOrder = keyOrder;
        
    }
    
    /**
     * @throws UnsupportedOperationException
     *             unless the caller specified an {@link IPredicate} to the
     *             ctor.
     */
    public IPredicate<E> getPredicate() {

        if (predicate == null)
            throw new UnsupportedOperationException();

        return predicate;

    }

    /**
     * @throws UnsupportedOperationException
     *             unless the caller specified an {@link IKeyOrder} to the ctor.
     */
    public IKeyOrder<E> getKeyOrder() {

        if (keyOrder == null)
            throw new UnsupportedOperationException();

        return keyOrder;
        
    }

    /**
     * @throws UnsupportedOperationException
     *             since no index was selected.
     */
    public IIndex getIndex() {
        
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
    public long rangeCount(boolean exact) {

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
    public IChunkedOrderedIterator<E> iterator() {
        
        return iterator(0,0);
        
    }

    /**
     * Visits nothing.
     */
    @SuppressWarnings("unchecked")
    public IChunkedOrderedIterator<E> iterator(int limit, int capacity) {
        
        return new ChunkedWrappedIterator<E>(Collections.EMPTY_LIST.iterator());
        
    }

    /**
     * Does nothing and always returns ZERO(0).
     */
    public long removeAll() {

        return 0L;

    }

}
