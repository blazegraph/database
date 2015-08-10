package com.bigdata.relation.accesspath;

import java.util.Collections;

import com.bigdata.bop.IPredicate;
import com.bigdata.btree.IIndex;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

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

//        if (predicate == null)
//            throw new IllegalArgumentException();
//
//        if (keyOrder == null)
//            throw new IllegalArgumentException();

        this.predicate = predicate;
      
        this.keyOrder = keyOrder;
        
    }
    
    /**
     * {@inheritDoc}
     * @throws UnsupportedOperationException
     *             unless the caller specified an {@link IPredicate} to the
     *             ctor.
     */
    @Override
    public IPredicate<E> getPredicate() {

        if (predicate == null)
            throw new UnsupportedOperationException();

        return predicate;

    }

    /**
     * {@inheritDoc}
     * @throws UnsupportedOperationException
     *             unless the caller specified an {@link IKeyOrder} to the ctor.
     */
    @Override
    public IKeyOrder<E> getKeyOrder() {

        if (keyOrder == null)
            throw new UnsupportedOperationException();

        return keyOrder;
        
    }

    /**
     * {@inheritDoc}
     * @throws UnsupportedOperationException
     *             since no index was selected.
     */
    @Override
    public IIndex getIndex() {
        
        throw new UnsupportedOperationException();
        
    }
    
    /**
     * {@inheritDoc}<p>
     * Always returns <code>true</code>.
     */
    @Override
    public boolean isEmpty() {

        return true;

    }

    /**
     * {@inheritDoc}<p>Always returns ZERO(0).
     */
    @Override
    public long rangeCount(boolean exact) {

        return 0;

    }

//    /**
//     * Visits nothing.
//     */
//    @SuppressWarnings("unchecked")
//    @Override
//    public ITupleIterator<E> rangeIterator() {
//
//        return EmptyTupleIterator.INSTANCE;
//        
//    }

    /**
     * Visits nothing.
     */
    @Override
    public IChunkedOrderedIterator<E> iterator() {
        
        return iterator(0L/* offset */, 0L/* limit */, 0/* capacity */);
        
    }

//    /**
//     * Visits nothing.
//     */
//    public IChunkedOrderedIterator<E> iterator(int limit, int capacity) {
//
//        return iterator(0L/* offset */, limit, capacity);
//        
//    }

    /**
     * Visits nothing.
     */
    @Override
    @SuppressWarnings("unchecked")
    public IChunkedOrderedIterator<E> iterator(final long offset,
            final long limit, final int capacity) {

        return new ChunkedWrappedIterator<E>(Collections.EMPTY_LIST.iterator());

    }

    /**
     * Does nothing and always returns ZERO(0).
     */
    @Override
    public long removeAll() {

        return 0L;

    }

}
