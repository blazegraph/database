package com.bigdata.btree.filter;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;

import cutthecrap.utils.striterators.FilterBase;

/**
 * Visits all elements visited by the source iterator and removes those
 * matching the filter.
 * <p>
 * Note: If you want to only visit those elements that are being removed,
 * then apply a {@link TupleFilter} first and then stack the {@link TupleRemover}
 * on top and make {@link #remove(Object)} always return <code>true</code>.
 * Note that you MUST also specify {@link IRangeQuery#CURSOR} if the index
 * is a local B+Tree since traversal with concurrent modification is
 * otherwise not supported.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 */
abstract public class TupleRemover<E> extends FilterBase implements ITupleFilter<E> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unchecked")
    @Override
    public ITupleIterator<E> filterOnce(final Iterator src, Object context) {

        return new TupleRemover.Removerator<E>((ITupleIterator<E>) src,
                context, this);

    }
    
    /**
     * <code>true</code> iff the tuple should be removed from the source
     * {@link IIndex}.
     * 
     * @param e
     *            The tuple.
     */
    abstract protected boolean remove(ITuple<E> e);

    /**
     * Implementation class.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     * @param <E>
     */
    private static class Removerator<E> implements ITupleIterator<E> {
        
        /**
         * The source iterator.
         */
        private final ITupleIterator<E> src;

        /**
         * The context object (ignored).
         */
        private final Object ctx;
        
        /**
         * The filter to be applied.
         */
        private final TupleRemover<E> filter;
        
        /**
         * @param src
         * @param filter
         */
        public Removerator(final ITupleIterator<E> src, final Object context,
                final TupleRemover<E> filter) {

            this.src = src;
            
            this.ctx = context;
            
            this.filter = filter;
        
        }
        
        public boolean hasNext() {
            
            return src.hasNext();
            
        }
        
        public ITuple<E> next() {

            if (!hasNext())
                throw new NoSuchElementException();

            // the next value to be returned.
            final ITuple<E> e = src.next();

            if (filter.remove(e)) {

                // Remove the visited element.
                src.remove();

            }

            return e;

        }

        /**
         * Note: Visited tuples which satisify
         * {@link TupleRemover#remove(Object)} will already have been removed
         * from the source iterator. Attempting to remove them again using
         * this method will cause an {@link IllegalStateException} to be
         * thrown by the source iterator.
         */
        public void remove() {
            
            src.remove();
            
        }
        
    }
    
}
