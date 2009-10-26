package com.bigdata.btree.filter;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;

import cutthecrap.utils.striterators.IFilter;
import cutthecrap.utils.striterators.IStriterator;
import cutthecrap.utils.striterators.Striterator;

/**
 * Implementation allows you to append {@link IFilter}s to an internal
 * ordered list. The corresponding stacked filter is then assembled by
 * {@link #newInstance(Iterator)}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FilterConstructor<E> implements IFilterConstructor<E>, Cloneable {

    private final List<IFilter> filters = new LinkedList<IFilter>();
    
    private static final long serialVersionUID = 8095075575678192516L;

    public FilterConstructor<E> clone() {
        
        final FilterConstructor<E> inst = new FilterConstructor<E>();
        
        for(IFilter filter : filters) {
            
            inst.addFilter(filter);
            
        }
        
        return inst;
        
    }
    
    /**
     * Add a(nother) filter to the filter stack. The filters are applied
     * in the order in which they are declared to this method.
     * 
     * @param filter The filter.
     * 
     * @return <i>this</i>
     */
    public FilterConstructor<E> addFilter(final IFilter filter) {
        
        if (filter == null)
            throw new IllegalArgumentException();
        
        filters.add(filter);
        
        return this;
        
    }
    
    /**
     * Create a filter stack.
     */
    @SuppressWarnings("unchecked")
    public ITupleIterator<E> newInstance(final ITupleIterator<E> src) {

        if (filters.isEmpty())
            return src;
        
        IStriterator tmp = new Striterator(src);
        
        for(IFilter filter : filters) {
            
            tmp = tmp.addFilter(filter);
            
        }

        // wrap Striterator up as an ITupleIterator.
        return new WrappedTupleIterator( tmp );
        
    }
    
    /**
     * Wraps an {@link Iterator}, typically one built up using an
     * {@link IFilterConstructor}, as an {@link ITupleIterator}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    private static class WrappedTupleIterator<E> implements ITupleIterator<E> {

        final private Iterator src;

        public WrappedTupleIterator(final Iterator src) {

            if (src == null)
                throw new IllegalArgumentException();

            this.src = src;

        }

        public boolean hasNext() {

            return src.hasNext();

        }

        @SuppressWarnings("unchecked")
        public ITuple<E> next() {

            return (ITuple<E>) src.next();

        }

        public void remove() {

            src.remove();

        }

    }

}
