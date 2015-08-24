package com.bigdata.btree.filter;

import java.util.Iterator;

import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;

/**
 * Wraps an {@link Iterator} as an {@link ITupleIterator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 */
public class WrappedTupleIterator<E> implements ITupleIterator<E> {

    final private Iterator<E> src;

    public WrappedTupleIterator(final Iterator<E> src) {

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
