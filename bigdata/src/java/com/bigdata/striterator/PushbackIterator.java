package com.bigdata.striterator;

import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * Allows pushback of the most recently visited element onto the iterator.
 * <p>
 * Note: There is no corresponding {@link IFilter} pattern for this class since
 * you need the interface offered by the {@link PushbackIterator} in order to
 * use pushback.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 */
public class PushbackIterator<E> implements Iterator<E>, ICloseableIterator<E> {

    private final Iterator<E> src;

    /**
     * The most recent element visited by the iterator.
     */
    private E current;

    /**
     * When non-<code>null</code>, this element was pushed back and is
     * the next element to be visited.
     */
    private E buffer;

    public PushbackIterator(Iterator<E> src) {

        if (src == null)
            throw new IllegalArgumentException();

        this.src = src;

    }

    public boolean hasNext() {

        return buffer != null || src.hasNext();

    }

    public E next() {

        if (!hasNext())
            throw new NoSuchElementException();

        final E tmp;

        if (buffer != null) {

            tmp = buffer;

            buffer = null;

        } else {

            tmp = src.next();

        }

        current = tmp;

        return tmp;

    }

    /**
     * Push the value onto the internal buffer. It will be returned by the
     * next call to {@link #next()}.
     * 
     * @param value
     *            The value.
     * 
     * @throws IllegalStateException
     *             if there is already a value pushed back.
     */
    public void pushback() {

        if (buffer != null)
            throw new IllegalStateException();

        // pushback the last visited element.
        buffer = current;

    }

    public void remove() {

        throw new UnsupportedOperationException();

    }

    public void close() {

        if (src instanceof ICloseableIterator) {

            ((ICloseableIterator<E>) src).close();

        }

    }
}
