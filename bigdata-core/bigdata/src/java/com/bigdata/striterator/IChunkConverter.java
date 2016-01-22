package com.bigdata.striterator;

/**
 * This is a chunk at a time type processor. Elements can be dropped, have their
 * state changed, or have their state replaced by another element, potentially
 * of a different generic type.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: SPOConvertingIterator.java,v 1.6 2008/08/25 17:21:01
 *          thompsonbry Exp $
 * @param <E>
 *            The generic type of the source elements.
 * @param <F>
 *            The generic type of the converted elements.
 */
public interface IChunkConverter<E, F> {

    /**
     * Convert the next chunk of element(s) from the source iterator into target
     * element(s).
     * <p>
     * Note: This method will only be invoked if
     * {@link ChunkedConvertingIterator#hasNext()} reports <code>true</code>
     * for the source iterator.
     * <p>
     * Note: Iterators are single-threaded so the implementation of this method
     * does not need to be thread-safe.
     * 
     * @param src
     *            The source iterator.
     * 
     * @return The target chunk (not null, but may be empty).
     */
    F[] convert(IChunkedOrderedIterator<E> src);

}
