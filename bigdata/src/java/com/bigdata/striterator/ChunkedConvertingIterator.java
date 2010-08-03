package com.bigdata.striterator;

import java.util.Arrays;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;

/**
 * Supports the chunk-at-a-time filter and conversion operations.
 * Chunk-at-a-time operations are potentially much more efficient as they allow
 * for ordered reads and ordered writes on {@link IIndex}s.
 * 
 * @version $Id$
 * 
 * @param <E>
 *            The generic type of the source elements.
 * @param <F>
 *            The generic type of the converted elements.
 * 
 * @see IChunkConverter
 * 
 * @todo This class is redundent with the {@link ChunkedFilter}
 */
public class ChunkedConvertingIterator<E, F> implements IChunkedOrderedIterator<F> {

    private final static Logger log = Logger
            .getLogger(ChunkedConvertingIterator.class);

    private final IChunkedOrderedIterator<E> src;

    private final IChunkConverter<E, F> converter;

    /**
     * The converted {@link IKeyOrder}.
     */
    private final IKeyOrder<F> keyOrder;

    private F[] converted = null;

    private int pos = 0;

    /**
     * Ctor when the element type is NOT being changed during conversion. This
     * ctor uses the {@link IKeyOrder} reported by the source
     * {@link IChunkedOrderedIterator}. This will result in runtime exceptions
     * if the generic types of the source and converted iterators differ.
     * 
     * @param src
     *            The source iterator.
     * @param converter
     *            The chunk-at-a-time converter.
     */
    public ChunkedConvertingIterator(IChunkedOrderedIterator<E> src,
            IChunkConverter<E, F> converter) {

        this(src, converter, (IKeyOrder<F>) src.getKeyOrder());
        
    }

    /**
     * Variant ctor when you are also converting the element type.
     * 
     * @param src
     *            The source iterator.
     * @param converter
     *            The chunk-at-a-time converter.
     * @param keyOrder
     *            The {@link IKeyOrder} for the converted chunks -or-
     *            <code>null</code> iff not known.
     */
    public ChunkedConvertingIterator(IChunkedOrderedIterator<E> src,
            IChunkConverter<E, F> converter, IKeyOrder<F> keyOrder) {
        
        if (src == null)
            throw new IllegalArgumentException();

        if (converter == null)
            throw new IllegalArgumentException();
        
        this.src = src;
        
        this.converter = converter;
        
        // MAY be null.
        this.keyOrder = keyOrder;
        
    }

    /**
     * Applies the chunk-at-a-time converter. The converted chunk MAY contain a
     * different number of elements. If it does not contain any elements then
     * another chunk will be fetched from the source iterator by
     * {@link #hasNext()}.
     * 
     * @param src
     *            A chunk of elements from the source iterator.
     * 
     * @return A converted chunk of elements.
     */
    protected F[] convert(final IChunkedOrderedIterator<E> src) {
        
        final F[] tmp = converter.convert(src);

        if (tmp == null)
            throw new AssertionError("Converter returns null: "
                    + converter.getClass());
        
        if (log.isInfoEnabled()) {

            log.info("Converted: chunkSize=" + tmp.length + " : chunk="
                    + Arrays.toString(tmp));
            
        }
        
        return tmp;
        
    }

    public void close() {
        
        src.close();
        
    }

    public F next() {
        
        if (!hasNext())
            throw new NoSuchElementException();
        
//        if ((converted == null || pos >= converted.length) && src.hasNext()) {
//        
//            // convert the next chunk
//            converted = convert(src.nextChunk());
//            
//            pos = 0;
//            
//        }
    
        if (log.isInfoEnabled())
            log.info("returning converted[" + pos + "]");
        
        return converted[pos++];
        
    }

    /*
     * @return true iff there is a non-empty chunk and pos is LT the length of
     * that chunk.
     */
    public boolean hasNext() {
        
        /*
         * Note: This loops until we either have a non-empty chunk or the source
         * is exhausted. This allows for the possibility that a converter can
         * return an empty chunk, e.g., because all elements in the chunk were
         * filtered out.
         */
        while ((converted == null || pos >= converted.length) && src.hasNext()) {

            // convert the next chunk
            converted = convert(src);

            pos = 0;

        }
        
        final boolean hasNext = converted != null && pos < converted.length;
        
        if (log.isInfoEnabled())
            log.info(hasNext);
        
        // StringWriter sw = new StringWriter();
        // new Exception("stack trace").printStackTrace(new PrintWriter(sw));
        // log.info(sw.toString());

        return hasNext;
        
    }

    public IKeyOrder<F> getKeyOrder() {
        
        return keyOrder;
        
    }

    public F[] nextChunk() {

        if (!hasNext())
            throw new NoSuchElementException();
        
//        if (pos >= converted.length && src.hasNext()) {
//
//            // convert the next chunk
//            converted = convert(src.nextChunk());
//
//            pos = 0;
//
//        }

        if (pos > 0) {

            /*
             * The chunk is partly consumed so we need to make it dense.
             */
            
            final int remaining = converted.length - pos;
            
            final F[] chunk = (F[]) java.lang.reflect.Array.newInstance(
                    converted[0].getClass(), remaining);
            
            System.arraycopy(converted, pos, chunk, 0, remaining);
            
            converted = chunk;
            
        }

        final F[] nextChunk = converted;
        
        converted = null;
        
        pos = 0;
        
        return nextChunk;
        
    }

    public F[] nextChunk(IKeyOrder<F> keyOrder) {

        if (keyOrder == null)
            throw new IllegalArgumentException();
        
        final F[] chunk = nextChunk();

        if (!keyOrder.equals(this.keyOrder)) {

            Arrays.sort(chunk, keyOrder.getComparator());

        }

        return chunk;
        
    }

    /**
     * Not supported.
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
    public void remove() {

        throw new UnsupportedOperationException();
        
    }

}
