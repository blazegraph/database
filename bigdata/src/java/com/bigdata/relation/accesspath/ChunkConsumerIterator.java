package com.bigdata.relation.accesspath;

import java.util.Arrays;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.ICloseableIterator;
import com.bigdata.striterator.IKeyOrder;

/**
 * A class that aligns a buffer of <code>E[]</code>s (a buffer of chunks)
 * with an {@link IChunkedOrderedIterator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 */
public class ChunkConsumerIterator<E> implements IChunkedOrderedIterator<E> {

    final protected static Logger log = Logger.getLogger(ChunkConsumerIterator.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected static boolean DEBUG = log.isDebugEnabled();
    
    /** The source iterator. */
    private final ICloseableIterator<E[]> src;
    
    /**
     * The order of the elements in the buffer or <code>null</code> iff not
     * known.
     */
    private final IKeyOrder<E> keyOrder;
    
    /**
     * The index of the last entry returned in the current {@link #chunk} and
     * <code>-1</code> until the first entry is returned.
     */
    private int lastIndex = -1;

    /**
     * The current chunk -or- <code>null</code> if we need to fetch another
     * chunk.
     */
    private E[] chunk = null;
    
//    /**
//     * Total elapsed time for the iterator instance.
//     */
//    private long elapsed = 0L;

    /**
     * #of chunks materialized so far via {@link #nextChunk()} or
     * {@link #nextChunk(IKeyOrder)}.
     */
    private long nchunks = 0L;

    /**
     * #of elements materialized so far.
     */
    private long nelements = 0L;

    /**
     * 
     * @param src
     *            The source iterator. Note that each element visited by the
     *            source iterator is treated as a chunk of elements by this
     *            iterator.
     */
    public ChunkConsumerIterator(final ICloseableIterator<E[]> src) {

        this(src,/* keyOrder */null);

    }
    
    /**
     * 
     * @param src
     *            The source iterator. Note that each element visited by the
     *            source iterator is treated as a chunk of elements by this
     *            iterator.
     * @param keyOrder
     *            The natural order in which the un-chunked elements in the
     *            source iterator will be visited -or- <code>null</code> if no
     *            known.
     */
    public ChunkConsumerIterator(final ICloseableIterator<E[]> src,
            final IKeyOrder<E> keyOrder) {

        if (src == null)
            throw new IllegalArgumentException();
        
        this.src = src;
        
        this.keyOrder = keyOrder;
        
    }
    
    public IKeyOrder<E> getKeyOrder() {

        return keyOrder;
        
    }

    public boolean hasNext() {
        
        if ((lastIndex != -1) && ((lastIndex + 1) < chunk.length)) {

            return true;
            
        }
        
        if(DEBUG) {
            
            log.debug("Testing source iterator.");
            
        }
        
        return src.hasNext();
        
    }
    
    public E next() {

        if (!hasNext())
            throw new NoSuchElementException();

        if ((lastIndex == -1) || ((lastIndex + 1) == chunk.length)) {

            // get the next chunk from the source iterator.
            chunk = src.next();
            
            // reset the index.
            lastIndex = -1;

            if (INFO)
                log.info("read chunk from source iterator: nchunks=" + nchunks
                        + ", size=" + chunk.length);

        }

        // the next element.
        final E e = chunk[++lastIndex];
        nelements++;
        
        if (DEBUG)
            log.debug("lastIndex=" + lastIndex + ", chunk.length="
                    + chunk.length + ", #chunks=" + nchunks + ", #elements="
                    + nelements + ", e=" + e);

//        elapsed += (System.currentTimeMillis() - begin);

        return e;
        
    }

    @SuppressWarnings("unchecked")
    public E[] nextChunk() {
        
        if ((lastIndex == -1) || ((lastIndex + 1) == chunk.length)) {

            /*
             * The next element from the source will be the next chunk.
             */

            final E[] a = src.next();
            
            nchunks++;
            nelements += a.length;

            if (INFO)
                log.info("read chunk from source iterator: nchunks=" + nchunks
                        + ", size=" + a.length);
            
            return a;
            
        }
        
        /*
         * There is a partly consumed chunk on hand so we make it dense and
         * return everything remaining in that chunk.
         */
        
        // index of the next element to be returned.
        final int index = lastIndex + 1;
        
        final int remaining = chunk.length - index;
        
        // Dynamic type instantiation of array.
        final E[] a = (E[]) java.lang.reflect.Array.newInstance(
                chunk[lastIndex + 1].getClass(), remaining);
        
        // Copy remaining elements.
        System.arraycopy(chunk, index, a, 0, remaining);
        
        // chunk has been consumed.
        chunk = null;
        lastIndex = -1;
        nchunks++;
        nelements += remaining;

        if (INFO)
            log.info("remainder chunk: nchunks=" + nchunks + ", size="
                    + remaining);

        // return dense chunk.
        return a;
        
    }

    public E[] nextChunk(final IKeyOrder<E> keyOrder) {

        if (keyOrder == null)
            throw new IllegalArgumentException();

        final E[] chunk = nextChunk();

        if (!keyOrder.equals(getKeyOrder())) {

            // sort into the required order.

            Arrays.sort(chunk, 0, chunk.length, keyOrder.getComparator());

        }

        return chunk;

    }

    public void remove() {

        throw new UnsupportedOperationException();
        
    }

    public void close() {
    
        src.close();
        
        chunk = null;
        
        if (INFO)
            log.info("#chunks="+nchunks+", #elements="+nelements);
//            log.info("elapsed=" + elapsed);

    }

}
