package com.bigdata.relation.accesspath;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.engine.BOpStats;

/**
 * Wraps the base class to update the caller's {@link BOpStats}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: UnsyncLocalOutputBuffer.java 3448 2010-08-18 20:55:58Z
 *          thompsonbry $
 * @param <E>
 */
public class UnsyncLocalOutputBuffer<E extends IBindingSet> extends
    AbstractUnsynchronizedArrayBuffer<E> {//UnsynchronizedOutputBuffer<E> {

//    private final BOpStats stats;
    private final IBlockingBuffer<E[]> syncBuffer;

//    * @param stats
//    *            Statistics object
    /**
     * @param capacity
     *            The capacity of this buffer.
     * @param syncBuffer
     *            The thread-safe buffer onto which this buffer writes when
     *            it overflows.
     */
    public UnsyncLocalOutputBuffer(//final BOpStats stats,
            final int capacity, final IBlockingBuffer<E[]> syncBuffer) {

        super(capacity);

//        this.stats = stats;
        
        this.syncBuffer = syncBuffer;

    }

    /**
     * Adds the chunk to the {@link #syncBuffer} and updates the
     * {@link BOpStats}.
     * 
     * @param chunk
     *            A chunk of {@link IBindingSet}s to be output.
     */
    @Override
    protected void handleChunk(final E[] chunk) {

        syncBuffer.add(chunk);

//        stats.chunksOut.increment();
//        stats.unitsOut.add(chunk.length);

    }
    
}