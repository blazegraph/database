package com.bigdata.relation.rule.eval.pipeline;

import com.bigdata.bop.IBindingSet;
import com.bigdata.relation.accesspath.AbstractUnsynchronizedArrayBuffer;
import com.bigdata.relation.accesspath.IBlockingBuffer;

/**
 * Keeps track of the chunks of binding sets that are generated on the caller's
 * {@link JoinStats}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: UnsyncLocalOutputBuffer.java 3448 2010-08-18 20:55:58Z
 *          thompsonbry $
 * @param <E>
 */
public class UnsyncLocalOutputBuffer<E extends IBindingSet> extends
    AbstractUnsynchronizedArrayBuffer<E> {//UnsynchronizedOutputBuffer<E> {

    private final JoinStats joinStats;
    private final IBlockingBuffer<E[]> syncBuffer;

    /**
     * @param joinTask
     *            The task that is writing on this buffer.
     * @param capacity
     *            The capacity of this buffer.
     * @param syncBuffer
     *            The thread-safe buffer onto which this buffer writes when
     *            it overflows.
     */
    public UnsyncLocalOutputBuffer(final JoinStats joinStats,
            final int capacity, final IBlockingBuffer<E[]> syncBuffer) {

        super(capacity);

        this.joinStats = joinStats;
        
        this.syncBuffer = syncBuffer;

    }

    /**
     * Adds the chunk to the {@link #syncBuffer} and updated the
     * {@link JoinStats} to reflect the #of {@link IBindingSet} chunks that
     * will be output and the #of {@link IBindingSet}s in those chunks.
     * 
     * @param chunk
     *            A chunk of {@link IBindingSet}s to be output.
     */
    @Override
    protected void handleChunk(final E[] chunk) {

        syncBuffer.add(chunk);

        joinStats.bindingSetChunksOut++;
        joinStats.bindingSetsOut += chunk.length;

    }
    
}