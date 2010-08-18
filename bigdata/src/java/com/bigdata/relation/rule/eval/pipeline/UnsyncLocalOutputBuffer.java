package com.bigdata.relation.rule.eval.pipeline;

import com.bigdata.bop.IBindingSet;
import com.bigdata.relation.accesspath.IBlockingBuffer;

/**
 * Implementation used for {@link LocalJoinTask}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 */
class UnsyncLocalOutputBuffer<E extends IBindingSet> extends
        UnsynchronizedOutputBuffer<E> {

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
    protected UnsyncLocalOutputBuffer(final LocalJoinTask joinTask,
            final int capacity, final IBlockingBuffer<E[]> syncBuffer) {

        super(joinTask, capacity);

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

        joinTask.stats.bindingSetChunksOut++;
        joinTask.stats.bindingSetsOut += chunk.length;

    }
    
}