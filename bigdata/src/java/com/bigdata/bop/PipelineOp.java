/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Aug 18, 2010
 */

package com.bigdata.bop;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Priority;

import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;

/**
 * An pipeline operator reads from a source and writes on a sink. This is an
 * abstract base class for pipelined operators regardless of the type of data
 * moving along the pipeline.
 * 
 * @param <E>
 *            The generic type of the objects processed by the operator.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class PipelineOp<E> extends BOpBase implements IPipelineOp<E> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Well known annotations pertaining to the binding set pipeline.
     */
    public interface Annotations extends BOp.Annotations {

        /**
         * The maximum #of chunks that can be buffered before an the producer
         * would block (default {@value #DEFAULT_CHUNK_OF_CHUNKS_CAPACITY}).
         * Note that partial chunks may be combined into full chunks whose
         * nominal capacity is specified by {@link #CHUNK_CAPACITY}.
         * 
         * @see #DEFAULT_CHUNK_OF_CHUNKS_CAPACITY
         */
        String CHUNK_OF_CHUNKS_CAPACITY = PipelineOp.class.getName()
                + ".chunkOfChunksCapacity";

        /**
         * Default for {@link #CHUNK_OF_CHUNKS_CAPACITY}
         * 
         * @todo was 100. dialed down to reduce heap consumption for arrays.
         *       test performance @ 100 and 1000.
         */
        int DEFAULT_CHUNK_OF_CHUNKS_CAPACITY = 100;

        /**
         * Sets the capacity of the {@link IBuffer}s used to accumulate a chunk
         * of {@link IBindingSet}s (default {@value #CHUNK_CAPACITY}). Partial
         * chunks may be automatically combined into full chunks.
         * 
         * @see #DEFAULT_CHUNK_CAPACITY
         * @see #CHUNK_OF_CHUNKS_CAPACITY
         */
        String CHUNK_CAPACITY = PipelineOp.class.getName() + ".chunkCapacity";

        /**
         * Default for {@link #CHUNK_CAPACITY}
         */
        int DEFAULT_CHUNK_CAPACITY = 100;

        /**
         * The timeout in milliseconds that the {@link BlockingBuffer} will wait
         * for another chunk to combine with the current chunk before returning
         * the current chunk (default {@value #DEFAULT_CHUNK_TIMEOUT}). This may
         * be ZERO (0) to disable the chunk combiner.
         * 
         * @see #DEFAULT_CHUNK_TIMEOUT
         */
        String CHUNK_TIMEOUT = PipelineOp.class.getName() + ".chunkTimeout";

        /**
         * The default for {@link #CHUNK_TIMEOUT}.
         * 
         * @todo Experiment with values for this. Low values will push chunks
         *       through quickly. High values will cause chunks to be combined
         *       and move larger chunks around. [But if we factor BlockingBuffer
         *       out of the query engine then this will go away].
         */
        int DEFAULT_CHUNK_TIMEOUT = 20;

        /**
         * If the estimated rangeCount for an {@link AccessPath#iterator()} is
         * LTE this threshold then use a fully buffered (synchronous) iterator.
         * Otherwise use an asynchronous iterator whose capacity is governed by
         * {@link #CHUNK_OF_CHUNKS_CAPACITY}.
         * 
         * @see #DEFAULT_FULLY_BUFFERED_READ_THRESHOLD
         */
        String FULLY_BUFFERED_READ_THRESHOLD = PipelineOp.class.getName()
                + ".fullyBufferedReadThreshold";

        /**
         * Default for {@link #FULLY_BUFFERED_READ_THRESHOLD}.
         * 
         * @todo Experiment with this. It should probably be something close to
         *       the branching factor, e.g., 100.
         */
        int DEFAULT_FULLY_BUFFERED_READ_THRESHOLD = 100;

        /**
         * Flags for the iterator ({@link IRangeQuery#KEYS},
         * {@link IRangeQuery#VALS}, {@link IRangeQuery#PARALLEL}).
         * <p>
         * Note: The {@link IRangeQuery#PARALLEL} flag here is an indication
         * that the iterator may run in parallel across the index partitions.
         * This only effects scale-out and only for simple triple patterns since
         * the pipeline join does something different (it runs inside the index
         * partition using the local index, not the client's view of a
         * distributed index).
         * 
         * @see #DEFAULT_FLAGS
         */
        String FLAGS = PipelineOp.class.getName() + ".flags";

        /**
         * The default flags will visit the keys and values of the non-deleted
         * tuples and allows parallelism in the iterator (when supported).
         */
        final int DEFAULT_FLAGS = IRangeQuery.KEYS | IRangeQuery.VALS
                | IRangeQuery.PARALLEL;

    }

    /**
     * Deep copy constructor.
     * 
     * @param op
     */
    protected PipelineOp(final PipelineOp<E> op) {

        super(op);

    }

    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    protected PipelineOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

    }

    public int getChunkCapacity() {
        
        return getProperty(Annotations.CHUNK_CAPACITY,
                Annotations.DEFAULT_CHUNK_CAPACITY);

    }

    public int getChunkOfChunksCapacity() {

        return getProperty(Annotations.CHUNK_OF_CHUNKS_CAPACITY,
                Annotations.DEFAULT_CHUNK_OF_CHUNKS_CAPACITY);

    }

    public long getChunkTimeout() {
        
        return getProperty(Annotations.CHUNK_TIMEOUT,
                Annotations.DEFAULT_CHUNK_TIMEOUT);
        
    }

    /**
     * The {@link TimeUnit}s in which the {@link #chunkTimeout} is measured.
     */
    protected static transient final TimeUnit chunkTimeoutUnit = TimeUnit.MILLISECONDS;

    /**
     * Return the {@link PipelineType} of the operator (default
     * {@link PipelineType#Vectored}).
     */
    public PipelineType getPipelineType() {
       
        return PipelineType.Vectored;
        
    }

    /**
     * Return <code>true</code> iff {@link #newStats()} must be shared across
     * all invocations of {@link #eval(BOpContext)} for this operator for a
     * given query (default <code>false</code>).
     * <p>
     * Note: {@link BOp#getEvaluationContext()} MUST be overridden to return
     * {@link BOpEvaluationContext#CONTROLLER} if this method is overridden to
     * return <code>true</code>.
     * <p>
     * When <code>true</code>, the {@link QueryEngine} will impose the necessary
     * constraints when the operator is evaluated.
     */
    public boolean isSharedState() {
    
        return false;
        
    }
    
    public BOpStats newStats() {

        return new BOpStats();

    }

    public IBlockingBuffer<E[]> newBuffer(final BOpStats stats) {

        if (stats == null)
            throw new IllegalArgumentException();
        
        return new BlockingBufferWithStats<E[]>(getChunkOfChunksCapacity(),
                getChunkCapacity(), getChunkTimeout(), chunkTimeoutUnit, stats);

    }

    private static class BlockingBufferWithStats<E> extends BlockingBuffer<E> {

        private final BOpStats stats;

        /**
         * @param chunkOfChunksCapacity
         * @param chunkCapacity
         * @param chunkTimeout
         * @param chunkTimeoutUnit
         * @param stats
         */
        public BlockingBufferWithStats(int chunkOfChunksCapacity,
                int chunkCapacity, long chunkTimeout,
                TimeUnit chunkTimeoutUnit, final BOpStats stats) {

            super(chunkOfChunksCapacity, chunkCapacity, chunkTimeout,
                    chunkTimeoutUnit);
            
            this.stats = stats;
            
        }

        /**
         * Overridden to track {@link BOpStats#unitsOut} and
         * {@link BOpStats#chunksOut}.
         * <p>
         * Note: {@link BOpStats#chunksOut} will report the #of chunks added to
         * this buffer. However, the buffer MAY combine chunks either on add()
         * or when drained by the iterator so the actual #of chunks read back
         * from the iterator MAY differ.
         * <p>
         * {@inheritDoc}
         */
        @Override
        public boolean add(final E e, final long timeout, final TimeUnit unit)
                throws InterruptedException {

            final boolean ret = super.add(e, timeout, unit);

            if (e.getClass().getComponentType() != null) {

                stats.unitsOut.add(((Object[]) e).length);

            } else {

                stats.unitsOut.increment();

            }

            stats.chunksOut.increment();

            return ret;

        }

        /**
         * You can uncomment a line in this method to see who is closing the
         * buffer.
         * <p>
         * {@inheritDoc}
         */
        @Override
        public void close() {

//            if (isOpen())
//                log.error(toString(), new RuntimeException("STACK TRACE"));

            super.close();
            
        }
        
    }

}
