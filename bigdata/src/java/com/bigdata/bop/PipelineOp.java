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

import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IBlockingBuffer;

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
    public interface Annotations extends BOp.Annotations, BufferAnnotations {

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
