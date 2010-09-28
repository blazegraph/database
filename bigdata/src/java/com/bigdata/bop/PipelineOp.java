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
 * Created on Sep 2, 2010
 */

package com.bigdata.bop;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IBlockingBuffer;

/**
 * Abstract base class for pipeline operators where the data moving along the
 * pipeline is chunks of {@link IBindingSet}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class PipelineOp extends BOpBase {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends BOp.Annotations, BufferAnnotations {

        /**
         * The value of the annotation is the {@link BOp.Annotations#BOP_ID} of
         * the ancestor in the operator tree which serves as the default sink
         * for binding sets (default is the parent).
         */
        String SINK_REF = PipelineOp.class.getName() + ".sinkRef";

        /**
         * The value of the annotation is the {@link BOp.Annotations#BOP_ID} of
         * the ancestor in the operator tree which serves as the alternative
         * sink for binding sets (default is no alternative sink).
         */
        String ALT_SINK_REF = PipelineOp.class.getName()
                + ".altSinkRef";

    }

    /**
     * Required deep copy constructor.
     * 
     * @param op
     */
    protected PipelineOp(final PipelineOp op) {
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

    /**
     * @see BufferAnnotations#CHUNK_CAPACITY
     */
    public int getChunkCapacity() {
        
        return getProperty(Annotations.CHUNK_CAPACITY,
                Annotations.DEFAULT_CHUNK_CAPACITY);

    }

    /**
     * @see BufferAnnotations#CHUNK_OF_CHUNKS_CAPACITY
     */
    public int getChunkOfChunksCapacity() {

        return getProperty(Annotations.CHUNK_OF_CHUNKS_CAPACITY,
                Annotations.DEFAULT_CHUNK_OF_CHUNKS_CAPACITY);

    }

    /**
     * @see BufferAnnotations#CHUNK_TIMEOUT
     */
    public long getChunkTimeout() {
        
        return getProperty(Annotations.CHUNK_TIMEOUT,
                Annotations.DEFAULT_CHUNK_TIMEOUT);
        
    }

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
    
    /**
     * Return a new object which can be used to collect statistics on the
     * operator evaluation (this may be overridden to return a more specific
     * class depending on the operator).
     */
    public BOpStats newStats() {

        return new BOpStats();

    }

    /**
     * Instantiate a buffer suitable as a sink for this operator. The buffer
     * will be provisioned based on the operator annotations.
     * <p>
     * Note: if the operation swallows binding sets from the pipeline (such as
     * operators which write on the database) then the operator MAY return an
     * immutable empty buffer.
     * 
     * @param stats
     *            The statistics on this object will automatically be updated as
     *            elements and chunks are output onto the returned buffer.
     * 
     * @return The buffer.
     */
    public IBlockingBuffer<IBindingSet[]> newBuffer(final BOpStats stats) {

        if (stats == null)
            throw new IllegalArgumentException();
        
        return new BlockingBufferWithStats<IBindingSet[]>(
                getChunkOfChunksCapacity(), getChunkCapacity(),
                getChunkTimeout(), Annotations.chunkTimeoutUnit, stats);

    }

    /**
     * Return a {@link FutureTask} which computes the operator against the
     * evaluation context. The caller is responsible for executing the
     * {@link FutureTask} (this gives them the ability to hook the completion of
     * the computation).
     * 
     * @param context
     *            The evaluation context.
     * 
     * @return The {@link FutureTask} which will compute the operator's
     *         evaluation.
     * 
     * @todo Modify to return a {@link Callable}s for now since we must run each
     *       task in its own thread until Java7 gives us fork/join pools and
     *       asynchronous file I/O. For the fork/join model we will probably
     *       return the ForkJoinTask.
     */
    abstract public FutureTask<Void> eval(BOpContext<IBindingSet> context);
    
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
