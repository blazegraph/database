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

import java.util.concurrent.Future;

import com.bigdata.bop.engine.BOpStats;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;

/**
 * An pipeline operator reads from a source and writes on a sink.
 * 
 * @param <E>
 *            The generic type of the objects processed by the operator.
 *            
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface PipelineOp<E> extends BOp {

    /**
     * Well known annotations pertaining to the binding set pipeline.
     */
    public interface Annotations extends BOp.Annotations {

        /**
         * The maximum #of chunks that can be buffered before an the producer
         * would block (default {@value #DEFAULT_CHUNK_OF_CHUNKS_CAPACITY}).
         * Note that partial chunks may be combined into full chunks whose
         * nominal capacity is specified by {@link #CHUNK_CAPACITY}.
         */
        String CHUNK_OF_CHUNKS_CAPACITY = PipelineOp.class.getName()
                + ".chunkOfChunksCapacity";

        /**
         * Default for {@link #CHUNK_OF_CHUNKS_CAPACITY}
         */
        int DEFAULT_CHUNK_OF_CHUNKS_CAPACITY = 1000;

        /**
         * Sets the capacity of the {@link IBuffer}s used to accumulate a chunk
         * of {@link IBindingSet}s (default {@value #CHUNK_CAPACITY}). Partial
         * chunks may be automatically combined into full chunks.
         * 
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
         */
        String CHUNK_TIMEOUT = PipelineOp.class.getName() + ".chunkTimeout";

        /**
         * The default for {@link #CHUNK_TIMEOUT}.
         * 
         * @todo this is probably much larger than we want. Try 10ms.
         */
        int DEFAULT_CHUNK_TIMEOUT = 1000;

    }

    /**
     * Return a new object which can be used to collect statistics on the
     * operator evaluation (this may be overridden to return a more specific
     * class depending on the operator).
     */
    BOpStats newStats();

    /**
     * Instantiate a buffer suitable as a sink for this operator. The buffer
     * will be provisioned based on the operator annotations.
     * 
     * @return The buffer.
     */
    IBlockingBuffer<E[]> newBuffer();

    /**
     * Initiate execution for the operator, returning a {@link Future} which for
     * that evaluation.
     * 
     * @param context
     *            The evaluation context.
     * 
     * @return The {@link Future} for the operator's evaluation.
     */
    Future<Void> eval(BOpContext<E> context);

}
