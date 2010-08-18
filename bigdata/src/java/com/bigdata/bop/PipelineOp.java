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

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;

/**
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
        String CHUNK_OF_CHUNKS_CAPACITY = BlockingBuffer.class.getName()
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
        String CHUNK_CAPACITY = IBuffer.class.getName() + ".chunkCapacity";

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
        String CHUNK_TIMEOUT = BlockingBuffer.class.getName() + ".chunkTimeout";

        /**
         * The default for {@link #CHUNK_TIMEOUT}.
         * 
         * @todo this is probably much larger than we want. Try 10ms.
         */
        int DEFAULT_CHUNK_TIMEOUT = 1000;

    }

    /**
     * Instantiate a buffer to serve as the sink for this operator.  The buffer
     * will be provisioned based on the {@link Annotations}.
     * 
     * @return The buffer.
     */
    IBlockingBuffer<E[]> newBuffer();

    /**
     * Initiate execution for the operator, returning a {@link Future} which for
     * that evaluation. Operator evaluation may be halted using
     * {@link Future#cancel(boolean)}.
     * <p>
     * Note: The use of the {@link Future} interface rather than
     * {@link Runnable} or {@link Callable} makes it possible to execute
     * operators against either an {@link Executor} or a {@link ForkJoinPool}.
     * 
     * @param fed
     *            The {@link IBigdataFederation} IFF the operator is being
     *            evaluated on an {@link IBigdataFederation}. When evaluating
     *            operations against an {@link IBigdataFederation}, this
     *            reference provides access to the scale-out view of the indices
     *            and to other bigdata services.
     * @param joinNexus
     *            An evaluation context with hooks for the <em>local</em>
     *            execution environment. When evaluating operators against an
     *            {@link IBigdataFederation} the {@link IJoinNexus} MUST be
     *            formulated with the {@link IIndexManager} of the local
     *            {@link IDataService} order perform efficient reads against the
     *            shards views as {@link ILocalBTreeView}s. It is an error if
     *            the {@link IJoinNexus#getIndexManager()} returns the
     *            {@link IBigdataFederation} since each read would use RMI. This
     *            condition should be checked by the operator implementation.
     * @param buffer
     *            Where to write the output of the operator.
     * 
     * @return The {@link Future} for the operator's evaluation.
     * 
     * @todo return the execution statistics here? Return Void?
     */
    Future<Void> eval(IBigdataFederation<?> fed, IJoinNexus joinNexus,
            IBlockingBuffer<E[]> buffer);

}
