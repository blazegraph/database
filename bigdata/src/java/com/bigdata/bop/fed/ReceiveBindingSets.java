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

package com.bigdata.bop.fed;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import com.bigdata.bop.AbstractPipelineOp;
import com.bigdata.bop.BOp;
import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.UnsynchronizedArrayBuffer;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.pipeline.JoinTask;
import com.bigdata.service.IBigdataFederation;

/**
 * Operator receives binding sets across a network boundary.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ReceiveBindingSets extends AbstractPipelineOp<IBindingSet> {

    protected static final Logger log = Logger
            .getLogger(ReceiveBindingSets.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * @param args
     * @param annotations
     */
    protected ReceiveBindingSets(final BOp[] args,
            final Map<String, Object> annotations) {
        
        super(args, annotations);
        
    }

    /**
     * Return the operator which is producing the binding sets.
     */
    protected BindingSetPipelineOp sourceOp() {

        return (BindingSetPipelineOp) args[0];

    }

    public Future<Void> eval(final IBigdataFederation<?> fed,
            final IJoinNexus joinNexus,
            final IBlockingBuffer<IBindingSet[]> buffer) {

        if (fed == null) {

            /*
             * When not running against a federation, delegate evaluation to the
             * sourceOp. Together with a similar delegation pattern for the map
             * operators, this effectively takes the map/receive operators out
             * of line.
             */

            return sourceOp().eval(fed, joinNexus, buffer);

        }

//        final FutureTask<Void> ft = new FutureTask<Void>(
//                new ReceiveBindingSetsTask(fed, joinNexus, buffer));
//
//        joinNexus.getIndexManager().getExecutorService().execute(ft);
//
//        return ft;
        
        throw new UnsupportedOperationException();
        
    }

//    /**
//     * An object used by a {@link JoinTask} to write on another {@link JoinTask}
//     * providing a sink for a specific index partition.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public class ReceiveBindingSetsTask  {
//
//        /**
//         * The future may be used to cancel or interrupt the downstream
//         * {@link JoinTask}.
//         */
//        private Future<?> future;
//        
//        /**
//         * The {@link Future} of the downstream {@link JoinTask}. This may be
//         * used to cancel or interrupt that {@link JoinTask}.
//         */
//        public Future<?> getFuture() {
//            
//            if (future == null)
//                throw new IllegalStateException();
//            
//            return future;
//            
//        }
//
//        protected void setFuture(final Future<?> f) {
//
//            if (future != null)
//                throw new IllegalStateException();
//
//            this.future = f;
//
//            if (log.isDebugEnabled())
//                log.debug("sinkOrderIndex=" + sinkOrderIndex
//                        + ", sinkPartitionId=" + locator.getPartitionId());
//
//        }
//
//        /**
//         * The orderIndex for the sink {@link JoinTask}.
//         */
//        final int sinkOrderIndex;
//        
//        /**
//         * The index partition that is served by the sink.
//         */
//        final PartitionLocator locator;
//
//        /**
//         * The individual {@link IBindingSet}s are written onto this
//         * unsynchronized buffer. The buffer gathers those {@link IBindingSet}s
//         * into chunks and writes those chunks onto the {@link #blockingBuffer}.
//         */
//        final UnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer;
//
//        /**
//         * This buffer provides {@link IBindingSet} chunks to the downstream
//         * {@link JoinTask}. That join task reads those chunks from a proxy for
//         * the {@link BlockingBuffer#iterator()}.
//         */
//        final BlockingBuffer<IBindingSet[]> blockingBuffer;
//
//        public String toString() {
//            
//            return "JoinSinkTask{ sinkOrderIndex=" + sinkOrderIndex
//                    + ", sinkPartitionId=" + locator.getPartitionId() + "}";
//            
//        }
//
//        /**
//         * Setups up the local buffers for a downstream {@link JoinTask}.
//         * <p>
//         * Note: The caller MUST create the task using a factory pattern on the
//         * target data service and assign its future to the returned object
//         * using {@link #setFuture(Future)}.
//         * 
//         * @param fed
//         *            The federation.
//         * @param locator
//         *            The locator for the index partition.
//         * @param sourceJoinTask
//         *            The current join dimension.
//         */
//        public ReceiveBindingSetsTask(final IBigdataFederation<?> fed,
//                final PartitionLocator locator, final JoinTask sourceJoinTask) {
//
//            if (fed == null)
//                throw new IllegalArgumentException();
//            
//            if (locator == null)
//                throw new IllegalArgumentException();
//            
//            if (sourceJoinTask == null)
//                throw new IllegalArgumentException();
//            
//            this.locator = locator;
//
//            final IJoinNexus joinNexus = sourceJoinTask.joinNexus;
//
//            this.sinkOrderIndex = sourceJoinTask.orderIndex + 1;
//            
//            /*
//             * The sink JoinTask will read from the asynchronous iterator
//             * drawing on the [blockingBuffer]. When we first create the sink
//             * JoinTask, the [blockingBuffer] will be empty, but the JoinTask
//             * will simply wait until there is something to be read from the
//             * asynchronous iterator.
//             */
//            this.blockingBuffer = new BlockingBuffer<IBindingSet[]>(joinNexus
//                    .getChunkOfChunksCapacity());
//
//            /*
//             * The JoinTask adds bindingSets to this buffer. On overflow, the
//             * binding sets are added as a chunk to the [blockingBuffer]. Once
//             * on the [blockingBuffer] they are available to be read by the sink
//             * JoinTask.
//             */
//            this.unsyncBuffer = new UnsynchronizedArrayBuffer<IBindingSet>(
//                    blockingBuffer, joinNexus.getChunkCapacity());
//
//            /*
//             * Note: The caller MUST create the task using a factory pattern on
//             * the target data service and assign its future.
//             */
//            this.future = null;
//
//        }
//
//    } // class ReceieveBindingSetsTask

}
