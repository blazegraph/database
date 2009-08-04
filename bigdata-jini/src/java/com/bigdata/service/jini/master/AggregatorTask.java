/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 5, 2009
 */

package com.bigdata.service.jini.master;

import java.util.UUID;

import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.IKeyArrayIndexProcedure;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IRunnableBuffer;
import com.bigdata.relation.rule.eval.pipeline.JoinMasterTask;
import com.bigdata.relation.rule.eval.pipeline.JoinTaskFactoryTask;
import com.bigdata.service.DataService;
import com.bigdata.service.FederationCallable;
import com.bigdata.service.IRemoteExecutor;
import com.bigdata.service.Session;
import com.bigdata.service.Split;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.ndx.IAsynchronousWriteBufferFactory;
import com.bigdata.service.ndx.pipeline.IDuplicateRemover;
import com.bigdata.service.ndx.pipeline.IndexWriteTask;

/**
 * A task which aggregates writes destined for a specific scale-out index. An
 * instance of this class may be submitted to any {@link IRemoteExecutor} and
 * used to concentrate the writes from multiple clients. The requirement for an
 * aggregation task grows in proportion to the #of index partitions.
 * <p>
 * The task creates a {@link BlockingBuffer} (the input queue) and an
 * {@link IndexWriteTask} which will write on the specified scale-out index. It
 * then exports a proxy for the input queue. Each client which attaches to this
 * {@link AggregatorTask} must allocate a local {@link BlockingBuffer} and
 * assign a worker task to drain chunks from that buffer and transfer them to
 * this task via the exposed proxy.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <T>
 *            The generic type of the procedure used to write on the index.
 * @param <O>
 *            The generic type for unserialized value objects.
 * @param <R>
 *            The type of the result from applying the index procedure to a
 *            single {@link Split} of data.
 * @param <A>
 *            The type of the aggregated result.
 * 
 * @todo The aggregator's input buffer should be closed by the
 *       {@link TaskMaster} for a given (name,timestamp) when the task is done.
 *       
 * @todo It should be transparent to use aggregators in combination with the
 *       asynchronous write API. This may require the introduction another level
 *       of indirection.
 *       <p>
 *       For example, the RDF parser task directly creates the asynchronous
 *       write buffers today. However, it should accept a factory which it then
 *       applies to obtain those buffers. When using an aggregator, the factory
 *       needs to return the proxy for the write buffer for the corresponding
 *       aggregator. If this is to happen dynamically, then the
 *       {@link TaskMaster} {@link ServiceMap} must be dynamic as well (which
 *       could introduce load balancing for the {@link IRemoteExecutor} s). When
 *       an (RDF parser) task needs an asynchronous write buffer, it must
 *       atomically start an aggregator if none exists for the given
 *       (name,timestamp). The responsibility for starting that aggregator needs
 *       to be either centralized or de-centralized with global synchronous
 *       locking.
 *       <p>
 *       This all sounds far too complex. If the task declares the indices on
 *       which it will write then we can do this statically when the
 *       {@link TaskMaster} starts. The clients receive the {@link UUID}s of the
 *       services on which the aggregator(s) are running. They can then lookup
 *       the appropriate aggregator on each such service in parallel, collecting
 *       the set of aggregators for each index on which they will write. This is
 *       similar to the session mechanism of the {@link DataService} which is
 *       used to support distributed joins. See {@link JoinMasterTask},
 *       {@link JoinTaskFactoryTask} and {@link Session}.
 * 
 * @todo For scale-out indices with an exceedingly large #of index partitions it
 *       may be necessary to take additional steps to limit the RAM burden of
 *       the aggregator. For example, either the aggregator can buffer the
 *       writes on local disk or the the clients could split their writes across
 *       a population of aggregators each of which takes on a key-range for the
 *       index (the key-ranges would be more coarse than those of the index
 *       partitions themselves).
 */
public class AggregatorTask<T extends IKeyArrayIndexProcedure, O, R, A> extends
        FederationCallable<Void> implements IAsynchronousWriteBufferFactory {

    /**
     * 
     */
    private static final long serialVersionUID = -1786088168899639231L;

    /**
     * The name of the index on which the aggregator will write.
     */
    private final String name;
    
    /**
     * The timestamp associated with the index view.
     */
    private final long timestamp;

    private final IResultHandler<R, A> resultHandler;
    private final IDuplicateRemover<O> duplicateRemover;
    private final AbstractKeyArrayIndexProcedureConstructor<T> ctor;
    
    /**
     * 
     * @param name
     *            The name of the index to which the writes will be directed.
     * @param timestamp
     *            The timestamp associated with the index view.
     */
    public AggregatorTask(final String name, final long timestamp,
            final IResultHandler<R, A> resultHandler,
            final IDuplicateRemover<O> duplicateRemover,
            final AbstractKeyArrayIndexProcedureConstructor<T> ctor) {

        super();

        if (name == null)
            throw new IllegalArgumentException();
        
        this.name = name;
        
        this.timestamp = timestamp;
        
        this.resultHandler = resultHandler;
        
        this.duplicateRemover = duplicateRemover;
        
        this.ctor = ctor;

    }
    
    /**
     * The federation object used by the {@link IRemoteExecutor} on which this
     * task is executing.
     */
    public JiniFederation getFederation() {

        return (JiniFederation) super.getFederation();

    }

    /**
     * Starts an {@link IndexWriteTask} and makes a proxy for the input buffer
     * for that task available via
     * {@link #newWriteBuffer(IResultHandler, IDuplicateRemover, AbstractKeyArrayIndexProcedureConstructor)}
     * . The task will run until the input buffer is closed. It may be closed
     * via its proxy or by an internal error. If this task is interrupted, the
     * interrupt is propagated to the inner {@link IndexWriteTask} which will
     * also be canceled.
     */
    public Void call() throws Exception {

        try {
            /*
             * Create the input buffer for asynchronous writes on the index.
             */
            writeBuffer = getFederation().getIndex(name, timestamp)
                    .newWriteBuffer(resultHandler, duplicateRemover, ctor);

            /*
             * Obtain a proxy for the buffer.
             */
            writeBufferProxy = getFederation().getProxy(writeBuffer);

            try {

                /*
                 * Wait for the task draining that buffer to complete.
                 * 
                 * Note: The Future evaluates to a statistics object, but this
                 * does not attempt to return the statistics object to the
                 * caller. The statistics are reported via the performance
                 * counters so that you can see what is going on.
                 */

                writeBuffer.getFuture().get();

            } catch (InterruptedException ex) {

                /*
                 * Cancel the inner task if the outer one is interrupted.
                 */

                writeBuffer.getFuture().cancel(true/* mayInterruptIfRunning */);

                // re-throw the interrupt.
                throw ex;

            }

        } finally {
            // clear references.
            writeBufferProxy = null;
            writeBuffer = null;
        }
        
        // Done.
        return null;
        
    }

    /**
     * The input buffer.
     */
    private transient IRunnableBuffer<KVO<O>[]> writeBuffer;

    /**
     * The proxy for that input buffer.
     */
    private transient IRunnableBuffer<KVO<O>[]> writeBufferProxy;

    /**
     * Note: This ignores its arguments (it uses those specified to the ctor
     * instead) and returns the proxy for the pre-existing write buffer. For a
     * given {@link AggregatorTask}, all caller's will obtain a proxy for the
     * same buffer. This provides the desired aggregation semantics.
     * 
     * @throws IllegalStateException
     *             if the proxy for the buffer does not exist (because the task
     *             is not running).
     */
    public <T extends IKeyArrayIndexProcedure, O, R, A> IRunnableBuffer<KVO<O>[]> newWriteBuffer(
            final IResultHandler<R, A> resultHandler,
            final IDuplicateRemover<O> duplicateRemover,
            final AbstractKeyArrayIndexProcedureConstructor<T> ctor) {

        if (writeBufferProxy == null)
            throw new IllegalStateException();

        /*
         * Note: Cast to unadorned type is required to align APIs since this method
         * declares generics for the same purposes as this class.
         */
        return (IBlockingBuffer) writeBufferProxy;

    }

}
