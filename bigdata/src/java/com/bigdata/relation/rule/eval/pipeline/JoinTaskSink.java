package com.bigdata.relation.rule.eval.pipeline;

import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.bigdata.mdi.PartitionLocator;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.UnsynchronizedArrayBuffer;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.service.IBigdataFederation;

/**
 * An object used by a {@link JoinTask} to write on another {@link JoinTask}
 * providing a sink for a specific index partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JoinTaskSink {

    protected static final Logger log = Logger.getLogger(JoinTaskSink.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    protected static final boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    protected static final boolean DEBUG = log.isDebugEnabled();

    /**
     * The future may be used to cancel or interrupt the downstream
     * {@link JoinTask}.
     */
    private Future future;
    
    /**
     * The {@link Future} of the downstream {@link JoinTask}. This may be
     * used to cancel or interrupt that {@link JoinTask}.
     */
    public Future getFuture() {
        
        if (future == null)
            throw new IllegalStateException();
        
        return future;
        
    }
    
    protected void setFuture(Future f) {
        
        if (future != null)
            throw new IllegalStateException();
        
        this.future = f;
        
        if(DEBUG)
            log.debug("sinkOrderIndex=" + sinkOrderIndex
                    + ", sinkPartitionId=" + locator.getPartitionId());
        
    }

    /**
     * The orderIndex for the sink {@link JoinTask}.
     */
    final int sinkOrderIndex;
    
    /**
     * The index partition that is served by the sink.
     */
    final PartitionLocator locator;

    /**
     * The individual {@link IBindingSet}s are written onto this
     * unsynchronized buffer. The buffer gathers those {@link IBindingSet}s
     * into chunks and writes those chunks onto the {@link #blockingBuffer}.
     */
    final UnsynchronizedArrayBuffer<IBindingSet> unsyncBuffer;

    /**
     * This buffer provides {@link IBindingSet} chunks to the downstream
     * {@link JoinTask}. That join task reads those chunks from a proxy for
     * the {@link BlockingBuffer#iterator()}.
     */
    final BlockingBuffer<IBindingSet[]> blockingBuffer;

    public String toString() {
        
        return "JoinSinkTask{ sinkOrderIndex=" + sinkOrderIndex
                + ", sinkPartitionId=" + locator.getPartitionId() + "}";
        
    }
    
    /**
     * Setups up the local buffers for a downstream {@link JoinTask}.
     * <p>
     * Note: The caller MUST create the task using a factory pattern on the
     * target data service and assign its future to the returned object
     * using {@link #setFuture(Future)}.
     * 
     * @param fed
     *            The federation.
     * @param locator
     *            The locator for the index partition.
     * @param sourceJoinTask
     *            The current join dimension.
     */
    public JoinTaskSink(final IBigdataFederation fed,
            final PartitionLocator locator, final JoinTask sourceJoinTask) {

        if (fed == null)
            throw new IllegalArgumentException();
        
        if (locator == null)
            throw new IllegalArgumentException();
        
        if (sourceJoinTask == null)
            throw new IllegalArgumentException();
        
        this.locator = locator;

        final IJoinNexus joinNexus = sourceJoinTask.joinNexus;

        this.sinkOrderIndex = sourceJoinTask.orderIndex + 1;
        
        /*
         * The sink JoinTask will read from the asynchronous iterator
         * drawing on the [blockingBuffer]. When we first create the sink
         * JoinTask, the [blockingBuffer] will be empty, but the JoinTask
         * will simply wait until there is something to be read from the
         * asynchronous iterator.
         */
        this.blockingBuffer = new BlockingBuffer<IBindingSet[]>(joinNexus
                .getChunkOfChunksCapacity());

        /*
         * The JoinTask adds bindingSets to this buffer. On overflow, the
         * binding sets are added as a chunk to the [blockingBuffer]. Once
         * on the [blockingBuffer] they are available to be read by the sink
         * JoinTask.
         */
        this.unsyncBuffer = new UnsynchronizedArrayBuffer<IBindingSet>(
                blockingBuffer, joinNexus.getChunkCapacity());

        /*
         * Note: The caller MUST create the task using a factory pattern on
         * the target data service and assign its future.
         */
        this.future = null;

    }

}