package com.bigdata.bop.engine;

import java.util.UUID;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.fed.FederatedRunningQuery;
import com.bigdata.service.ResourceService;

/**
 * A message describing a chunk of intermediate results which are available for
 * processing. There are several implementations of this interface supporting
 * same-JVM messages, thick RMI messages, and RMI messages where the payload is
 * materialized using NIO transfers from the {@link ResourceService}.
 * 
 * @param <E>
 *            The generic type of the elements in the chunk (binding sets,
 *            elements from a relation, etc).
 */
public interface IChunkMessage<E> {

    /** The proxy for the query controller. */
    IQueryClient getQueryController();

    /** The query identifier. */
    UUID getQueryId();

    /** The identifier for the target {@link BOp}. */
    int getBOpId();
    
    /** The identifier for the target index partition. */
    int getPartitionId();

    /**
     * Return true iff the {@link IChunkMessage} is for the last evaluation pass
     * of an operator. The last evaluation pass for an operator must be
     * requested using an annotation. When it is requested, the operator will be
     * invoked one more time for each node or shard on which it was run
     * (depending on the {@link BOpEvaluationContext}). When so invoked, the
     * {@link IChunkMessage} will be associated with an empty source and this
     * flag will be set.
     * 
     * @see PipelineOp.Annotations#LAST_PASS
     */
    boolean isLastInvocation();
    
    /**
     * Return <code>true</code> if the chunk is materialized on the receiver.
     */
    boolean isMaterialized();

    /**
     * Materialize the chunk on the receiver.
     * 
     * @param runningQuery
     *            The running query.
     */
    void materialize(FederatedRunningQuery runningQuery);

    /**
     * Release all resources associated with this chunk. If the source has been
     * opened, then ensure that it is closed. If the data has been materialized,
     * then discard the materialized data.
     */
    void release();

    /**
     * Return an interface which may be used to access the chunk's data.
     */
    IChunkAccessor<E> getChunkAccessor();

}
