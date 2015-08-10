package com.bigdata.bop.engine;

import java.util.UUID;

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
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/475"> Optimize
 *      serialization for query messages on cluster </a>
 */
public interface IChunkMessage<E> extends IOpMessage {

    /**
     * The proxy for the query controller.
     * 
     * @deprecated This forces us to serialize and send the proxy for the query
     *             controller on a cluster. The message format is slimmer if we
     *             instead rely on resolution of {@link #getQueryControllerId()}
     *             against a service discovery cache.
     *             <p>
     *             We can not discover the query controller using river because
     *             it is not registered as a service. We can find the query
     *             peers on the data service nodes easily enough because they
     *             are all registered with river. However, the QueryEngine
     *             serving as the query controller is not currently registered
     *             with river and hence it can not be discovered using the UUID
     *             of the query controller alone. Probably the right thing to do
     *             is to register the query controller with river so it can be
     *             discovered. We could then modify getQueryPeer() (or add
     *             getQueryClient(UUID)) which would hit the discovery cache.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/475">
     *      Optimize serialization for query messages on cluster</a>
     * 
     * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/487"> The
     *      query controller should be discoverable</a>
     */
    IQueryClient getQueryController();

    /**
     * The UUID of the query controller (the {@link IQueryClient} to which the
     * query was submitted).
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/475
     */
    UUID getQueryControllerId();

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

    /**
     * Return the #of solutions which are available from this message.
     */
    int getSolutionCount();
    
}
