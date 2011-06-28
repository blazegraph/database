package com.bigdata.bop.engine;

import java.util.UUID;

import com.bigdata.bop.BOp;
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
     * Discard the materialized data.
     */
    void release();

    /**
     * Return an interface which may be used to access the chunk's data.
     */
    IChunkAccessor<E> getChunkAccessor();

}
