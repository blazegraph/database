package com.bigdata.bop.engine;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.fed.FederatedRunningQuery;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
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
 * @todo Compressed representations of binding sets with the ability to read
 *       them in place or materialize them onto the java heap. The
 *       representation should be amenable to processing in C since we want to
 *       use them on GPUs as well. See {@link IChunkMessage} and perhaps
 *       {@link IRaba}.
 */
public interface IChunkMessage<E> {

    /** The proxy for the query controller. */
    IQueryClient getQueryController();

    /** The query identifier. */
    long getQueryId();

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
