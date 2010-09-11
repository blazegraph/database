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
     * Visit the binding sets in the chunk.
     * 
     * @todo we do not need to use {@link IAsynchronousIterator} any more. This
     *       could be much more flexible and should be harmonized to support
     *       high volume operators, GPU operators, etc. probably the right thing
     *       to do is introduce another interface here with a getChunk():IChunk
     *       where IChunk let's you access the chunks data in different ways
     *       (and chunks can be both {@link IBindingSet}[]s and element[]s so we
     *       might need to raise that into the interfaces and/or generics as
     *       well).
     * 
     * @todo It is likely that we can convert to the use of
     *       {@link BlockingQueue} instead of {@link BlockingBuffer} in the
     *       operators and then handle the logic for combining chunks inside of
     *       the {@link QueryEngine}. E.g., by scanning this list for chunks for
     *       the same bopId and combining them logically into a single chunk.
     *       <p>
     *       For scale-out, chunk combination will naturally occur when the node
     *       on which the operator will run requests the {@link ByteBuffer}s
     *       from the source nodes. Those will get wrapped up logically into a
     *       source for processing. For selective operators, those chunks can be
     *       combined before we execute the operator. For unselective operators,
     *       we are going to run over all the data anyway.
     * 
     * @throws IllegalStateException
     *             if the payload is not materialized.
     */
    IAsynchronousIterator<E[]> iterator();

}
