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
 */
public interface IChunkMessage {

    /** The proxy for the query controller. */
    IQueryClient getQueryController();

    /** The query identifier. */
    long getQueryId();

    /** The identifier for the target {@link BOp}. */
    int getBOpId();
    
    /** The identifier for the target index partition. */
    int getPartitionId();

    /*
     * @todo Report the #of bytes available with this message. However, first
     * figure out if that if the #of bytes in this {@link OutputChunk} or across
     * all {@link OutputChunk}s available for the target service and sink.
     */ 
    // @todo move to concrete subclass or allow ZERO if data are in memory (no RMI).
//    /** The #of bytes of data which are available for that operator. */
//    int getBytesAvailable();
    
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
     */
    IAsynchronousIterator<IBindingSet[]> iterator();

    // /**
    // * The Internet address and port of a {@link ResourceService} from which
    // * the receiver may demand the data.
    // */
    // InetSocketAddress getServiceAddr();
    //
    // /**
    // * The set of resources on the sender which comprise the data.
    // */
    // Iterator<UUID> getChunkIds();

}
