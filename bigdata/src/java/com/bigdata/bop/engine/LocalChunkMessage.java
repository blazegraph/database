package com.bigdata.bop.engine;

import java.io.Serializable;

import com.bigdata.bop.BOp;
import com.bigdata.bop.fed.FederatedRunningQuery;
import com.bigdata.relation.accesspath.IAsynchronousIterator;

/**
 * An non-{@link Serializable} chunk of intermediate results which are ready to
 * be consumed by some {@link BOp} in a specific query (this is only used in
 * query evaluation for the standalone database).
 * 
 * @todo test suite
 */
public class LocalChunkMessage<E> implements IChunkMessage<E> {

    /** The query controller. */
    private final IQueryClient queryController;

    /**
     * The query identifier.
     */
    private final long queryId;
    
    /**
     * The target {@link BOp}.
     */
    private final int bopId;

    /**
     * The index partition which is being targeted for that {@link BOp}.
     */
    private final int partitionId;
    
    /**
     * The binding sets to be consumed by that {@link BOp}.
     */
    private IAsynchronousIterator<E[]> source;

    public IQueryClient getQueryController() {
        return queryController;
    }

    public long getQueryId() {
        return queryId;
    }

    public int getBOpId() {
        return bopId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public boolean isMaterialized() {
        return true;
    }

    public LocalChunkMessage(final IQueryClient queryController,
            final long queryId, final int bopId, final int partitionId,
            final IAsynchronousIterator<E[]> source) {

        if (queryController == null)
            throw new IllegalArgumentException();
        
        if (source == null)
            throw new IllegalArgumentException();
        
        this.queryController = queryController;
        
        this.queryId = queryId;
        
        this.bopId = bopId;
        
        this.partitionId = partitionId;
        
        this.source = source;
        
    }

    public String toString() {

        return getClass().getName() + "{queryId=" + queryId + ",bopId=" + bopId
                + ",partitionId=" + partitionId + "}";
        
    }

    public void materialize(FederatedRunningQuery runningQuery) {
        // NOP
    }

    public void release() {
        // NOP
    }
    
    public IChunkAccessor<E> getChunkAccessor() {
        return new ChunkAccessor();
    }

    private class ChunkAccessor implements IChunkAccessor<E> {

        public IAsynchronousIterator<E[]> iterator() {
            return source;
        }

    }

}
