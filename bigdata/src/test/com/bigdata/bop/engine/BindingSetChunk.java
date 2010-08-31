package com.bigdata.bop.engine;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.relation.accesspath.IAsynchronousIterator;

/**
 * A chunk of intermediate results which are ready to be consumed by some
 * {@link BOp} in a specific query.
 */
public class BindingSetChunk {

    /**
     * The query identifier.
     */
    final long queryId;
    
    /**
     * The target {@link BOp}.
     */
    final int bopId;

    /**
     * The index partition which is being targeted for that {@link BOp}.
     */
    final int partitionId;
    
    /**
     * The binding sets to be consumed by that {@link BOp}.
     */
    final IAsynchronousIterator<IBindingSet[]> source;

    public BindingSetChunk(final long queryId, final int bopId,
            final int partitionId,
            final IAsynchronousIterator<IBindingSet[]> source) {
        if (source == null)
            throw new IllegalArgumentException();
        this.queryId = queryId;
        this.bopId = bopId;
        this.partitionId = partitionId;
        this.source = source;
    }

    public String toString() {

        return getClass().getName() + "{queryId=" + queryId + ",bopId=" + bopId
                + ",partitionId=" + partitionId + "}";
        
    }
    
}
