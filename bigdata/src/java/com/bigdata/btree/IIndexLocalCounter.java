package com.bigdata.btree;

/**
 * An interface for accessing an index local counter.
 */
public interface IIndexLocalCounter {

    /**
     * A restart-safe counter. For an unpartitioned index, this a single counter
     * for the entire index with an initial value of zero (0) and it is stored
     * in the index {@link Checkpoint} record. For a partitioned index, there is a
     * distinct counter for each index partition, the partition identifier is
     * used as the high int32 bits of the counter, and the low int32 of the
     * counter has an initial value of zero (0) in each index partition.
     */
    public ICounter getCounter();
    
}
