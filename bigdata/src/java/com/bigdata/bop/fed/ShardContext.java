package com.bigdata.bop.fed;

/**
 * An allocation context which is shared by all operators running in the
 * same query which target the same shard (the same shard implies the same
 * service, at least until we have HA with shard affinity).
 */
class ShardContext extends AllocationContextKey {

    private final Long queryId;

    private final int bopId;
    
    private final int partitionId;

    ShardContext(final Long queryId, final int bopId, final int partitionId) {
        this.queryId = queryId;
        this.bopId = bopId;
        this.partitionId = partitionId;
    }

    public int hashCode() {
        return (queryId.hashCode() * 961) + (bopId * 31) + partitionId;
    }

    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (!(o instanceof ShardContext))
            return false;
        if (!queryId.equals(((ShardContext) o).queryId))
            return false;
        if (bopId != ((ShardContext) o).bopId)
            return false;
        if (partitionId != ((ShardContext) o).partitionId)
            return false;
        return true;
    }
    
    @Override
    public boolean hasOperatorScope(final int bopId) {
        return this.bopId == bopId;
    }

}