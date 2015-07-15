package com.bigdata.bop.fed;

import java.util.UUID;

/**
 * An allocation context which is shared by all operators running in the
 * same query which target the same service.
 */
class ServiceContext extends AllocationContextKey {
    
    private final UUID queryId;

    private final UUID serviceUUID;

    ServiceContext(final UUID queryId, final UUID serviceUUID) {
        if (queryId == null)
            throw new IllegalArgumentException();
        if (serviceUUID == null)
            throw new IllegalArgumentException();
        this.queryId = queryId;
        this.serviceUUID = serviceUUID;
    }

    public int hashCode() {
        return queryId.hashCode() * 31 + serviceUUID.hashCode();
    }

    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (!(o instanceof ServiceContext))
            return false;
        if (!queryId.equals(((ServiceContext) o).queryId))
            return false;
        if (!serviceUUID.equals(((ServiceContext) o).serviceUUID))
            return false;
        return true;
    }

    @Override
    public boolean hasOperatorScope(int bopId) {
        return false;
    }

}