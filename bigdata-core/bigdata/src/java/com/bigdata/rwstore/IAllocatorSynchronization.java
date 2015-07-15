package com.bigdata.rwstore;

import com.bigdata.io.writecache.WriteCache.FileChannelScatteredWriteCache;

/**
 * This is a callback interface that can be used to synchronize store allocation
 * structures.
 * 
 * It has been conceived to allow two {@link RWStore}s to maintain the in-memory
 * {@link FixedAllocator}s w/o the need to initialize from the root block on
 * each commit.
 * 
 * The implementing instance would be passed to a
 * {@link FileChannelScatteredWriteCache} and the callbacks made from
 * resetRecordMapFromBuffer.
 * 
 * @author Martyn Cutcher
 */
public interface IAllocatorSynchronization {

    /**
     * The address has been allocated on the leader.
     * 
     * @param latchedAddr
     *            The latched address.
     * @param size
     *            The size of the allocation in bytes.
     */
    void addAddress(int latchedAddr, int size);

    /**
     * The address has been deleted on the leader.
     * 
     * @param latchedAddr
     *            The latched address.
     */
    void removeAddress(int latchedAddr);

}
