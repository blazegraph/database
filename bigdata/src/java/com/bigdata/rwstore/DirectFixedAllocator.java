package com.bigdata.rwstore;

/**
 * The DirectFixedAllocator is used to manage in-memory Direct ByteBuffer
 * allocated memory.
 *
 */
public class DirectFixedAllocator extends FixedAllocator {

	DirectFixedAllocator(RWStore store, int size) {
		super(store, size);
	}

	protected int grabAllocation(RWStore store, int blockSize) {
		return store.allocateDirect(blockSize);
	}

}
