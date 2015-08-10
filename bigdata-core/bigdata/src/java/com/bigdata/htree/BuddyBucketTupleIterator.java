package com.bigdata.htree;

import java.util.NoSuchElementException;

import com.bigdata.btree.AbstractTuple;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.LeafTupleIterator;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.util.BytesUtil;

/**
 * Iterator visits all tuples in a buddy bucket having the desired key. 
 * <p>
 * Note: This implementation is NOT thread-safe.
 */
class BuddyBucketTupleIterator<E> implements
		ITupleIterator<E> {

	/** The key. */
	private final byte[] key;
	/** The bucket. */
	private final BucketPage bucket;
//		/** The index of the first slot in the buddy bucket. */
//		private final int buddyOffset;
	/** The index of the last slot in the buddy bucket. */
	private final int lastSlot;
	/** The index of the next slot to be visited. This is set by the
	 * constructor to the first slot and the tuple at that slot is pre-fetched.
	 */
	private int index;
    private int lastVisited = -1;
    private final AbstractTuple<E> tuple;

    public BuddyBucketTupleIterator(final byte[] key, final BucketPage bucket) {//, final int buddyOffset) {

		if (key == null)
			throw new IllegalArgumentException();

		if (bucket == null)
			throw new IllegalArgumentException();

		this.key = key;
		this.bucket = bucket;

//		// #of slots on the page.
//		final int slotsOnPage = bucket.slotsOnPage();

		/*
		 * The index of the last slot in the buddy bucket.
		 * 
		 * Note: The bucket page is not divided into buddy buckets, so we want
		 * to start at the first slot (index zero) and scan until the last slot
		 * (slotsOnPage-1).
		 * 
		 * Note: Optimized (assumes tuples in the bucket page are known to be
		 * dense (no gaps), in which case we do not have to scan more than
		 * bucket.data.getKeyCount() slots).
		 */
		lastSlot = bucket.getKeys().size();//slotsOnPage;

		// Lookup first slot to test.
		index = bucket.lookupIndex(key);

		tuple = new Tuple<E>(bucket.htree, IRangeQuery.DEFAULT);
		
	}
	
    /**
     * Examines the entry at {@link #index}. If it passes the criteria for an
     * entry to visit then return true. Otherwise increment the {@link #index}
     * until either all entries in this leaf have been exhausted -or- the an
     * entry is identified that passes the various criteria.
     */
    public boolean hasNext() {
    	
    	if (index == -1) {
    		return false;
    	}

		final IRaba keys = bucket.getKeys();
		
		assert keys.size() == lastSlot;
		assert keys.size() <= keys.capacity();

		for( ; index < lastSlot; index++) {
         
//	            /*
//	             * TODO Skip deleted entries unless specifically requested.
//	             */
//	            if (hasDeleteMarkers && !visitDeleted
//	                    && leaf.getDeleteMarker(index)) {
//
//	                // skipping a deleted version.
//	                continue;
//	                
//	            }

				if (!keys.isNull(index)) {
					if (BytesUtil.bytesEqual(key, keys.get(index))) {
						// entry @ index is next to visit.
						return true;
					}
				}
			
			// if key doesn't match terminate early since the keys are sorted
			index = lastSlot;

		} // next index.

        // nothing left to visit in this buddy bucket.
        return false;
        
    }

    public ITuple<E> next() {

        if (!hasNext()) {

            throw new NoSuchElementException();

        }

        lastVisited = index++;

        tuple.copy(lastVisited, bucket);
        
        return tuple;
        
    }

	/**
	 * Operation is not supported.
	 * 
	 * TODO ITupleCursor and delete-behind are two ways to achieve this. See
	 * {@link LeafTupleIterator#remove()}. I also did a listener based
	 * iterator for GOM which supports concurrent mutation (as long as you
	 * obey the thread safety for the API).
	 */
    public void remove() {

    	throw new UnsupportedOperationException();
    	
	}

}
