package com.bigdata.htree;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.raba.IRaba;

/**
 * Visits the non-empty tuples in each {@link BucketPage} visited by the
 * source iterator.
 * 
 * TODO This might be reworked as an expander and an iterator visiting the
 * tuples on a single bucket page.  That could provide more reuse.  See
 * {@link BucketPage#tuples()}
 */
class BucketPageTupleIterator<E> implements
		ITupleIterator<E> {

	private final Iterator<BucketPage> src;
	private BucketPage currentBucketPage = null;
	private int nextNonEmptySlot = 0;

	private final Tuple<E> tuple;

	BucketPageTupleIterator(final AbstractHTree htree, final int flags,
			final Iterator<BucketPage> src) {

		if (htree == null)
			throw new IllegalArgumentException();

		if(src == null)
			throw new IllegalArgumentException();
		
		this.tuple = new Tuple<E>(htree,flags);
		
		this.src = src;
		
	}
	
	/**
	 * Scan to the next non-empty slot in the current {@link BucketPage}.
	 * 
	 * @return <code>true</code> iff there is a non-empty slot on the
	 *         current {@link BucketPage}.
	 */
	private boolean findNextSlot() {
		if (currentBucketPage == null)
			throw new IllegalStateException();
		final IRaba keys = currentBucketPage.getKeys();
		final int size = keys.size();
		for (; nextNonEmptySlot < size; nextNonEmptySlot++) {
			if (keys.isNull(nextNonEmptySlot))
				continue;
			return true;
		}
		// The current page is exhausted.  We need to fetch another page.
		currentBucketPage = null;
		
		return false;
	}
	
	public boolean hasNext() {
		if(currentBucketPage != null) {
			return true;
		}
		// Scan for the next bucket page having a visitable tuple.
		while(src.hasNext()) {
			currentBucketPage = src.next();
			nextNonEmptySlot = 0;
			if (findNextSlot())
				return true;
		}
		return false;
	}

	public ITuple<E> next() {
		if (!hasNext())
			throw new NoSuchElementException();
		// Copy the data for the current tuple into the Tuple buffer.
		tuple.copy(nextNonEmptySlot, currentBucketPage);
		/*
		 * Advance to the next slot on the current page. if there is non,
		 * then the current page reference will be cleared and we will need
		 * to fetch a new page in hasNext() on the next invocation.
		 */
		nextNonEmptySlot++; // skip past the current tuple.
		findNextSlot(); // find the next non-null slot (next tuple).
		// Return the Tuple buffer.
		return tuple;
	}
	
	public void remove() {
		throw new UnsupportedOperationException();
	}

}