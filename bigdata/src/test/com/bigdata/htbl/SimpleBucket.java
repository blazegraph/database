/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Nov 29, 2010
 */
package com.bigdata.htbl;

import java.util.Iterator;

import org.apache.log4j.Logger;

import com.bigdata.btree.PO;

/**
 * A (very) simple hash bucket. The bucket holds N int32 keys.
 * 
 * @todo There should be transient metadata for the address of the bucket (both
 *       the slot index and the store address).
 * 
 * @todo Extend {@link PO}
 */
class SimpleBucket {// extends PO {

	private final transient static Logger log = Logger
			.getLogger(SimpleExtensibleHashMap.class);

	/**
	 * The #of hash code bits which are in use by this {@link SimpleBucket}.
	 * <p>
	 * Note: There are <code>2^(globalBits-localBits)</code> dictionary entries
	 * which address a given page. Initially, globalBits := 1 and localBits :=
	 * 0. For these values, we have <code>2^(1-0) == 2</code> references to the
	 * initial page of the hash table.
	 * 
	 * @todo If we need to examine this when we change the size of the address
	 *       space then it makes more sense to have this as local metadata in
	 *       the address table than as local data in the bucket (the latter
	 *       would require us to visit each bucket when expanding the address
	 *       space). This only needs to be 4 bits to express values in [0:31].
	 * 
	 * @todo When overflow buckets are chained together, does each bucket have
	 *       {@link #localHashBits}? If they do, then we need to make sure that
	 *       all buckets in the chain are updated. If {@link #localHashBits} is
	 *       only marked on the first bucket in the chain then we need to
	 *       correctly ignore it on overflow buckets.
	 * 
	 * @todo adjusting this dirties the bucket (unless the #of local bits its
	 *       stored in the address table entry, but that increases the in-memory
	 *       burden of the address table).
	 */
	int localHashBits;

	/**
	 * The #of keys stored in the bucket. The keys are stored in a dense array.
	 * For a given {@link #size}, the only indices of the array which have any
	 * data are [0:{@link #size}-1].
	 */
	int size;

	/**
	 * The user data for the bucket.
	 * 
	 * @todo IRaba keys plus IRaba vals, but the encoded representation must
	 *       support out of line keys/values. That means that the IRaba will
	 *       have to have access to the store or ITuple will have to have
	 *       indirection support.
	 */
	final int[] data;

	/**
	 * Human friendly representation.
	 */
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append(getClass().getName()); // @todo super.toString() with PO.
		// sb.append("{addr="+addr);
		sb.append("{localHashBits=" + localHashBits);
		sb.append(",size=" + size);
		sb.append(",values={");
		for (int i = 0; i < size; i++) {
			if (i > 0)
				sb.append(',');
			sb.append(Integer.toString(data[i]));
		}
		sb.append("}}");
		return sb.toString();
	}

	public SimpleBucket(final int localHashBits, final int bucketSize) {

		if (localHashBits < 0 || localHashBits > 32)
			throw new IllegalArgumentException();

		this.localHashBits = localHashBits;

		this.data = new int[bucketSize];

	}

	/**
	 * Return <code>true</code> if the bucket contains the key.
	 * 
	 * @param h
	 *            The hash code of the key.
	 * @param key
	 *            The key.
	 * 
	 * @return <code>true</code> if the key was found in the bucket.
	 * 
	 * @todo passing in the hash code here makes sense when the bucket stores
	 *       the hash values, e.g., if we always do that or if we have an out of
	 *       bucket reference to a raw record because the tuple did not fit in
	 *       the bucket.
	 */
	public boolean contains(final int h, final int key) {

		for (int i = 0; i < size; i++) {

			if (data[i] == key)
				return true;

		}

		return false;

	}

	/**
	 * Insert the key into the bucket (duplicates are allowed). It is an error
	 * if the bucket is full.
	 * 
	 * @param h
	 *            The hash code of the key.
	 * @param key
	 *            The key.
	 * 
	 * @return <code>false</code> iff the bucket must be split.
	 * 
	 * @todo The caller needs to be careful that [h] is the full hash code for
	 *       the key. Normally this is not a problem, but we sometimes wind up
	 *       with masked off hash codes, especially during splits and merges,
	 *       and those must not be passed in here.
	 */
	public boolean insert(final int h, final int key) {

		if (size == data.length) {
			/*
			 * The bucket must be split, potentially recursively.
			 * 
			 * Note: Splits need to be triggered based on information which is
			 * only available to the bucket when it considers the insert of a
			 * specific tuple, including whether the tuple is promoted to a raw
			 * record reference, whether the bucket has deleted tuples which can
			 * be compacted, etc.
			 * 
			 * @todo I need to figure out where the control logic goes to manage
			 * the split. If the bucket handles splits, then we need to pass in
			 * the table reference.
			 */
			return false;
		}

		data[size++] = key;

		return true;

	}

	/**
	 * Delete a tuple having the specified key. If there is more than one such
	 * tuple, then a random tuple having the key is deleted.
	 * 
	 * @param h
	 *            The hash code of the key.
	 * @param key
	 *            The key.
	 * 
	 * @todo return the delete tuple.
	 */
	public boolean delete(final int h, final int key) {

		for (int i = 0; i < size; i++) {

			if (data[i] == key) {

				// #of tuples remaining beyond this point.
				final int length = size - i - 1;

				if (length > 0) {

					// Keep the array dense by copying down by one.
					System.arraycopy(data, i + 1/* srcPos */, data/* dest */,
							i/* destPos */, length);

				}

				size--;

				return true;

			}

		}

		return false;

	}

	/**
	 * The #of entries in the bucket.
	 */
	public int getEntryCount() {

		return size;

	}

	/**
	 * Visit the entries in any order.
	 */
	public Iterator<Integer/* key */> getEntries() {

		return new EntryIterator();

	}

	/**
	 * Visits the entries in the page.
	 */
	private class EntryIterator implements Iterator<Integer> {

		private int current = 0;

		private EntryIterator() {

		}

		@Override
		public boolean hasNext() {
			return current < size;
		}

		@Override
		public Integer next() {
			return data[current++];
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

}
