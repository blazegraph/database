package com.bigdata.htree.data;

import it.unimi.dsi.fastutil.Hash;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.bigdata.btree.data.MockLeafData;
import com.bigdata.btree.raba.IRaba;

/**
 * Mock object for a hash bucket.
 */
public class MockBucketData extends MockLeafData implements IBucketData {

	private final int lengthMSB;
	private final int[] hashCodes;

	/**
	 * 
	 * @param keys
	 * @param vals
	 * @param lengthMSB
	 *            The bit length of the MSB prefix for the hash bucket.
	 * @param hashCodes
	 *            The int32 hash codes for each key in the hash bucket.
	 */
	public MockBucketData(final IRaba keys, final IRaba vals,
			final int lengthMSB, final int[] hashCodes) {

		this(keys, vals, null/* deleteMarkers */, null/* versionTimestamps */,
				lengthMSB, hashCodes);

	}

	/**
	 * 
	 * @param keys
	 * @param vals
	 * @param deleteMarkers
	 * @param versionTimestamps
	 * @param lengthMSB
	 *            The bit length of the MSB prefix for the hash bucket.
	 * @param hashCodes
	 *            The int32 hash codes for each key in the hash bucket.
	 */
	public MockBucketData(final IRaba keys, final IRaba vals,
			final boolean[] deleteMarkers, final long[] versionTimestamps,
			final int lengthMSB, final int[] hashCodes) {

		super(keys, vals, deleteMarkers, versionTimestamps);

		if (lengthMSB < 0 || lengthMSB > 32)
			throw new IllegalArgumentException();

		if (hashCodes == null)
			throw new IllegalArgumentException();

		if (hashCodes.length != keys.size())
			throw new IllegalArgumentException();

		this.lengthMSB = lengthMSB;

		this.hashCodes = hashCodes;

    }

	public int getLengthMSB() {

		return lengthMSB;

	}

	public int getHash(final int index) {

		return hashCodes[index];

	}

	public Iterator<Integer> hashIterator(final int h) {
		
		return new HashMatchIterator(h);
		
	}

	/**
	 * Visits the index of each bucket entry having a matching hash code.
	 */
	private class HashMatchIterator implements Iterator<Integer> {
		
		private final int h;
		private int currentIndex = 0;
		private Integer nextResult = null;

		private HashMatchIterator(final int h) {
			this.h = h;
		}

		public boolean hasNext() {
			final int n = getKeyCount();
			while (nextResult == null && currentIndex < n) {
				final int index = currentIndex++;
				final int h1 = getHash(index);
				if (h1 == h) {
					nextResult = Integer.valueOf(index);
					break;
				}
			}
			return nextResult != null;
		}

		public Integer next() {
			
			if (!hasNext())
				throw new NoSuchElementException();
			
			final Integer tmp = nextResult;
			
			nextResult = null;
			
			return tmp;
			
		}

		public void remove() {
			
			throw new UnsupportedOperationException();
			
		}

	}

}
