package com.bigdata.htree.data;

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

}
