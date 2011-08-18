package com.bigdata.htree;

import com.bigdata.btree.BTree;
import com.bigdata.htree.data.IDirectoryData;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.rawstore.IRawStore;

/**
 * Implementation maintains Java objects corresponding to the persistent data
 * and defines methods for a variety of mutations on the {@link IDirectoryData}
 * record which operate by direct manipulation of the Java objects.
 * <p>
 * Note: package private fields are used so that they may be directly accessed
 * by the {@link DirectoryPage} class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: MutableNodeData.java 2265 2009-10-26 12:51:06Z thompsonbry $
 * 
 * FIXME Unit tests for this class, including constructors.
 */
public class MutableDirectoryPageData implements IDirectoryData {

    final byte[] overflowKey;
    
	/**
	 * The persistent address of each child page (each child may be either a
	 * {@link DirectoryPage} or a {@link BucketPage}). The capacity of this
	 * array is <code>1&lt;&lt;addressBits</code>. An entry in this array is
	 * {@link #NULL} until the child has been persisted. The protocol for
	 * persisting child nodes requires that we use a pre-order traversal (the
	 * general case is a directed graph) so that we can update the addresses
	 * record in the parent before the parent itself is persisted. Unlike the
	 * {@link BTree}, a {@link IRawStore#NULL} entry is permitted in the array.
	 * This is because the {@link HTree} is not balanced and empty paths may be
	 * folded up.
	 */
	final long[] childAddr;

//	/**
//	 * The #of entries spanned by this node. This value should always be equal
//	 * to the sum of the defined values in {@link #childEntryCounts}.
//	 * <p>
//	 * When a node is split, the value is updated by subtracting off the counts
//	 * for the children that are being moved to the new sibling.
//	 * <p>
//	 * When a node is joined, the value is updated by adding in the counts for
//	 * the children that are being moved to the new sibling.
//	 * <p>
//	 * When a key is redistributed from a node to a sibling, the value is
//	 * updated by subtracting off the count for the child from the source
//	 * sibling and adding it in to this node.
//	 * <p>
//	 * This field is initialized by the various {@link Node} constructors.
//	 */
//	int nentries;

//	/**
//	 * The #of entries spanned by each direct child of this node.
//	 * <p>
//	 * The appropriate element in this array is incremented on all ancestor
//	 * nodes by {@link Leaf#insert(Object, Object)} and decremented on all
//	 * ancestors nodes by {@link Leaf#remove(Object)}. Since the ancestors are
//	 * guaranteed to be mutable as preconditions for those operations we are
//	 * able to traverse the {@link AbstractNode#parent} reference in a straight
//	 * forward manner.
//	 */
//	final int[] childEntryCounts;

	/**
	 * <code>true</code> iff the B+Tree is maintaining per tuple revision
	 * timestamps.
	 */
	final boolean hasVersionTimestamps;

	/**
	 * The minimum tuple revision timestamp for any leaf spanned by this node
	 * IFF the B+Tree is maintaining tuple revision timestamps.
	 */
	long minimumVersionTimestamp;

	/**
	 * The maximum tuple revision timestamp for any leaf spanned by this node
	 * IFF the B+Tree is maintaining tuple revision timestamps.
	 */
	long maximumVersionTimestamp;

	/**
	 * Create an empty mutable data record.
	 * 
	 * @param addressBits
	 *            The #of address bits.
	 * @param hasVersionTimestamps
	 *            <code>true</code> iff the HTree is maintaining per tuple
	 *            version timestamps.
	 */
	public MutableDirectoryPageData(final byte[] overflowKey,
	        final int addressBits, 
			final boolean hasVersionTimestamps) {

//		nentries = 0;

	    this.overflowKey = overflowKey;
	    
		childAddr = new long[1 << addressBits];

//		childEntryCounts = new int[branchingFactor + 1];

		this.hasVersionTimestamps = hasVersionTimestamps;

		minimumVersionTimestamp = maximumVersionTimestamp = 0L;

	}

	/**
	 * Makes a mutable copy of the source data record.
	 * 
	 * @param addressBits
	 *            The #of address bits owning {@link HTree}. This is used to
	 *            initialize the various arrays to the correct capacity.
	 * @param src
	 *            The source data record.
	 */
	public MutableDirectoryPageData(final int addressBits, final IDirectoryData src) {

		if (src == null)
			throw new IllegalArgumentException();

		this.overflowKey = src.getOverflowKey();
		
//		nentries = src.getSpannedTupleCount();

		childAddr = new long[1 << addressBits];

        this.hasVersionTimestamps = src.hasVersionTimestamps();

		copyFrom(src);
		
	}

	void copyFrom(final IDirectoryData src) {

//	     childEntryCounts = new int[branchingFactor + 1];

        final int nchildren = src.getChildCount();

//      int sum = 0;

        for (int i = 0; i < nchildren; i++) {

            childAddr[i] = src.getChildAddr(i);

//          final int tmp = childEntryCounts[i] = src.getChildEntryCount(i);

//          sum += tmp;

        }

        if (src.hasVersionTimestamps()) {

            minimumVersionTimestamp = src.getMinimumVersionTimestamp();

            maximumVersionTimestamp = src.getMaximumVersionTimestamp();

        }

//      assert sum == nentries;

	}
	
	/**
	 * Ctor based on just the "data" -- used by unit tests.
	 * 
	 * @param nentries
	 * @param keys
	 * @param childAddr
	 * @param childEntryCounts
	 */
	public MutableDirectoryPageData(//final int nentries, final IRaba keys,
	        final byte[] overflowKey,//
			final long[] childAddr, //final int[] childEntryCounts,
			final boolean hasVersionTimestamps,
			final long minimumVersionTimestamp,
			final long maximumVersionTimestamp) {

		assert childAddr != null;
//		assert childEntryCounts != null;
//		assert keys.capacity() + 1 == childAddr.length;
//		assert childAddr.length == childEntryCounts.length;

		this.overflowKey = overflowKey;
		
//		this.nentries = nentries;

		this.childAddr = childAddr;

//		this.childEntryCounts = childEntryCounts;

		this.hasVersionTimestamps = hasVersionTimestamps;

		this.minimumVersionTimestamp = minimumVersionTimestamp;

		this.maximumVersionTimestamp = maximumVersionTimestamp;

	}

	/**
	 * No - this is a mutable data record.
	 */
	final public boolean isReadOnly() {

		return false;

	}

	/**
	 * No.
	 */
	final public boolean isCoded() {

		return false;

	}

	final public AbstractFixedByteArrayBuffer data() {

		throw new UnsupportedOperationException();

	}

	public final long getChildAddr(final int index) {

		return childAddr[index];

	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Overridden to report the size of the address space.
	 */
	final public int getChildCount() {

		return childAddr.length;

	}

	final public boolean isOverflowDirectory() {
	    
	    return overflowKey != null;
	    
	}
	
	final public boolean isLeaf() {

		return false;

	}

	final public boolean hasVersionTimestamps() {

		return hasVersionTimestamps;

	}

	final public long getMaximumVersionTimestamp() {

		if (!hasVersionTimestamps)
			throw new UnsupportedOperationException();

		return maximumVersionTimestamp;

	}

	final public long getMinimumVersionTimestamp() {

		if (!hasVersionTimestamps)
			throw new UnsupportedOperationException();

		return minimumVersionTimestamp;

	}

	public byte[] getOverflowKey() {
		return overflowKey;
	}
}
