/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Aug 25, 2009
 */

package com.bigdata.btree;

import it.unimi.dsi.bits.BitVector;

import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.MutableKeyBuffer;
import com.bigdata.btree.raba.MutableValueBuffer;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.rawstore.IRawStore;

/**
 * Implementation maintains Java objects corresponding to the persistent data
 * and defines methods for a variety of mutations on the {@link ILeafData}
 * record which operate by direct manipulation of the Java objects.
 * <p>
 * Note: package private fields are used so that they may be directly accessed
 * by the {@link Leaf} class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo consider mutable implementation based on a compacting record ala GOM.
 */
public class MutableLeafData implements ILeafData {

    /**
     * A representation of each key in the node or leaf. Each key is a variable
     * length unsigned byte[]. There are various implementations of
     * {@link IRaba} that are optimized for mutable and immutable keys.
     * <p>
     * The #of keys depends on whether this is a {@link Node} or a {@link Leaf}.
     * A leaf has one key per value - that is, the maximum #of keys for a leaf
     * is specified by the branching factor. In contrast a node has m-1 keys
     * where m is the maximum #of children (aka the branching factor).
     * <p>
     * For both a {@link Node} and a {@link Leaf}, this array is dimensioned to
     * accept one more key than the maximum capacity so that the key that causes
     * overflow and forces the split may be inserted. This greatly simplifies
     * the logic for computing the split point and performing the split.
     * Therefore you always allocate this object with a capacity <code>m</code>
     * keys for a {@link Node} and <code>m+1</code> keys for a {@link Leaf}.
     * 
     * @see IRaba#search(byte[])
     */
    final MutableKeyBuffer keys;

    /**
     * <p>
     * The values of the tree. There is one value per key for a leaf.
     * </p>
     * <p>
     * This array is dimensioned to one more than the maximum capacity so that
     * the value corresponding to the key that causes overflow and forces the
     * split may be inserted. This greatly simplifies the logic for computing
     * the split point and performing the split.
     * </p>
     */
    final MutableValueBuffer vals;

    /**
     * The deletion markers IFF isolation is supported by the {@link BTree}.
     * 
     * @todo {@link BitVector}? The code in {@link Leaf} which makes changes to
     *       this array would have to be updated.
     */
    final boolean[] deleteMarkers;
    
    /**
     * The version timestamps IFF isolation is supported by the {@link BTree}.
     */
    final long[] versionTimestamps;

    /**
     * The minimum version timestamp.
     * 
     * @todo these fields at 16 bytes to each {@link MutableLeafData} object
     *       even when we do not use them. It would be better to use a subclass
     *       or tack them onto the end of the {@link #versionTimestamps} array.
     */
    long minimumVersionTimestamp;
    long maximumVersionTimestamp;

	/**
	 * Bit markers indicating whether the value associated with the tuple is a
	 * raw record or an inline byte[] stored within {@link #getValues() values
	 * raba}.
     * 
     * @todo {@link BitVector}? The code in {@link Leaf} which makes changes to
     *       this array would have to be updated.
	 */
    final boolean[] rawRecords;

	/**
	 * Create an empty data record with internal arrays dimensioned for the
	 * specified branching factor.
	 * 
	 * @param branchingFactor
	 *            The branching factor for the owning B+Tree.
	 * @param hasVersionTimestamps
	 *            <code>true</code> iff version timestamps will be maintained.
	 * @param hasDeleteMarkers
	 *            <code>true</code> iff delete markers will be maintained.
	 * @param hasRawRecords
	 *            <code>true</code> iff raw record markers will be maintained.
	 */
    public MutableLeafData(final int branchingFactor,
            final boolean hasVersionTimestamps, final boolean hasDeleteMarkers,
            final boolean hasRawRecords) {

        keys = new MutableKeyBuffer(branchingFactor + 1);

        vals = new MutableValueBuffer(branchingFactor + 1);

        versionTimestamps = (hasVersionTimestamps ? new long[branchingFactor + 1]
                : null);

        // init per API specification.
        minimumVersionTimestamp = Long.MAX_VALUE;
        maximumVersionTimestamp = Long.MIN_VALUE;
        
        deleteMarkers = (hasDeleteMarkers ? new boolean[branchingFactor + 1]
                : null);
        
		rawRecords = (hasRawRecords ? new boolean[branchingFactor + 1] : null);

    }

    /**
     * Copy ctor.
     * 
     * @param branchingFactor
     *            The branching factor for the owning B+Tree.
     * @param src
     *            The source leaf.
     */
    public MutableLeafData(final int branchingFactor, final ILeafData src) {

        keys = new MutableKeyBuffer(branchingFactor + 1, src.getKeys());

        vals = new MutableValueBuffer(branchingFactor + 1, src.getValues());

        versionTimestamps = (src.hasVersionTimestamps() ? new long[branchingFactor + 1]
                : null);

        deleteMarkers = (src.hasDeleteMarkers() ? new boolean[branchingFactor + 1]
                                                              : null);

		rawRecords = (src.hasRawRecords() ? new boolean[branchingFactor + 1]
				: null);

        final int nkeys = keys.size();
        
        if (versionTimestamps != null) {

            for (int i = 0; i < nkeys; i++) {

                versionTimestamps[i] = src.getVersionTimestamp(i);
                
            }

            minimumVersionTimestamp = src.getMinimumVersionTimestamp();

            maximumVersionTimestamp = src.getMaximumVersionTimestamp();

        } else {

            minimumVersionTimestamp = Long.MAX_VALUE;

            maximumVersionTimestamp = Long.MIN_VALUE;

        }
        
        if (deleteMarkers != null) {

            for (int i = 0; i < nkeys; i++) {

                deleteMarkers[i] = src.getDeleteMarker(i);
                
            }
            
        }

        if (rawRecords != null) {

            for (int i = 0; i < nkeys; i++) {

				rawRecords[i] = src.getRawRecord(i) != IRawStore.NULL;
                
            }
            
        }

    }

    /**
     * Ctor based on just "data" -- used by unit tests.
     * 
     * @param keys
     *            A representation of the defined keys in the leaf.
     * @param values
     *            An array containing the values found in the leaf.
     * @param versionTimestamps
     *            An array of the version timestamps (iff the version metadata
     *            is being maintained).
     * @param deleteMarkers
     *            An array of the delete markers (iff the version metadata is
     *            being maintained).
     */
    MutableLeafData(final MutableKeyBuffer keys,
            final MutableValueBuffer values, final long[] versionTimestamps,
            final boolean[] deleteMarkers, final boolean[] rawRecords) {
        
        assert keys != null;
        assert values != null;
        assert keys.capacity() == values.capacity();
        if (versionTimestamps != null) {
            assert versionTimestamps.length == keys.capacity();
        }
        if (deleteMarkers != null) {
            assert deleteMarkers.length == keys.capacity();
        }
        if (rawRecords != null) {
            assert rawRecords.length == keys.capacity();
        }

        this.keys = keys;
        this.vals = values;
        this.versionTimestamps = versionTimestamps;
        this.deleteMarkers = deleteMarkers;
        this.rawRecords = rawRecords;

        if (versionTimestamps != null)
            recalcMinMaxVersionTimestamp();

    }
    
    /**
     * Range check a tuple index.
     * 
     * @param index
     *            The index of a tuple in [0:nkeys].
     * @return <code>true</code>
     * 
     * @throws IndexOutOfBoundsException
     *             if the index is not in the legal range.
     */
    final protected boolean rangeCheckTupleIndex(final int index) {
        
        if (index < 0 || index > getKeys().size())
            throw new IndexOutOfBoundsException();

        return true;
        
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

    public final long getVersionTimestamp(final int index) {

        if (versionTimestamps == null)
            throw new UnsupportedOperationException();

        assert rangeCheckTupleIndex(index);
        
        return versionTimestamps[index];

    }
    
    final public long getMinimumVersionTimestamp() {

        if (versionTimestamps == null)
            throw new UnsupportedOperationException();

        return minimumVersionTimestamp;

    }
    
    final public long getMaximumVersionTimestamp() {

        if (versionTimestamps == null)
            throw new UnsupportedOperationException();

        return maximumVersionTimestamp;

    }

    public final boolean getDeleteMarker(final int index) {

        if (deleteMarkers == null)
            throw new UnsupportedOperationException();

        assert rangeCheckTupleIndex(index);

        return deleteMarkers[index];

    }

    public final long getRawRecord(final int index) {

        if (rawRecords == null)
            throw new UnsupportedOperationException();

        assert rangeCheckTupleIndex(index);

		if (!rawRecords[index])
			return IRawStore.NULL;

		final byte[] val = vals.get(index);
		
		final long addr = AbstractBTree.decodeRecordAddr(val);
		
		return addr;
		
    }

    final public IRaba getValues() {
        
        return vals;
        
    }
    
    final public IRaba getKeys() {
        
        return keys;
        
    }
    
    /**
     * Always returns <code>true</code>.
     */
    final public boolean isLeaf() {

        return true;

    }

    /**
     * For a leaf the #of entries is always the #of keys.
     */
    final public int getSpannedTupleCount() {
        
        return getKeys().size();
        
    }
    
    final public int getValueCount() {
        
        return vals.size();
        
    }
    
    final public boolean hasRawRecords() {
        
        return rawRecords != null; 
        
    }
    
    final public boolean hasDeleteMarkers() {
        
        return deleteMarkers != null; 
        
    }
    
    final public boolean hasVersionTimestamps() {
        
        return versionTimestamps != null; 
        
    }

    final public int getKeyCount() {
        
        return keys.size();
        
    }

    /**
     * No - this class does not support double-linked leaves (only the
     * {@link IndexSegment} actually uses double-linked leaves).
     */
    final public boolean isDoubleLinked() {
     
        return false;

    }

    final public long getNextAddr() {
    
        throw new UnsupportedOperationException();
        
    }

    final public long getPriorAddr() {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Recalculate the min/max version timestamp on the leaf. The caller is
     * responsible for propagating the new min/max to the ancestors of the leaf.
     * 
     * @throws UnsupportedOperationException
     *             if the leaf is not maintaining per-tuple version timestamps.
     */
    void recalcMinMaxVersionTimestamp() {

        // must be maintaining version timestamps.
        if (versionTimestamps == null)
            throw new UnsupportedOperationException();

        final int nkeys = keys.nkeys;

        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;

        for (int i = 0; i < nkeys; i++) {

            final long t = versionTimestamps[i];

            if (t < min)
                min = t;

            if (t > max)
                max = t;

        }

        minimumVersionTimestamp = min;
        maximumVersionTimestamp = max;

    }

}
