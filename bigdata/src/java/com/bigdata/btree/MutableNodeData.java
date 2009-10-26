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

import com.bigdata.btree.data.INodeData;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.MutableKeyBuffer;
import com.bigdata.io.AbstractFixedByteArrayBuffer;

/**
 * Implementation maintains Java objects corresponding to the persistent data
 * and defines methods for a variety of mutations on the {@link INodeData}
 * record which operate by direct manipulation of the Java objects.
 * <p>
 * Note: package private fields are used so that they may be directly accessed
 * by the {@link Node} class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MutableNodeData implements INodeData {

    /**
     * A representation of each key in the node. Each key is a variable length
     * unsigned byte[].
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
     * @see Node#findChild(int searchKeyOffset, byte[] searchKey)
     * @see IRaba#search(byte[])
     */
    final MutableKeyBuffer keys;

    /**
     * <p>
     * The persistent address of each child node (may be nodes or leaves). The
     * capacity of this array is m, where m is the {@link #branchingFactor}.
     * Valid indices are in [0:nkeys+1] since nchildren := nkeys+1 for a
     * {@link Node}. The key is {@link #NULL} until the child has been
     * persisted. The protocol for persisting child nodes requires that we use a
     * pre-order traversal (the general case is a directed graph) so that we can
     * update the keys on the parent before the parent itself is persisted.
     * </p>
     * <p>
     * Note: It is an error if there is an attempt to serialize a node having a
     * null entry in this array and a non-null entry in the {@link #keys} array.
     * </p>
     * <p>
     * This array is dimensioned to one more than the maximum capacity so that
     * the child reference corresponding to the key that causes overflow and
     * forces the split may be inserted. This greatly simplifies the logic for
     * computing the split point and performing the split.
     * </p>
     */
    final long[] childAddr;

    /**
     * The #of entries spanned by this node. This value should always be equal
     * to the sum of the defined values in {@link #childEntryCounts}.
     * <p>
     * When a node is split, the value is updated by subtracting off the counts
     * for the children that are being moved to the new sibling.
     * <p>
     * When a node is joined, the value is updated by adding in the counts for
     * the children that are being moved to the new sibling.
     * <p>
     * When a key is redistributed from a node to a sibling, the value is
     * updated by subtracting off the count for the child from the source
     * sibling and adding it in to this node.
     * <p>
     * This field is initialized by the various {@link Node} constructors.
     */
    int nentries;

    /**
     * The #of entries spanned by each direct child of this node.
     * <p>
     * The appropriate element in this array is incremented on all ancestor
     * nodes by {@link Leaf#insert(Object, Object)} and decremented on all
     * ancestors nodes by {@link Leaf#remove(Object)}. Since the ancestors are
     * guaranteed to be mutable as preconditions for those operations we are
     * able to traverse the {@link AbstractNode#parent} reference in a straight
     * forward manner.
     */
    final int[] childEntryCounts;

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
     * @param branchingFactor
     *            The branching factor for the owning B+Tree. This is used to
     *            initialize the various arrays to the correct capacity.
     * @param hasVersionTimestamps
     *            <code>true</code> iff the B+Tree is maintaining per tuple
     *            version timestamps.
     */
    public MutableNodeData(final int branchingFactor,
            final boolean hasVersionTimestamps) {

        nentries = 0;

        keys = new MutableKeyBuffer(branchingFactor);

        childAddr = new long[branchingFactor + 1];

        childEntryCounts = new int[branchingFactor + 1];

        this.hasVersionTimestamps = hasVersionTimestamps;
        
        minimumVersionTimestamp = maximumVersionTimestamp = 0L;

    }

    /**
     * Makes a mutable copy of the source data record.
     * 
     * @param branchingFactor
     *            The branching factor for the owning B+Tree. This is used to
     *            initialize the various arrays to the correct capacity.
     * @param src
     *            The source data record.
     */
    public MutableNodeData(final int branchingFactor, final INodeData src) {

        if (src == null)
            throw new IllegalArgumentException();

        keys = new MutableKeyBuffer(branchingFactor, src.getKeys());
        
        nentries = src.getSpannedTupleCount();
        
        childAddr = new long[branchingFactor + 1];

        childEntryCounts = new int[branchingFactor + 1];
        
        final int nkeys = keys.size();

        int sum = 0;
        
        for (int i = 0; i <= nkeys; i++) {

            childAddr[i] = src.getChildAddr(i);

            final int tmp = childEntryCounts[i] = src.getChildEntryCount(i);

            sum += tmp;
            
        }

        this.hasVersionTimestamps = src.hasVersionTimestamps();
        
        if(src.hasVersionTimestamps()) {
        
            minimumVersionTimestamp = src.getMinimumVersionTimestamp();
            
            maximumVersionTimestamp = src.getMaximumVersionTimestamp();
            
        }
        
        assert sum == nentries;

    }

    /**
     * Ctor based on just the "data" -- used by unit tests.
     *  
     * @param nentries
     * @param keys
     * @param childAddr
     * @param childEntryCounts
     */
    public MutableNodeData(final int nentries, final IRaba keys,
            final long[] childAddr, final int[] childEntryCounts,
            final boolean hasVersionTimestamps,
            final long minimumVersionTimestamp,
            final long maximumVersionTimestamp) {

        assert keys != null;
        assert childAddr != null;
        assert childEntryCounts != null;
        assert keys.capacity() + 1 == childAddr.length;
        assert childAddr.length == childEntryCounts.length;
        
        this.nentries = nentries;
        
        this.keys = (MutableKeyBuffer) keys;
        
        this.childAddr = childAddr;
        
        this.childEntryCounts = childEntryCounts;

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

    public final int getSpannedTupleCount() {
        
        return nentries;
        
    }

    /**
     * Range check a child index.
     * 
     * @param index
     *            The index of a child in [0:nkeys+1].
     * @return <code>true</code>
     * 
     * @throws IndexOutOfBoundsException
     *             if the index is not in the legal range.
     */
    final protected boolean rangeCheckChildIndex(final int index) {
        
        if (index < 0 || index > getKeys().size() + 1)
            throw new IndexOutOfBoundsException();

        return true;
        
    }

    public final long getChildAddr(final int index) {

        assert rangeCheckChildIndex(index);
        
        return childAddr[index];

    }

    final public int getChildEntryCount(final int index) {

        assert rangeCheckChildIndex(index);
        
        return childEntryCounts[index];
        
    }

    final public int getChildCount() {

        return getKeys().size() + 1;

    }

    final public int getKeyCount() {

        return keys.size();

    }

    final public IRaba getKeys() {

        return keys;

    }

    final public boolean isLeaf() {

        return false;

    }

    final public boolean hasVersionTimestamps() {
        
        return hasVersionTimestamps;
        
    }

    final public long getMaximumVersionTimestamp() {

        if(!hasVersionTimestamps)
            throw new UnsupportedOperationException();
        
        return maximumVersionTimestamp;
        
    }

    final public long getMinimumVersionTimestamp() {
        
        if(!hasVersionTimestamps)
            throw new UnsupportedOperationException();
        
        return minimumVersionTimestamp;
        
    }

}
