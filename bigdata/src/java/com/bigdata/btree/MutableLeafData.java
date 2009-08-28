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
     * where m is the maximum #of children (aka the branching factor). Therefore
     * this field is initialized by the {@link Leaf} or {@link Node} - NOT by
     * the {@link AbstractNode}.
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
    final MutableValueBuffer values;

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
     * Create an empty data record with internal arrays dimensioned for the
     * specified branching factor.
     * 
     * @param branchingFactor
     *            The branching factor for the owning B+Tree.
     * @param versionTimestamps
     *            <code>true</code> iff version timestamps will be maintained.
     * @param deleteMarkers
     *            <code>true</code> iff delete markers will be maintained.
     */
    public MutableLeafData(final int branchingFactor,
            final boolean versionTimestamps, final boolean deleteMarkers) {

        keys = new MutableKeyBuffer(branchingFactor + 1);

        values = new MutableValueBuffer(0/* size */,
                new byte[branchingFactor + 1][]);

        this.versionTimestamps = (versionTimestamps ? new long[branchingFactor + 1]
                : null);

        this.deleteMarkers = (deleteMarkers ? new boolean[branchingFactor + 1]
                : null);

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

        keys = new MutableKeyBuffer(branchingFactor, src.getKeys());

        values = new MutableValueBuffer(branchingFactor, src.getValues());

        this.versionTimestamps = (src.hasVersionTimestamps() ? new long[branchingFactor + 1]
                : null);

        this.deleteMarkers = (src.hasDeleteMarkers() ? new boolean[branchingFactor + 1]
                : null);
        
        final int nkeys = keys.size();
        
        if (versionTimestamps != null) {

            for (int i = 0; i < nkeys; i++) {

                versionTimestamps[i] = src.getVersionTimestamp(i);
                
            }
            
        }
        
        if (deleteMarkers != null) {

            for (int i = 0; i < nkeys; i++) {

                deleteMarkers[i] = src.getDeleteMarker(i);
                
            }
            
        }

    }

    /**
     * @param keys
     *            A representation of the defined keys in the node.
     * @param values
     *            An array containing the values found in the leaf.
     * @param versionTimestamps
     *            An array of the version timestamps (iff the version metadata
     *            is being maintained).
     * @param deleteMarkers
     *            An array of the delete markers (iff the version metadata is
     *            being maintained).
     */
    public MutableLeafData(final MutableKeyBuffer keys,
            final MutableValueBuffer values, final long[] versionTimestamps,
            final boolean[] deleteMarkers) {
        
        assert keys != null;
        assert values != null;
        assert keys.capacity() == values.capacity();
        if (versionTimestamps != null) {
            assert versionTimestamps.length == keys.capacity();
        }
        if (deleteMarkers != null) {
            assert deleteMarkers.length == keys.capacity();
        }

        this.keys = keys;
        this.values = values;
        this.versionTimestamps = versionTimestamps;
        this.deleteMarkers = deleteMarkers;
        
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

    public final long getVersionTimestamp(final int index) {

        if (versionTimestamps == null)
            throw new UnsupportedOperationException();

        assert rangeCheckTupleIndex(index);
        
        return versionTimestamps[index];

    }
    
    public final boolean getDeleteMarker(final int index) {

        if (deleteMarkers == null)
            throw new UnsupportedOperationException();

        assert rangeCheckTupleIndex(index);

        return deleteMarkers[index];

    }

    final public IRaba getValues() {
        
        return values;
        
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
        
        return values.size();
        
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

}
