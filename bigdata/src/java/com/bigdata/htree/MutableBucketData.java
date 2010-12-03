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

package com.bigdata.htree;

import java.util.Iterator;

import cern.colt.map.OpenIntIntHashMap;

import com.bigdata.btree.ITuple;
import com.bigdata.btree.MutableLeafData;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.htree.data.IBucketData;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.IDataRecord;
import com.bigdata.rawstore.Bytes;

/**
 * Implementation maintains Java objects corresponding to the persistent data
 * and defines methods for a variety of mutations on the {@link IBucketData}
 * record which operate by direct manipulation of the Java objects.
 * <p>
 * Note: package private fields are used so that they may be directly accessed
 * by the {@link HashBucket} class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: MutableLeafData.java 2265 2009-10-26 12:51:06Z thompsonbry $
 * 
 * @todo Consider mutable implementation based on a compacting record ala GOM.
 *       This is especially attractive for the hash tree. The implementation
 *       would have to be wholly different from the {@link MutableLeafData}
 *       class. Instead of managing the {@link IRaba} for the keys and values
 *       separately, each {@link ITuple} would be appended into a byte[] (or
 *       {@link IDataRecord}). There would be a budget for that backing buffer
 *       which is the maximum in-memory size of the bucket. An index would
 *       provide random access into the buffer for only those entries which are
 *       "live" and a counter is maintain of the #of entries in the buffer which
 *       are no longer in use. When the buffer capacity is reached, the buffer
 *       is compacted by copying all of the entries accessible from the index
 *       into a new buffer and the old buffer is released.
 *       <p>
 *       Records which are too large for the buffer should be moved out of line.
 *       <p>
 *       This can be used in combination with a dynamically maintained trie for
 *       fast hash lookup, or we could just scan the entries.
 *       <p>
 *       Lexicon key search can scan the entries using the index. Scanning can
 *       have a side-effect in which the position of the entry offsets in the
 *       index is swapped if the keys are out of order. This would give us
 *       MonetDB style "cracking". The keys would have to be put into full order
 *       no later than when the record is made persistent.
 *       <p>
 *       Even though mutation is not thread safe, compacting the data record
 *       must not cause the assignment of indices to tuples to change when the
 *       caller is iterating through the entries by index.
 * 
 * @todo When the record is serialized, do we need to allow a decision function
 *       to examine the record and decide whether it must be split? Since we do
 *       not have a fixed target for the page size, but only a budget, and since
 *       compression of keys, values, metadata, and the encoded record can all
 *       be applied, it seems that the decision function should be in terms of
 *       the buffer budget and a maximum #of entries (e.g., B+Tree branching
 *       factor or an equivalent hash bucket threshold).
 */
public class MutableBucketData implements IBucketData {

	private IDataRecord buf;

	private /*@todo final*/ OpenIntIntHashMap index;
	
	/**
	 * Constructor used when converting a persistent data record into a mutable
	 * one.
	 * 
	 * @param data
	 */
	public MutableBucketData(final IBucketData data) {
		
	}

	/**
	 * 
	 * @param bufferSize
	 *            The initial size of the backing byte[].
	 * @param branchingFactor
	 *            The maximum #of tuples which may be stored in the data record.
	 * 
	 * @todo The buffer must be permitted grow until it is sufficient to encode
	 *       approximately one page worth of tuples.
	 *       <p>
	 *       is typically on the order of the size of a page, e.g., 4k. Since
	 *       the data record will be encoded and possible compressed before it
	 *       is written onto the store, this can be larger than the target.
	 *       <p>
	 *       In order to avoid problems where the objects are much smaller than
	 *       expected, we should allow the backing buffer to grow or we should
	 *       fit a model which estimates the size of the resulting page based on
	 *       the size of the buffer and then grow the buffer until we can
	 *       satisfy the target page size.
	 */
	public MutableBucketData(final int bufferSize, final int branchingFactor) {

		final int initialBufferSize = Math
				.min(Bytes.kilobyte32 * 4, bufferSize);

		buf = new ByteArrayBuffer(initialBufferSize);

		index = new OpenIntIntHashMap(branchingFactor/* initialCapacity */);

	}
	
	@Override
	public int getHash(int index) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getKeyCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getLengthMSB() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Iterator<Integer> hashIterator(int h) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean getDeleteMarker(int index) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public long getNextAddr() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getPriorAddr() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getValueCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public IRaba getValues() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getVersionTimestamp(int index) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean hasDeleteMarkers() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean hasVersionTimestamps() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isDoubleLinked() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public AbstractFixedByteArrayBuffer data() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IRaba getKeys() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getMaximumVersionTimestamp() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getMinimumVersionTimestamp() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getSpannedTupleCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isCoded() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isLeaf() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isReadOnly() {
		// TODO Auto-generated method stub
		return false;
	}
	
//	/**
//	 * The keys for the entries in the bucket. Unlike a B+Tree, the keys are NOT
//	 * maintained in a sorted order. Search proceeds by scanning for matching
//	 * hash codes and filtering for matching keys.
//	 */
//    final MutableKeyBuffer keys;
//
//	/**
//	 * The values for the entries in the bucket. There is one value per key. The
//	 * value MAY be null.
//	 */
//    final MutableValueBuffer vals;
//
//    /**
//     * The deletion markers IFF isolation is supported by the {@link HTree}.
//     */
//    final boolean[] deleteMarkers;
//    
//    /**
//     * The version timestamps IFF isolation is supported by the {@link HTree}.
//     */
//    final long[] versionTimestamps;
//
//    /**
//     * The minimum version timestamp.
//     * 
//     * @todo these fields add 16 bytes to each {@link MutableBucketData} object
//     *       even when we do not use them. It would be better to use a subclass
//     *       or tack them onto the end of the {@link #versionTimestamps} array.
//     */
//    long minimumVersionTimestamp;
//    long maximumVersionTimestamp;
//
//	/**
//	 * Create an empty data record with internal arrays dimensioned for the
//	 * specified branching factor.
//	 * 
//	 * @param branchingFactor
//	 *            The maximum #of entries in the hash bucket before it will
//	 *            overflow or be split.  Since the goal is to manage the size
//	 *            of the bucket on the disk and since we do not known the size
//	 *            of the bucket's data record until it is being evicted, this
//	 *            value places an upper bound after which the bucket will be
//	 * @param hasVersionTimestamps
//	 *            <code>true</code> iff version timestamps will be maintained.
//	 * @param hasDeleteMarkers
//	 *            <code>true</code> iff delete markers will be maintained.
//	 */
//    public MutableBucketData(final int branchingFactor,
//            final boolean hasVersionTimestamps, final boolean hasDeleteMarkers) {
//
//        keys = new MutableKeyBuffer(branchingFactor + 1);
//
//        vals = new MutableValueBuffer(branchingFactor + 1);
//
//        versionTimestamps = (hasVersionTimestamps ? new long[branchingFactor + 1]
//                : null);
//
//        // init per API specification.
//        minimumVersionTimestamp = Long.MAX_VALUE;
//        maximumVersionTimestamp = Long.MIN_VALUE;
//        
//        deleteMarkers = (hasDeleteMarkers ? new boolean[branchingFactor + 1]
//                : null);
//
//    }
//
//    /**
//     * Copy ctor.
//     * 
//     * @param branchingFactor
//     *            The branching factor for the owning B+Tree.
//     * @param src
//     *            The source leaf.
//     */
//    public MutableBucketData(final int branchingFactor, final ILeafData src) {
//
//        keys = new MutableKeyBuffer(branchingFactor + 1, src.getKeys());
//
//        vals = new MutableValueBuffer(branchingFactor + 1, src.getValues());
//
//        versionTimestamps = (src.hasVersionTimestamps() ? new long[branchingFactor + 1]
//                : null);
//
//        deleteMarkers = (src.hasDeleteMarkers() ? new boolean[branchingFactor + 1]
//                : null);
//
//        final int nkeys = keys.size();
//        
//        if (versionTimestamps != null) {
//
//            for (int i = 0; i < nkeys; i++) {
//
//                versionTimestamps[i] = src.getVersionTimestamp(i);
//                
//            }
//
//            minimumVersionTimestamp = src.getMinimumVersionTimestamp();
//
//            maximumVersionTimestamp = src.getMaximumVersionTimestamp();
//
//        } else {
//
//            minimumVersionTimestamp = Long.MAX_VALUE;
//
//            maximumVersionTimestamp = Long.MIN_VALUE;
//
//
//        }
//        
//        if (deleteMarkers != null) {
//
//            for (int i = 0; i < nkeys; i++) {
//
//                deleteMarkers[i] = src.getDeleteMarker(i);
//                
//            }
//            
//        }
//
//    }
//
//    /**
//     * Ctor based on just "data" -- used by unit tests.
//     * 
//     * @param keys
//     *            A representation of the defined keys in the leaf.
//     * @param values
//     *            An array containing the values found in the leaf.
//     * @param versionTimestamps
//     *            An array of the version timestamps (iff the version metadata
//     *            is being maintained).
//     * @param deleteMarkers
//     *            An array of the delete markers (iff the version metadata is
//     *            being maintained).
//     */
//    public MutableBucketData(final MutableKeyBuffer keys,
//            final MutableValueBuffer values, final long[] versionTimestamps,
//            final boolean[] deleteMarkers) {
//        
//        assert keys != null;
//        assert values != null;
//        assert keys.capacity() == values.capacity();
//        if (versionTimestamps != null) {
//            assert versionTimestamps.length == keys.capacity();
//        }
//        if (deleteMarkers != null) {
//            assert deleteMarkers.length == keys.capacity();
//        }
//
//        this.keys = keys;
//        this.vals = values;
//        this.versionTimestamps = versionTimestamps;
//        this.deleteMarkers = deleteMarkers;
//
//        if (versionTimestamps != null)
//            recalcMinMaxVersionTimestamp();
//
//    }
//    
//    /**
//     * Range check a tuple index.
//     * 
//     * @param index
//     *            The index of a tuple in [0:nkeys].
//     * @return <code>true</code>
//     * 
//     * @throws IndexOutOfBoundsException
//     *             if the index is not in the legal range.
//     */
//    final protected boolean rangeCheckTupleIndex(final int index) {
//        
//        if (index < 0 || index > getKeys().size())
//            throw new IndexOutOfBoundsException();
//
//        return true;
//        
//    }
//    
//    /**
//     * No - this is a mutable data record.
//     */
//    final public boolean isReadOnly() {
//        
//        return false;
//        
//    }
//
//    /**
//     * No.
//     */
//    final public boolean isCoded() {
//        
//        return false;
//        
//    }
//    
//    final public AbstractFixedByteArrayBuffer data() {
//        
//        throw new UnsupportedOperationException();
//        
//    }
//
//    public final long getVersionTimestamp(final int index) {
//
//        if (versionTimestamps == null)
//            throw new UnsupportedOperationException();
//
//        assert rangeCheckTupleIndex(index);
//        
//        return versionTimestamps[index];
//
//    }
//    
//    final public long getMinimumVersionTimestamp() {
//
//        if (versionTimestamps == null)
//            throw new UnsupportedOperationException();
//
//        return minimumVersionTimestamp;
//
//    }
//    
//    final public long getMaximumVersionTimestamp() {
//
//        if (versionTimestamps == null)
//            throw new UnsupportedOperationException();
//
//        return maximumVersionTimestamp;
//
//    }
//
//    public final boolean getDeleteMarker(final int index) {
//
//        if (deleteMarkers == null)
//            throw new UnsupportedOperationException();
//
//        assert rangeCheckTupleIndex(index);
//
//        return deleteMarkers[index];
//
//    }
//
//    final public IRaba getValues() {
//        
//        return vals;
//        
//    }
//    
//    final public IRaba getKeys() {
//        
//        return keys;
//        
//    }
//    
//    /**
//     * Always returns <code>true</code>.
//     */
//    final public boolean isLeaf() {
//
//        return true;
//
//    }
//
//    /**
//     * For a leaf the #of entries is always the #of keys.
//     */
//    final public int getSpannedTupleCount() {
//        
//        return getKeys().size();
//        
//    }
//    
//    final public int getValueCount() {
//        
//        return vals.size();
//        
//    }
//    
//    final public boolean hasDeleteMarkers() {
//        
//        return deleteMarkers != null; 
//        
//    }
//    
//    final public boolean hasVersionTimestamps() {
//        
//        return versionTimestamps != null; 
//        
//    }
//
//    final public int getKeyCount() {
//        
//        return keys.size();
//        
//    }
//
//    /**
//     * No - this class does not support double-linked leaves (only the
//     * {@link IndexSegment} actually uses double-linked leaves).
//     */
//    final public boolean isDoubleLinked() {
//     
//        return false;
//
//    }
//
//    final public long getNextAddr() {
//    
//        throw new UnsupportedOperationException();
//        
//    }
//
//    final public long getPriorAddr() {
//        
//        throw new UnsupportedOperationException();
//        
//    }
//
//    /**
//     * Recalculate the min/max version timestamp on the leaf. The caller is
//     * responsible for propagating the new min/max to the ancestors of the leaf.
//     * 
//     * @throws UnsupportedOperationException
//     *             if the leaf is not maintaining per-tuple version timestamps.
//     */
//    void recalcMinMaxVersionTimestamp() {
//
//        // must be maintaining version timestamps.
//        if (versionTimestamps == null)
//            throw new UnsupportedOperationException();
//
//        final int nkeys = keys.nkeys;
//
//        long min = Long.MAX_VALUE;
//        long max = Long.MIN_VALUE;
//
//        for (int i = 0; i < nkeys; i++) {
//
//            final long t = versionTimestamps[i];
//
//            if (t < min)
//                min = t;
//
//            if (t > max)
//                max = t;
//
//        }
//
//        minimumVersionTimestamp = min;
//        maximumVersionTimestamp = max;
//
//    }

}
