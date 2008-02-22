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
 * Created on Feb 21, 2008
 */

package com.bigdata.journal;

import java.io.Serializable;
import java.util.Iterator;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.mdi.PartitionMetadataWithSeparatorKeys;
import com.bigdata.sparse.SparseRowStore;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSplitJoinTasks extends AbstractResourceManagerTestCase {

    /**
     * 
     */
    public TestSplitJoinTasks() {
        super();
    }

    /**
     * @param arg0
     */
    public TestSplitJoinTasks(String arg0) {
        super(arg0);
    }

    /**
     * Factory for iterators allowing an application choose both the #of splits
     * and the split points. The implementation should examine the
     * {@link IIndex}, returning a sequence of keys that are acceptable index
     * partition separators for the {@link IIndex}.
     * <p>
     * Various kinds of indices have constraints on the keys that may be used as
     * index partition separators. For example, a {@link SparseRowStore}
     * constrains its index partition separators such that
     * 
     * @see IIndex
     * @see 
     * @see PartitionMetadataWithSeparatorKeys
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface ISplitFactory extends Serializable {
        
        /**
         * Create and return an {@link Iterator} that will return a sequence of
         * zero or more keys that identify the leftSeparator of index partitions
         * to be created from the given index.
         * <p>
         * Note: The factory is given a reference to an {@link IIndex}, which
         * is typically a {@link FusedView} of an index partition. Fused views
         * do NOT implement the {@link AbstractBTree#keyAt(int)} method which
         * relies on a total ordering rather than a merge of total orderings. As
         * a result the {@link Iterator}s returned by the factory typically
         * need to perform a key range scan to identify suitable index partition
         * separators.
         * <p>
         * An index partition separator is a key that forms the
         * <em>leftSeparator</em> key for that index partition. The
         * leftSeparator describes the first key that will enter that index
         * partition. Note that the leftSeparator keys do NOT have to be
         * literally present in the index - they may be (and should be) the
         * shortest prefix that creates the necessary separation between the
         * index partitions.
         * 
         * @param ndx
         *            The index to be split.
         * 
         * @return The iterator that will visit the keys at which the index will
         *         be split into partitions (that is, it will visit the keys to
         *         be used as leftSeparator keys for those index partitions).
         */
        public Iterator<byte[]/*key*/> newInstance(IIndex ndx);
        
    };

    /**
     * Simple rule divides the index into N splits of roughly M index entries.
     * <p>
     * Note: This does not account for deletion markers which can falsely
     * inflate the range count of an index.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class DefaultSplitFactory implements ISplitFactory {
        
        private int m;
        
        public int getM() {
            
            return m;
            
        }
        
        public DefaultSplitFactory(int m) {

            if (m <= 0)
                throw new IllegalArgumentException();

            this.m = m;
            
        }
        
        public Iterator<byte[]/*key*/> newInstance(IIndex ndx) {
            
            return new DefaultSpliterator(ndx, m);
            
        }
        
    }

    /**
     * Given M, the goal is to divide the index into N partitions of
     * approximately M entries each.
     * 
     * <pre>
     *         
     *         n = round( rangeCount / m ) - the integer #of index partitions to be created.
     *         
     *         m' = round( rangeCount / n ) - the integer #of entries per partition.
     * </pre>
     * 
     * To a first approximation, the splits are every m' index entries.
     * 
     * However, the split rule then needs to examine the actual keys at and
     * around those split points and adjust them as necessary.
     * 
     * The running over/under run in sum(m') is maintained and the suggested
     * split points are adjusted by that over/under run.
     * 
     * Repeat until all split points have been identifed and validated by the
     * split run.
     * 
     * @todo could do special case version when the index is an
     *       {@link AbstractBTree} and {@link AbstractBTree#keyAt(int)} can be
     *       used.
     * 
     * @todo make sure that view of the index partition does not expose keyAt() -
     *       this may break a lot of code assumptions - or at least throws an
     *       exception for it.
     * 
     * FIXME when the index partition is a fused view we do not have a keyAt()
     * implementation. (It's possible that this could be achieved using keyAt()
     * to do a binary search probing on each of the components of that view and
     * computing the rangeCount(null,k) for each key pulled out of the
     * underlying indices, but this seems pretty complex.)
     * 
     * Another approach is a prefix scan. This is nice because it will only
     * touch on the possible index separator keys, but it only works when the
     * split points are constrained. If we allow any key as an index separator
     * key then there is no prefix that we can use. Also, this might not work
     * with the sparse row store since the [schema,primaryKey] are not fixed
     * length fields.
     * 
     * An incremental approach is to consume M index entries halting at the key
     * that forms an acceptable index partition separator closest to M (either
     * before or after). This suggests that we can not pre-compute the split
     * points when the index is a fused view but rather that we have to
     * sequentially process the index deciding as we go where the split points
     * are.
     * 
     * The inputs are then: (a) the #of index entries per partition, m'; (b) the
     * desired size in bytes of each index partition; (c) the split rule which
     * will decide what is an acceptable index partition separator; and (d) an
     * iterator scanning the index partition view.
     * 
     * @todo rule variants that attempt to divide the index partition into equal
     *       size (#of bytes) splits.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class DefaultSpliterator implements Iterator<byte[]> {

        private final IIndex ndx;

        private final long rangeCount;

        private final IEntryIterator src;
        
        private final int m;
        
        private final int n;
        
        /**
         * 
         * @param m
         *            The maximum #of index entries in a split.
         */
        public DefaultSpliterator(IIndex ndx, int m) {

            if (ndx == null)
                throw new IllegalArgumentException();
            
            if (m <= 0)
                throw new IllegalArgumentException();

            // the source index.
            this.ndx = ndx;

            // upper bound on #of non-deleted index entries.
            this.rangeCount = ndx.rangeCount(null, null);
            
            // iterator visiting keys for the non-deleted index entries.
            this.src = ndx.rangeIterator(null, null, 0/* capacity */,
                    IRangeQuery.KEYS, null/*filter*/);
            
            // target #of index partitions.
            this.n = Math.round(rangeCount / m);

            // adjusted value for [m]
            this.m = Math.round(rangeCount / n);

        }

        /**
         * Computes the key for the next split point and returns true iff one
         * was found.
         */
        public boolean hasNext() {
            // TODO Auto-generated method stub
            return false;
        }

        /**
         * Returns the key for the next split point, which is computed by
         * {@link #hasNext()}.
         */
        public byte[] next() {
            // TODO Auto-generated method stub
            return null;
        }

        /**
         * @throws UnsupportedOperationException always.
         */
        public void remove() {

            throw new UnsupportedOperationException();
            
        }

    }
    
//    /**
//     * Task builds an {@link IndexSegment} by joining an existing
//     * {@link IndexSegment} for a partition with an existing
//     * {@link IndexSegment} for either the left or right sibling of that
//     * partition.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static class JoinTask implements IPartitionTask {
//        
//    }
//    /**
//     * Task splits an {@link IndexSegment} into two new {@link IndexSegment}s.
//     * This task is executed when a partition is split in order to breakdown the
//     * {@link IndexSegment} for that partition into data for the partition and
//     * its new left/right sibling.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    public static class SplitTask implements IPartitionTask {
//        
//    }
    
//    /**
//     * Update the metadata index to reflect the split of one index segment
//     * into two index segments.
//     * 
//     * @param separatorKey
//     *            Requests greater than or equal to the separatorKey (and
//     *            less than the next largest separatorKey in the metadata
//     *            index) are directed into seg2. Requests less than the
//     *            separatorKey (and greated than any proceeding separatorKey
//     *            in the metadata index) are directed into seg1.
//     * @param seg1
//     *            The metadata for the index segment that was split.
//     * @param seg2
//     *            The metadata for the right sibling of the split index
//     *            segment in terms of the key range of the distributed
//     *            index.
//     */
//    public void split(Object separatorKey, PartitionMetadata md1, PartitionMetadata md2) {
//        
//    }
//
////    /**
//
//    In order to make joins atomic, the journal on which the operation is
//    taking place obtains an exclusive write lock source index
//    partition(s).  The data are join and the metadata index is updated
//    while the lock is in place.  After join operation the source index
//    partitions are no longer valid and will be eventually be removed from
//    service.
//
//    @todo This suggests that index partitions must be co-located for a
//    join.
//
//     * @todo join of index segment with left or right sibling. unlike the
//     *       nodes of a btree we merge nodes whenever a segment goes under
//     *       capacity rather than trying to redistribute part of the key
//     *       range from one index segment to another.
//     */
//    public void join() {
//        
//    }

}
