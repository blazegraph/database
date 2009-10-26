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
 * Created on Jul 1, 2009
 */

package com.bigdata.resources;

import junit.framework.TestCase2;

import com.bigdata.btree.BTree;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.ITupleCursor;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.Node;
import com.bigdata.sparse.SparseRowStore;

/**
 * Unit tests for identifying splits in an index partition view by choosing
 * separator keys which divide the key-range into approximately equal numbers of
 * tuples based on the data reported by {@link Node#getChildEntryCounts()} for
 * the nodes in each source in the view.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME This is a place holder for a feature which has not been finished yet.
 */
public class TestViewSplitter extends TestCase2 {

    /**
     * 
     */
    public TestViewSplitter() {
    }

    /**
     * @param arg0
     */
    public TestViewSplitter(String arg0) {
        super(arg0);
    }

    public interface IViewSplitter {

        /**
         * Find N-1 separator keys that split the view into N key-ranges where
         * each key-range spans an (approximately) equal #of elements. If there
         * are not enough entries to generate that many non-empty splits, then
         * return up to ntuples-1 splits, where ntuples is the #of keys in the
         * view.
         * <p>
         * Note: A separatorKey does not necessarily correspond to the key for
         * an actual tuple. If the application will place a constraint on the
         * separator keys, then it must create an {@link ITupleCursor} for the
         * {@link ILocalBTreeView}, {@link ITupleCursor#seek(byte[])} to each
         * separator key in turn, and scan backwards and/or forwards until it
         * identifies a suitable separator key. For example, the
         * {@link SparseRowStore} place a constraint on the separator keys such
         * that the may not divide a logical row. That constaint combined with
         * the ACID contract for unisolated operations on an
         * {@link ILocalBTreeView} provides the emergent guarantee that
         * operations on logical rows are ACID.
         * 
         * @param view
         *            The view.
         * @param nsplits
         *            The #of splits.
         * 
         * @return The separator keys.
         */
        public byte[][] getSeparatorKeys(ILocalBTreeView view, int nsplits);
        
    }
    
    /**
     * This class identifies separator keys which divide the key-range into
     * approximately equal numbers of tuples based on the data reported by
     * {@link Node#getChildEntryCounts()} for the nodes in each source in the
     * view. The separator key(s) are built up incrementally, one byte at a
     * time. Each byte is chosen based on the manner in which it would divide
     * the #of tuples in the view. This approach allows us to work with sources
     * in the view which have different tree depths. No more than NTUPLES splits
     * will be returned. {@link Node#getChildEntryCounts()} reflects both non-
     * deleted and deleted tuples, so some splits MAY be empty after a
     * compacting merge.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo test when NTUPLES LTE nsplits.
     * 
     * @todo test when some tuples are deleted such that some splits would be
     *       empty after a compacting merge.
     * 
     * @todo do we always perform a compacting merge for a split? I think so.
     * 
     * @todo test when nsplits GT 2.
     * 
     * @todo test correct rejection of ctor, args.
     * 
     * @todo verify that the (left,right) separator keys for the view are
     *       respected throughput since it is possible that some index segments
     *       in the view will span a larger key range than the view (if they are
     *       shared with other views).
     * 
     * @todo test when NSPLITS GT the branching factor of the {@link BTree}.
     *       This case can easily arise in scatter splits. It is less likely
     *       that NSPLITS is GT the branching factor of the {@link IndexSegment},
     *       but that case could also arise in large federations or when the
     *       branching factor of the index segment was configured to be
     *       relatively small.
     */
    public class ViewSplitter implements IViewSplitter {

        public byte[][] getSeparatorKeys(final ILocalBTreeView view, int nsplits) {

            if (view == null)
                throw new IllegalArgumentException();
            
            if (nsplits < 2)
                throw new IllegalArgumentException();

            /*
             * Create an array of nsplits-1 separatorKeys. Each key is initially
             * an empty byte[].
             */
            /*
             * Obtain sumChildEntryCounts() for each separatorKey. When the
             * separatorKey is an empty byte[], this is done using the
             * childEntryCounts[] for the root node of each source. This must
             * respect the (left,right) separator key for the view.
             */
            /*
             * Divide sumChildEntryCount[] by NSPLITS-1, which is the ~ #of
             * children which will be spanned by each separator key, call this
             * NTUPLES_PER_SPLIT.
             */
            /*
             * Extend each separator key by one byte such that the #of tuples
             * within the split would be approximately equal. This is done by
             * descending each B+Tree source in the view to the level of the
             * current separatorKey and finding the byte(s) which, when
             * appending to the current separatorKey cause the
             * sumChildEntryCount for the separatorKey to be ~ EQ
             * NTUPLES_PER_SPLIT.
             */

            throw new UnsupportedOperationException();

        }
        
    }
    
//    /**
//     * Unit test for the case where there is one source (a {@link BTree}) in
//     * the view and nsplits=2, implying that a single separator key will be
//     * chosen to split the key range.
//     */
//    public void test_1Sources_2Splits() {
//
//        fail("write test");
//        
//    }
//
//    /**
//     * Split a view with 1 source into nsplits=3 new views.
//     */
//    public void test_1Source_3Splits() {
//
//        fail("write test");
//        
//    }
//
//    /**
//     * Split a view with two sources into 2 new views.
//     */
//    public void test_2Sources_2Splits() {
//
//        fail("write test");
//        
//    }
//
//    /**
//     * Split a view with 2 sources into nsplits=3 new views.
//     */
//    public void test_2Sources_3Splits() {
//
//        fail("write test");
//        
//    }
//
//    /**
//     * Stress test with random data.
//     */
//    public void test_stress() {
//
//        fail("write test");
//        
//    }

    // keeps junit quite for now.
    public void test_nothing() {
        
    }

}
