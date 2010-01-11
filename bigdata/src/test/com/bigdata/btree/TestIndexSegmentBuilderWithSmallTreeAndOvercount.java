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
package com.bigdata.btree;

import java.io.File;

import com.bigdata.btree.IndexSegment.ImmutableNodeFactory.ImmutableEmptyLastLeaf;
import com.bigdata.btree.IndexSegment.ImmutableNodeFactory.ImmutableLeaf;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.raba.ReadOnlyKeysRaba;

/**
 * Test suite based on a small B+Tree with known keys and values. This version
 * of the test suite uses the same parameters to generate the
 * {@link IndexSegmentPlan} but delete some tuples before executing the build
 * with the result that the reported tuple count is too high for the data and
 * the build plan will underflow for some leaves and even some nodes. This
 * models the use case of a compacting merge when a fast range count is used to
 * generate the build plan. The resulting {@link IndexSegment} is not a perfect
 * B+Tree since some leaves and even some nodes may underflow, since it may not
 * be balanced, etc. However, this approach avoids a key-range scan to determine
 * the exact range count and is thus much faster. The resulting
 * {@link IndexSegment} will still be very efficient - it just runs out of
 * tuples before it can fill in the rightmost edge of the B+Tree.
 * 
 * @see src/architecture/btree.xls, which has the detailed examples.
 * 
 *      FIXME Enable testing of over reporting in
 *      {@link TestIndexSegmentBuilderWithLargeTrees#doBuildIndexSegmentAndCompare(BTree)}
 *      (change the constant in the code in that method to .2).
 * 
 * @see TestIndexSegmentBuilderWithLargeTrees#test_overreportDuringBuild()
 * @see TestIndexSegmentBuilderWithLargeTrees#doBuildIndexSegmentAndCompare(BTree)
 * @see TestIndexSegmentBuilderWithLargeTrees#doBuildIndexSegmentAndCompare2(BTree)
 */
public class TestIndexSegmentBuilderWithSmallTreeAndOvercount extends
        AbstractIndexSegmentTestCase {

    File outFile;

    File tmpDir;

    public TestIndexSegmentBuilderWithSmallTreeAndOvercount() {
    }

    public TestIndexSegmentBuilderWithSmallTreeAndOvercount(String name) {
        super(name);
    }

    public void setUp() throws Exception {

        super.setUp();
        
        outFile = new File(getName() + ".seg");

        if (outFile.exists() && !outFile.delete()) {

            throw new RuntimeException("Could not delete file: " + outFile);

        }
        
        tmpDir = outFile.getAbsoluteFile().getParentFile();

    }

    public void tearDown() throws Exception {

        if (outFile != null && outFile.exists() && !outFile.delete()) {

            log.warn("Could not delete file: " + outFile);

        }

        super.tearDown();
        
    }

    /*
     * problem1
     */

    /**
     * Create, populate, and return a btree with a branching factor of (3) and
     * ten sequential keys [1:10]. The values are {@link SimpleEntry} objects
     * whose state is the same as the corresponding key.
     * 
     * @return The btree.
     * 
     * @see src/architecture/btree.xls, which details this input tree and a
     *      series of output trees with various branching factors.
     */
    public BTree getProblem1() {

        final BTree btree = getBTree(3);

        /*
         * Make sure that delete markers are not enabled since deleting a tuple
         * will not cause the tuple to be removed.
         * 
         * Note: You could run these unit tests with delete markers since the
         * tuple will be purged by the compacting merge iterator which ignores
         * deleted tuples. Just be aware that BTree#getEntryCount() will over
         * report for the source BTree once you delete some tuples.
         */
        assertFalse(btree.getIndexMetadata().getDeleteMarkers());
        
        for (int i = 1; i <= 10; i++) {

            btree.insert(KeyBuilder.asSortKey(i), new SimpleEntry(i));

        }
        
        return btree;

    }

    /**
     * Test ability to build an index segment from a {@link BTree}. One key is
     * removed from the B+Tree returned by {@link #getProblem1()} but the
     * original rangeCount is used to build the {@link IndexSegment}. This
     * results in one less key in the rightmost leaf and corresponding
     * adjustments in the spanned entry count values in the rightmost nodes up
     * to the root node.
     */
    public void test_buildOrder3_removeOne() throws Exception {

        final BTree btree = getProblem1();

        final long commitTime = System.currentTimeMillis();
        
        // The range count which will be used to do the build.
        final int reportedRangeCount = btree.getEntryCount();

        /*
         * Remove the last key.
         * 
         * Note: it is easier to adapt the original test since if we delete the
         * keys from the right hand size this causes the least change in the
         * keys of the nodes and the leaves.
         */
        btree.remove(btree.keyAt(reportedRangeCount - 1));

        // The actual range count (subtract out the #of deleted tuples).
        final int expectedRangeCount = reportedRangeCount - 1;

        // Do the build (a compacting merge, so delete tuples will be purged).
        new IndexSegmentBuilder(outFile, tmpDir, reportedRangeCount, btree
                .rangeIterator(), 3/* m */, btree.getIndexMetadata(), commitTime,
                true/*compactingMerge*/).call();

         /*
          * Verify can load the index file and that the metadata
          * associated with the index file is correct (we are only
          * checking those aspects that are easily defined by the test
          * case and not, for example, those aspects that depend on the
          * specifics of the length of serialized nodes or leaves).
          */
        
        final IndexSegmentStore segStore = new IndexSegmentStore(outFile);
        
        assertEquals(commitTime,segStore.getCheckpoint().commitTime);
        assertEquals(2,segStore.getCheckpoint().height);
        assertEquals(4,segStore.getCheckpoint().nleaves);
        assertEquals(3,segStore.getCheckpoint().nnodes);
        assertEquals(expectedRangeCount,segStore.getCheckpoint().nentries);
        
        final IndexSegment seg = segStore.loadIndexSegment();
        
        try {
        
        assertEquals(3,seg.getBranchingFactor());
        assertEquals(2,seg.getHeight());
        assertEquals(4,seg.getLeafCount());
        assertEquals(3,seg.getNodeCount());
        assertEquals(expectedRangeCount,seg.getEntryCount());

        testForwardScan(seg);
        testReverseScan(seg);
        
        // test index segment structure.
        dumpIndexSegment(seg);
        
        /*
         * Test the tree in detail.
         */
        {
        
            final Node C = (Node)seg.getRoot();
            final Node A = (Node)C.getChild(0);
            final Node B = (Node)C.getChild(1);
            final Leaf a = (Leaf)A.getChild(0);
            final Leaf b = (Leaf)A.getChild(1);
            final Leaf c = (Leaf)B.getChild(0);
            final Leaf d = (Leaf)B.getChild(1);
           
            // verify double-linked list of leaves.
            assertEquals(0L, a.getPriorAddr());
            assertEquals(b.getIdentity(), a.getNextAddr());
            assertEquals(a.getIdentity(), b.getPriorAddr());
            assertEquals(c.getIdentity(), b.getNextAddr());
            assertEquals(b.getIdentity(), c.getPriorAddr());
            assertEquals(d.getIdentity(), c.getNextAddr());
            assertEquals(c.getIdentity(), d.getPriorAddr());
            assertEquals(0L, d.getNextAddr());

            assertKeys(new int[]{7},C);
            assertEntryCounts(new int[]{6,3},C); // one less here

            assertKeys(new int[]{4},A);
            assertEntryCounts(new int[]{3,3},A);
            
            assertKeys(new int[]{9},B);
            assertEntryCounts(new int[]{2,1},B); // one less here
            
            assertKeys(new int[]{1,2,3},a);
            assertKeys(new int[]{4,5,6},b);
            assertKeys(new int[]{7,8},c);
            assertKeys(new int[]{9/*,10*/},d);

            // Note: values are verified by testing the total order.

        }
        
        /*
         * Verify the total index order.
         */
        assertSameBTree(btree, seg);
        
        } finally {
            
            // close so we can delete the backing store.
            seg.close();
            
        }

    }

    /**
     * Test ability to build an index segment from a {@link BTree}. Two keys are
     * removed from the B+Tree returned by {@link #getProblem1()} but the
     * original rangeCount is used to build the {@link IndexSegment}. This
     * results in the rightmost leaf being empty, so it is not emitted. The
     * address of that rightmost leaf in the parent node (B) is written as a
     * <code>null</code>.
     * 
     * FIXME done. The {@link AbstractBTree} and/or the {@link IndexSegment}
     * need to be modified to handle a <code>null</code> reference to a child
     * (the addr of the child is 0L). In general, this is illegal since the
     * B+Tree must be balanced, but this special case is allowed for an
     * {@link IndexSegment} since the build plan can result in an unbalanced
     * B+Tree if the range count is an overestimate.
     * 
     * FIXME Keep going until we drive the underflow up the tree, perhaps all
     * the way down to zero keys to cover all possible edge cases for
     * {@link #getProblem1()}. All of the examples have been mapped out the in
     * worksheet for problem1.
     * <p>
     * Then do this for the other problems to gain more confidence that we have
     * covered all edge cases. Finally, do a stress test variant with random
     * deletes or just overreporting of the range count.
     * <p>
     * Note: Nodes other than those immediately dominating leaves can have a
     * mock rightmost leaf as a child.  Do an example which covers this case.
     * See {@link ImmutableEmptyLastLeaf}.
     */
    public void test_buildOrder3_removeTwo() throws Exception {

        final BTree btree = getProblem1();

        final long commitTime = System.currentTimeMillis();
        
        // The range count which will be used to do the build.
        final int reportedRangeCount = btree.getEntryCount();

        /*
         * Remove the last key.
         * 
         * Note: it is easier to adapt the original test since if we delete the
         * keys from the right hand size this causes the least change in the
         * keys of the nodes and the leaves.
         */
        btree.remove(btree.keyAt(reportedRangeCount - 1));
        btree.remove(btree.keyAt(reportedRangeCount - 2));

        // The actual range count (subtract out the #of deleted tuples).
        final int expectedRangeCount = reportedRangeCount - 2;

        // Do the build (a compacting merge, so delete tuples will be purged).
        new IndexSegmentBuilder(outFile, tmpDir, reportedRangeCount, btree
                .rangeIterator(), 3/* m */, btree.getIndexMetadata(), commitTime,
                true/*compactingMerge*/).call();

         /*
          * Verify can load the index file and that the metadata
          * associated with the index file is correct (we are only
          * checking those aspects that are easily defined by the test
          * case and not, for example, those aspects that depend on the
          * specifics of the length of serialized nodes or leaves).
          */
        
        final IndexSegmentStore segStore = new IndexSegmentStore(outFile);
        
        assertEquals(commitTime,segStore.getCheckpoint().commitTime);
        assertEquals(2,segStore.getCheckpoint().height);
        assertEquals(3,segStore.getCheckpoint().nleaves);
        assertEquals(3,segStore.getCheckpoint().nnodes);
        assertEquals(expectedRangeCount,segStore.getCheckpoint().nentries);
        
        final IndexSegment seg = segStore.loadIndexSegment();
        
        try {

            assertEquals(3, seg.getBranchingFactor());
            assertEquals(2, seg.getHeight());
            assertEquals(3, seg.getLeafCount());
            assertEquals(3, seg.getNodeCount());
            assertEquals(expectedRangeCount, seg.getEntryCount());

            testForwardScan(seg);
            testReverseScan(seg);

            // test index segment structure.
            dumpIndexSegment(seg);

            /*
             * Test the tree in detail.
             */
            {

                final Node C = (Node) seg.getRoot();
                final Node A = (Node) C.getChild(0);
                final Node B = (Node) C.getChild(1);
                final Leaf a = (Leaf) A.getChild(0);
                final Leaf b = (Leaf) A.getChild(1);
                final Leaf c = (Leaf) B.getChild(0);
                // Note: link to (d) does not exist.
                assertEquals(1, B.getKeyCount());
                // getChildCount() is 2 but the 2nd childAddr is 0L since (d) does not exist.
                assertEquals(2, B.getChildCount());
                // childAddr is dimensioned for that index but childAddr[1] is 0L.
                assertEquals(0L, B.getChildAddr(1));
                {
                    // Note: d does not exist but can be materialized
                    // dynamically and its priorAddr will be c.
                    final Leaf d = (Leaf) B.getChild(1);
                    assertTrue(d instanceof ImmutableEmptyLastLeaf);
                    assertEquals(c.getIdentity(), d.getPriorAddr());
                    assertEquals(c.getIdentity(),((ImmutableLeaf)d).priorLeaf().getIdentity());
                }

                // verify double-linked list of leaves.
                assertEquals(0L, a.getPriorAddr());
                assertEquals(b.getIdentity(), a.getNextAddr());
                assertEquals(a.getIdentity(), b.getPriorAddr());
                assertEquals(c.getIdentity(), b.getNextAddr());
                assertEquals(b.getIdentity(), c.getPriorAddr());
                // Note: c.next==0L.
                assertEquals(0L, c.getNextAddr());
//                assertEquals(d.getIdentity(), c.getNextAddr());
//                assertEquals(c.getIdentity(), d.getPriorAddr());
//                assertEquals(0L, d.getNextAddr());

                assertKeys(new int[] { 7 }, C);
                assertEntryCounts(new int[] { 6, 2 }, C); // two less here

                assertKeys(new int[] { 4 }, A);
                assertEntryCounts(new int[] { 3, 3 }, A);

                /*
                 * Note: GT 8 goes to an empty leaf! The separator key is formed
                 * from successor(i2k(8)).
                 */
                assertKeys(new ReadOnlyKeysRaba(new byte[][] { BytesUtil
                        .successor(i2k(8)) }), B.getKeys());
//                assertKeys(new int[] { 9 }, B);
                assertEntryCounts(new int[] { 2, 0 }, B); // two less here

                assertKeys(new int[] { 1, 2, 3 }, a);
                assertKeys(new int[] { 4, 5, 6 }, b);
                assertKeys(new int[] { 7, 8 }, c);
//                System.err.println("d:"+d.getKeys());
//                assertKeys(new int[] {/* 9,10 */}, d); // empty leaf.

                // Note: values are verified by testing the total order.

            }

            /*
             * Verify the total index order.
             */
            assertSameBTree(btree, seg);

        } finally {

            // close so we can delete the backing store.
            seg.close();

        }

    }

//    /**
//     * This case results in a root node and two leaves. Each leaf is at its
//     * minimum capacity (5). This tests an edge case for the algorithm that
//     * distributes the keys among the leaves when there would otherwise be
//     * an underflow in the last leaf.
//     * 
//     * @throws IOException
//     */
//    public void test_buildOrder9() throws Exception {
//        
//        final BTree btree = getProblem1();
//        
//        final long commitTime = System.currentTimeMillis();
//        
////        IndexSegmentBuilder.log.setLevel(Level.DEBUG); 
//        
//        new IndexSegmentBuilder(outFile, tmpDir, btree.getEntryCount(), btree
//                .rangeIterator(), 9/* m */, btree.getIndexMetadata(), commitTime,
//                true/*compactingMerge*/).call();
//
//        /*
//         * Verify that we can load the index file and that the metadata
//         * associated with the index file is correct (we are only checking those
//         * aspects that are easily defined by the test case and not, for
//         * example, those aspects that depend on the specifics of the length of
//         * serialized nodes or leaves).
//         */
//        final IndexSegmentStore segStore = new IndexSegmentStore(outFile);
//        
//        assertEquals("#nodes",1,segStore.getCheckpoint().nnodes);
//        assertEquals("#leaves",2,segStore.getCheckpoint().nleaves);
//        assertEquals("#entries",10,segStore.getCheckpoint().nentries);
//        assertEquals("height",1,segStore.getCheckpoint().height);
//        assertNotSame(segStore.getCheckpoint().addrRoot,segStore.getCheckpoint().addrFirstLeaf);
//        assertNotSame(segStore.getCheckpoint().addrFirstLeaf,segStore.getCheckpoint().addrLastLeaf);
//
//        final IndexSegment seg = segStore.loadIndexSegment();
//
//        try {
//        
//        assertEquals(9, seg.getBranchingFactor());
//        assertEquals(1, seg.getHeight());
//        assertEquals(2, seg.getLeafCount());
//        assertEquals(1, seg.getNodeCount());
//        assertEquals(10, seg.getEntryCount());
//
//        final ImmutableLeaf firstLeaf = seg
//                .readLeaf(segStore.getCheckpoint().addrFirstLeaf);
//
//        assertEquals("priorAddr", 0L, firstLeaf.getPriorAddr());
//
//        assertEquals("nextAddr", segStore.getCheckpoint().addrLastLeaf,
//                firstLeaf.getNextAddr());
//
//        final ImmutableLeaf lastLeaf = seg
//                .readLeaf(segStore.getCheckpoint().addrLastLeaf);
//
//        assertEquals("priorAddr", segStore.getCheckpoint().addrFirstLeaf,
//                lastLeaf.getPriorAddr());
//
//        assertEquals("nextAddr", 0L, lastLeaf.getNextAddr());
//
//        // test forward scan
//        {
//         
//            final ImmutableLeafCursor itr = seg.newLeafCursor(SeekEnum.First);
//            
//            assertTrue(firstLeaf.getDelegate() == itr.leaf().getDelegate()); // Note: test depends on cache!
//            assertNull(itr.prior());
//
//            assertTrue(lastLeaf.getDelegate() == itr.next().getDelegate()); // Note: test depends on cache!
//            assertTrue(lastLeaf.getDelegate() == itr.leaf().getDelegate()); // Note: test depends on cache!
//            
//        }
//        
//        /*
//         * test reverse scan
//         * 
//         * Note: the scan starts with the last leaf in the key order and then
//         * proceeds in reverse key order.
//         */
//        {
//            
//            final ImmutableLeafCursor itr = seg.newLeafCursor(SeekEnum.Last);
//            
//            assertTrue(lastLeaf.getDelegate() == itr.leaf().getDelegate()); // Note: test depends on cache!
//            assertNull(itr.next());
//
//            assertTrue(firstLeaf.getDelegate() == itr.prior().getDelegate()); // Note: test depends on cache!
//            assertTrue(firstLeaf.getDelegate() == itr.leaf().getDelegate()); // Note: test depends on cache!
//
//        }
//        
//        // test index segment structure.
//        dumpIndexSegment(seg);
//
//        /*
//         * Test the tree in detail.
//         */
//        {
//        
//            final Node A = (Node)seg.getRoot(); 
//            final Leaf a = (Leaf)A.getChild(0);
//            final Leaf b = (Leaf)A.getChild(1);
//           
//            assertKeys(new int[]{6},A);
//            assertEntryCounts(new int[]{5,5},A);
//            
//            assertKeys(new int[]{1,2,3,4,5},a);
//            assertKeys(new int[]{6,7,8,9,10},b);
//
//            // Note: values are verified by testing the total order.
//
//        }
//        
//        /*
//         * Verify the total index order.
//         */
//        assertSameBTree(btree, seg);
//        
//        } finally {
//            
//            // close so we can delete the backing store.
//            seg.close();
//            
//        }
//
//    }
//
//    /**
//     * This case results in a single root leaf filled to capacity.
//     * 
//     * @throws IOException
//     */
//    public void test_buildOrder10() throws Exception {
//        
//        final BTree btree = getProblem1();
//
//        final long commitTime = System.currentTimeMillis();
//        
//        new IndexSegmentBuilder(outFile, tmpDir, btree.getEntryCount(), btree
//                .rangeIterator(), 10/* m */, btree.getIndexMetadata(), commitTime,
//                true/*compactingMerge*/).call();
//
//        /*
//         * Verify that we can load the index file and that the metadata
//         * associated with the index file is correct (we are only checking those
//         * aspects that are easily defined by the test case and not, for
//         * example, those aspects that depend on the specifics of the length of
//         * serialized nodes or leaves).
//         */
//
//        final IndexSegmentStore segStore = new IndexSegmentStore(outFile);
//        
//        assertEquals("#nodes",0,segStore.getCheckpoint().nnodes);
//        assertEquals("#leaves",1,segStore.getCheckpoint().nleaves);
//        assertEquals("#entries",10,segStore.getCheckpoint().nentries);
//        assertEquals("height",0,segStore.getCheckpoint().height);
//        assertEquals(segStore.getCheckpoint().addrRoot,segStore.getCheckpoint().addrFirstLeaf);
//        assertEquals(segStore.getCheckpoint().addrFirstLeaf,segStore.getCheckpoint().addrLastLeaf);
//
//        final IndexSegment seg = segStore.loadIndexSegment();
//
//        try {
//        
//        assertEquals(10,seg.getBranchingFactor());
//        assertEquals(0,seg.getHeight());
//        assertEquals(1,seg.getLeafCount());
//        assertEquals(0,seg.getNodeCount());
//        assertEquals(10,seg.getEntryCount());
//
//        final ImmutableLeaf leaf = seg.readLeaf(segStore.getCheckpoint().addrRoot);
//        assertEquals("priorAddr", 0L, leaf.getPriorAddr());
//        assertEquals("nextAddr", 0L, leaf.getNextAddr());
//
//        final ImmutableLeafCursor itr = seg.newLeafCursor(SeekEnum.First);
//        assertTrue(leaf.getDelegate() == itr.leaf().getDelegate()); // Note: test depends on cache.
//        assertNull(itr.prior());
//        assertNull(itr.next());
//        
//        // test index segment structure.
//        dumpIndexSegment(seg);
//
//        /*
//         * verify the right keys in the right leaves.
//         */
//        {
//            
//            Leaf root = (Leaf)seg.getRoot();
//            assertKeys(new int[]{1,2,3,4,5,6,7,8,9,10},root);
//            
//        }
//        
//        /*
//         * Verify the total index order.
//         */
//        assertSameBTree(btree, seg);
//        
//        } finally {
//            
//            // close so we can delete the backing store.
//            seg.close();
//            
//        }
//
//    }
//    
//    /*
//     * Examples based on an input tree with 9 entries.
//     */
//
//    /**
//     * Create, populate, and return a btree with a branching factor of (3) and
//     * nine sequential keys [1:9]. The values are {@link SimpleEntry} objects
//     * whose state is the same as the corresponding key.
//     * 
//     * @return The btree.
//     * 
//     * @see src/architecture/btree.xls, which details this input tree and a
//     *      series of output trees with various branching factors.
//     */
//    public BTree getProblem2() {
//
//        final BTree btree = getBTree(3);
//
//        for (int i = 1; i <= 9; i++) {
//
//            btree.insert(KeyBuilder.asSortKey(i), new SimpleEntry(i));
//
//        }
//        
//        return btree;
//
//    }
//
//    /**
//     * This case results in a single root leaf filled to capacity.
//     * 
//     * @throws IOException
//     */
//    public void test_problem2_buildOrder3() throws Exception {
//        
//        final BTree btree = getProblem2();
//        
//        btree.dump(Level.DEBUG,System.err);        
//
//        final long commitTime = System.currentTimeMillis();
//        
//        new IndexSegmentBuilder(outFile, tmpDir, btree.getEntryCount(), btree
//                .rangeIterator(), 3/* m */, btree.getIndexMetadata(), commitTime,
//                true/*compactingMerge*/).call();
//        
////        new IndexSegmentBuilder(outFile,tmpDir,btree,3,0.);
//
//         /*
//          * Verify can load the index file and that the metadata
//          * associated with the index file is correct (we are only
//          * checking those aspects that are easily defined by the test
//          * case and not, for example, those aspects that depend on the
//          * specifics of the length of serialized nodes or leaves).
//          */
//        final IndexSegment seg = new IndexSegmentStore(outFile).loadIndexSegment();
//
//        try {
//        
//        assertEquals(3,seg.getBranchingFactor());
//        assertEquals(1,seg.getHeight());
//        assertEquals(3,seg.getLeafCount());
//        assertEquals(1,seg.getNodeCount());
//        assertEquals(9,seg.getEntryCount());
//        
//        // test index segment structure.
//        dumpIndexSegment(seg);
//
//        /*
//         * Test the tree in detail.
//         */
//        {
//        
//            final Node A = (Node)seg.getRoot();
//            final Leaf a = (Leaf)A.getChild(0);
//            final Leaf b = (Leaf)A.getChild(1);
//            final Leaf c = (Leaf)A.getChild(2);
//           
//            assertKeys(new int[]{4,7},A);
//            assertEntryCounts(new int[]{3,3,3},A);
//            
//            assertKeys(new int[]{1,2,3},a);
//            assertKeys(new int[]{4,5,6},b);
//            assertKeys(new int[]{7,8,9},c);
//
//            // Note: values are verified by testing the total order.
//
//        }
//        
//        /*
//         * Verify the total index order.
//         */
//        assertSameBTree(btree, seg);
//        
//        } finally {
//        
//            // close so we can delete the backing store.
//            seg.close();
//            
//        }
//        
//    }
//
//    /*
//     * problem3
//     */
//
//    /**
//     * Create, populate, and return a btree with a branching factor of (3) and
//     * 20 sequential keys [1:20]. The resulting tree has a height of (3). The
//     * values are {@link SimpleEntry} objects whose state is the same as the
//     * corresponding key.
//     * 
//     * @return The btree.
//     * 
//     * @see src/architecture/btree.xls, which details this input tree and a
//     *      series of output trees with various branching factors.
//     */
//    public BTree getProblem3() {
//
//        final BTree btree = getBTree(3);
//
//        for (int i = 1; i <= 20; i++) {
//
//            btree.insert(KeyBuilder.asSortKey(i), new SimpleEntry(i));
//
//        }
//        
//        return btree;
//
//    }
//
//    /**
//     * Note: This problem requires us to short a node in the level above the
//     * leaves so that the last node in that level does not underflow.
//     * 
//     * @throws IOException
//     */
//    public void test_problem3_buildOrder3() throws Exception {
//
//        final BTree btree = getProblem3();
//
//        btree.dump(Level.DEBUG,System.err);
//
//        final long commitTime = System.currentTimeMillis();
//        
//        new IndexSegmentBuilder(outFile, tmpDir, btree.getEntryCount(), btree
//                .rangeIterator(), 3/* m */, btree.getIndexMetadata(), commitTime,
//                true/*compactingMerge*/).call();
//
////        new IndexSegmentBuilder(outFile,tmpDir,btree,3,0.);
//
//         /*
//          * Verify can load the index file and that the metadata
//          * associated with the index file is correct (we are only
//          * checking those aspects that are easily defined by the test
//          * case and not, for example, those aspects that depend on the
//          * specifics of the length of serialized nodes or leaves).
//          */
//        final IndexSegment seg = new IndexSegmentStore(outFile).loadIndexSegment();
//
//        try {
//        
//        assertEquals(3,seg.getBranchingFactor());
//        assertEquals(2,seg.getHeight());
//        assertEquals(7,seg.getLeafCount());
//        assertEquals(4,seg.getNodeCount());
//        assertEquals(20,seg.getEntryCount());
//
//        // test index segment structure.
//        dumpIndexSegment(seg);
//
//        /*
//         * Test the tree in detail.
//         */
//        {
//        
//            final Node D = (Node)seg.getRoot();
//            final Node A = (Node)D.getChild(0); 
//            final Node B = (Node)D.getChild(1);
//            final Node C = (Node)D.getChild(2);
//            final Leaf a = (Leaf)A.getChild(0);
//            final Leaf b = (Leaf)A.getChild(1);
//            final Leaf c = (Leaf)A.getChild(2);
//            final Leaf d = (Leaf)B.getChild(0);
//            final Leaf e = (Leaf)B.getChild(1);
//            final Leaf f = (Leaf)C.getChild(0);
//            final Leaf g = (Leaf)C.getChild(1);
//           
//            assertKeys(new int[]{10,16},D);
//            assertEntryCounts(new int[]{9,6,5},D);
//            
//            assertKeys(new int[]{4,7},A);
//            assertEntryCounts(new int[]{3,3,3},A);
//            
//            assertKeys(new int[]{13},B);
//            assertEntryCounts(new int[]{3,3},B);
//            
//            assertKeys(new int[]{19},C);
//            assertEntryCounts(new int[]{3,2},C);
//            
//            assertKeys(new int[]{1,2,3},a);
//            assertKeys(new int[]{4,5,6},b);
//            assertKeys(new int[]{7,8,9},c);
//            assertKeys(new int[]{10,11,12},d);
//            assertKeys(new int[]{13,14,15},e);
//            assertKeys(new int[]{16,17,18},f);
//            assertKeys(new int[]{19,20},g);
//
//            // Note: values are verified by testing the total order.
//        }
//        
//        /*
//         * Verify the total index order.
//         */
//        assertSameBTree(btree, seg);
//
//        } finally {
//            
//            // close so we can delete the backing store.
//            seg.close();
//            
//        }
//        
//    }
    
}
