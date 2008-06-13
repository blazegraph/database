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
import java.io.IOException;

import org.apache.log4j.Level;

import com.bigdata.btree.IndexSegment.ImmutableLeafCursor;
import com.bigdata.btree.IndexSegment.ImmutableNodeFactory.ImmutableLeaf;

/**
 * Test suite based on a small btree with known keys and values.
 * 
 * @see src/architecture/btree.xls, which has the detailed examples.
 */
public class TestIndexSegmentBuilderWithSmallTree extends AbstractIndexSegmentTestCase {

    File outFile;
    File tmpDir;
    
    public TestIndexSegmentBuilderWithSmallTree() {}
    
    public TestIndexSegmentBuilderWithSmallTree(String name) {super(name);}
    
    /**
     * Sets up the {@link #btree} and ensures that the {@link #outFile} on
     * which the {@link IndexSegment} will be written does not exist.
     */
    public void setUp() {

        outFile = new File(getName() + ".seg");

        if (outFile.exists() && !outFile.delete()) {

            throw new RuntimeException("Could not delete file: " + outFile);

        }
        
        tmpDir = outFile.getAbsoluteFile().getParentFile();

    }

    public void tearDown() {

        if (outFile != null && outFile.exists() && !outFile.delete()) {

            log.warn("Could not delete file: " + outFile);

        }

    }

    /**
     * apply dump() as a structural validation of the tree.  note that we
     * have to materialize the leaves in the generated index segment since
     * dump does not materialize a child from its Addr if it is not already
     * resident.
     */
    protected void dumpIndexSegment(IndexSegment seg) {

        // materialize the leaves.
        
        ILeafCursor cursor = seg.newLeafCursor(SeekEnum.First);
        
        int n = 0;
        while(cursor.next()!=null) {
            n++;
        }

        cursor.last();
        while(cursor.prior()!=null) {
            n--;
        }
        
        /*
         * Note: n will be zero if the same number of leaves were visited in
         * each direction.
         */
        assertEquals(0, n);
        
        // dump the tree to validate it.

        System.err.println("dumping index segment");

        assert seg.dump(Level.DEBUG, System.err);

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

        BTree btree = getBTree(3);

        for (int i = 1; i <= 10; i++) {

            btree.insert(KeyBuilder.asSortKey(i), new SimpleEntry(i));

        }
        
        return btree;

    }
    
    /**
     * Test ability to build an index segment from a {@link BTree}.
     */
    public void test_buildOrder3() throws Exception {

        final BTree btree = getProblem1();

        final long commitTime = System.currentTimeMillis();
        
        new IndexSegmentBuilder(outFile, tmpDir, btree.getEntryCount(), btree
                .rangeIterator(), 3/* m */, btree.getIndexMetadata(), commitTime).call();

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
        assertEquals(10,segStore.getCheckpoint().nentries);
        
        final IndexSegment seg = segStore.loadIndexSegment();
        
        assertEquals(3,seg.getBranchingFactor());
        assertEquals(2,seg.getHeight());
        assertEquals(4,seg.getLeafCount());
        assertEquals(3,seg.getNodeCount());
        assertEquals(10,seg.getEntryCount());

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
           
            assertKeys(new int[]{7},C);
            assertEntryCounts(new int[]{6,4},C);

            assertKeys(new int[]{4},A);
            assertEntryCounts(new int[]{3,3},A);
            
            assertKeys(new int[]{9},B);
            assertEntryCounts(new int[]{2,2},B);
            
            assertKeys(new int[]{1,2,3},a);
            assertKeys(new int[]{4,5,6},b);
            assertKeys(new int[]{7,8},c);
            assertKeys(new int[]{9,10},d);

            // Note: values are verified by testing the total order.

        }
        
        /*
         * Verify the total index order.
         */
        assertSameBTree(btree, seg);
        
    }

    /**
     * This case results in a root node and two leaves. Each leaf is at its
     * minimum capacity (5). This tests an edge case for the algorithm that
     * distributes the keys among the leaves when there would otherwise be
     * an underflow in the last leaf.
     * 
     * @throws IOException
     */
    public void test_buildOrder9() throws Exception {
        
        final BTree btree = getProblem1();
        
        final long commitTime = System.currentTimeMillis();
        
//        IndexSegmentBuilder.log.setLevel(Level.DEBUG); 
        
        new IndexSegmentBuilder(outFile, tmpDir, btree.getEntryCount(), btree
                .rangeIterator(), 9/* m */, btree.getIndexMetadata(), commitTime).call();

        /*
         * Verify that we can load the index file and that the metadata
         * associated with the index file is correct (we are only checking those
         * aspects that are easily defined by the test case and not, for
         * example, those aspects that depend on the specifics of the length of
         * serialized nodes or leaves).
         */
        final IndexSegmentStore segStore = new IndexSegmentStore(outFile);
        
        assertEquals("#nodes",1,segStore.getCheckpoint().nnodes);
        assertEquals("#leaves",2,segStore.getCheckpoint().nleaves);
        assertEquals("#entries",10,segStore.getCheckpoint().nentries);
        assertEquals("height",1,segStore.getCheckpoint().height);
        assertNotSame(segStore.getCheckpoint().addrRoot,segStore.getCheckpoint().addrFirstLeaf);
        assertNotSame(segStore.getCheckpoint().addrFirstLeaf,segStore.getCheckpoint().addrLastLeaf);

        final IndexSegment seg = segStore.loadIndexSegment();

        assertEquals(9, seg.getBranchingFactor());
        assertEquals(1, seg.getHeight());
        assertEquals(2, seg.getLeafCount());
        assertEquals(1, seg.getNodeCount());
        assertEquals(10, seg.getEntryCount());

        final ImmutableLeaf firstLeaf = seg.readLeaf(segStore.getCheckpoint().addrFirstLeaf);

        assertEquals("priorAddr", 0L, firstLeaf.priorAddr);

        assertEquals("nextAddr", segStore.getCheckpoint().addrLastLeaf, firstLeaf.nextAddr);

        final ImmutableLeaf lastLeaf = seg.readLeaf(segStore.getCheckpoint().addrLastLeaf);

        assertEquals("priorAddr", segStore.getCheckpoint().addrFirstLeaf,lastLeaf.priorAddr);

        assertEquals("nextAddr", 0L, lastLeaf.nextAddr);

        // test forward scan
        {
         
            final ImmutableLeafCursor itr = seg.newLeafCursor(SeekEnum.First);
            
            assertTrue(firstLeaf == itr.leaf()); // Note: test depends on cache!
            assertNull(itr.prior());

            assertTrue(lastLeaf == itr.next()); // Note: test depends on cache!
            assertTrue(lastLeaf == itr.leaf()); // Note: test depends on cache!
            
        }
        
        /*
         * test reverse scan
         * 
         * Note: the scan starts with the last leaf in the key order and then
         * proceeds in reverse key order.
         */
        {
            
            final ImmutableLeafCursor itr = seg.newLeafCursor(SeekEnum.Last);
            
            assertTrue(lastLeaf == itr.leaf()); // Note: test depends on cache!
            assertNull(itr.next());

            assertTrue(firstLeaf == itr.prior()); // Note: test depends on cache!
            assertTrue(firstLeaf == itr.leaf()); // Note: test depends on cache!

        }
        
        // test index segment structure.
        dumpIndexSegment(seg);

        /*
         * Test the tree in detail.
         */
        {
        
            final Node A = (Node)seg.getRoot(); 
            final Leaf a = (Leaf)A.getChild(0);
            final Leaf b = (Leaf)A.getChild(1);
           
            assertKeys(new int[]{6},A);
            assertEntryCounts(new int[]{5,5},A);
            
            assertKeys(new int[]{1,2,3,4,5},a);
            assertKeys(new int[]{6,7,8,9,10},b);

            // Note: values are verified by testing the total order.

        }
        
        /*
         * Verify the total index order.
         */
        assertSameBTree(btree, seg);
        
    }

    /**
     * This case results in a single root leaf filled to capacity.
     * 
     * @throws IOException
     */
    public void test_buildOrder10() throws Exception {
        
        final BTree btree = getProblem1();

        final long commitTime = System.currentTimeMillis();
        
        new IndexSegmentBuilder(outFile, tmpDir, btree.getEntryCount(), btree
                .rangeIterator(), 10/* m */, btree.getIndexMetadata(), commitTime).call();

        /*
         * Verify that we can load the index file and that the metadata
         * associated with the index file is correct (we are only checking those
         * aspects that are easily defined by the test case and not, for
         * example, those aspects that depend on the specifics of the length of
         * serialized nodes or leaves).
         */

        final IndexSegmentStore segStore = new IndexSegmentStore(outFile);
        
        assertEquals("#nodes",0,segStore.getCheckpoint().nnodes);
        assertEquals("#leaves",1,segStore.getCheckpoint().nleaves);
        assertEquals("#entries",10,segStore.getCheckpoint().nentries);
        assertEquals("height",0,segStore.getCheckpoint().height);
        assertEquals(segStore.getCheckpoint().addrRoot,segStore.getCheckpoint().addrFirstLeaf);
        assertEquals(segStore.getCheckpoint().addrFirstLeaf,segStore.getCheckpoint().addrLastLeaf);

        final IndexSegment seg = segStore.loadIndexSegment();

        assertEquals(10,seg.getBranchingFactor());
        assertEquals(0,seg.getHeight());
        assertEquals(1,seg.getLeafCount());
        assertEquals(0,seg.getNodeCount());
        assertEquals(10,seg.getEntryCount());

        final ImmutableLeaf leaf = seg.readLeaf(segStore.getCheckpoint().addrRoot);
        assertEquals("priorAddr",0L,leaf.priorAddr);
        assertEquals("nextAddr",0L,leaf.nextAddr);
        
        final ImmutableLeafCursor itr = seg.newLeafCursor(SeekEnum.First);
        assertTrue(leaf == itr.leaf());
        assertNull(itr.prior());
        assertNull(itr.next());
        
        // test index segment structure.
        dumpIndexSegment(seg);

        /*
         * verify the right keys in the right leaves.
         */
        {
            
            Leaf root = (Leaf)seg.getRoot();
            assertKeys(new int[]{1,2,3,4,5,6,7,8,9,10},root);
            
        }
        
        /*
         * Verify the total index order.
         */
        assertSameBTree(btree, seg);
        
    }
    
    /*
     * Examples based on an input tree with 9 entries.
     */

    /**
     * Create, populate, and return a btree with a branching factor of (3) and
     * nine sequential keys [1:9]. The values are {@link SimpleEntry} objects
     * whose state is the same as the corresponding key.
     * 
     * @return The btree.
     * 
     * @see src/architecture/btree.xls, which details this input tree and a
     *      series of output trees with various branching factors.
     */
    public BTree getProblem2() {

        BTree btree = getBTree(3);

        for (int i = 1; i <= 9; i++) {

            btree.insert(KeyBuilder.asSortKey(i), new SimpleEntry(i));

        }
        
        return btree;

    }

    /**
     * This case results in a single root leaf filled to capacity.
     * 
     * @throws IOException
     */
    public void test_problem2_buildOrder3() throws Exception {
        
        final BTree btree = getProblem2();
        
        btree.dump(Level.DEBUG,System.err);        

        final long commitTime = System.currentTimeMillis();
        
        new IndexSegmentBuilder(outFile, tmpDir, btree.getEntryCount(), btree
                .rangeIterator(), 3/* m */, btree.getIndexMetadata(), commitTime).call();
        
//        new IndexSegmentBuilder(outFile,tmpDir,btree,3,0.);

         /*
          * Verify can load the index file and that the metadata
          * associated with the index file is correct (we are only
          * checking those aspects that are easily defined by the test
          * case and not, for example, those aspects that depend on the
          * specifics of the length of serialized nodes or leaves).
          */
        final IndexSegment seg = new IndexSegmentStore(outFile).loadIndexSegment();

        assertEquals(3,seg.getBranchingFactor());
        assertEquals(1,seg.getHeight());
        assertEquals(3,seg.getLeafCount());
        assertEquals(1,seg.getNodeCount());
        assertEquals(9,seg.getEntryCount());
        
        // test index segment structure.
        dumpIndexSegment(seg);

        /*
         * Test the tree in detail.
         */
        {
        
            final Node A = (Node)seg.getRoot();
            final Leaf a = (Leaf)A.getChild(0);
            final Leaf b = (Leaf)A.getChild(1);
            final Leaf c = (Leaf)A.getChild(2);
           
            assertKeys(new int[]{4,7},A);
            assertEntryCounts(new int[]{3,3,3},A);
            
            assertKeys(new int[]{1,2,3},a);
            assertKeys(new int[]{4,5,6},b);
            assertKeys(new int[]{7,8,9},c);

            // Note: values are verified by testing the total order.

        }
        
        /*
         * Verify the total index order.
         */
        assertSameBTree(btree, seg);
        
    }

    /*
     * problem3
     */

    /**
     * Create, populate, and return a btree with a branching factor of (3) and
     * 20 sequential keys [1:20]. The resulting tree has a height of (3). The
     * values are {@link SimpleEntry} objects whose state is the same as the
     * corresponding key.
     * 
     * @return The btree.
     * 
     * @see src/architecture/btree.xls, which details this input tree and a
     *      series of output trees with various branching factors.
     */
    public BTree getProblem3() {

        BTree btree = getBTree(3);

        for (int i = 1; i <= 20; i++) {

            btree.insert(KeyBuilder.asSortKey(i), new SimpleEntry(i));

        }
        
        return btree;

    }

    /**
     * Note: This problem requires us to short a node in the level above the
     * leaves so that the last node in that level does not underflow.
     * 
     * @throws IOException
     */
    public void test_problem3_buildOrder3() throws Exception {

        final BTree btree = getProblem3();

        btree.dump(Level.DEBUG,System.err);

        final long commitTime = System.currentTimeMillis();
        
        new IndexSegmentBuilder(outFile, tmpDir, btree.getEntryCount(), btree
                .rangeIterator(), 3/* m */, btree.getIndexMetadata(), commitTime).call();

//        new IndexSegmentBuilder(outFile,tmpDir,btree,3,0.);

         /*
          * Verify can load the index file and that the metadata
          * associated with the index file is correct (we are only
          * checking those aspects that are easily defined by the test
          * case and not, for example, those aspects that depend on the
          * specifics of the length of serialized nodes or leaves).
          */
        final IndexSegment seg = new IndexSegmentStore(outFile).loadIndexSegment();

        assertEquals(3,seg.getBranchingFactor());
        assertEquals(2,seg.getHeight());
        assertEquals(7,seg.getLeafCount());
        assertEquals(4,seg.getNodeCount());
        assertEquals(20,seg.getEntryCount());

        // test index segment structure.
        dumpIndexSegment(seg);

        /*
         * Test the tree in detail.
         */
        {
        
            final Node D = (Node)seg.getRoot();
            final Node A = (Node)D.getChild(0); 
            final Node B = (Node)D.getChild(1);
            final Node C = (Node)D.getChild(2);
            final Leaf a = (Leaf)A.getChild(0);
            final Leaf b = (Leaf)A.getChild(1);
            final Leaf c = (Leaf)A.getChild(2);
            final Leaf d = (Leaf)B.getChild(0);
            final Leaf e = (Leaf)B.getChild(1);
            final Leaf f = (Leaf)C.getChild(0);
            final Leaf g = (Leaf)C.getChild(1);
           
            assertKeys(new int[]{10,16},D);
            assertEntryCounts(new int[]{9,6,5},D);
            
            assertKeys(new int[]{4,7},A);
            assertEntryCounts(new int[]{3,3,3},A);
            
            assertKeys(new int[]{13},B);
            assertEntryCounts(new int[]{3,3},B);
            
            assertKeys(new int[]{19},C);
            assertEntryCounts(new int[]{3,2},C);
            
            assertKeys(new int[]{1,2,3},a);
            assertKeys(new int[]{4,5,6},b);
            assertKeys(new int[]{7,8,9},c);
            assertKeys(new int[]{10,11,12},d);
            assertKeys(new int[]{13,14,15},e);
            assertKeys(new int[]{16,17,18},f);
            assertKeys(new int[]{19,20},g);

            // Note: values are verified by testing the total order.
        }
        
        /*
         * Verify the total index order.
         */
        assertSameBTree(btree, seg);
        
    }
    
}
