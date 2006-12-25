package com.bigdata.objndx;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Level;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.objndx.IndexSegment.FileStore;

/**
 * Test suite based on a small btree with known keys and values.
 * 
 * @see src/architecture/btree.xls, which has the detailed examples.
 * 
 * @todo test post-condition for the temporary file.
 * 
 * @todo test it all again with other interesting output branching factors.
 * 
 * @todo write another test that builds a tree of at least height 3 (four
 *       levels).
 */
public class TestIndexSegmentBuilderWithSmallTree extends AbstractBTreeTestCase {

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

            System.err
                    .println("Warning: Could not delete file: " + outFile);

        }

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

            btree.insert(i, new SimpleEntry(i));

        }
        
        return btree;

    }
    
    /**
     * Test ability to build an index segment from a {@link BTree}.
     */
    public void test_buildOrder3() throws IOException {

        BTree btree = getProblem1();
        
        new IndexSegmentBuilder(outFile,tmpDir,btree,3);

         /*
          * Verify can load the index file and that the metadata
          * associated with the index file is correct (we are only
          * checking those aspects that are easily defined by the test
          * case and not, for example, those aspects that depend on the
          * specifics of the length of serialized nodes or leaves).
          */
        final IndexSegment seg = new IndexSegment(new FileStore(outFile),
                // setup reference queue to hold all leaves and nodes.
                new HardReferenceQueue<PO>(new DefaultEvictionListener(),
                        7, 7),
                // take the other parameters from the btree.
                btree.NEGINF, btree.comparator,
                btree.nodeSer.keySerializer, btree.nodeSer.valueSerializer);
        TestIndexSegmentPlan.assertEquals(3,seg.getBranchingFactor());
        TestIndexSegmentPlan.assertEquals(2,seg.getHeight());
        TestIndexSegmentPlan.assertEquals(ArrayType.INT,seg.getKeyType());
        TestIndexSegmentPlan.assertEquals(4,seg.getLeafCount());
        TestIndexSegmentPlan.assertEquals(3,seg.getNodeCount());
        TestIndexSegmentPlan.assertEquals(10,seg.size());
        
        /*
         * @todo verify the right keys in the right leaves.
         */
        
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
    public void test_buildOrder9() throws IOException {
        
        BTree btree = getProblem1();
        
        new IndexSegmentBuilder(outFile,tmpDir,btree,9);

         /*
          * Verify can load the index file and that the metadata
          * associated with the index file is correct (we are only
          * checking those aspects that are easily defined by the test
          * case and not, for example, those aspects that depend on the
          * specifics of the length of serialized nodes or leaves).
          */
        final IndexSegment seg = new IndexSegment(new FileStore(outFile),
                // setup reference queue to hold all leaves and nodes.
                new HardReferenceQueue<PO>(new DefaultEvictionListener(),
                        3, 3),
                // take the other parameters from the btree.
                btree.NEGINF, btree.comparator,
                btree.nodeSer.keySerializer, btree.nodeSer.valueSerializer
                );
        TestIndexSegmentPlan.assertEquals(9,seg.getBranchingFactor());
        TestIndexSegmentPlan.assertEquals(1,seg.getHeight());
        TestIndexSegmentPlan.assertEquals(ArrayType.INT,seg.getKeyType());
        TestIndexSegmentPlan.assertEquals(2,seg.getLeafCount());
        TestIndexSegmentPlan.assertEquals(1,seg.getNodeCount());
        TestIndexSegmentPlan.assertEquals(10,seg.size());
        
        /*
         * @todo verify the right keys in the right leaves.
         */
        
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
    public void test_buildOrder10() throws IOException {
        
        BTree btree = getProblem1();
        
        new IndexSegmentBuilder(outFile,tmpDir,btree,10);

         /*
          * Verify can load the index file and that the metadata
          * associated with the index file is correct (we are only
          * checking those aspects that are easily defined by the test
          * case and not, for example, those aspects that depend on the
          * specifics of the length of serialized nodes or leaves).
          */
        final IndexSegment seg = new IndexSegment(new FileStore(outFile),
                // setup reference queue to hold all leaves and nodes.
                new HardReferenceQueue<PO>(new DefaultEvictionListener(),
                        1, 1),
                // take the other parameters from the btree.
                btree.NEGINF, btree.comparator,
                btree.nodeSer.keySerializer, btree.nodeSer.valueSerializer
                );
        TestIndexSegmentPlan.assertEquals(10,seg.getBranchingFactor());
        TestIndexSegmentPlan.assertEquals(0,seg.getHeight());
        TestIndexSegmentPlan.assertEquals(ArrayType.INT,seg.getKeyType());
        TestIndexSegmentPlan.assertEquals(1,seg.getLeafCount());
        TestIndexSegmentPlan.assertEquals(0,seg.getNodeCount());
        TestIndexSegmentPlan.assertEquals(10,seg.size());
        
        /*
         * @todo verify the right keys in the right leaves.
         */
        
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

            btree.insert(i, new SimpleEntry(i));

        }
        
        return btree;

    }

    /**
     * This case results in a single root leaf filled to capacity.
     * 
     * @throws IOException
     */
    public void test_problem2_buildOrder3() throws IOException {
        
        BTree btree = getProblem2();
        
        btree.dump(Level.DEBUG,System.err);
        
        new IndexSegmentBuilder(outFile,tmpDir,btree,3);

         /*
          * Verify can load the index file and that the metadata
          * associated with the index file is correct (we are only
          * checking those aspects that are easily defined by the test
          * case and not, for example, those aspects that depend on the
          * specifics of the length of serialized nodes or leaves).
          */
        final IndexSegment seg = new IndexSegment(new FileStore(outFile),
                // setup reference queue to hold all leaves and nodes.
                new HardReferenceQueue<PO>(new DefaultEvictionListener(),
                        3, 3),
                // take the other parameters from the btree.
                btree.NEGINF, btree.comparator,
                btree.nodeSer.keySerializer, btree.nodeSer.valueSerializer
                );
        TestIndexSegmentPlan.assertEquals(3,seg.getBranchingFactor());
        TestIndexSegmentPlan.assertEquals(1,seg.getHeight());
        TestIndexSegmentPlan.assertEquals(ArrayType.INT,seg.getKeyType());
        TestIndexSegmentPlan.assertEquals(3,seg.getLeafCount());
        TestIndexSegmentPlan.assertEquals(1,seg.getNodeCount());
        TestIndexSegmentPlan.assertEquals(9,seg.size());
        
        /*
         * @todo verify the right keys in the right leaves.
         */
        
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

            btree.insert(i, new SimpleEntry(i));

        }
        
        return btree;

    }

    /**
     * Note: This problem requires us to short a node in the level above the
     * leaves so that the last node in that level does not underflow.
     * 
     * @throws IOException
     */
    public void test_problem3_buildOrder3() throws IOException {

        BTree btree = getProblem3();

        btree.dump(Level.DEBUG,System.err);
        
        new IndexSegmentBuilder(outFile,tmpDir,btree,3);

         /*
          * Verify can load the index file and that the metadata
          * associated with the index file is correct (we are only
          * checking those aspects that are easily defined by the test
          * case and not, for example, those aspects that depend on the
          * specifics of the length of serialized nodes or leaves).
          */
        final IndexSegment seg = new IndexSegment(new FileStore(outFile),
                // setup reference queue to hold all leaves and nodes.
                new HardReferenceQueue<PO>(new DefaultEvictionListener(),
                        11, 11),
                // take the other parameters from the btree.
                btree.NEGINF, btree.comparator,
                btree.nodeSer.keySerializer, btree.nodeSer.valueSerializer
                );
        TestIndexSegmentPlan.assertEquals(3,seg.getBranchingFactor());
        TestIndexSegmentPlan.assertEquals(2,seg.getHeight());
        TestIndexSegmentPlan.assertEquals(ArrayType.INT,seg.getKeyType());
        TestIndexSegmentPlan.assertEquals(7,seg.getLeafCount());
        TestIndexSegmentPlan.assertEquals(4,seg.getNodeCount());
        TestIndexSegmentPlan.assertEquals(20,seg.size());
        
        /*
         * @todo verify the right keys in the right leaves.
         */
        
        /*
         * Verify the total index order.
         */
        assertSameBTree(btree, seg);
        
    }
    
}
