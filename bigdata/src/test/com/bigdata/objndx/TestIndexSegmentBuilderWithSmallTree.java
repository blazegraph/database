package com.bigdata.objndx;

import java.io.File;
import java.io.IOException;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.objndx.IndexSegment.FileStore;

/**
 * Test suite based on a small btree with known keys and values.
 * 
 * @see src/architecture/btree.xls, which has the detailed examples.
 * 
 * @todo test it all again with other interesting output branching factors.
 * 
 * @todo write another test that builds a tree of at least height 3 (four
 *       levels).
 * 
 * @todo write a stress test that builds a tree with at least 1M entries,
 *       exports it as an index segment, and then validates the index segment.
 */
public class TestIndexSegmentBuilderWithSmallTree extends AbstractBTreeTestCase {

    BTree btree;
    File outFile;

    public TestIndexSegmentBuilderWithSmallTree() {}
    
    public TestIndexSegmentBuilderWithSmallTree(String name) {super(name);}
    
    /**
     * Sets up the {@link #btree} and ensures that the {@link #outFile} on
     * which the {@link IndexSegment} will be written does not exist.
     */
    public void setUp() {

        btree = getBTree(3);

        for (int i = 1; i <= 10; i++) {

            btree.insert(i, new SimpleEntry(i));

        }

        outFile = new File(getName() + ".seg");

        if (outFile.exists() && !outFile.delete()) {

            throw new RuntimeException("Could not delete file: " + outFile);

        }

    }

    public void tearDown() {

        if (outFile != null && outFile.exists() && !outFile.delete()) {

            System.err
                    .println("Warning: Could not delete file: " + outFile);

        }

    }
    
    /**
     * Test ability to build an index segment from a {@link BTree}.
     */
    public void test_buildOrder3() throws IOException {
        
        new IndexSegmentBuilder(outFile,null,btree,3);

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
        TestIndexSegmentBuilderStatics.assertEquals(3,seg.getBranchingFactor());
        TestIndexSegmentBuilderStatics.assertEquals(2,seg.getHeight());
        TestIndexSegmentBuilderStatics.assertEquals(ArrayType.INT,seg.getKeyType());
        TestIndexSegmentBuilderStatics.assertEquals(4,seg.getLeafCount());
        TestIndexSegmentBuilderStatics.assertEquals(3,seg.getNodeCount());
        TestIndexSegmentBuilderStatics.assertEquals(10,seg.size());
        
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
        
        new IndexSegmentBuilder(outFile,null,btree,9);

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
        TestIndexSegmentBuilderStatics.assertEquals(9,seg.getBranchingFactor());
        TestIndexSegmentBuilderStatics.assertEquals(1,seg.getHeight());
        TestIndexSegmentBuilderStatics.assertEquals(ArrayType.INT,seg.getKeyType());
        TestIndexSegmentBuilderStatics.assertEquals(2,seg.getLeafCount());
        TestIndexSegmentBuilderStatics.assertEquals(1,seg.getNodeCount());
        TestIndexSegmentBuilderStatics.assertEquals(10,seg.size());
        
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
        
        new IndexSegmentBuilder(outFile,null,btree,10);

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
        TestIndexSegmentBuilderStatics.assertEquals(10,seg.getBranchingFactor());
        TestIndexSegmentBuilderStatics.assertEquals(0,seg.getHeight());
        TestIndexSegmentBuilderStatics.assertEquals(ArrayType.INT,seg.getKeyType());
        TestIndexSegmentBuilderStatics.assertEquals(1,seg.getLeafCount());
        TestIndexSegmentBuilderStatics.assertEquals(0,seg.getNodeCount());
        TestIndexSegmentBuilderStatics.assertEquals(10,seg.size());
        
        /*
         * @todo verify the right keys in the right leaves.
         */
        
        /*
         * Verify the total index order.
         */
        assertSameBTree(btree, seg);
        
    }
    
}
