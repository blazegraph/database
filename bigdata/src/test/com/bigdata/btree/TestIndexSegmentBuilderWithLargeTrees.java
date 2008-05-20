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
/*
 * Created on Dec 21, 2006
 */

package com.bigdata.btree;

import java.io.File;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;

/**
 * Test build trees on the journal, evicts them into an {@link IndexSegment},
 * and then compares the trees for the same total ordering.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIndexSegmentBuilderWithLargeTrees extends AbstractIndexSegmentTestCase {

    public TestIndexSegmentBuilderWithLargeTrees() {
    }

    public TestIndexSegmentBuilderWithLargeTrees(String name) {
        super(name);
    }

    public Properties getProperties() {

        if (properties == null) {

            properties = super.getProperties();

            properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk
                    .toString());

            properties.setProperty(Options.CREATE_TEMP_FILE, "true");

            properties.setProperty(Options.DELETE_ON_EXIT, "true");

        }

        return properties;

    }

    private Properties properties;

    /**
     * Return a btree backed by a journal with the indicated branching factor.
     * The serializer requires that values in leaves are {@link SimpleEntry}
     * objects.
     * 
     * @param branchingFactor
     *            The branching factor.
     * 
     * @return The btree.
     */
    public BTree getBTree(int branchingFactor) {

        Journal journal = new Journal(getProperties());

        IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        
        metadata.setBranchingFactor(branchingFactor);
        
        BTree btree = BTree.create(journal,metadata);

        return btree;

    }
    
// /**
// * Test exercises a known problem case.
// */
// public void test_buildOrder10n3() throws IOException {
//        
//        int[] keys = new int[]{1,3,5};
//        SimpleEntry[] vals = new SimpleEntry[] {
//                new SimpleEntry(1),
//                new SimpleEntry(3),
//                new SimpleEntry(5),
//        };
//
//        BTree btree = getBTree(3);
//        
//        for(int i=0;i<keys.length; i++) {
//            btree.insert(keys[i], vals[i]);
//        }
//
//        File outFile = new File(getName());
//        
//        if(outFile.exists() && !outFile.delete()) {
//            fail("Could not delete file: "+outFile);
//        }
//        
//        new IndexSegmentBuilder(outFile,outFile.getAbsoluteFile().getParentFile(),btree,10);
//
//         /*
//          * Verify can load the index file and that the metadata
//          * associated with the index file is correct (we are only
//          * checking those aspects that are easily defined by the test
//          * case and not, for example, those aspects that depend on the
//          * specifics of the length of serialized nodes or leaves).
//          */
//        final IndexSegment seg = new IndexSegment(new IndexSegmentFileStore(outFile),
//                // setup reference queue to hold all leaves and nodes.
//                new HardReferenceQueue<PO>(new DefaultEvictionListener(),
//                        1, 1),
//                // take the other parameters from the btree.
//                btree.NEGINF, btree.comparator,
//                btree.nodeSer.keySerializer, btree.nodeSer.valueSerializer);
//        TestIndexSegmentPlan.assertEquals(10,seg.getBranchingFactor());
//        TestIndexSegmentPlan.assertEquals(0,seg.getHeight());
//        TestIndexSegmentPlan.assertEquals(ArrayType.INT,seg.getKeyType());
//        TestIndexSegmentPlan.assertEquals(1,seg.getLeafCount());
//        TestIndexSegmentPlan.assertEquals(0,seg.getNodeCount());
//        TestIndexSegmentPlan.assertEquals(3,seg.size());
//        
//        /*
//         * Verify the total index order.
//         */
//        assertSameBTree(btree, seg);
//        
//    }

    /**
     * Branching factors for the source btree that is then used to build an
     * {@link IndexSegment}. This parameter indirectly determines both the #of
     * leaves and the #of entries in the source btree.
     * <p>
     * Note: Regardless of the branching factor in the source btree, the same
     * {@link IndexSegment} should be built for a given set of entries
     * (key-value pairs) and a given output branching factor for the
     * {@link IndexSegment}. However, input trees of different heights also
     * stress different parts of the algorithm.
     */
    final int[] branchingFactors = new int[]{
            3,4,5,
            32
            };
    //64};//128};//,512};
    
    /**
     * A stress test for building {@link IndexSegment}s. A variety of
     * {@link BTree}s are built from dense random keys in [1:n] using a variety
     * of branching factors. For each {@link BTree}, a variety of
     * {@link IndexSegment}s are built using a variety of output branching
     * factors. For each {@link IndexSegment}, we then compare it against its
     * source {@link BTree} for the same total ordering.
     */
    public void test_randomDenseKeys() throws Exception {

        for(int i=0; i<branchingFactors.length; i++) {
            
            int m = branchingFactors[i];
            
            doBuildIndexSegmentAndCompare( doSplitWithRandomDenseKeySequence( getBTree(m), m, m ) );
            
            doBuildIndexSegmentAndCompare( doSplitWithRandomDenseKeySequence( getBTree(m), m, m*m ) );

            doBuildIndexSegmentAndCompare( doSplitWithRandomDenseKeySequence( getBTree(m), m, m*m*m ) );

//            // Note: overflows the initial journal extent.
//            doBuildIndexSegmentAndCompare( doSplitWithRandomDenseKeySequence( getBTree(m), m, m*m*m*m ) );

        }
        
    }
    
    /**
     * A stress test for building {@link IndexSegment}s. A variety of
     * {@link BTree}s are built from spase random keys using a variety of
     * branching factors. For each {@link BTree}, a variety of
     * {@link IndexSegment}s are built using a variety of output branching
     * factors. For each {@link IndexSegment}, we then compare it against its
     * source {@link BTree} for the same total ordering.
     */
    public void test_randomSparseKeys() throws Exception {

        int trace = 0;
        
        for(int i=0; i<branchingFactors.length; i++) {
            
            int m = branchingFactors[i];
            
            doBuildIndexSegmentAndCompare( doInsertRandomSparseKeySequenceTest(m,m,trace));
            
            doBuildIndexSegmentAndCompare( doInsertRandomSparseKeySequenceTest(m,m*m,trace) );

            doBuildIndexSegmentAndCompare( doInsertRandomSparseKeySequenceTest(m,m*m*m,trace) );

            // Note: overflows the initial journal extent.
//            doBuildIndexSegmentAndCompare( doInsertRandomSparseKeySequenceTest(m,m*m*m*m,trace) );

        }
    
    }

    /**
     * Test helper builds an index segment from the btree using several
     * different branching factors and each time compares the resulting total
     * ordering to the original btree.
     * 
     * @param btree The source btree.
     */
    public void doBuildIndexSegmentAndCompare(BTree btree) throws Exception {
        
        // branching factors used for the index segment.
        final int branchingFactors[] = new int[] { 257, 512, 4196, 8196};
        
        for( int i=0; i<branchingFactors.length; i++ ) {
        
            int m = branchingFactors[i];

            final File outFile = new File(getName()+"_m"+m+ ".seg");

            if( outFile.exists() && ! outFile.delete() ) {
                fail("Could not delete old index segment: "+outFile.getAbsoluteFile());
            }
            
            final File tmpDir = outFile.getAbsoluteFile().getParentFile(); 
            
            /*
             * Build the index segment.
             */
            System.err.println("Building index segment: in(m="
                    + btree.getBranchingFactor() + ", nentries=" + btree.getEntryCount()
                    + "), out(m=" + m + ")");

            final long commitTime = System.currentTimeMillis();
            
            new IndexSegmentBuilder(outFile, tmpDir, btree.getEntryCount(),
                    btree.entryIterator(), m, btree.getIndexMetadata(), commitTime).call();

            /*
             * Verify can load the index file and that the metadata associated
             * with the index file is correct (we are only checking those
             * aspects that are easily defined by the test case and not, for
             * example, those aspects that depend on the specifics of the length
             * of serialized nodes or leaves).
             */
            System.err.println("Opening index segment.");
            final IndexSegment seg = new IndexSegmentStore(outFile).loadIndexSegment();
            
            // verify fast forward leaf scan.
            testForwardScan(seg);

            // verify fast reverse leaf scan.
            testReverseScan(seg);

            /*
             * Verify the total index order.
             */
            System.err.println("Verifying index segment.");
            assertSameBTree(btree, seg);

            System.err.println("Closing index segment.");
            seg.close();

            /*
             * Note: close() is a reversable operation. This verifies that by
             * immediately re-verifying the index segment. The index segment
             * SHOULD be transparently re-opened for this operation.
             */
            System.err.println("Re-verifying index segment.");
            assertSameBTree(btree, seg);

            // Close again so that we can delete the backing file.
            System.err.println("Re-closing index segment.");
            seg.close();
            
            if (!outFile.delete()) {

                log.warn("Could not delete index segment: " + outFile);

            }

        } // build index segment with the next branching factor.

        /*
         * Closing the journal.
         */
        System.err.println("Closing journal.");
        btree.getStore().closeAndDelete();
        
    }

}
