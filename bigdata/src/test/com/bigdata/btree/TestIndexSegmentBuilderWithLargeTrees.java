/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Dec 21, 2006
 */

package com.bigdata.btree;

import java.io.File;
import java.io.IOException;
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
public class TestIndexSegmentBuilderWithLargeTrees extends AbstractBTreeTestCase {

    public TestIndexSegmentBuilderWithLargeTrees() {
    }

    public TestIndexSegmentBuilderWithLargeTrees(String name) {
        super(name);
    }

    public Properties getProperties() {

        if (properties == null) {

            properties = super.getProperties();

            properties.setProperty(Options.BUFFER_MODE, BufferMode.Direct
                    .toString());

            properties.setProperty(Options.CREATE_TEMP_FILE, "true");

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

        BTree btree = new BTree(journal, branchingFactor, UUID.randomUUID(),
                SimpleEntry.Serializer.INSTANCE);

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
     * 
     * Note: Regardless of the branching factor in the source btree, the same
     * {@link IndexSegment} should be build for a given set of entries
     * (key-value pairs) and a given output branching factor for the
     * {@link IndexSegment}. However, input trees of different heights also
     * stress different parts of the algorithm.
     */
    final int[] branchingFactors = new int[]{3,4,5,10,20};//64};//128};//,512};
    
    /**
     * A stress test for building {@link IndexSegment}s. A variety of
     * {@link BTree}s are built from dense random keys in [1:n] using a variety
     * of branching factors. For each {@link BTree}, a variety of
     * {@link IndexSegment}s are built using a variety of output branching
     * factors. For each {@link IndexSegment}, we then compare it against its
     * source {@link BTree} for the same total ordering.
     */
    public void test_randomDenseKeys() throws IOException {

        for(int i=0; i<branchingFactors.length; i++) {
            
            int m = branchingFactors[i];
            
            doBuildIndexSegmentAndCompare( doSplitWithRandomDenseKeySequence( getBTree(m), m, m ) );
            
            doBuildIndexSegmentAndCompare( doSplitWithRandomDenseKeySequence( getBTree(m), m, m*m ) );

            doBuildIndexSegmentAndCompare( doSplitWithRandomDenseKeySequence( getBTree(m), m, m*m*m ) );

            // Note: overflows the initial journal extent.
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
    public void test_randomSparseKeys() throws IOException {

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
    public void doBuildIndexSegmentAndCompare(BTree btree) throws IOException {
        
        // branching factors used for the index segment.
        final int branchingFactors[] = new int[]{3,4,5,10,20,60,100,256,1024,4096,8192};
        
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
            
            new IndexSegmentBuilder(outFile, tmpDir, btree, m, 0.);

            /*
             * Verify can load the index file and that the metadata associated
             * with the index file is correct (we are only checking those
             * aspects that are easily defined by the test case and not, for
             * example, those aspects that depend on the specifics of the length
             * of serialized nodes or leaves).
             */
            System.err.println("Opening index segment.");
            final IndexSegment seg = new IndexSegmentFileStore(outFile).load();
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
