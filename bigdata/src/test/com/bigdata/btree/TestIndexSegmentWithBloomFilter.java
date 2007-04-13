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

import it.unimi.dsi.mg4j.util.BloomFilter;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.btree.IndexSegmentFileStore;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;

/**
 * Test build trees on the journal, evicts them into an {@link IndexSegment},
 * and then compares the performance and correctness of index point tests with
 * and without the use of the bloom filter.
 * 
 * @todo compare performance with and without the bloom filter.
 * 
 * @todo test points that will not be in the index as well as those that are.
 * 
 * @todo report on the cost to construct the filter and its serialized size and
 *       runtime space.
 * 
 * @todo verify the target error rate.
 * 
 * @todo explore different error rates, including Fast.mostSignificantBit( n ) +
 *       1 which would provide an expectation of no false positives.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIndexSegmentWithBloomFilter extends AbstractBTreeTestCase {

    public TestIndexSegmentWithBloomFilter() {
    }

    public TestIndexSegmentWithBloomFilter(String name) {
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

        Properties properties = getProperties();

        String filename = properties.getProperty(Options.FILE);

        if (filename != null) {

            File file = new File(filename);

            if (file.exists() && !file.delete()) {

                throw new RuntimeException("Could not delete file: "
                        + file.getAbsoluteFile());

            }

        }

        System.err.println("Opening journal: " + filename);
        Journal journal = new Journal(properties);

        BTree btree = new BTree(journal, branchingFactor, UUID.randomUUID(),
                SimpleEntry.Serializer.INSTANCE);

        return btree;

    }
    
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

            // @todo overflows the initial journal extent.
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

            //@todo overflows the initial journal extent.
//            doBuildIndexSegmentAndCompare( doInsertRandomSparseKeySequenceTest(m,m*m*m*m,trace) );

        }
    
    }

    /**
     * Test when the input tree is a root leaf with three values.  The output
     * tree will also be a root leaf.
     * 
     * @throws IOException
     */
    public void test_rootLeaf() throws IOException {

        final int m = 3; // for input and output trees.
        
        BTree btree = getBTree(m);
        
        SimpleEntry v3 = new SimpleEntry(3);
        SimpleEntry v5 = new SimpleEntry(5);
        SimpleEntry v7 = new SimpleEntry(7);

        btree.insert(3, v3);
        btree.insert(5, v5);
        btree.insert(7, v7);
       
        final File outFile2 = new File(getName()+"_m"+m+ "_bloom.seg");

        if( outFile2.exists() && ! outFile2.delete() ) {
            fail("Could not delete old index segment: "+outFile2.getAbsoluteFile());
        }
        
        final File tmpDir = outFile2.getAbsoluteFile().getParentFile(); 
        
        /*
         * Build the index segment with a bloom filter.
         */
        System.err.println("Building index segment (w/ bloom): in(m="
                + btree.getBranchingFactor() + ", nentries=" + btree.getEntryCount()
                + "), out(m=" + m + ")");

        IndexSegmentBuilder builder2 = new IndexSegmentBuilder(outFile2,
                tmpDir, btree, m, 1/64.);

        // the bloom filter instance before serialization.
        BloomFilter bloomFilter = builder2.bloomFilter;
        
        // false positive tests (should succeed with resonable errorRate).
        assertTrue("3",bloomFilter.contains(i2k(3)));
        assertTrue("5",bloomFilter.contains(i2k(5)));
        assertTrue("7",bloomFilter.contains(i2k(7)));
        // correct rejections (must succeed)
        assertFalse("4",bloomFilter.contains(i2k(4)));
        assertFalse("9",bloomFilter.contains(i2k(9)));

        /*
         * Verify can load the index file and that the metadata
         * associated with the index file is correct (we are only
         * checking those aspects that are easily defined by the test
         * case and not, for example, those aspects that depend on the
         * specifics of the length of serialized nodes or leaves).
         */
        System.err.println("Opening index segment w/ bloom filter.");
        final IndexSegment seg2 = new IndexSegmentFileStore(outFile2).load();

        /*
         * Verify the total index order.
         */
        System.err.println("Verifying index segments.");
        assertSameBTree(btree, seg2);

        // the bloom filter instance that was de-serialized.
        bloomFilter = seg2.bloomFilter;
        
        // false positive tests (should succeed with resonable errorRate).
        assertTrue("3",bloomFilter.contains(i2k(3)));
        assertTrue("5",bloomFilter.contains(i2k(5)));
        assertTrue("7",bloomFilter.contains(i2k(7)));
        // correct rejections (must succeed)
        assertFalse("4",bloomFilter.contains(i2k(4)));
        assertFalse("9",bloomFilter.contains(i2k(9)));
        
        byte[][] keys = new byte[btree.getEntryCount()][];
        Object[] vals = new Object[btree.getEntryCount()];

        getKeysAndValues(btree,keys,vals);

        doRandomLookupTest("btree", btree, keys, vals);
        doRandomLookupTest("w/ bloom", seg2, keys, vals);
        
        System.err.println("Closing index segments.");
        seg2.close();

        if (!outFile2.delete()) {

            log.warn("Could not delete index segment: " + outFile2);

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
            final File outFile2 = new File(getName()+"_m"+m+ "_bloom.seg");

            if( outFile.exists() && ! outFile.delete() ) {
                fail("Could not delete old index segment: "+outFile.getAbsoluteFile());
            }
            
            if( outFile2.exists() && ! outFile2.delete() ) {
                fail("Could not delete old index segment: "+outFile2.getAbsoluteFile());
            }
            
            final File tmpDir = outFile.getAbsoluteFile().getParentFile(); 
            
            /*
             * Build the index segment.
             */
            System.err.println("Building index segment (w/o bloom): in(m="
                    + btree.getBranchingFactor() + ", nentries=" + btree.getEntryCount()
                    + "), out(m=" + m + ")");
            
            new IndexSegmentBuilder(outFile, tmpDir, btree, m, 0.);

            System.err.println("Building index segment (w/ bloom): in(m="
                    + btree.getBranchingFactor() + ", nentries=" + btree.getEntryCount()
                    + "), out(m=" + m + ")");

            IndexSegmentBuilder builder2 = new IndexSegmentBuilder(outFile2,
                    tmpDir, btree, m, 1/64.);

            /*
             * Verify can load the index file and that the metadata
             * associated with the index file is correct (we are only
             * checking those aspects that are easily defined by the test
             * case and not, for example, those aspects that depend on the
             * specifics of the length of serialized nodes or leaves).
             */
            System.err.println("Opening index segment w/o bloom filter.");
            final IndexSegment seg = new IndexSegmentFileStore(outFile).load();

            /*
             * Verify can load the index file and that the metadata
             * associated with the index file is correct (we are only
             * checking those aspects that are easily defined by the test
             * case and not, for example, those aspects that depend on the
             * specifics of the length of serialized nodes or leaves).
             */
            System.err.println("Opening index segment w/ bloom filter.");
            final IndexSegment seg2 = new IndexSegmentFileStore(outFile2).load();

            /*
             * Explicitly test the bloom filter against ground truth. 
             */
            
            byte[][] keys = new byte[btree.getEntryCount()][];
            Object[] vals = new Object[btree.getEntryCount()];

            getKeysAndValues(btree,keys,vals);

            /*
             * vet the bloom filter on the index segment builder
             * (pre-serialization).
             */
            doBloomFilterTest("pre-serialization", builder2.bloomFilter, keys);
            
            /*
             * vet the bloom filter on the loaded index segment
             * (post-serialization).
             */
            doBloomFilterTest("pre-serialization", seg2.bloomFilter, keys);

            /*
             * Verify index segments against the source btree and against one
             * another.
             */
            System.err.println("Verifying index segments.");
            assertSameBTree(btree, seg);
            assertSameBTree(btree, seg2);
            seg2.close(); // close seg w/ bloom filter and the verify with implicit reopen.
            assertSameBTree(seg, seg2);

            System.err.println("Closing index segments.");
            seg.close();
            seg2.close();

            if (!outFile.delete()) {

                log.warn("Could not delete index segment: " + outFile);

            }

            if (!outFile2.delete()) {

                log.warn("Could not delete index segment: " + outFile2);

            }

        } // build index segment with the next branching factor.

        /*
         * Closing the journal.
         */
        System.err.println("Closing journal.");
        btree.getStore().closeAndDelete();
        
    }

    /**
     * Test the bloom filter for false negatives given the ground truth set of
     * keys. if it reports that a key was not in the bloom filter then that is a
     * false negative. bloom filters are not supposed to have false negative.
     * 
     * @param keys
     *            The ground truth keys that were inserted into the bloom
     *            filter.
     */
    protected void doBloomFilterTest(String label, BloomFilter bloomFilter, byte[][] keys) {
        
        System.err.println("\ncondition: "+label+", size="+bloomFilter.size());
        
        int[] order = getRandomOrder(keys.length);

        for(int i=0; i<order.length; i++) {
            
            byte[] key = keys[order[i]];
            
            boolean found = bloomFilter.contains(key);
            
            assertTrue("false negative: i="+i+", key="+key, found);
            
        }
 
    }
}
