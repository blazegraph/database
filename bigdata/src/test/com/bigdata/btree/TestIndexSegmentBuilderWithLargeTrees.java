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

import com.bigdata.LRUNexus;
import com.bigdata.btree.IndexSegmentBuilder.BuildEnum;
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
    
    // all the builds in this test suite use this flag.
    static private final boolean compactingMerge = true;

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
     * Return a {@link BTree} backed by a journal with the indicated branching
     * factor.
     * <em>The caller MUST take responsibility for destroying the backing
     * {@link Journal}</strong>.  For example, {@link #doBuildIndexSegmentAndCompare(BTree)}
     * does this after running a series of builds based on the returned {@link BTree}. If
     * you do not close and destroy the backing journal then the thread pool will grow during
     * the test runs.
     * 
     * @param branchingFactor
     *            The branching factor.
     * 
     * @return The btree.
     */
    @Override
    public BTree getBTree(final int branchingFactor) {

        final Journal journal = new Journal(getProperties());

        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

        metadata.setBranchingFactor(branchingFactor);

        final BTree btree = BTree.create(journal, metadata);

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
            
            final int m = branchingFactors[i];
            
            doBuildIndexSegmentAndCompare( doSplitWithRandomDenseKeySequence( getBTree(m), m, m ) );
            
            doBuildIndexSegmentAndCompare( doSplitWithRandomDenseKeySequence( getBTree(m), m, m*m ) );

            doBuildIndexSegmentAndCompare( doSplitWithRandomDenseKeySequence( getBTree(m), m, m*m*m ) );

//            // Note: overflows the initial journal extent.
//            doBuildIndexSegmentAndCompare( doSplitWithRandomDenseKeySequence( getBTree(m), m, m*m*m*m ) );

        }
        
    }
    
    /**
     * A stress test for building {@link IndexSegment}s. A variety of
     * {@link BTree}s are built from sparse random keys using a variety of
     * branching factors. For each {@link BTree}, a variety of
     * {@link IndexSegment}s are built using a variety of output branching
     * factors. For each {@link IndexSegment}, we then compare it against its
     * source {@link BTree} for the same total ordering.
     */
    public void test_randomSparseKeys() throws Exception {

        final int trace = 0;
        
        for (int i = 0; i < branchingFactors.length; i++) {

            final int m = branchingFactors[i];

            doBuildIndexSegmentAndCompare(doInsertRandomSparseKeySequenceTest(
                    getBTree(m), m, trace));

            doBuildIndexSegmentAndCompare(doInsertRandomSparseKeySequenceTest(
                    getBTree(m), m * m, trace));

            doBuildIndexSegmentAndCompare(doInsertRandomSparseKeySequenceTest(
                    getBTree(m), m * m * m, trace));

            // Note: overflows the initial journal extent.
//            doBuildIndexSegmentAndCompare( doInsertRandomSparseKeySequenceTest(m,m*m*m*m,trace) );

        }
    
    }
    
//    /**
//     * This stress test is designed to expose problems when most or all of the
//     * rangeCount given to the {@link IndexSegmentBuilder} represents an over
//     * count of the tuples actually in the source {@link BTree}. A series of
//     * source B+Trees are generated with random data. An {@link IndexSegment} is
//     * built for each source {@link BTree} and then compared against the source
//     * {@link BTree}. The {@link IndexSegment} is then destroyed, some tuples
//     * are removed from the source {@link BTree}, and a new {@link IndexSegment}
//     * is built from the source {@link BTree} giving the original rangeCount for
//     * the source {@link BTree}. This process continues until the source
//     * {@link BTree} is empty.
//     * 
//     * @todo This test fails due to an incomplete feature in the
//     *       {@link IndexSegmentBuilder} which currently does not support
//     *       overreporting of the range count for the build. I am working
//     *       on that right now.
//     */
//    public void test_overreportDuringBuild() throws Exception {
//
//        final int trace = 0;
//
//        /*
//         * This is the source branching factor. It does not have much impact on
//         * the build since the build runs from an iterator.
//         */
//        final int m = 3;
//        
//        final int[] ntuples = new int[] { 10, 100, 1000, 10000 };
//
//        for (int n : ntuples) {
//
//            doBuildIndexSegmentAndCompare2(doInsertRandomSparseKeySequenceTest(
//                    m, n, trace));
//
//        }
//
//    }

    /**
     * Test helper builds an index segment from the btree using several
     * different branching factors and each time compares the resulting total
     * ordering to the original btree.
     * 
     * @param btree The source btree.
     */
    public void doBuildIndexSegmentAndCompare(final BTree btree)
            throws Exception {

        try {
            // branching factors used for the index segment.
            final int branchingFactors[] = new int[] {
            /*
             * This is the minimum branching factor (maximum depth, lots of edge
             * conditions).
             */
            3,
            /*
             * This is the caller's branching factor, whatever that might be.
             */
            btree.getBranchingFactor(),
            /*
             * Various large branching factors, at least one of which should be
             * odd to exercise the fence posts involved in handling odd
             * branching factors.
             */
            257, 512, 4196, 8196 };

            for (int i = 0; i < branchingFactors.length; i++) {

                final int m = branchingFactors[i];

                for (BuildEnum buildEnum : BuildEnum.values()) {

                    doBuildIndexSegmentAndCompare(getName(), btree, m,
                            buildEnum, true/* bufferNodes */);

                    doBuildIndexSegmentAndCompare(getName(), btree, m,
                            buildEnum, false/* bufferNodes */);

                }

            } // build index segment with the next branching factor.

        } finally {
            
            /*
             * Closing the journal.
             */
            
            btree.getStore().destroy();
            
            if (log.isInfoEnabled())
                log.info("Closed journal.");

        }

    }

    /**
     * Builds an {@link IndexSegment} from the source {@link BTree} using each
     * of the supported build mechanisms and validates the generated
     * {@link IndexSegment} against the source {@link BTree}.
     * 
     * @param prefix
     *            A file name prefix. The actual file name is formed using the
     *            branching factor and {@link BuildEnum} as well.
     * @param btree
     *            The source {@link BTree}.
     * @param m
     *            The output branching factor for the build.
     * @param buildEnum
     *            The algorithm to be used.
     * @return The builder, which has metadata about the operation.
     * @throws Exception
     */
    static private IndexSegmentBuilder doBuildIndexSegmentAndCompare(
            final String prefix, final BTree btree, final int m,
            final BuildEnum buildEnum, final boolean bufferNodes)
            throws Exception {

        final IndexSegmentBuilder builder = doBuildIndexSegment(prefix, btree,
                m, buildEnum, bufferNodes);

        if (log.isInfoEnabled())
            log.info("Opening index segment.");

        final IndexSegment seg = new IndexSegmentStore(builder.outFile)
                .loadIndexSegment();
        
        try {

            doIndexSegmentCompare(btree, seg);

        } finally {

            seg.getStore().destroy();

            if (builder.outFile.exists()) {
                
                log.warn("Did not delete index segment: " + builder.outFile);
                
            }

        }

        return builder;

    }

    /**
     * Does the build, returns the builder. Clears the cache for the generated
     * index segment so the caller can verify that the data on the disk is good,
     * rather than the records as placed into the cache.
     * 
     * @param prefix
     *            A file name prefix. The actual file name is formed using the
     *            branching factor and {@link BuildEnum} as well.
     * @param btree
     * @param m
     * @param buildEnum
     * @return
     * @throws Exception
     */
    static IndexSegmentBuilder doBuildIndexSegment(final String prefix,
            final BTree btree, final int m, final BuildEnum buildEnum,
            final boolean bufferNodes) throws Exception {

        final File outFile = new File(prefix + "_m" + m + "_" + buildEnum
                + ".seg");

        if (outFile.exists() && !outFile.delete()) {
            fail("Could not delete old index segment: "
                    + outFile.getAbsoluteFile());
        }

        final File tmpDir = outFile.getAbsoluteFile().getParentFile();

        final long commitTime = System.currentTimeMillis();

        if (log.isInfoEnabled())
            log.info("Building index segment: in(m="
                + btree.getBranchingFactor() + ", rangeCount="
                + btree.rangeCount() + "), out(m=" + m + "), buildEnum="
                + buildEnum+", bufferNodes="+bufferNodes);

        final IndexSegmentBuilder builder;
        switch (buildEnum) {
        case TwoPass:
            builder = IndexSegmentBuilder.newInstanceTwoPass(btree, outFile,
                    tmpDir, m, compactingMerge, commitTime, null/* fromKey */,
                    null/* toKey */, bufferNodes);
            break;
        case FullyBuffered:
            builder = IndexSegmentBuilder.newInstanceFullyBuffered(btree,
                    outFile, tmpDir, m, compactingMerge, commitTime,
                    null/* fromKey */, null/* toKey */, bufferNodes);
            break;
        default:
            throw new AssertionError(buildEnum.toString());
        }
        
        final IndexSegmentCheckpoint checkpoint = builder.call();
        
        if (LRUNexus.INSTANCE != null) {

            /*
             * Clear the records for the index segment from the cache so we will
             * read directly from the file. This is necessary to ensure that the
             * data on the file is good rather than just the data in the cache.
             */
            
            LRUNexus.INSTANCE.deleteCache(checkpoint.segmentUUID);

        }

        return builder;

    }

    /**
     * Extends {@link #assertSameBTree(AbstractBTree, IIndex)} to also test the
     * fast forward and reverse scans and similar {@link IndexSegment} specific
     * logic.
     */
    static private void doIndexSegmentCompare(final BTree btree,
            final IndexSegment seg) {

        if (log.isInfoEnabled())
            log.info("Verifying index segment: " + seg.getStore().getFile());

        try {

            // verify fast forward leaf scan.
            testForwardScan(seg);

            // verify fast reverse leaf scan.
            testReverseScan(seg);

            // test the multi-block iterator.
            testMultiBlockIterator(btree, seg);
            
            /*
             * Verify the total index order.
             */
            if (log.isInfoEnabled())
                log.info("Verifying index segment.");
            assertSameBTree(btree, seg);

            if (log.isInfoEnabled())
                log.info("Closing index segment.");
            seg.close();

            /*
             * Note: close() is a reversible operation. This verifies that by
             * immediately re-verifying the index segment. The index segment
             * SHOULD be transparently re-opened for this operation.
             */
            if (log.isInfoEnabled())
                log.info("Re-verifying index segment.");
            assertSameBTree(btree, seg);
            
        } finally {
            // Close again so that we can delete the backing file.
            if (log.isInfoEnabled())
                log.info("Re-closing index segment.");
            seg.close();
        }

    }
    
//    /**
//     * Test helper builds an index segment from the btree using several
//     * different branching factors and each time compares the resulting total
//     * ordering to the original btree.
//     * 
//     * @param btree
//     *            The source btree.
//     */
//    public void doBuildIndexSegmentAndCompare2(final BTree btree)
//            throws Exception {
//
//        /*
//         * Note: If the source BTree maintains delete markers then the
//         * entryCount will not decrease and we can not locate the undeleted
//         * tuples using the linear list API.
//         */
//        assert !btree.getIndexMetadata().getDeleteMarkers() : "The source BTree must not maintain delete markers for this unit test.";
//
//        final int reportedRangeCount = btree.getEntryCount();
//
//        /*
//         * Branching factors used for the index segment.
//         * 
//         * Note: Large branching factors are not used for this stress test
//         * because they tend to place all of the tuples into a root leaf and
//         * that case does not stress the structural adaptations which the index
//         * segment builder needs to make when it discovers that the source
//         * B+Tree was exhausted prematurely.
//         */
//        final int branchingFactors[] = new int[] {
//        /*
//         * This is the minimum branching factor (maximum depth, lots of edge
//         * conditions). m=4 and m=5 can also be included here to increase the
//         * likelihood of uncovering a structural edge case problem in the
//         * generated index segment.
//         */
//        3, 4, 5,
//        /*
//         * This is the caller's branching factor, whatever that might be.
//         */
//        btree.getBranchingFactor(),
//        /*
//         * A nice prime, but not too big.
//         */
//        13 };
//
////        /*
////         * This is the index of the current branching factor (modulo the #of
////         * branching factors.
////         */
////        int branchingFactorIndex = 0;
//        
//        /*
//         * This is incremented on each build for the source B+Tree and is used
//         * to detect non-termination.
//         */
//        int pass = 0;
//        
//        try {
//
//            // until the btree is empty.
//            while (btree.getEntryCount() > 0) {
//
//                pass++;
//
////                // select the branching factors using a round robin.
////                final int m = branchingFactors[branchingFactorIndex++
////                        % branchingFactors.length];
//
//                // for each branching factor.
//                for(int m : branchingFactors) {
//                
//                final File outFile = new File(getName() + "_m" + m + ".seg");
//
//                if (outFile.exists() && !outFile.delete()) {
//                    fail("Could not delete old index segment: "
//                            + outFile.getAbsoluteFile());
//                }
//
//                final File tmpDir = outFile.getAbsoluteFile().getParentFile();
//               
//                /*
//                 * Build the index segment.
//                 */
//                {
//                    
//                    final long commitTime = System.currentTimeMillis();
//
//                    // The actual #of tuples in the source BTree.
//                    final int actualRangeCount = btree.getEntryCount();
//
//                    System.err.println("Building index segment: in(m="
//                            + btree.getBranchingFactor()
//                            + ", actualRangeCount=" + actualRangeCount
//                            + ", reportedRangeCount=" + reportedRangeCount
//                            + "), out(m=" + m + "), pass=" + pass);
//
//                    new IndexSegmentBuilder(outFile, tmpDir, reportedRangeCount,
//                            btree.rangeIterator(), m, btree.getIndexMetadata(),
//                            commitTime, compactingMerge).call();
//                    
//                }
//                
//                /*
//                 * Verify can load the index file and that the metadata
//                 * associated with the index file is correct (we are only
//                 * checking those aspects that are easily defined by the test
//                 * case and not, for example, those aspects that depend on the
//                 * specifics of the length of serialized nodes or leaves).
//                 */
//                System.err.println("Opening index segment.");
//                final IndexSegment seg = new IndexSegmentStore(outFile)
//                        .loadIndexSegment();
//                try {
//
//                    // verify fast forward leaf scan.
//                    testForwardScan(seg);
//
//                    // verify fast reverse leaf scan.
//                    testReverseScan(seg);
//
//                    /*
//                     * Verify the total index order.
//                     */
//                    System.err.println("Verifying index segment.");
//                    assertSameBTree(btree, seg);
//
//                    System.err.println("Closing index segment.");
//                    seg.close();
//
//                    /*
//                     * Note: close() is a reversible operation. This verifies
//                     * that by immediately re-verifying the index segment. The
//                     * index segment SHOULD be transparently re-opened for this
//                     * operation.
//                     */
//                    System.err.println("Re-verifying index segment.");
//                    assertSameBTree(btree, seg);
//                } finally {
//                    // Close again so that we can delete the backing file.
//                    System.err.println("Re-closing index segment.");
//                    seg.close();
//                    seg.getStore().destroy();
//                    if (outFile.exists()) {
//                        log.warn("Did not delete index segment: " + outFile);
//                    }
//                }
//                
//                }
//
//                /*
//                 * Delete randomly selected tuples from the source BTree.
//                 * 
//                 * Note: This MUST eventually reduce the #of tuples in the
//                 * source BTree to ZERO (0) or the unit test WILL NOT terminate.
//                 */
//                {
//
//                    int entryCount = btree.getEntryCount();
//                    int ndeleted = 0;
//                    
//                    final int inc = entryCount / 5;
//
//                    final int targetCount = inc == 0
//                            // count down the tuples by one until zero.
//                            ? entryCount - 1
//                            // remove inc plus some random #of tuples down to zero.
//                            : Math.max(0, entryCount - (inc + r.nextInt(inc)));
//
//                    assert targetCount >= 0;
//                    assert targetCount < entryCount;
//                    
//                    // delete tuples.
//                    while ((entryCount = btree.getEntryCount()) > targetCount) {
//
//                        // choose a tuple at random.
//                        final int index = r.nextInt(entryCount);
//
//                        // and delete it from the source B+Tree.
//                        if (btree.remove(btree.keyAt(index)) == null) {
//
//                            // should return the deleted tuple.
//                            throw new AssertionError();
//                            
//                        }
//
//                        ndeleted++;
//                        
//                    }
//        
//                    System.err.println("Deleted " + ndeleted + " tuples of "
//                            + reportedRangeCount
//                            + " original tuples: entryCount is now "
//                            + btree.getEntryCount());
//
//                }
//                
//            } // build another index segment with the next branching factor.
//
//        } finally {
//            /*
//             * Closing the journal.
//             */
//            System.err.println("Closing journal.");
//            btree.getStore().destroy();
//        }
//
//    }

    /*
     * Note: Use IndexSegmentBuilder#main() instead.
     */
//    /**
//     * Driver for comparison of index segment build performance using different
//     * algorithms against the same source data.
//     * 
//     * @param args
//     *            <code>[opts] [ntuples]</code>, where <i>ntuples</i> defaults
//     *            to <code>1,000,000</code> and <i>opts</i> are any of:
//     *            <dl>
//     *            <dt>-m</dt>
//     *            <dd>The branching factor for the generated index segment
//     *            (default is 512).</dd>
//     *            <dt>-verify</dt>
//     *            <dd>Verify the generated index segment against the source
//     *            B+Tree.</dd>
//     *            <dt>-bloom</dt>
//     *            <dd>Enable the bloom filter for the B+Tree.</dd>
//     *            <dt>-deleteMarkers</dt>
//     *            <dd>Enable delete markers in the B+Tree.</dd>
//     *            <dt>-revisionTimestamps</dt>
//     *            <dd>Enable revision timestamps in the B+Tree.</dd>
//     *            <dt>-revisionTimestampFilters</dt>
//     *            <dd>Enable filtering on revision timestamps in the B+Tree.</dd>
//     *            </dl>
//     *            .
//     * 
//     * @throws Exception
//     * 
//     * @todo Nearly all the run time for the build is to generate the data!
//     *       Either add a parameter for the synthetic data generator or separate
//     *       that out into another main() routine and have one to generate test
//     *       data sets and another to run the builds [my preference is the
//     *       latter].
//     */
//    public static void main(final String[] args) throws Exception {
//        
//        // The target number of tuples for the build.
//        int ntuples = 1000000; // 1M.
//
//        // The output branching factor.
//        int m = 512;
//        
//        // When true, performs a correctness check against the source BTree.
//        boolean verify = false;
//
//        // When true, the bloom filter will be enabled on the index.
//        boolean bloom = false;
//
//        boolean deleteMarkers = false;
//        
//        boolean revisionTimestamps = false;
//
//        boolean revisionTimestampFilters = false;
//
//        // The journal file iff opening a pre-existing journal.
//        File file = null;
//        
//        // The name of the index from which an index segment will be generated. 
//        String name = null;
//        
//        /*
//         * Parse the command line, overriding various properties.
//         */
//        {
//
//            for (int i = 0; i < args.length; i++) {
//
//                final String arg = args[i];
//
//                if (arg.startsWith("-")) {
//
//                    if (arg.equals("-m")) {
//
//                        m = Integer.valueOf(args[++i]);
//
//                    } else if (arg.equals("-j")) {
//
//                        file = new File(args[++i]);
//
//                        if (!file.exists()) {
//
//                            throw new FileNotFoundException(file.toString());
//
//                        }
//                        
//                    } else if (arg.equals("-verify")) {
//
//                        verify = true;
//
//                    } else if (arg.equals("-deleteMarkers")) {
//
//                        deleteMarkers = true;
//
//                    } else if (arg.equals("-revisionTimestamps")) {
//
//                        revisionTimestamps = true;
//
//                    } else if (arg.equals("-revisionTimestampFilters")) {
//
//                        revisionTimestamps = revisionTimestampFilters = true;
//
//                    } else {
//
//                        throw new UnsupportedOperationException("Unknown option: "+arg);
//                        
//                    }
//                    
//                } else {
//                    
//                    ntuples = Integer.valueOf(arg);
//                    
//                }
//                
//            }
//            
//        }
//        
//        // Prefix for the files created by this test (in the temp dir).
//        final String prefix = TestIndexSegmentBuilderWithLargeTrees.class
//                .getSimpleName();
//
//        // Setup journal on which the generated B+Tree will be written.
//        final Journal journal;
//        {
//
//            if (file == null) {
//
//                // create a temporary file.
//                file = File.createTempFile(prefix, Journal.Options.JNL);
//
//            }
//
//            final Properties properties = new Properties();
//            
//            properties.setProperty(Journal.Options.FILE, file.toString());
//            
//            journal = new Journal(properties);
//            
//        }
//
//        try {
//
//            final BTree btree;
//
//            {
//
//                if (log.isInfoEnabled())
//                    log.info("Generating data: ninserts=" + ntuples);
//
//                final IndexMetadata metadata = new IndexMetadata(UUID
//                        .randomUUID());
//
//                if (bloom) {
//
//                    metadata.setBloomFilterFactory(BloomFilterFactory.DEFAULT);
//
//                }
//
//                metadata.setDeleteMarkers(deleteMarkers);
//
//                metadata.setVersionTimestampFilters(revisionTimestamps);
//
//                metadata.setVersionTimestampFilters(revisionTimestampFilters);
//
//                btree = BTree.create(journal, metadata);
//
//                doInsertRandomSparseKeySequenceTest(//
//                        btree, ntuples, // #of inserts into the source btree.
//                        0// disable trace
//                );
//
//                if (log.isInfoEnabled())
//                    log
//                            .info("Generated data: ninserts="
//                                    + ntuples
//                                    + ", userExtent="
//                                    + (journal.getBufferStrategy()
//                                            .getUserExtent() / Bytes.megabyte)
//                                    + "mb");
//
//            }
//
//            class Result {
//
//                final BuildEnum buildEnum;
//
//                final long elapsed;
//                
//                final IndexSegmentBuilder builder;
//
//                public Result(final BuildEnum buildEnum, final long elapsed,
//                        final IndexSegmentBuilder builder) {
//
//                    this.buildEnum = buildEnum;
//
//                    this.elapsed = elapsed;
//                    
//                    this.builder = builder;
//
//                }
//
//                public String toString() {
//
//                    return buildEnum//
//                            + ", " + elapsed// note: total build time!
//                            + ", " + builder.elapsed_build//
//                            + ", " + builder.elapsed_write//
//                            + ", " + builder.mbPerSec//
//                            + ", " + (builder.outFile.length()/Bytes.megabyte)//
//                    ;
//
//                }
//
//            }
//
//            final String header = "algorithm, elapsed(ms), build(ms), write(ms), mbPerSec, size(mb)";
//
//            final List<Result> results = new LinkedList<Result>();
//
//            /*
//             * Do a full scan.
//             * 
//             * Note: There can be large side effects from the order in which we
//             * do the builds since each build does at least one full scan of the
//             * source B+Tree. We hide that side effect by pre-materializing all
//             * tuples using a range iterator. Another approach would be to close
//             * out the journal, discard its cache, and then reopening the
//             * journal before each pass.
//             */
//            {
//                System.err.println("Warming up: full B+Tree scan.");
//                final ITupleIterator<?> itr = btree.rangeIterator();
//                while (itr.hasNext())
//                    itr.next();
//            }
//
//            /*
//             * Apply each build algorithm in turn.
//             */
//            for (BuildEnum buildEnum : BuildEnum.values()) {
//
//                System.err.println("Doing build: " + buildEnum);
//
//                final long begin = System.currentTimeMillis();
//                
//                final IndexSegmentBuilder builder = doBuildIndexSegment(prefix,
//                        btree, m, buildEnum);
//
//                // The total elapsed build time, including range count or pre-materialization of tuples. 
//                final long elapsed = System.currentTimeMillis() - begin;
//                
//                final Result result = new Result(buildEnum, elapsed, builder);
//
//                results.add(result);
//
//                System.out.println(result);
//
//                if (verify) {
//                    
//                    final IndexSegment seg = new IndexSegmentStore(
//                            builder.outFile).loadIndexSegment();
//
//                    try {
//
//                        doIndexSegmentCompare(btree, seg);
//
//                    } finally {
//
//                        if (builder.outFile.exists()) {
//
//                            if (!builder.outFile.delete()) {
//
//                                log.warn("Did not delete index segment: "
//                                        + builder.outFile);
//
//                            }
//
//                        }
//
//                    }
//
//                }
//
//            }
//
//            System.out.println("---- Results ----");
//            System.out.println(header);
//            for (Result result : results) {
//
//                System.out.println(result);
//
//            }
//
//        } finally {
//
//            journal.destroy();
//
//        }
//
//    }

}
