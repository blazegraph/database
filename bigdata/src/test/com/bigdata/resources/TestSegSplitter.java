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
 * Created on Dec 20, 2009
 */

package com.bigdata.resources;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import junit.framework.AssertionFailedError;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.ISimpleSplitHandler;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.btree.IndexSegmentStore;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.TestKeyBuilder;
import com.bigdata.journal.IJournal;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.service.Split;

/**
 * Unit tests for splitting an index segment based on its size on the disk, the
 * nominal size of an index partition, and an optional application level
 * constraint on the choice of the separator keys. This approach presumes a
 * compacting merge has been performed such that all history other than the
 * buffered writes is on a single index segment. The buffered writes are not
 * considered when choosing the #of splits to make and the separator keys for
 * those splits. They are simply copied afterwards onto the new index partition
 * which covers their key-range.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see src/architecture/SplitMath.xls
 */
public class TestSegSplitter extends AbstractTestSegSplitter {

    /**
     * 
     */
    public TestSegSplitter() {
    }

    /**
     * @param name
     */
    public TestSegSplitter(String name) {
        super(name);
    }

//    protected IJournal getStore() throws IOException {
//        
//        Properties p = new Properties();
//        
//        p.setProperty(Options.FILE, File.createTempFile(getName(), Options.JNL).toString());
//        
//        return new Journal(p);
//        
//    }
//    
//    protected IPartitionIdFactory pidFactory = new MockPartitionIdFactory();
//    
//    /**
//     * Always accepts the recommended separator key.
//     */
//    protected static final ISimpleSplitHandler acceptAllSplits = new ISimpleSplitHandler() {
//        
//        public byte[] getSeparatorKey(IndexSegment seg, int fromIndex, int toIndex,
//                int splitAt) {
//
//            return seg.keyAt(splitAt);
//            
//        }
//    };
//    
//    /**
//     * Always returns <code>null</code> (never accepts any splits).
//     */
//    protected static final ISimpleSplitHandler rejectAllSplits = new ISimpleSplitHandler() {
//        
//        public byte[] getSeparatorKey(IndexSegment seg, int fromIndex, int toIndex,
//                int splitAt) {
//
//            return null;
//            
//        }
//    };
//
//    /**
//     * Accepts the recommended separator key unless it is GTE the key given to
//     * the constructor, in which case it refuses to accept any splits.
//     */
//    private static class RejectSplitsAfterKey implements ISimpleSplitHandler {
//
//        private final byte[] rejectKey;
//
//        public RejectSplitsAfterKey(final byte[] rejectKey) {
//
//            if (rejectKey == null)
//                throw new IllegalArgumentException();
//
//            this.rejectKey = rejectKey;
//            
//        }
//        
//        public byte[] getSeparatorKey(final IndexSegment seg,
//                final int fromIndex, final int toIndex, final int splitAt) {
//
//            final byte[] a = seg.keyAt(splitAt);
//
//            if (BytesUtil.compareBytes(a, rejectKey) >= 0) {
//
//                // reject any split GTE the key specified to the ctor.
//                return null;
//
//            }
//
//            // Otherwise accept the recommended separator key.
//            return a;
//            
//        }
//    };
//
//    /**
//     * Generate an {@link IndexSegment} from the given BTree.
//     * 
//     * @param src
//     *            The source {@link BTree}.
//     * 
//     * @return The {@link IndexSegmentBuilder}.
//     */
//    static public IndexSegmentBuilder doBuild(final String name,
//            final ILocalBTreeView src, final long commitTime,
//            final byte[] fromKey, final byte[] toKey) throws Exception {
//
//        if (src == null)
//            throw new IllegalArgumentException();
//
//        // final String name = getName();
//
//        final File tmpDir = new File(System.getProperty("java.io.tmpdir"));
//
//        File outFile = null;
//        final IndexSegmentBuilder builder;
//        try {
//
//            // the file to be generated.
//            outFile = File.createTempFile(name, Options.SEG, tmpDir);
//
//            // new builder.
//            builder = IndexSegmentBuilder.newInstance(/*name, */src, outFile,
//                    tmpDir, true/* compactingMerge */, commitTime, fromKey,
//                    toKey);
//
//            // build the index segment.
//            builder.call();
//
//            return builder;
//        
//        } catch (Throwable t) {
//
//            if (outFile != null && outFile.exists()) {
//
//                try {
//
//                    outFile.delete();
//
//                } catch (Throwable t2) {
//
//                    log.warn(t2.getLocalizedMessage(), t2);
//
//                }
//
//            }
//
//            if (t instanceof Exception)
//                throw (Exception) t;
//
//            throw new RuntimeException(t);
//
//        }
//
//    }
//
//    /**
//     * Register a {@link BTree} against the journal, generate some data in the
//     * specified key range, and commit the data.
//     *<p>
//     * Note: this uses int values to generate the keys. If you specify the
//     * fromKey as anything other than an empty byte[] or the toKey as anything
//     * other than null then the key must be at least 4 bytes long and the 1st 4
//     * bytes will be decoded as an integer.
//     * 
//     * @param store
//     * @param ntuples
//     * @param pmd
//     */
//    private BTree generateData(final IJournal store, final int ntuples,
//            final LocalPartitionMetadata pmd) {
//        
//        assert store!=null;
//        assert pmd!=null;
//        
//        final byte[] fromKey = pmd.getLeftSeparatorKey();
//        final byte[] toKey = pmd.getRightSeparatorKey();
//
//        // inclusive lower bound.
//        final int low;
//        if (fromKey.length > 0) {
//            low = KeyBuilder.decodeInt(fromKey/* buf */, 0/* off */);
//        } else
//            low = Integer.MIN_VALUE;
//
//        // exclusive upper bound.
//        final int high;
//        if (toKey != null) {
//            high = KeyBuilder.decodeInt(toKey/* buf */, 0/* off */);
//        } else
//            high = Integer.MAX_VALUE;
//
//        /*
//         * @todo to apply an application constraint we need to pass it in here
//         * when we register the BTree (alternatively, you can work around this
//         * with how the unit test is setup).
//         */
//        final BTree btree;
//        {
//
//            final IndexMetadata md = new IndexMetadata(UUID.randomUUID());
//            
//            md.setPartitionMetadata(pmd);
//            
//            btree = (BTree) store.registerIndex(getName(), md);
//            
//        }
//
//        // generate evenly space tuples.
//        final long inc = ((long) high - (long) low) / ntuples;
//        assert inc > 0 : "inc=" + inc;
//        int ninsert = 0;
//        for (int i = low; i <= high && ninsert < ntuples; i += inc) {
//
//            btree.insert(TestKeyBuilder.asSortKey(i), i);
//
//            ninsert++;
//
//        }
//
//        // verify generated correct #of tuples.
//        assertEquals(ntuples, btree.getEntryCount());
//        
//        store.commit();
//        
//        // return view with lastCommitTime set.
//        return (BTree) store.getIndex(getName());
//        
//    }
//
//    /**
//     * Register a {@link BTree} against the journal, generate some data and
//     * commit the data (the data corresponds to a simple triple index schema but
//     * does not handle statement indices with variable length keys).
//     * 
//     * @param store
//     * @param ntuples
//     * @param pmd
//     * @param splitHandler
//     */
//    private BTree generateSPOData(final IJournal store, final int ntuples,
//            final LocalPartitionMetadata pmd) {
//        
//        assert store!=null;
//        assert pmd!=null;
//        
//        final byte[] fromKey = pmd.getLeftSeparatorKey();
//        final byte[] toKey = pmd.getRightSeparatorKey();
//        assert fromKey != null;
////        assert toKey != null;
//
//        final BTree btree;
//        {
//
//            final IndexMetadata md = new IndexMetadata(UUID.randomUUID());
//            
//            md.setPartitionMetadata(pmd);
//            
//            btree = (BTree) store.registerIndex(getName(), md);
//            
//        }
//
//        final Random r = new Random();
//        
//        final long[] s = new long[Math.max(ntuples/4,20)];
//        final long[] p = new long[Math.max(ntuples/50,5)];
//        final long[] o = new long[Math.max(ntuples/10,100)];
//        
//        long v = 0;
//        for (int i = 0; i < s.length; i++) {
//            v = (s[i] = v + r.nextInt(100));
//        }
//        for (int i = 0; i < p.length; i++) {
//            v = (p[i] = v + r.nextInt(100));
//        }
//        for (int i = 0; i < o.length; i++) {
//            v = (o[i] = v + r.nextInt(100));
//        }
//
//        final IKeyBuilder keyBuilder = KeyBuilder
//                .newInstance(Bytes.SIZEOF_LONG * 3);
//        
//        int ninsert = 0;
//        int ntries = 0;
//        while (ninsert < ntuples) {
//
//            final byte[] key = keyBuilder.reset()//
//                    .append(s[r.nextInt(s.length)])//
//                    .append(p[r.nextInt(p.length)])//
//                    .append(o[r.nextInt(o.length)])//
//                    .getKey();
//            
//            if (!btree.contains(key)) {
//
//                btree.insert(key, null/* val */);
//
//                ninsert++;
//                
//            }
//            
//            ntries++;
//
//            if (ntries > ntuples * 4) {
//
//                throw new RuntimeException(
//                        "Test setup is not terminating: ntuples=" + ntuples
//                                + ", ntries=" + ntries + ", ninsert=" + ninsert);
//
//            }
//            
//        }
//
//        // verify generated correct #of tuples.
//        assertEquals(ntuples, btree.getEntryCount());
//        
//        store.commit();
//        
//        // return view with lastCommitTime set.
//        return (BTree) store.getIndex(getName());
//        
//    }

//    /**
//     * A {@link SparseRowStore} schema used by some unit tests.
//     */
//    private final Schema schema = new Schema("Employee", "Id", KeyType.Long);
//
//    /**
//     * Register a {@link BTree} against the journal, generate some data and
//     * commit the data.
//     * <p>
//     * Note: This is a bit slow since it does a commit after each logical row
//     * inserted into the B+Tree.
//     * 
//     * @param store
//     * @param nrows
//     * @param pmd
//     */
//    private BTree generateSparseRowStoreData(final IJournal store,
//            final int nrows, final LocalPartitionMetadata pmd) {
//
//        assert store!=null;
//        assert pmd!=null;
//        
//        final byte[] fromKey = pmd.getLeftSeparatorKey();
//        final byte[] toKey = pmd.getRightSeparatorKey();
//        assert fromKey != null;
////        assert toKey != null;
//
//        final BTree btree;
//        {
//
//            final IndexMetadata md = new IndexMetadata(UUID.randomUUID());
//            
//            md.setPartitionMetadata(pmd);
//            
//            btree = (BTree) store.registerIndex(getName(), md);
//            
//        }
//
//        final SparseRowStore srs = new SparseRowStore(btree);
//
//        final String[] names = new String[] { "Bryan", "Mike", "Martyn",
//                "Paul", "Julie" };
//
//        final String[] states = new String[] {"MD","DC","UT","MA","NC"};
//        
//        // put a little randomness in to exercise the split logic more.
//        final Random r = new Random();
//        long id = 0;
//        long nadded = 0;
//        for (int i = 0; i < nrows; i++) {
//
//            final Map<String, Object> propertySet = new HashMap<String, Object>();
//
//            id += r.nextInt(20);
//            
//            propertySet.put("Id", id); // primary key.
//            propertySet.put("Name", names[i % names.length] + id);
//            propertySet.put("State", states[i % states.length] + id);
//            nadded += 3;
//
//            final int nextra = r.nextInt(10);
//            for (int j = 0; j < nextra; j++) {
//                propertySet.put("attr" + j, r.nextInt(20));
//                nadded++;
//            }
//
//            srs.write(schema, propertySet);
//
//        }
//
//        // verify generated correct #of tuples.
//        assertEquals(nadded, btree.getEntryCount());
//
//        store.commit();
//        
//        // return view with lastCommitTime set.
//        return (BTree) store.getIndex(getName());
//        
//    }
//
//    /**
//     * Mock implementation assigns index partitions from a counter beginning
//     * with ZERO (0), which is the first legal index partition identifier. The
//     * name parameter is ignored.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     */
//    static class MockPartitionIdFactory implements IPartitionIdFactory {
//
//        private int i = 0;
//
//        public int nextPartitionId(String nameIsIgnored) {
//
//            return i++;
//
//        }
//
//    }

    /**
     * Register a {@link BTree} against the journal, generate some data in the
     * specified key range, and commit the data.
     *<p>
     * Note: this uses int values to generate the keys. If you specify the
     * fromKey as anything other than an empty byte[] or the toKey as anything
     * other than null then the key must be at least 4 bytes long and the 1st 4
     * bytes will be decoded as an integer.
     * 
     * @param store
     * @param ntuples
     * @param pmd
     */
    protected BTree generateData(final IJournal store, final String name,
            final int ntuples, final LocalPartitionMetadata pmd) {
        
        assert store!=null;
        assert pmd!=null;
        
        final byte[] fromKey = pmd.getLeftSeparatorKey();
        final byte[] toKey = pmd.getRightSeparatorKey();

        // inclusive lower bound.
        final int low;
        if (fromKey.length > 0) {
            low = KeyBuilder.decodeInt(fromKey/* buf */, 0/* off */);
        } else
            low = Integer.MIN_VALUE;

        // exclusive upper bound.
        final int high;
        if (toKey != null) {
            high = KeyBuilder.decodeInt(toKey/* buf */, 0/* off */);
        } else
            high = Integer.MAX_VALUE;

        /*
         * @todo to apply an application constraint we need to pass it in here
         * when we register the BTree (alternatively, you can work around this
         * with how the unit test is setup).
         */
        final BTree btree;
        {

            final IndexMetadata md = new IndexMetadata(UUID.randomUUID());
            
            md.setPartitionMetadata(pmd);
            
            btree = (BTree) store.registerIndex(name, md);
            
        }

        // generate evenly space tuples.
        final long inc = ((long) high - (long) low) / ntuples;
        assert inc > 0 : "inc=" + inc;
        int ninsert = 0;
        for (int i = low; i <= high && ninsert < ntuples; i += inc) {

            btree.insert(TestKeyBuilder.asSortKey(i), i);

            ninsert++;

        }

        // verify generated correct #of tuples.
        assertEquals(ntuples, btree.getEntryCount());
        
        store.commit();
        
        // return view with lastCommitTime set.
        return (BTree) store.getIndex(name);
        
    }

    /**
     * Unit test the edge cases surrounding when an index partition is split.
     * 
     * @see src/architecture/SplitMath.xls, for the math concerning the #of
     *      splits to be generated from an index segment file of a given size.
     * 
     * @throws Exception
     */
    public void test_split_segment_underflow() throws Exception {

        final byte[] fromKey = new byte[0];
        final byte[] toKey = null;
        final ISimpleSplitHandler splitHandler = null;

        final int ntuples = 1000;
        
        IndexSegmentBuilder builder = null;
        final IJournal store = getStore();
        
        try {

            final LocalPartitionMetadata pmd = new LocalPartitionMetadata(
                    pidFactory.nextPartitionId(getName()),//
                    -1, // sourcePartitionId
                    fromKey, //
                    toKey,//
                    new IResourceMetadata[] { store.getResourceMetadata() }, //
                    null // cause
//                    , null // history
            );

            // Generates BTree w/ constrained keys and commits to store.
            final BTree src = generateData(store, getName(), ntuples, pmd);

            // Build the index segment (a compacting merge).
            builder = doBuild(getName(), src, src.getLastCommitTime(), fromKey, toKey);

            final IndexSegmentStore segStore = new IndexSegmentStore(
                    builder.outFile);

            /*
             * (A) The segment has one byte less than the maximum so it MUST NOT
             * be split.
             */
            try {

                final long nominalShardSize = segStore.size() + 1;

                final IndexSegment seg = segStore.loadIndexSegment();

                // Compute splits.
                final Split[] splits = SplitUtility.getSplits(pidFactory, pmd,
                        seg, nominalShardSize, splitHandler);

                assertNull("Not expecting any splits: "
                        + Arrays.toString(splits), splits);

            } finally {

                segStore.close();

            }

            /*
             * (B) This is the fence post case where segment is exactly at the
             * maximum size on the disk and MUST be split.
             * 
             * @see src/architecture/SplitMath.xls
             */
            try {

                final long nominalShardSize = segStore.size();

                final IndexSegment seg = segStore.loadIndexSegment();

                // Compute splits.
                final Split[] splits = SplitUtility.getSplits(pidFactory, pmd,
                        seg, nominalShardSize, splitHandler);

                assertNotNull(splits);

                assertEquals("#splits", 2, splits.length);

                // Validate splits.
                SplitUtility.validateSplits(pmd, splits, true/* checkStuff */);

            } finally {

                segStore.close();

            }

            /*
             * (C) Just like (B) above really.
             */
            try {

                final long nominalShardSize = segStore.size() - 1;

                final IndexSegment seg = segStore.loadIndexSegment();

                // Compute splits.
                final Split[] splits = SplitUtility.getSplits(pidFactory, pmd,
                        seg, nominalShardSize, splitHandler);

                assertNotNull(splits);

                assertEquals("#splits", 2, splits.length);

                // Validate splits.
                SplitUtility.validateSplits(pmd, splits, true/* checkStuff */);
            
            } finally {

                segStore.close();

            }

        } finally {

            if (builder != null) {

                // delete the generated index segment.
                if (!builder.outFile.delete()) {

                    log.warn("Could not delete: " + builder.outFile);

                }

            }

            store.destroy();

        }

    }

    /**
     * A unit test when the split would create two or three index segments (both
     * cases are tested here to cover the lower bound and the near lower bound).
     */
    public void test_split_lower_bounds() throws Exception {

        /*
         * Test parameters.
         */
        final byte[] fromKey = new byte[0];
        final byte[] toKey = null;
        final ISimpleSplitHandler splitHandler = null;

        final int ntuples = 1000;
        
        IndexSegmentBuilder builder = null;
        final IJournal store = getStore();
        
        try {

            final LocalPartitionMetadata pmd = new LocalPartitionMetadata(
                    pidFactory.nextPartitionId(getName()),//
                    -1, // sourcePartitionId
                    fromKey, //
                    toKey,//
                    new IResourceMetadata[] { store.getResourceMetadata() }, //
                    null // cause
//                    , null // history
            );

            // Generates BTree w/ constrained keys and commits to store.
            final BTree src = generateData(store, getName(), ntuples, pmd);

            // Build the index segment (a compacting merge).
            builder = doBuild(getName(), src, src.getLastCommitTime(), fromKey, toKey);

            final IndexSegmentStore segStore = new IndexSegmentStore(
                    builder.outFile);

            /*
             * Test ability to create two splits from the data.
             */
            try {

                final int expectedSplitCount = 2;

                final long nominalShardSize = (long) (segStore.size() / (expectedSplitCount / 2.));

                final IndexSegment seg = segStore.loadIndexSegment();

                // Compute splits.
                final Split[] splits = SplitUtility.getSplits(pidFactory, pmd,
                        seg, nominalShardSize, splitHandler);

                // Validate splits.
                SplitUtility.validateSplits(pmd, splits, true/* checkStuff */);

                assertEquals("#splits", expectedSplitCount, splits.length);

            } finally {

                segStore.close();

            }

            /*
             * Test ability to create three splits from the data.
             */
            try {

                final int expectedSplitCount = 3;

                final long nominalShardSize = (long) (segStore.size() / (expectedSplitCount / 2.));

                final IndexSegment seg = segStore.loadIndexSegment();

                // Compute splits.
                final Split[] splits = SplitUtility.getSplits(pidFactory, pmd,
                        seg, nominalShardSize, splitHandler);

                // Validate splits.
                SplitUtility.validateSplits(pmd, splits, true/* checkStuff */);

                assertEquals("#splits", expectedSplitCount, splits.length);

            } finally {

                segStore.close();

            }
            
        } finally {

            if (builder != null) {

                // delete the generated index segment.
                if(!builder.outFile.delete()) {

                    log.warn("Could not delete: "+builder.outFile);
                    
                }
                
            }
            
            store.destroy();

        }
        
    }

    /**
     * A unit test for fence posts when nominal shard size is so small that we
     * will get only one tuple into each generated split.
     */
    public void test_split_upper_bound() throws Exception {

        /*
         * Test parameters.
         */
        final byte[] fromKey = new byte[0];
        final byte[] toKey = null;
        final ISimpleSplitHandler splitHandler = null;

        final int ntuples = 1000;
        
        IndexSegmentBuilder builder = null;
        final IJournal store = getStore();
        
        try {

            final LocalPartitionMetadata pmd = new LocalPartitionMetadata(
                    pidFactory.nextPartitionId(getName()),//
                    -1, // sourcePartitionId
                    fromKey, //
                    toKey,//
                    new IResourceMetadata[] { store.getResourceMetadata() }, //
                    null // cause
//                    ,null // history
            );

            // Generates BTree w/ constrained keys and commits to store.
            final BTree src = generateData(store, getName(), ntuples, pmd);

            // Build the index segment (a compacting merge).
            builder = doBuild(getName(), src, src.getLastCommitTime(), fromKey, toKey);

            final IndexSegmentStore segStore = new IndexSegmentStore(
                    builder.outFile);

            /*
             * Test ability to create splits, each of which has two tuples.
             */
            try {

                final int expectedSplitCount = ntuples;

                final long nominalShardSize = segStore.size()
                        / expectedSplitCount;

                final IndexSegment seg = segStore.loadIndexSegment();

                // Compute splits.
                final Split[] splits = SplitUtility.getSplits(pidFactory, pmd,
                        seg, nominalShardSize, splitHandler);

                // Validate splits.
                SplitUtility.validateSplits(pmd, splits, true/* checkStuff */);

                assertEquals("#splits", expectedSplitCount, ntuples);

                for (Split split : splits) {

                    assertEquals("#tuples", 1, split.ntuples);

                }

            } finally {

                segStore.close();

            }

            
        } finally {

            if (builder != null) {

                // delete the generated index segment.
                if(!builder.outFile.delete()) {

                    log.warn("Could not delete: "+builder.outFile);
                    
                }
                
            }
            
            store.destroy();

        }
        
    }

    /**
     * A stress test which increases the target number of splits until each
     * split would have only one tuple.
     * 
     * @throws Exception
     */
    public void test_split_stress() throws Exception {

        /*
         * Test parameters.
         */
        final byte[] fromKey = new byte[0];
        final byte[] toKey = null;
        final ISimpleSplitHandler splitHandler = null;

        final int ntuples = 1000;
        
        IndexSegmentBuilder builder = null;
        final IJournal store = getStore();
        
        try {

            final LocalPartitionMetadata pmd = new LocalPartitionMetadata(
                    pidFactory.nextPartitionId(getName()),//
                    -1, // sourcePartitionId
                    fromKey, //
                    toKey,//
                    new IResourceMetadata[] { store.getResourceMetadata() }, //
                    null // cause
//                    ,null // history
            );

            // Generates BTree w/ constrained keys and commits to store.
            final BTree src = generateData(store, getName(), ntuples, pmd);

            // Build the index segment (a compacting merge).
            builder = doBuild(getName(), src, src.getLastCommitTime(), fromKey, toKey);

            final IndexSegmentStore segStore = new IndexSegmentStore(
                    builder.outFile);

            /*
             * Test ability to create N splits from the data. This does not test
             * each possible value since we cover the edge cases in other unit
             * tests in this test suite. It just advances randomly through the
             * possible interval.
             */
            final Random r = new Random();
            for (int expectedSplitCount = 2; expectedSplitCount <= ntuples; expectedSplitCount += (r
                    .nextInt(20) + 1)) {

                try {

                    final long nominalShardSize = segStore.size()
                            / expectedSplitCount;

                    final IndexSegment seg = segStore.loadIndexSegment();

                    // Compute splits.
                    final Split[] splits = SplitUtility.getSplits(pidFactory,
                            pmd, seg, nominalShardSize, splitHandler);

                    // Validate splits.
                    SplitUtility
                            .validateSplits(pmd, splits, true/* checkStuff */);

                    /*
                     * Note: This is not trying to validate that
                     * nsplits==expectedSplitCount. Should it?
                     */ 
                    
                } finally {

                    segStore.close();

                }
            }

        } finally {

            if (builder != null) {

                // delete the generated index segment.
                if(!builder.outFile.delete()) {

                    log.warn("Could not delete: "+builder.outFile);
                    
                }
                
            }
            
            store.destroy();

        }

    }

    /**
     * A unit test which examines correctness when the fromKey and/or toKey are
     * non-<code>null</code>. This models the normal case when the index segment
     * is part of any shard except the first shard of the scale-out index.
     * 
     * @throws Exception
     */
    public void test_split_fromToKeyConstraints() throws Exception {

        /*
         * Test parameters.
         */
        final ISimpleSplitHandler splitHandler = null;

        final int ntuples = 1000;

        // any builders/segments we create for the 3 output segments below.
        final List<IndexSegmentBuilder> builders = new LinkedList<IndexSegmentBuilder>();
        final List<IndexSegment> segments = new LinkedList<IndexSegment>();

        IndexSegmentBuilder builder = null;
        final IJournal store = getStore();
        
        try {

            final byte[] fromKey = new byte[0];
            final byte[] toKey = null;
            final LocalPartitionMetadata pmd = new LocalPartitionMetadata(
                    pidFactory.nextPartitionId(getName()),//
                    -1, // sourcePartitionId
                    fromKey, //
                    toKey,//
                    new IResourceMetadata[] { store.getResourceMetadata() }, //
                    null // cause
//                    ,null // history
            );

            // Generates BTree w/ constrained keys and commits to store.
            final BTree src = generateData(store, getName(), ntuples, pmd);

            // Build the index segment (a compacting merge).
            builder = doBuild(getName(), src, src.getLastCommitTime(), fromKey, toKey);

            final IndexSegmentStore segStore = new IndexSegmentStore(
                    builder.outFile);

            final IndexSegment seg = segStore.loadIndexSegment();
            try {

                /*
                 * Create three splits from the data. The first will have an
                 * empty byte[] as its leftSeparator. The second will have
                 * non-null left and right separator keys. The last will have a
                 * null-right separator key. These are our three test
                 * conditions.
                 */

                // Generate and verify the splits.
                final Split[] splits;
                {
                    final int expectedSplitCount = 3;

                    final long nominalShardSize = (long) (segStore.size() / (expectedSplitCount / 2.));

                    // Compute splits.
                    splits = SplitUtility.getSplits(pidFactory, pmd, seg,
                            nominalShardSize, splitHandler);

                    // Validate splits.
                    SplitUtility
                            .validateSplits(pmd, splits, true/* checkStuff */);

                    assertEquals("#splits", 3, splits.length);
                    
                    // 1st.
                    assertEquals(new byte[0], splits[0].pmd
                            .getLeftSeparatorKey());
                    assertNotNull(splits[0].pmd.getRightSeparatorKey());

                    // 2nd.
                    assertNotNull(splits[1].pmd.getLeftSeparatorKey());
                    assertNotNull(splits[1].pmd.getRightSeparatorKey());

                    // 3rd.
                    assertNull(splits[2].pmd.getRightSeparatorKey());
                    
                }

                /*
                 * Generate an index segment from each of those splits.
                 */
                for (int i = 0; i < 3; i++) {

                    // The split for this build.
                    final Split split = splits[i];

                    final byte[] aFromKey = src.keyAt(split.fromIndex);
                    final byte[] aToKey = src.keyAt(split.toIndex);
                    
                    // Build the index segment (a compacting merge).
                    final IndexSegmentBuilder aBuilder;
                    builders.add(aBuilder = doBuild(getName(), src, src
                            .getLastCommitTime(), aFromKey, aToKey));

                    final IndexSegmentStore aSegStore = new IndexSegmentStore(
                            aBuilder.outFile);

                    final IndexSegment aSeg;
                    segments.add(aSeg = aSegStore.loadIndexSegment());
                    

                    /*
                     * Verify that the entry iterator for the key range on the
                     * source B+Tree is the same as the entry iterator for the
                     * generated index segment.
                     */
                    try {
                        AbstractBTreeTestCase.assertSameEntryIterator(src
                                .rangeIterator(aFromKey, aToKey), aSeg
                                .rangeIterator());
                    } catch (AssertionFailedError ex) {
                        fail("Validation failed: split#=" + i + ", split="
                                + split, ex);
                    } finally {
                        aSeg.close();
                        aSegStore.close();
                    }

                }
                
            } finally {

                segStore.close();

            }
            
        } finally {

            for (IndexSegment tmp : segments) {

                // Close open segment files.
                if (tmp.isOpen())
                    tmp.close();

            }

            for (IndexSegmentBuilder tmp : builders) {

                // delete the generated index segment.
                if (!tmp.outFile.delete()) {

                    log.warn("Could not delete: " + tmp.outFile);

                }
                
            }
            
            if (builder != null) {

                // delete the generated index segment.
                if(!builder.outFile.delete()) {

                    log.warn("Could not delete: "+builder.outFile);
                    
                }
                
            }
            
            store.destroy();

        }
        
    }

    /**
     * Unit test using an {@link ISimpleSplitHandler} which accepts all splits
     * (the behavior should be the same as if there were no override).
     * 
     * @throws Exception
     */
    public void test_split_applicationConstraint_acceptAllSplits()
            throws Exception {

        /*
         * Test parameters.
         */
        final byte[] fromKey = new byte[0];
        final byte[] toKey = null;
        final ISimpleSplitHandler splitHandler = acceptAllSplits;

        final int ntuples = 1000;
        
        IndexSegmentBuilder builder = null;
        final IJournal store = getStore();
        
        try {

            final LocalPartitionMetadata pmd = new LocalPartitionMetadata(
                    pidFactory.nextPartitionId(getName()),//
                    -1, // sourcePartitionId
                    fromKey, //
                    toKey,//
                    new IResourceMetadata[] { store.getResourceMetadata() }, //
                    null // cause
//                    ,null // history
            );

            // Generates BTree w/ constrained keys and commits to store.
            final BTree src = generateData(store, getName(), ntuples, pmd);

            // Build the index segment (a compacting merge).
            builder = doBuild(getName(), src, src.getLastCommitTime(), fromKey, toKey);

            final IndexSegmentStore segStore = new IndexSegmentStore(
                    builder.outFile);

            /*
             * Test when two splits would be generated using the default
             * behavior.
             */
            try {

                final int expectedSplitCount = 2;

                final long nominalShardSize = (long) (segStore.size() / (expectedSplitCount / 2.));

                final IndexSegment seg = segStore.loadIndexSegment();

                // Compute splits.
                final Split[] splits = SplitUtility.getSplits(pidFactory, pmd,
                        seg, nominalShardSize, splitHandler);

                // Validate splits.
                SplitUtility.validateSplits(pmd, splits, true/* checkStuff */);

                assertEquals("#splits", expectedSplitCount, splits.length);

            } finally {

                segStore.close();

            }
            
        } finally {

            if (builder != null) {

                // delete the generated index segment.
                if(!builder.outFile.delete()) {

                    log.warn("Could not delete: "+builder.outFile);
                    
                }
                
            }
            
            store.destroy();

        }
        
    }

    /**
     * Unit test using an {@link ISimpleSplitHandler} which rejects all splits.
     * No {@link Split}s will be chosen and the {@link IndexSegment} WILL NOT be
     * split.
     * 
     * @throws Exception
     */
    public void test_split_applicationConstraint_rejectAllSplits()
            throws Exception {

        /*
         * Test parameters.
         */
        final byte[] fromKey = new byte[0];
        final byte[] toKey = null;
        final ISimpleSplitHandler splitHandler = rejectAllSplits;

        final int ntuples = 1000;
        
        IndexSegmentBuilder builder = null;
        final IJournal store = getStore();
        
        try {

            final LocalPartitionMetadata pmd = new LocalPartitionMetadata(
                    pidFactory.nextPartitionId(getName()),//
                    -1, // sourcePartitionId
                    fromKey, //
                    toKey,//
                    new IResourceMetadata[] { store.getResourceMetadata() }, //
                    null // cause
//                    ,null // history
            );

            // Generates BTree w/ constrained keys and commits to store.
            final BTree src = generateData(store, getName(), ntuples, pmd);

            // Build the index segment (a compacting merge).
            builder = doBuild(getName(), src, src.getLastCommitTime(), fromKey, toKey);

            final IndexSegmentStore segStore = new IndexSegmentStore(
                    builder.outFile);

            /*
             * Test when two splits would be generated using the default
             * behavior.
             */
            try {

                final int expectedSplitCount = 2;

                final long nominalShardSize = (long) (segStore.size() / (expectedSplitCount / 2.));

                final IndexSegment seg = segStore.loadIndexSegment();

                // Compute splits.
                final Split[] splits = SplitUtility.getSplits(pidFactory, pmd,
                        seg, nominalShardSize, splitHandler);

                if (splits != null) {
                 
                    fail("Not expecting any splits: " + Arrays.toString(splits));
                    
                }

            } finally {

                segStore.close();

            }
            
        } finally {

            if (builder != null) {

                // delete the generated index segment.
                if(!builder.outFile.delete()) {

                    log.warn("Could not delete: "+builder.outFile);
                    
                }
                
            }
            
            store.destroy();

        }
        
    }

    /**
     * Unit test when the application specifies a split handler which accepts
     * the first N splits of a multiway split but then rejects all other split
     * points. The test verifies that all tuples after the last accepted split
     * point flows into the last split.
     * 
     * @throws Exception
     */
    public void test_split_applicationConstraint_remainerGoesIntoSplit()
            throws Exception {

        /*
         * Test parameters.
         */
        final byte[] fromKey = new byte[0];
        final byte[] toKey = null;

        final int ntuples = 1000;
        
        IndexSegmentBuilder builder = null;
        final IJournal store = getStore();
        
        try {

            final LocalPartitionMetadata pmd = new LocalPartitionMetadata(
                    pidFactory.nextPartitionId(getName()),//
                    -1, // sourcePartitionId
                    fromKey, //
                    toKey,//
                    new IResourceMetadata[] { store.getResourceMetadata() }, //
                    null // cause
//                    ,null // history
            );

            // Generates BTree w/ constrained keys and commits to store.
            final BTree src = generateData(store, getName(), ntuples, pmd);

            // Build the index segment (a compacting merge).
            builder = doBuild(getName(), src, src.getLastCommitTime(), fromKey, toKey);

            final IndexSegmentStore segStore = new IndexSegmentStore(
                    builder.outFile);

            /*
             * Test when two splits would be generated using a split handler
             * which accepts all recommended split points.
             */
            try {

                final int expectedSplitCount = 10;

                final long nominalShardSize = (long) (segStore.size() / (expectedSplitCount / 2.));

                final IndexSegment seg = segStore.loadIndexSegment();

                /*
                 * Figure out where the Mth split would start.
                 */
                final int M = 7;
                final byte[] rejectKey;
                {

                    // Compute splits.
                    final Split[] splits = SplitUtility.getSplits(pidFactory,
                            pmd, seg, nominalShardSize, acceptAllSplits);

                    // Validate splits.
                    SplitUtility
                            .validateSplits(pmd, splits, true/* checkStuff */);

                    assertEquals("#splits", expectedSplitCount, splits.length);

                    // The first key excluded from the Mth split.
                    final byte[] _toKey = seg.keyAt(splits[M - 1].toIndex);

                    // The first key rejected for a split.
                    rejectKey = BytesUtil.successor(_toKey);

                    // log as unsigned byte[].
                    if (log.isInfoEnabled())
                        log.info("rejectKey=" + BytesUtil.toString(rejectKey));
                    
                }

                /*
                 * Now recompute the splits using that rejectKey and verify that
                 * all tuples from that rejectKey on are in the last split and
                 * that the expected number of splits were generated.
                 */
                {

                    // Compute splits.
                    final Split[] splits = SplitUtility.getSplits(pidFactory,
                            pmd, seg, nominalShardSize,
                            new RejectSplitsAfterKey(rejectKey));

                    // Validate splits.
                    SplitUtility
                            .validateSplits(pmd, splits, true/* checkStuff */);

                    assertEquals("#splits", M + 1, splits.length);

                    final Split lastSplit = splits[splits.length-1];

                    if (log.isInfoEnabled()) {
                        log.info("lastSplit=" + lastSplit);
                    }
                    
                    // The first key in the last split (based on the data).
                    final byte[] firstKeyInLastSplit = seg.keyAt(lastSplit.fromIndex);

                    // Verify that successor(firstKeyInLastSplit) is the rejectKey.
                    assertEquals("rejectKey", rejectKey, BytesUtil
                            .successor(firstKeyInLastSplit));
                    
                }

            } finally {

                segStore.close();

            }
            
        } finally {

            if (builder != null) {

                // delete the generated index segment.
                if(!builder.outFile.delete()) {

                    log.warn("Could not delete: "+builder.outFile);
                    
                }
                
            }
            
            store.destroy();

        }
    
    }
    
}
