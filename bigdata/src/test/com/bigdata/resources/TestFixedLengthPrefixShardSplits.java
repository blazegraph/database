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

import java.util.Random;
import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.FixedLengthPrefixSplits;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.btree.IndexSegmentStore;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.TestKeyBuilder;
import com.bigdata.journal.IJournal;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.rawstore.Bytes;
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
public class TestFixedLengthPrefixShardSplits extends AbstractTestSegSplitter {

    /**
     * 
     */
    public TestFixedLengthPrefixShardSplits() {
    }

    /**
     * @param name
     */
    public TestFixedLengthPrefixShardSplits(String name) {
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
     * Register a {@link BTree} against the journal, generate some data and
     * commit the data (the data corresponds to a simple triple index schema but
     * does not handle statement indices with variable length keys).
     * 
     * @param store
     * @param ntuples
     * @param pmd
     * @param splitHandler
     */
    protected BTree generateSPOData(final IJournal store, final int ntuples,
            final LocalPartitionMetadata pmd) {
        
        assert store!=null;
        assert pmd!=null;
        
        final byte[] fromKey = pmd.getLeftSeparatorKey();
        final byte[] toKey = pmd.getRightSeparatorKey();
        assert fromKey != null;
//        assert toKey != null;

        final BTree btree;
        {

            final IndexMetadata md = new IndexMetadata(UUID.randomUUID());
            
            md.setPartitionMetadata(pmd);
            
            btree = (BTree) store.registerIndex(getName(), md);
            
        }

        final Random r = new Random();
        
        final long[] s = new long[Math.max(ntuples/4,20)];
        final long[] p = new long[Math.max(ntuples/50,5)];
        final long[] o = new long[Math.max(ntuples/10,100)];
        
        long v = 0;
        for (int i = 0; i < s.length; i++) {
            v = (s[i] = v + r.nextInt(100));
        }
        for (int i = 0; i < p.length; i++) {
            v = (p[i] = v + r.nextInt(100));
        }
        for (int i = 0; i < o.length; i++) {
            v = (o[i] = v + r.nextInt(100));
        }

        final IKeyBuilder keyBuilder = KeyBuilder
                .newInstance(Bytes.SIZEOF_LONG * 3);
        
        int ninsert = 0;
        int ntries = 0;
        while (ninsert < ntuples) {

            final byte[] key = keyBuilder.reset()//
                    .append(s[r.nextInt(s.length)])//
                    .append(p[r.nextInt(p.length)])//
                    .append(o[r.nextInt(o.length)])//
                    .getKey();
            
            if (!btree.contains(key)) {

                btree.insert(key, null/* val */);

                ninsert++;
                
            }
            
            ntries++;

            if (ntries > ntuples * 4) {

                throw new RuntimeException(
                        "Test setup is not terminating: ntuples=" + ntuples
                                + ", ntries=" + ntries + ", ninsert=" + ninsert);

            }
            
        }

        // verify generated correct #of tuples.
        assertEquals(ntuples, btree.getEntryCount());
        
        store.commit();
        
        // return view with lastCommitTime set.
        return (BTree) store.getIndex(getName());
        
    }

    /**
     * Unit test for the logic handling application constraints on the choice of
     * the separator key, which can also change the #of splits which may be
     * generated from a given input B+Tree.  Specifically, this test the
     * {@link FixedLengthPrefixSplits} constraint.
     */
    public void test_split_applicationConstraint_nbytePrefix() throws Exception {

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
            final BTree src = generateSPOData(store, ntuples, pmd);

            // Build the index segment (a compacting merge).
            builder = doBuild(getName(), src, src.getLastCommitTime(), fromKey, toKey);

            final IndexSegmentStore segStore = new IndexSegmentStore(
                    builder.outFile);

            /*
             * Test ability to create two splits from the data when the split
             * handler accepts anything.
             */
            try {

                final int expectedSplitCount = 2;

                final long nominalShardSize = (long) (segStore.size() / (expectedSplitCount / 2.));

                final IndexSegment seg = segStore.loadIndexSegment();

                // Compute splits.
                final Split[] splits = SplitUtility.getSplits(pidFactory, pmd,
                        seg, nominalShardSize, acceptAllSplits);

                // Validate splits.
                SplitUtility.validateSplits(pmd, splits, true/* checkStuff */);

                assertEquals("#splits", expectedSplitCount, splits.length);

            } finally {

                segStore.close();

            }

            /*
             * Test ability to create two splits when the split handler is
             * constrained to only accept an 8 byte prefix boundary.
             */
            try {

                final int expectedSplitCount = 2;

                final long nominalShardSize = (long) (segStore.size() / (expectedSplitCount / 2.));

                final IndexSegment seg = segStore.loadIndexSegment();

                // Compute splits.
                final Split[] splits = SplitUtility.getSplits(pidFactory, pmd,
                        seg, nominalShardSize, new FixedLengthPrefixSplits(
                                Bytes.SIZEOF_LONG));

                // Validate splits.
                SplitUtility.validateSplits(pmd, splits, true/* checkStuff */);

                assertEquals("#splits", expectedSplitCount, splits.length);

                // the separator key between the two splits.
                final byte[] prefix = splits[0].pmd.getRightSeparatorKey();

                // verify the prefix non-null and also its length.
                assertNotNull(prefix);
                assertEquals("prefix length", Bytes.SIZEOF_LONG, prefix.length);

                /*
                 * Lookup the indexOf the prefix in the source B+Tree. For this
                 * unit test (and for the SPO indices) it will be an insertion
                 * point. Convert that to an index and compare the keyAt that
                 * index and keyAt(index-1). They should differ in their first 8
                 * bytes (this is the constraint which is being imposed).
                 * 
                 * Note: We could get this right by chance with a NOP split
                 * handler. However, the odds are against it and this test will
                 * nearly always correctly fail a broken split handler.
                 */
                
                // Get insertion point (there are no 8-byte keys in the ndx).
                final int pos = seg.indexOf(prefix);
                assertTrue(pos < 0);
                
                // Convert to a tuple index.
                final int index = -(pos) - 1;

                // The actual key before the separator key.
                final byte[] keyBefore = seg.keyAt(index - 1);
                assertEquals(Bytes.SIZEOF_LONG * 3, keyBefore.length);

                // The actual key after the separator key.
                final byte[] keyAfter = seg.keyAt(index);
                assertEquals(Bytes.SIZEOF_LONG * 3, keyAfter.length);

                // Compare the first 8 bytes of those keys (unsigned byte[] comparison).
                final int cmp = BytesUtil.compareBytesWithLenAndOffset(//
                        0/* aoff */, Bytes.SIZEOF_LONG/* alen */, keyBefore,//
                        0/* boff */, Bytes.SIZEOF_LONG/* blen */, keyAfter//
                        );
                
                // The 1st key prefix must be strictly LT the 2nd key prefix.
                assertTrue(cmp < 0);

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
