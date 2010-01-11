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
 * Created on Dec 20, 2009
 */

package com.bigdata.resources;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import junit.framework.TestCase2;

import com.bigdata.btree.BTree;
import com.bigdata.btree.ISimpleSplitHandler;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.btree.IndexSegmentStore;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.service.Split;
import com.bigdata.sparse.SparseRowStore;

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
public class TestSegSplitter extends TestCase2 {

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

    protected IJournal getStore() throws IOException {
        
        Properties p = new Properties();
        
        p.setProperty(Options.FILE, File.createTempFile(getName(), Options.JNL).toString());
        
        return new Journal(p);
        
    }
    
    final protected IPartitionIdFactory pidFactory = new MockPartitionIdFactory();
    
    /**
     * Generate an {@link IndexSegment} from the given BTree.
     * 
     * @param src
     *            The source {@link BTree}.
     * 
     * @return The {@link IndexSegmentBuilder}.
     */
    public IndexSegmentBuilder doBuild(final BTree src, final long commitTime,
            final byte[] fromKey, final byte[] toKey) throws Exception {

        if (src == null)
            throw new IllegalArgumentException();

        final String name = getName();

        final File tmpDir = new File(System.getProperty("java.io.tmpdir"));

        File outFile = null;
        final IndexSegmentBuilder builder;
        try {

            // the file to be generated.
            outFile = File.createTempFile(name, Options.SEG, tmpDir);

            // new builder.
            builder = IndexSegmentBuilder.newInstance(name, src, outFile,
                    tmpDir, true/* compactingMerge */, commitTime, fromKey,
                    toKey);

            // build the index segment.
            builder.call();

            return builder;
        
        } catch (Throwable t) {

            if (outFile != null && outFile.exists()) {

                try {

                    outFile.delete();

                } catch (Throwable t2) {

                    log.warn(t2.getLocalizedMessage(), t2);

                }

            }

            if (t instanceof Exception)
                throw (Exception) t;

            throw new RuntimeException(t);

        }

    }

    /**
     * Register a {@link BTree} against the journal, generate some data in the
     * specified key range, and commit the data.
     *<p>
     * Note: this uses int values to generate the keys. If you specific the
     * fromKey as anything other than an empty byte[] or the toKey as anything
     * other than null then the key must be at least 4 bytes long and the 1st 4
     * bytes will be decoded as an integer.
     * 
     * @param store
     * @param ntuples
     * @param fromKey
     * @param toKey
     * 
     * @return
     */
    private BTree generateData(final IJournal store, final int ntuples,
            final LocalPartitionMetadata pmd) {
//            final byte[] fromKey, final byte[] toKey) {
        
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
         * when we register the BTree.
         */
        final BTree btree;
        {

            final IndexMetadata md = new IndexMetadata(UUID.randomUUID());
            
            md.setPartitionMetadata(pmd);
            
            btree = (BTree) store.registerIndex(getName(), md);
            
        }

        // generate evenly space tuples.
        final long inc = ((long) high - (long) low) / ntuples;
        assert inc > 0 : "inc=" + inc;
        int ninsert = 0;
        for (int i = low; i <= high && ninsert < ntuples; i += inc) {

            btree.insert(KeyBuilder.asSortKey(i), i);

            ninsert++;

        }

        // verify generated correct #of tuples.
        assertEquals(ntuples, btree.getEntryCount());
        
        store.commit();
        
        // return view with lastCommitTime set.
        return (BTree) store.getIndex(getName());
        
    }

    /**
     * Mock implementation assigns index partitions from a counter beginning
     * with ZERO (0), which is the first legal index partition identifier. The
     * name parameter is ignored.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class MockPartitionIdFactory implements IPartitionIdFactory {

        private int i = 0;

        public int nextPartitionId(String nameIsIgnored) {

            return i++;

        }

    }
    
    /**
     * A basic test of the ability to split an {@link IndexSegment}.
     * 
     * @todo test when not large enough for a split.
     * 
     * @todo test when large enough to create two splits.
     * 
     * @todo test when large enough to create three splits.
     * 
     * @todo test various fence posts.
     * 
     * @todo test with fromKey/toKey=([],null), which spans all keys. When
     *       testing with constraints on the fromKey or toKey the generated
     *       BTree must have keys lying within the constrained key range.
     * 
     * @todo test with fromKey=[] and non-null toKey.
     * 
     * @todo test with non-empty fromKey and null toKey.
     * 
     * @todo test with non-empty fromKey and non-null toKey.
     * 
     * @todo Test with application constraint on the choice of the separator
     *       keys.
     *       <p>
     *       Note: the SPO index can have a constraint imposed which makes a
     *       star join trivial -- just truncate the selected separator key to 8
     *       bytes. If the resulting key is less than the last/left separator
     *       key then use its successor. If the resulting key is greater than
     *       the right separator key, then no split is possible. (That would
     *       imply 200MB of attribute values for a single subject which is
     *       pretty unlikely!).
     *       <p>
     *       Note: The {@link SparseRowStore} constraint is also easy. We just
     *       need to find/create a separator key which is equal to some
     *       {schema+primaryKey}. The same concerns about the left/right
     *       separator keys apply. Again, it is very unlikely to have 200MB of
     *       data for a specific schema and primary key!
     */
    public void test_split01() throws Exception {

        /*
         * Test parameters.
         * 
         * @todo things will have to be done a bit differently to test when the
         * expectation is that no split is possible either because there is not
         * enough data or because the application constraint on the separator
         * key would be violated.
         * 
         * @todo to test with fractional splits we really need to control for
         * the size on disk.
         */
        final byte[] fromKey = new byte[0];
        final byte[] toKey = null;
        final int expectedSplitCount = 2;
        final ISimpleSplitHandler splitHandler = null;

        final IJournal store = getStore();
        final int ntuples = 1000;
        final LocalPartitionMetadata pmd = new LocalPartitionMetadata(
                pidFactory.nextPartitionId(getName()),//
                -1, // sourcePartitionId
                fromKey, //
                toKey,//
                new IResourceMetadata[] { store.getResourceMetadata() }, //
                null, // cause
                null // history
        );
        IndexSegmentBuilder builder = null;
        try {

            // Generates BTree w/ constrained keys and commits to store.
            final BTree src = generateData(store, ntuples, pmd);

            // Build the index segment (a compacting merge).
            builder = doBuild(src, src.getLastCommitTime(), fromKey, toKey);

            final IndexSegmentStore segStore = new IndexSegmentStore(
                    builder.outFile);

            final long nominalShardSize = segStore.size() / expectedSplitCount;

            try {

                final IndexSegment seg = segStore.loadIndexSegment();

                // Compute splits.
                final Split[] splits = SplitUtility.getSplits(pidFactory, pmd,
                        seg, nominalShardSize, splitHandler);

                // Validate splits.
                SplitUtility.validateSplits(pmd, splits, true/*checkStuff*/);
            
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
