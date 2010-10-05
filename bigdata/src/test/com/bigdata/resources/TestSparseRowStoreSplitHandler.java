/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Oct 4, 2010
 */

package com.bigdata.resources;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.btree.IndexSegmentStore;
import com.bigdata.journal.IJournal;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.service.Split;
import com.bigdata.sparse.KeyDecoder;
import com.bigdata.sparse.KeyType;
import com.bigdata.sparse.LogicalRowSplitHandler;
import com.bigdata.sparse.Schema;
import com.bigdata.sparse.SparseRowStore;

/**
 * Tetst suite for {@link LogicalRowSplitHandler}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSparseRowStoreSplitHandler extends AbstractTestSegSplitter {

    /**
     * 
     */
    public TestSparseRowStoreSplitHandler() {
    }

    /**
     * @param name
     */
    public TestSparseRowStoreSplitHandler(String name) {
        super(name);
    }

    /**
     * A {@link SparseRowStore} schema used by some unit tests.
     */
    static private final Schema schema = new Schema("Employee", "Id",
            KeyType.Long);

    /**
     * Register a {@link BTree} against the journal, generate some data and
     * commit the data.
     * <p>
     * Note: This is a bit slow since it does a commit after each logical row
     * inserted into the B+Tree.
     * 
     * @param store
     * @param nrows
     * @param pmd
     */
    private BTree generateSparseRowStoreData(final IJournal store,
            final String name, final int nrows, final LocalPartitionMetadata pmd) {

        assert store!=null;
        assert pmd!=null;
        
        final byte[] fromKey = pmd.getLeftSeparatorKey();
//        final byte[] toKey = pmd.getRightSeparatorKey();
        assert fromKey != null;
//        assert toKey != null;

        final BTree btree;
        {

            final IndexMetadata md = new IndexMetadata(UUID.randomUUID());
            
            md.setPartitionMetadata(pmd);
            
            btree = (BTree) store.registerIndex(name, md);
            
        }

        final SparseRowStore srs = new SparseRowStore(btree);

        final String[] names = new String[] { "Bryan", "Mike", "Martyn",
                "Paul", "Julie" };

        final String[] states = new String[] {"MD","DC","UT","MA","NC"};
        
        // put a little randomness in to exercise the split logic more.
        final Random r = new Random();
        long id = 0;
        long nadded = 0;
        for (int i = 0; i < nrows; i++) {

            final Map<String, Object> propertySet = new HashMap<String, Object>();

            id += r.nextInt(20);
            
            propertySet.put("Id", id); // primary key.
            propertySet.put("Name", names[i % names.length] + id);
            propertySet.put("State", states[i % states.length] + id);
            nadded += 3;

            final int nextra = r.nextInt(10);
            for (int j = 0; j < nextra; j++) {
                propertySet.put("attr" + j, r.nextInt(20));
                nadded++;
            }

            srs.write(schema, propertySet);

        }

        // verify generated correct #of tuples.
        assertEquals(nadded, btree.getEntryCount());

        store.commit();
        
        // return view with lastCommitTime set.
        return (BTree) store.getIndex(name);
        
    }

    /**
     * Note: The {@link SparseRowStore} constraint is also easy. We just need to
     * find/create a separator key which is equal to some {schema+primaryKey}.
     * The same concerns about the left/right separator keys apply. Again, it is
     * very unlikely to have 200MB of data for a specific schema and primary
     * key!
     * 
     * @see SparseRowStore
     * @see Schema
     * @see KeyDecoder
     * 
     * @throws Exception
     */
    public void test_split_applicationConstraint_rowStore() throws Exception {

        /*
         * Test parameters.
         */
        final byte[] fromKey = new byte[0];
        final byte[] toKey = null;

        final int nrows = 1000;
        
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
//                    null // history
            );

            // Generates BTree w/ constrained keys and commits to store.
            final BTree src = generateSparseRowStoreData(store, getName(), nrows, pmd);

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
             * constrained to only accept a logical row boundary.
             */
            try {

                final int expectedSplitCount = 2;

                final long nominalShardSize = (long) (segStore.size() / (expectedSplitCount / 2.));

                final IndexSegment seg = segStore.loadIndexSegment();

                // Compute splits.
                final Split[] splits = SplitUtility.getSplits(pidFactory, pmd,
                        seg, nominalShardSize, new LogicalRowSplitHandler());

                // Validate splits.
                SplitUtility.validateSplits(pmd, splits, true/* checkStuff */);

                assertEquals("#splits", expectedSplitCount, splits.length);

                // the separator key between the two splits.
                final byte[] prefix = splits[0].pmd.getRightSeparatorKey();
                
                // verify the prefix non-null and non-empty.
                assertNotNull(prefix);
                assertNotSame("prefix length", 0, prefix.length);

                /*
                 * Lookup the indexOf the prefix in the source B+Tree. For this
                 * unit test it will be an insertion point (there is no
                 * columnName or timestamp in the prefix so it is not a complete
                 * key for the sparse row store). Convert that to an index and
                 * compare the keyAt that index and keyAt(index-1). They should
                 * differ in their first prefix.length bytes (this is the
                 * constraint which is being imposed).
                 */
                
                // Get insertion point (per above, this is not an actual key).
                final int pos = seg.indexOf(prefix);
                assertTrue(pos < 0);
                
                // Convert to a tuple index.
                final int index = -(pos) - 1;

                // The actual key before the separator key.
                final byte[] keyBefore = seg.keyAt(index - 1);
                // That key must be longer than the prefix for a row store.
                assertTrue(prefix.length < keyBefore.length);

                // The actual key after the separator key.
                final byte[] keyAfter = seg.keyAt(index);
                // That key must be longer than the prefix for a row store.
                assertTrue(prefix.length < keyAfter.length);

                // Compare the first prefix.length bytes of those keys (unsigned
                // byte[] comparison).
                final int cmp = BytesUtil.compareBytesWithLenAndOffset(//
                        0/* aoff */, prefix.length/* alen */, keyBefore,//
                        0/* boff */, prefix.length/* blen */, keyAfter//
                        );
                
                // The 1st key prefix must be strictly LT the 2nd key prefix.
                assertTrue(cmp < 0);

                /*
                 * Look at the 1st logical row before and after the separator
                 * key. These logical rows must have distinct primary key
                 * values.  (Because of how the data are generated, they will
                 * in fact be separated by ONE (1).)
                 */
                {

                    final SparseRowStore rowStore = new SparseRowStore(seg);
                    
                    final KeyDecoder keyDecoderBefore = new KeyDecoder(keyBefore);

                    final KeyDecoder keyDecoderAfter = new KeyDecoder(keyAfter);

                    final Long primaryKeyBefore = (Long) keyDecoderBefore
                            .getPrimaryKey();

                    assertNotNull(primaryKeyBefore);
                    
                    final Long primaryKeyAfter = (Long) keyDecoderAfter
                            .getPrimaryKey();
                    
                    assertNotNull(primaryKeyAfter);
                    
                    assertNotSame(primaryKeyBefore, primaryKeyAfter);

                    // Verify the primary keys are ordered.
                    assertTrue(primaryKeyBefore.longValue() < primaryKeyAfter
                            .longValue());

//                    // because of how the data are generated these primary keys
//                    // will in fact be successors.
//                    assertEquals(primaryKeyBefore.longValue() + 1,
//                            primaryKeyAfter.longValue());

                    // Extract the logical row for those primary keys.
                    final Map<String, Object> rowBefore = rowStore.read(schema,
                            primaryKeyBefore);

                    assertNotNull(rowBefore);

                    // Extract the logical row for those primary keys.
                    final Map<String, Object> rowAfter = rowStore.read(schema,
                            primaryKeyAfter);

                    assertNotNull(rowAfter);

                    // Write out those rows
                    if (log.isInfoEnabled())
                        log.info("\nbeforeSeparatorKey=" + rowBefore
                                + "\nafterSeparatorKey=" + rowAfter);

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
