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
 * Created on Dec 3, 2008
 */

package com.bigdata.btree;

import java.io.File;
import java.util.UUID;

import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * A test of the {@link IndexSegmentBuilder} in which there are some deleted
 * tuples in the source {@link BTree} and a compacting merge is performed.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIndexSegmentBuilderWithCompactingMerge extends
        AbstractIndexSegmentTestCase {

    /**
     * 
     */
    public TestIndexSegmentBuilderWithCompactingMerge() {
    }

    /**
     * @param name
     */
    public TestIndexSegmentBuilderWithCompactingMerge(String name) {
        super(name);
    }

    File outFile;
    File tmpDir;
    
    public void setUp() throws Exception {

        super.setUp();
        
        outFile = new File(getName() + ".seg");

        if (outFile.exists() && !outFile.delete()) {

            throw new RuntimeException("Could not delete file: " + outFile);

        }
        
        tmpDir = outFile.getAbsoluteFile().getParentFile();

    }

    public void tearDown() throws Exception {

        if (outFile != null && outFile.exists() && !outFile.delete()) {

            log.warn("Could not delete file: " + outFile);

        }
        
        super.tearDown();

    }

    /**
     * Unit test explores the correct propagation of deleted index entries to
     * the generated index segment.
     * 
     * @throws Exception
     * 
     * @todo modify test to verify correct propagation of the version timestamps
     *       (or create an entire test suite for that purpose). Note that
     *       version timestamps are only used for full transactions.
     */
    public void test_deletedEntries() throws Exception {
        
        final long createTime = System.currentTimeMillis();
        
        final String name = "testIndex";
        
        final IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());

        // enable delete markers.
        metadata.setDeleteMarkers(true);
        
        // use the same branching factor on the BTree and the IndexSegment for
        // ease of comparison.
        metadata.setBranchingFactor(3);
        metadata.setIndexSegmentBranchingFactor(3);
        
        final BTree expected = BTree.create(new SimpleMemoryRawStore(), metadata);

        for (int i = 1; i <= 10; i++) {

            expected.insert(KeyBuilder.asSortKey(i), new SimpleEntry(i));

        }

        /*
         * First, build an index segment from the original BTree.
         */
        {

            final byte[] fromKey = null;
            final byte[] toKey = null;

            // should cover all keys.
            assertEquals(10, expected.getEntryCount());
            assertEquals(10, expected.rangeCount(fromKey, toKey));
            assertEquals(10, expected.rangeCountExact(fromKey, toKey));

            final IndexSegmentBuilder builder = IndexSegmentBuilder
                    .newInstance(name, expected, outFile, tmpDir,
                            true/* compactingMerge */, createTime, fromKey,
                            toKey);

            IndexSegmentStore segmentStore = null;
            try {

                final IndexSegmentCheckpoint checkpoint = builder.call();

                segmentStore = new IndexSegmentStore(outFile);

                final IndexSegment actual = segmentStore.loadIndexSegment();

                assertSameEntryIterator(expected.rangeIterator(fromKey, toKey),
                        actual.rangeIterator(fromKey, toKey));

                /*
                 * The index segment's iterator WITH the DELETED tuples should
                 * visit the same tuples in the same order as the ground truth
                 * BTree's iterator WITHOUT the DELETED tuples (because the
                 * deleted tuples SHOULD NOT have been copied into the generated
                 * index segment).
                 */
                assertSameEntryIterator(
                        expected
                                .rangeIterator(fromKey, toKey,
                                        0/* capacity */, IRangeQuery.DEFAULT,
                                        null/* filter */),
                        actual
                                .rangeIterator(fromKey, toKey,
                                        0/* capacity */, IRangeQuery.DEFAULT
                                                | IRangeQuery.DELETED, null/* filter */));

            } finally {

                if (segmentStore != null)
                    segmentStore.destroy();
                
                outFile.delete();
                
            }
        
        }
        
        /*
         * Now delete a few tuples.
         */
        {

            final byte[] fromKey = null;
            final byte[] toKey = null;

            expected.remove(KeyBuilder.asSortKey(3));
            expected.remove(KeyBuilder.asSortKey(5));
            
            // should cover all keys (since deleted tuples are still present).
            assertEquals(10, expected.getEntryCount());
            // should cover all keys (since deleted tuples are still present).
            assertEquals(10, expected.rangeCount(fromKey, toKey));
            // should exclude the deleted tuples.
            assertEquals(8, expected.rangeCountExact(fromKey, toKey));

            final IndexSegmentBuilder builder = IndexSegmentBuilder
                    .newInstance(name, expected, outFile, tmpDir,
                            true/* compactingMerge */, createTime, fromKey,
                            toKey);

            IndexSegmentStore segmentStore = null;
            try {

                final IndexSegmentCheckpoint checkpoint = builder.call();

                segmentStore = new IndexSegmentStore(outFile);

                final IndexSegment actual = segmentStore.loadIndexSegment();

                assertSameEntryIterator(expected.rangeIterator(fromKey, toKey),
                        actual.rangeIterator(fromKey, toKey));

                /*
                 * The index segment's iterator WITH the DELETED tuples should
                 * visit the same tuples in the same order as the ground truth
                 * BTree's iterator WITHOUT the DELETED tuples (because the
                 * deleted tuples SHOULD NOT have been copied into the generated
                 * index segment).
                 */
                assertSameEntryIterator(
                        expected
                                .rangeIterator(fromKey, toKey,
                                        0/* capacity */, IRangeQuery.DEFAULT,
                                        null/* filter */),
                        actual
                                .rangeIterator(fromKey, toKey,
                                        0/* capacity */, IRangeQuery.DEFAULT
                                                | IRangeQuery.DELETED, null/* filter */));

            } finally {

                if (segmentStore != null)
                    segmentStore.destroy();
                
                outFile.delete();
                
            }
        
        }

        /*
         * Now delete all the tuples.  
         */
        {

            final byte[] fromKey = null;
            final byte[] toKey = null;

            /*
             * Note: removeAll() MUST write delete markers when the index
             * supports them rather than just replacing the root with a new root
             * leaf.
             */
            expected.removeAll();
            
            // should cover all keys (since deleted tuples are still present).
            assertEquals(10, expected.getEntryCount());
            // should cover all keys (since deleted tuples are still present).
            assertEquals(10, expected.rangeCount(fromKey, toKey));
            // should exclude the deleted tuples.
            assertEquals(0, expected.rangeCountExact(fromKey, toKey));

            final IndexSegmentBuilder builder = IndexSegmentBuilder
                    .newInstance(name, expected, outFile, tmpDir,
                            true/* compactingMerge */, createTime, fromKey,
                            toKey);

            IndexSegmentStore segmentStore = null;
            try {

                final IndexSegmentCheckpoint checkpoint = builder.call();

                segmentStore = new IndexSegmentStore(outFile);

                final IndexSegment actual = segmentStore.loadIndexSegment();

                assertSameEntryIterator(expected.rangeIterator(fromKey, toKey),
                        actual.rangeIterator(fromKey, toKey));

                /*
                 * The index segment's iterator WITH the DELETED tuples should
                 * visit the same tuples in the same order as the ground truth
                 * BTree's iterator WITHOUT the DELETED tuples (because the
                 * deleted tuples SHOULD NOT have been copied into the generated
                 * index segment).
                 */
                assertSameEntryIterator(
                        expected
                                .rangeIterator(fromKey, toKey,
                                        0/* capacity */, IRangeQuery.DEFAULT,
                                        null/* filter */),
                        actual
                                .rangeIterator(fromKey, toKey,
                                        0/* capacity */, IRangeQuery.DEFAULT
                                                | IRangeQuery.DELETED, null/* filter */));

            } finally {

                if (segmentStore != null)
                    segmentStore.destroy();
                
                outFile.delete();
                
            }
        
        }

    }
    
}
