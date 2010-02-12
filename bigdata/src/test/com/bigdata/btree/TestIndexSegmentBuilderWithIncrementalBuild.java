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

import com.bigdata.LRUNexus;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * A test of the {@link IndexSegmentBuilder} in which there are some deleted
 * tuples in the source {@link BTree} which are to be copied to the destination
 * {@link IndexSegment} (this is really a test of the incremental build scenario
 * since deleted tuples are never copied for a compacting merge).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIndexSegmentBuilderWithIncrementalBuild extends
        AbstractIndexSegmentTestCase {

    /**
     * 
     */
    public TestIndexSegmentBuilderWithIncrementalBuild() {
    }

    /**
     * @param name
     */
    public TestIndexSegmentBuilderWithIncrementalBuild(String name) {
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
                    .newInstance(/*name, */expected, outFile, tmpDir,
                            false/* compactingMerge */, createTime,
                            fromKey, toKey);

            IndexSegmentStore segmentStore = null;
            try {
            
            final IndexSegmentCheckpoint checkpoint = builder.call();

            if (LRUNexus.INSTANCE != null) {

                /*
                 * Clear the records for the index segment from the cache so we will
                 * read directly from the file. This is necessary to ensure that the
                 * data on the file is good rather than just the data in the cache.
                 */
                
                LRUNexus.INSTANCE.deleteCache(checkpoint.segmentUUID);

            }

            segmentStore = new IndexSegmentStore(outFile);

            final IndexSegment actual = segmentStore.loadIndexSegment();

            assertSameEntryIterator(expected.rangeIterator(fromKey, toKey),
                    actual.rangeIterator(fromKey, toKey));

            assertSameEntryIterator(
                        expected
                                .rangeIterator(fromKey, toKey,
                                        0/* capacity */, IRangeQuery.DEFAULT
                                                | IRangeQuery.DELETED, null/* filter */),
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
         * Now build an index segment from a key-range of that BTree.
         */
        {
            
            final byte[] fromKey = KeyBuilder.asSortKey(3);
            final byte[] toKey = KeyBuilder.asSortKey(7);
            
            // should cover keys {3, 4, 5, and 6}.
            assertEquals(4, expected.rangeCount(fromKey, toKey));
            assertEquals(4, expected.rangeCountExact(fromKey, toKey));
            
            final IndexSegmentBuilder builder = IndexSegmentBuilder
                    .newInstance(/*name, */expected, outFile, tmpDir,
                            false/* compactingMerge */, createTime, fromKey,
                            toKey);

            IndexSegmentStore segmentStore = null;
            try {
            
            final IndexSegmentCheckpoint checkpoint = builder.call();

            if (LRUNexus.INSTANCE != null) {

                /*
                 * Clear the records for the index segment from the cache so we will
                 * read directly from the file. This is necessary to ensure that the
                 * data on the file is good rather than just the data in the cache.
                 */
                
                LRUNexus.INSTANCE.deleteCache(checkpoint.segmentUUID);

            }

            segmentStore = new IndexSegmentStore(outFile);

            final IndexSegment actual = segmentStore.loadIndexSegment();

            assertSameEntryIterator(expected.rangeIterator(fromKey, toKey),
                        actual.rangeIterator(fromKey, toKey));

            assertSameEntryIterator(
                        expected
                                .rangeIterator(fromKey, toKey,
                                        0/* capacity */, IRangeQuery.DEFAULT
                                                | IRangeQuery.DELETED, null/* filter */),
                        actual
                                .rangeIterator(fromKey, toKey,
                                        0/* capacity */, IRangeQuery.DEFAULT
                                                | IRangeQuery.DELETED, null/* filter */
                                                ));
            
            } finally {
                
                if (segmentStore != null)
                    segmentStore.destroy();
                
                outFile.delete();
                
            }

        }

        /*
         * Now delete some index entries and build an index segment from a
         * key-range of that BTree that contains the deleted index entries.
         * Since this is an incremental build (vs a compacting merge), the
         * expectation is that the deleted index entries are copied to the
         * generated index segment.
         */
        {
            
            final byte[] fromKey = KeyBuilder.asSortKey(3);
            final byte[] toKey = KeyBuilder.asSortKey(7);
            
            expected.remove(KeyBuilder.asSortKey(3));
            expected.remove(KeyBuilder.asSortKey(5));

            // entry count is NOT changed since delete markers are enabled.
            assertEquals(10, expected.getEntryCount());
            // should cover keys {3, 4, 5, and 6} since delete markers are counted.
            assertEquals(4, expected.rangeCount(fromKey, toKey));
            // should cover keys {4, 6} since {3,5} were deleted.
            assertEquals(2, expected.rangeCountExact(fromKey, toKey));
            
            final IndexSegmentBuilder builder = IndexSegmentBuilder
                    .newInstance(/*name, */expected, outFile, tmpDir,
                            false/* compactingMerge */, createTime, fromKey,
                            toKey);

            IndexSegmentStore segmentStore = null;
            try {
            
            final IndexSegmentCheckpoint checkpoint = builder.call();

            if (LRUNexus.INSTANCE != null) {

                /*
                 * Clear the records for the index segment from the cache so we will
                 * read directly from the file. This is necessary to ensure that the
                 * data on the file is good rather than just the data in the cache.
                 */
                
                LRUNexus.INSTANCE.deleteCache(checkpoint.segmentUUID);

            }

            segmentStore = new IndexSegmentStore(outFile);

            final IndexSegment actual = segmentStore.loadIndexSegment();

            dumpIndexSegment(actual);
            
            assertSameEntryIterator(expected.rangeIterator(fromKey, toKey),
                        actual.rangeIterator(fromKey, toKey));
            
            assertSameEntryIterator(
                    expected
                            .rangeIterator(fromKey, toKey,
                                    0/* capacity */, IRangeQuery.DEFAULT
                                            | IRangeQuery.DELETED, null/* filter */),
                    actual
                            .rangeIterator(fromKey, toKey,
                                    0/* capacity */, IRangeQuery.DEFAULT
                                            | IRangeQuery.DELETED, null/* filter */
                                            ));

            } finally {
                
                if (segmentStore != null)
                    segmentStore.destroy();
                
                outFile.delete();
                
            }

        }

        /*
         * Verify a build which does not overlap the deleted tuples. 
         */
        {
            
            final byte[] fromKey = KeyBuilder.asSortKey(6);
            final byte[] toKey = KeyBuilder.asSortKey(11);
            
            // should cover keys {6, 7, 8, 9, 10}.
            assertEquals(5, expected.rangeCount(fromKey, toKey));
            // should cover keys {6, 7, 8, 9, 10}.
            assertEquals(5, expected.rangeCountExact(fromKey, toKey));
            
            final IndexSegmentBuilder builder = IndexSegmentBuilder
                    .newInstance(/*name, */expected, outFile, tmpDir,
                            false/* compactingMerge */, createTime, fromKey,
                            toKey);

            IndexSegmentStore segmentStore = null;
            try {
            
            final IndexSegmentCheckpoint checkpoint = builder.call();

            if (LRUNexus.INSTANCE != null) {

                /*
                 * Clear the records for the index segment from the cache so we will
                 * read directly from the file. This is necessary to ensure that the
                 * data on the file is good rather than just the data in the cache.
                 */
                
                LRUNexus.INSTANCE.deleteCache(checkpoint.segmentUUID);

            }

            segmentStore = new IndexSegmentStore(outFile);

            final IndexSegment actual = segmentStore.loadIndexSegment();

            dumpIndexSegment(actual);
            
            assertSameEntryIterator(expected.rangeIterator(fromKey, toKey),
                        actual.rangeIterator(fromKey, toKey));
            
            assertSameEntryIterator(
                    expected
                            .rangeIterator(fromKey, toKey,
                                    0/* capacity */, IRangeQuery.DEFAULT
                                            | IRangeQuery.DELETED, null/* filter */),
                    actual
                            .rangeIterator(fromKey, toKey,
                                    0/* capacity */, IRangeQuery.DEFAULT
                                            | IRangeQuery.DELETED, null/* filter */
                                            ));

            } finally {
                
                if (segmentStore != null)
                    segmentStore.destroy();
                
                outFile.delete();
                
            }

        }

        /*
         * Verify a build for the entire key range (which will include the
         * deleted tuples).
         */
        {
            
            final byte[] fromKey = null;
            final byte[] toKey = null;
            
            // should cover keys {1 - 10} since deleted keys are reported.
            assertEquals(10, expected.getEntryCount());
            // should cover keys {1 - 10} since deleted keys are reported.
            assertEquals(10, expected.rangeCount(fromKey, toKey));
            // should cover keys {1, 2, 4, 6, 7, 8, 9, 10} (ignores deleted keys).
            assertEquals(8, expected.rangeCountExact(fromKey, toKey));
            
            final IndexSegmentBuilder builder = IndexSegmentBuilder
                    .newInstance(/*name, */expected, outFile, tmpDir,
                            false/* compactingMerge */, createTime, fromKey,
                            toKey);

            IndexSegmentStore segmentStore = null;
            try {
            
            final IndexSegmentCheckpoint checkpoint = builder.call();

            if (LRUNexus.INSTANCE != null) {

                /*
                 * Clear the records for the index segment from the cache so we will
                 * read directly from the file. This is necessary to ensure that the
                 * data on the file is good rather than just the data in the cache.
                 */
                
                LRUNexus.INSTANCE.deleteCache(checkpoint.segmentUUID);

            }

            segmentStore = new IndexSegmentStore(outFile);

            final IndexSegment actual = segmentStore.loadIndexSegment();

            dumpIndexSegment(actual);
            
            assertSameEntryIterator(expected.rangeIterator(fromKey, toKey),
                        actual.rangeIterator(fromKey, toKey));
            
            assertSameEntryIterator(
                    expected
                            .rangeIterator(fromKey, toKey,
                                    0/* capacity */, IRangeQuery.DEFAULT
                                            | IRangeQuery.DELETED, null/* filter */),
                    actual
                            .rangeIterator(fromKey, toKey,
                                    0/* capacity */, IRangeQuery.DEFAULT
                                            | IRangeQuery.DELETED, null/* filter */
                                            ));

            } finally {
                
                if (segmentStore != null)
                    segmentStore.destroy();
                
                outFile.delete();
                
            }

        }

        /*
         * Re-insert the index entries and re-verify full build.
         */
        {

            final byte[] fromKey = null;
            final byte[] toKey = null;

            expected.insert(KeyBuilder.asSortKey(3), new SimpleEntry(3));
            expected.insert(KeyBuilder.asSortKey(5), new SimpleEntry(5));
            
            // should cover all keys.
            assertEquals(10, expected.getEntryCount());
            assertEquals(10, expected.rangeCount(fromKey, toKey));
            assertEquals(10, expected.rangeCountExact(fromKey, toKey));

            final IndexSegmentBuilder builder = IndexSegmentBuilder
                    .newInstance(/*name, */expected, outFile, tmpDir,
                            false/* compactingMerge */, createTime,
                            fromKey, toKey);

            IndexSegmentStore segmentStore = null;
            try {
            
            final IndexSegmentCheckpoint checkpoint = builder.call();

            if (LRUNexus.INSTANCE != null) {

                /*
                 * Clear the records for the index segment from the cache so we will
                 * read directly from the file. This is necessary to ensure that the
                 * data on the file is good rather than just the data in the cache.
                 */
                
                LRUNexus.INSTANCE.deleteCache(checkpoint.segmentUUID);

            }

            segmentStore = new IndexSegmentStore(outFile);

            final IndexSegment actual = segmentStore.loadIndexSegment();

            assertSameEntryIterator(expected.rangeIterator(fromKey, toKey),
                    actual.rangeIterator(fromKey, toKey));

            assertSameEntryIterator(
                        expected
                                .rangeIterator(fromKey, toKey,
                                        0/* capacity */, IRangeQuery.DEFAULT
                                                | IRangeQuery.DELETED, null/* filter */),
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
         * Delete *ALL* index entries. Verify that an incremental build is
         * permitted since the index entries have been converted to delete
         * markers.
         */
        {

            final byte[] fromKey = null;
            final byte[] toKey = null;

            for (int i = 1; i <= 10; i++) {
                
                expected.remove(KeyBuilder.asSortKey(i));
                
            }
                        
            // should cover all keys since there are delete markers for each.
            assertEquals(10, expected.getEntryCount());
            // should cover all keys since there are delete markers for each.
            assertEquals(10, expected.rangeCount(fromKey, toKey));
            // reports zero since all tuples are delete markers.
            assertEquals(0, expected.rangeCountExact(fromKey, toKey));

            final IndexSegmentBuilder builder = IndexSegmentBuilder
                    .newInstance(/*name,*/expected, outFile, tmpDir,
                            false/* compactingMerge */, createTime,
                            fromKey, toKey);

            IndexSegmentStore segmentStore = null;
            try {
            
            final IndexSegmentCheckpoint checkpoint = builder.call();

            if (LRUNexus.INSTANCE != null) {

                /*
                 * Clear the records for the index segment from the cache so we will
                 * read directly from the file. This is necessary to ensure that the
                 * data on the file is good rather than just the data in the cache.
                 */
                
                LRUNexus.INSTANCE.deleteCache(checkpoint.segmentUUID);

            }

            segmentStore = new IndexSegmentStore(outFile);

            final IndexSegment actual = segmentStore.loadIndexSegment();

            assertSameEntryIterator(expected.rangeIterator(fromKey, toKey),
                    actual.rangeIterator(fromKey, toKey));

            assertSameEntryIterator(
                        expected
                                .rangeIterator(fromKey, toKey,
                                        0/* capacity */, IRangeQuery.DEFAULT
                                                | IRangeQuery.DELETED, null/* filter */),
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
