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
 * Created on Jul 15, 2010
 */

package com.bigdata.btree;

import java.io.File;
import java.util.Random;
import java.util.UUID;

import com.bigdata.btree.IndexSegmentBuilder.BuildEnum;
import com.bigdata.io.DirectBufferPool;

/**
 * Test suite for {@link IndexSegmentMultiBlockIterator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIndexSegmentMultiBlockIterators extends
        AbstractIndexSegmentTestCase {

    /**
     * 
     */
    public TestIndexSegmentMultiBlockIterators() {
    }

    /**
     * @param name
     */
    public TestIndexSegmentMultiBlockIterators(String name) {
        super(name);
    }

    protected File outFile;

//    File tmpDir;

    static final boolean bufferNodes = true;

    @Override
    public void setUp() throws Exception {

        super.setUp();
        
        outFile = new File(getName() + ".seg");

        if (outFile.exists() && !outFile.delete()) {

            throw new RuntimeException("Could not delete file: " + outFile);

        }
        
//        tmpDir = outFile.getAbsoluteFile().getParentFile();

    }

    @Override
    public void tearDown() throws Exception {

        if (outFile != null && outFile.exists() && !outFile.delete()) {

            log.warn("Could not delete file: " + outFile);

        }

        super.tearDown();
        
    }

    public void test_ctor() throws Exception {

        final BTree btree = BTree.createTransient(new IndexMetadata(UUID
                .randomUUID()));

//        for (int i = 0; i < 10; i++) {
//            btree.insert(i, i);
//        }

        final IndexSegmentBuilder builder = TestIndexSegmentBuilderWithLargeTrees
                .doBuildIndexSegment(getName(), btree, 32/* m */,
                        BuildEnum.TwoPass, bufferNodes);

        final IndexSegment seg = new IndexSegmentStore(builder.outFile)
                .loadIndexSegment();

        try {

            // correct rejection test.
            try {
                new IndexSegmentMultiBlockIterator(null/* seg */,
                        DirectBufferPool.INSTANCE, null/* fromKey */,
                        null/* toKey */, IRangeQuery.DEFAULT);
                fail("Expecting: " + IllegalArgumentException.class);
            } catch (IllegalArgumentException ex) {
                if (log.isInfoEnabled())
                    log.info("Ignoring expected exception: " + ex);
            }

            // correct rejection test.
            try {
                new IndexSegmentMultiBlockIterator(seg, null/* pool */,
                        null/* fromKey */, null/* toKey */, IRangeQuery.DEFAULT);
                fail("Expecting: " + IllegalArgumentException.class);
            } catch (IllegalArgumentException ex) {
                if (log.isInfoEnabled())
                    log.info("Ignoring expected exception: " + ex);
            }

            // correct rejection test.
            try {
                new IndexSegmentMultiBlockIterator(seg,
                        DirectBufferPool.INSTANCE, null/* fromKey */,
                        null/* toKey */, IRangeQuery.DEFAULT
                                | IRangeQuery.REMOVEALL);
                fail("Expecting: " + IllegalArgumentException.class);
            } catch (IllegalArgumentException ex) {
                if (log.isInfoEnabled())
                    log.info("Ignoring expected exception: " + ex);
            }

            // correct rejection test.
            try {
                new IndexSegmentMultiBlockIterator(seg, null/* pool */,
                        null/* fromKey */, null/* toKey */, IRangeQuery.DEFAULT
                                | IRangeQuery.CURSOR);
                fail("Expecting: " + IllegalArgumentException.class);
            } catch (IllegalArgumentException ex) {
                if (log.isInfoEnabled())
                    log.info("Ignoring expected exception: " + ex);
            }

            // correct rejection test.
            try {
                new IndexSegmentMultiBlockIterator(seg, null/* pool */,
                        null/* fromKey */, null/* toKey */, IRangeQuery.DEFAULT
                                | IRangeQuery.REVERSE);
                fail("Expecting: " + IllegalArgumentException.class);
            } catch (IllegalArgumentException ex) {
                if (log.isInfoEnabled())
                    log.info("Ignoring expected exception: " + ex);
            }

        } finally {
            
            seg.getStore().destroy();
            
        }

    }

    /**
     * Test build around an {@link IndexSegment} having a branching factor of
     * THREE (3) and three leaves, which are fully populated.
     */
    public void test_simple() throws Exception {
        
        final BTree btree = BTree.createTransient(new IndexMetadata(UUID
                .randomUUID()));

        for (int i = 0; i < 9; i++) {
            btree.insert(i, i);
        }

        final IndexSegmentBuilder builder = TestIndexSegmentBuilderWithLargeTrees
                .doBuildIndexSegment(getName(), btree, 3/* m */,
                        BuildEnum.TwoPass, bufferNodes);

        // System.err.println("plan: "+builder.plan);

        // The plan should generate a B+Tree with 3 leaves.
        assertEquals(3, builder.plan.nleaves);
      
        final IndexSegment seg = new IndexSegmentStore(builder.outFile)
                .loadIndexSegment();

        try {

            // Cursor let's us visit any leaf.
            final ILeafCursor<?> leafCursor = seg.newLeafCursor(SeekEnum.First);

            // multi-block iterator.
            final IndexSegmentMultiBlockIterator<?> itr = new IndexSegmentMultiBlockIterator(
                    seg, DirectBufferPool.INSTANCE, null/* fromKey */,
                    null/* toKey */, IRangeQuery.DEFAULT);

            /*
             * First leaf.
             */
            
            // nothing was ready yet.
            assertNull(itr.getLeaf());

            // hasNext() will force a block into memory.
            assertTrue(itr.hasNext());

            // verify a leaf is now available (the 1st leaf).
            assertNotNull(itr.getLeaf());

            // verify we are looking at the same leaf data.
            assertSameLeafData(leafCursor.leaf(), itr.getLeaf());

            /*
             * Second leaf.
             */
            
            // skip three tuples (to the 2nd leaf).
            itr.next();
            itr.next();
            itr.next();

            // force read of the next leaf.
            assertTrue(itr.hasNext());
            
            // the next leaf.
            leafCursor.next();

            // verify we are looking at the same leaf data.
            assertSameLeafData(leafCursor.leaf(), itr.getLeaf());

            /*
             * Third leaf.
             */
            
            // skip three tuples (to the 3rd leaf).
            itr.next();
            itr.next();
            itr.next();
            
            // force read of the next leaf.
            assertTrue(itr.hasNext());
            
            // the next leaf.
            leafCursor.next();

            // verify we are looking at the same leaf data.
            assertSameLeafData(leafCursor.leaf(), itr.getLeaf());

            /*
             * Exhausted.
             */
            
            // skip three tuples.
            itr.next();
            itr.next();
            itr.next();
            
            // verify that the iterator is exhausted.
            assertFalse(itr.hasNext());
            
            doRandomScanTest(btree, seg, 10/* ntests */);
            
        } finally {

            seg.getStore().destroy();

        }

    }

    /**
     * Unit test builds an empty index segment and then verifies the behavior of
     * the {@link IndexSegmentMultiBlockIterator}.
     * 
     * @throws Exception 
     */
    public void test_emptyIndexSegment() throws Exception {

        final BTree btree = BTree.createTransient(new IndexMetadata(UUID
                .randomUUID()));

        final IndexSegmentBuilder builder = TestIndexSegmentBuilderWithLargeTrees
                .doBuildIndexSegment(getName(), btree, 32/* m */,
                        BuildEnum.TwoPass, bufferNodes);

        final IndexSegment seg = new IndexSegmentStore(builder.outFile)
                .loadIndexSegment();

        try {

            final IndexSegmentMultiBlockIterator<?> itr = new IndexSegmentMultiBlockIterator(
                    seg, DirectBufferPool.INSTANCE_10M, null/* fromKey */,
                    null/* toKey */, IRangeQuery.DEFAULT);

            assertFalse(itr.hasNext());
            
            // verify the data.
            testMultiBlockIterator(btree, seg);
            
        } finally {

            seg.getStore().destroy();

        }

    }
    
    /**
     * Test build around an {@link IndexSegment} having a default branching
     * factor and a bunch of leaves totally more than 1M in size on the disk.
     */
    public void test_moderate() throws Exception {
        
        final BTree btree = BTree.createTransient(new IndexMetadata(UUID
                .randomUUID()));

        final int LIMIT = 1000000;

        // populate the index.
        for (int i = 0; i < LIMIT; i++) {

            btree.insert(i, i);
            
        }

        final IndexSegmentBuilder builder = TestIndexSegmentBuilderWithLargeTrees
                .doBuildIndexSegment(getName(), btree, 32/* m */,
                        BuildEnum.TwoPass, bufferNodes);

        final IndexSegment seg = new IndexSegmentStore(builder.outFile)
                .loadIndexSegment();

        try {

            final DirectBufferPool pool = DirectBufferPool.INSTANCE;

            if (seg.getStore().getCheckpoint().maxNodeOrLeafLength > pool
                    .getBufferCapacity()) {

                /*
                 * The individual leaves must be less than the buffer size in
                 * order for us to read at least one leaf per block.
                 */
                
                fail("Run the test with smaller branching factor.");

            }

            // The #of blocks we will have to read.
            final long nblocks = seg.getStore().getCheckpoint().extentLeaves
                    / pool.getBufferCapacity();

            if (log.isInfoEnabled())
                log.info("Will read " + nblocks + " blocks.");

            if (nblocks < 2) {

                /*
                 * The leaves extent needs to be larger than the buffer size in
                 * order for us to test with more than one block read from the
                 * backing file.
                 */
                
                fail("Run the test with more tuples.");

            }

            // verify the data.
            testMultiBlockIterator(btree, seg);

            // random iterator scan tests.
            doRandomScanTest(btree, seg, 1000/* ntests */);
            
        } finally {

            seg.getStore().destroy();

        }

    }

    /**
     * Do a bunch of random iterator scans. Each scan will start at a random key
     * and run to a random key.
     * 
     * @param groundTruth
     *            The ground truth B+Tree.
     * @param actual
     *            The index segment built from that B+Tree.
     * @param ntests
     *            The #of scans to run.
     */
    private void doRandomScanTest(final BTree groundTruth,
            final IndexSegment actual, final int ntests) {

        final Random r = new Random();
        
        final int n = groundTruth.getEntryCount();

        // point query beyond the last tuple in the index segment.
        {
            
            final int fromIndex = n - 1;

            final byte[] fromKey = groundTruth.keyAt(fromIndex);

            final byte[] toKey = BytesUtil.successor(fromKey.clone());

            final ITupleIterator<?> expectedItr = groundTruth
                    .rangeIterator(fromKey, toKey, 0/* capacity */,
                            IRangeQuery.DEFAULT, null/* filter */);

            final IndexSegmentMultiBlockIterator<?> actualItr = new IndexSegmentMultiBlockIterator(
                    actual, DirectBufferPool.INSTANCE_10M, fromKey, toKey,
                    IRangeQuery.DEFAULT);
            
            assertSameEntryIterator(expectedItr, actualItr);

        }
        
        // random point queries.
        for (int i = 0; i < ntests; i++) {

            final int fromIndex = r.nextInt(n);

            final byte[] fromKey = groundTruth.keyAt(fromIndex);

            final byte[] toKey = BytesUtil.successor(fromKey.clone());

            final ITupleIterator<?> expectedItr = groundTruth
                    .rangeIterator(fromKey, toKey, 0/* capacity */,
                            IRangeQuery.DEFAULT, null/* filter */);

            final IndexSegmentMultiBlockIterator<?> actualItr = new IndexSegmentMultiBlockIterator(
                    actual, DirectBufferPool.INSTANCE_10M, fromKey, toKey,
                    IRangeQuery.DEFAULT);
            
            assertSameEntryIterator(expectedItr, actualItr);

        }
        
        // random range queries with small range of spanned keys (0 to 10).
        for (int i = 0; i < ntests; i++) {

            final int fromIndex = r.nextInt(n);

            final byte[] fromKey = groundTruth.keyAt(fromIndex);

            final byte[] toKey = groundTruth.keyAt(Math.min(fromIndex
                    + r.nextInt(10), n - 1));

            final ITupleIterator<?> expectedItr = groundTruth
                    .rangeIterator(fromKey, toKey, 0/* capacity */,
                            IRangeQuery.DEFAULT, null/* filter */);

            final IndexSegmentMultiBlockIterator<?> actualItr = new IndexSegmentMultiBlockIterator(
                    actual, DirectBufferPool.INSTANCE_10M, fromKey, toKey,
                    IRangeQuery.DEFAULT);

            assertSameEntryIterator(expectedItr, actualItr);

        }

        // random range queries with random #of spanned keys.
        for (int i = 0; i < ntests; i++) {

            final int fromIndex = r.nextInt(n);

            final int toIndex = fromIndex + r.nextInt(n - fromIndex + 1);

            final byte[] fromKey = groundTruth.keyAt(fromIndex);

            final byte[] toKey = toIndex >= n ? null : groundTruth
                    .keyAt(toIndex);

            final ITupleIterator<?> expectedItr = groundTruth
                    .rangeIterator(fromKey, toKey, 0/* capacity */,
                            IRangeQuery.DEFAULT, null/* filter */);

            final IndexSegmentMultiBlockIterator<?> actualItr = new IndexSegmentMultiBlockIterator(
                    actual, DirectBufferPool.INSTANCE_10M, fromKey, toKey,
                    IRangeQuery.DEFAULT);

            assertSameEntryIterator(expectedItr, actualItr);

        }

    }

}
