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
 * Created on Apr 1, 2009
 */

package com.bigdata.resources;

import java.io.File;
import java.util.Random;
import java.util.UUID;

import junit.framework.TestCase2;

import com.bigdata.btree.BTree;
import com.bigdata.btree.ITupleCursor;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.mdi.IndexPartitionCause;
import com.bigdata.mdi.JournalMetadata;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.rawstore.Bytes;
import com.bigdata.service.Split;
import com.bigdata.sparse.SparseRowStore;

/**
 * Unit tests for the {@link DefaultSplitHandler}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestDefaultSplitHandler extends TestCase2 {

    /**
     * 
     */
    public TestDefaultSplitHandler() {
    }

    /**
     * @param arg0
     */
    public TestDefaultSplitHandler(String arg0) {
        super(arg0);
    }

    /**
     * @todo write the ctor correct rejection tests.
     */
    public void test_ctor_correctRejection() {

        final int minimumEntryCount = 500 * Bytes.kilobyte32;
        final int entryCountPerSplit = 1 * Bytes.megabyte32;
        final double overCapacityMultiplier = 1.5;
        final double underCapacityMultiplier = .75;
        final int sampleRate = 20;
        new DefaultSplitHandler(minimumEntryCount, entryCountPerSplit,
                overCapacityMultiplier, underCapacityMultiplier, sampleRate);
        fail("write tests");
    }

    /**
     * Unit test for the split handler with the default configuration used by
     * {@link IndexMetadata}.
     */
    public void test_ctor_defaultSplitConfiguration() {
        
        final int minimumEntryCount = 500 * Bytes.kilobyte32;
        final int entryCountPerSplit = 1 * Bytes.megabyte32;
        final double overCapacityMultiplier = 1.5;
        final double underCapacityMultiplier = .75;
        final int sampleRate = 20;
       
        final DefaultSplitHandler splitHandler = new DefaultSplitHandler(
                minimumEntryCount, entryCountPerSplit, overCapacityMultiplier,
                underCapacityMultiplier, sampleRate);

        assertEquals("minimumEntryCount", minimumEntryCount, splitHandler
                .getMinimumEntryCount());

        assertEquals("entryCountPerSplit", entryCountPerSplit, splitHandler
                .getEntryCountPerSplit());

        assertEquals("overCapacityMultiplier", overCapacityMultiplier,
                splitHandler.getOverCapacityMultiplier());

        assertEquals("underCapacityMultiplier", underCapacityMultiplier,
                splitHandler.getUnderCapacityMultiplier());

        assertEquals("sampleRate", sampleRate, splitHandler.getSampleRate());

        // verify shouldSplit
        assertFalse(
                "shouldSplit",
                splitHandler
                        .shouldSplit((long) (entryCountPerSplit - 1 * overCapacityMultiplier)/* rangeCount */));

        assertTrue(
                "shouldSplit",
                splitHandler
                        .shouldSplit((long) (entryCountPerSplit * overCapacityMultiplier)/* rangeCount */));

        // verify shouldJoin
        assertTrue("shouldJoin", splitHandler
                .shouldJoin(minimumEntryCount/* rangeCount */));

        assertFalse("shouldJoin", splitHandler
                .shouldJoin(minimumEntryCount + 1/* rangeCount */));

        // verify percentOfSplit
        assertEquals("percentOfSplit", 1d, splitHandler
                .percentOfSplit(entryCountPerSplit));

        assertTrue("percentOfSplit", .2d - splitHandler
                .percentOfSplit((long) (entryCountPerSplit * .2)) < .0001);

        assertTrue("percentOfSplit", 1.2d - splitHandler
                .percentOfSplit((long) (entryCountPerSplit * 1.2)) < .0001);

    }

    /**
     * Unit test for {@link DefaultSplitHandler#setSampleRate(int)}.
     * 
     * @todo write unit tests for the other setters.
     */
    public void test_setSampleRate() {
        
        final int minimumEntryCount = 500 * Bytes.kilobyte32;
        final int entryCountPerSplit = 1 * Bytes.megabyte32;
        final double overCapacityMultiplier = 1.5;
        final double underCapacityMultiplier = .75;
        final int sampleRate = 20;
       
        final DefaultSplitHandler splitHandler = new DefaultSplitHandler(
                minimumEntryCount, entryCountPerSplit, overCapacityMultiplier,
                underCapacityMultiplier, sampleRate);

        assertEquals("sampleRate", sampleRate, splitHandler.getSampleRate());

        try {
            splitHandler.setSampleRate(0);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
        }
        
        try {
            splitHandler.setSampleRate(-1);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
        }
        
        assertEquals("sampleRate", sampleRate, splitHandler.getSampleRate());
        
        splitHandler.setSampleRate(10);

        assertEquals("sampleRate", 10
                , splitHandler.getSampleRate());

    }

    /**
     * @todo write unit test
     *       {@link DefaultSplitHandler#getAdjustedSplitHandler(int, long)}.
     *       This does not need to be a stress test, just investigate some
     *       interesting parameters and their effect on the internal state
     *       of the adjusted split handler.
     */
    public void test_splitAcceleration() {

//        final int minimumEntryCount = 500 * Bytes.kilobyte32;
//        final int entryCountPerSplit = 1 * Bytes.megabyte32;
//        final double overCapacityMultiplier = 1.5;
//        final double underCapacityMultiplier = .75;
//        final int sampleRate = 20;
//       
//        final DefaultSplitHandler splitHandler = new DefaultSplitHandler(
//                minimumEntryCount, entryCountPerSplit, overCapacityMultiplier,
//                underCapacityMultiplier, sampleRate);
//
//        splitHandler.getAdjustedSplitHandler(accelerateSplitThreshold, npartitions);

        fail("write test");
        
    }
    
    /**
     * Stress test based on a randomly populated index which attempts to create
     * TWO (2) splits (the minimum) through N splits, where N is the #of tuples
     * in the index (the maximum). This adjusts the {@link DefaultSplitHandler}
     * for each case so that it will seek the desired #of equal splits. All
     * splits are validated to make sure that we have complete coverage of the
     * source index.
     * 
     * @see DefaultSplitHandler#getAdjustedSplitHandlerForEqualSplits(int, long)
     */
    public void test_equal_splits_stressTest() {
        
        final IndexMetadata md = new IndexMetadata(UUID.randomUUID());
        
        final int minimumEntryCount = 500 * Bytes.kilobyte32;
        final int entryCountPerSplit = 1 * Bytes.megabyte32;
        final double overCapacityMultiplier = 1.5;
        final double underCapacityMultiplier = .75;
        final int sampleRate = 20;

        final DefaultSplitHandler splitHandler = new DefaultSplitHandler(
                minimumEntryCount, entryCountPerSplit, overCapacityMultiplier,
                underCapacityMultiplier, sampleRate);
        
        md.setSplitHandler(splitHandler);

        md.setPartitionMetadata(new LocalPartitionMetadata(//
                0, // partitionId
                -1, // sourcePartitionId (iff during a move).
                new byte[]{}, // leftSeparatorKey
                null, // rightSeparatorKey,
                new IResourceMetadata[]{
                        // fake resource.
                        new JournalMetadata(new File("testFile.jnl"), UUID
                                .randomUUID(), System.currentTimeMillis()/* createTime */)
                }, // resources (will be set by the journal).
                // cause (fake data).
                new IndexPartitionCause(
                                IndexPartitionCause.CauseEnum.Register,
                                0L/*overflowCounter*/, System
                                        .currentTimeMillis()/*lastCommitTime*/),
                "" // history
                ));
        
        final BTree btree = BTree.createTransient(md);

        // Note: This controls how stressful this test will be.
        final int maxKeys = 2000; // 10000

        // Populate the B+Tree.
        {

            final Random r = new Random();

            long lastKey = r.nextInt(Integer.MAX_VALUE);

            final int maxIncrement = 200;

            for (int i = 0; i < maxKeys; i++) {

                // strictly increasing.
                final long key = lastKey + r.nextInt(maxIncrement) + 1;

                btree.insert(key, i);

                lastKey = key;

            }
            
        }

        final int entryCount = btree.getEntryCount();

        if (log.isInfoEnabled())
            log.info("Populated index with " + entryCount + " tuples");

        final IPartitionIdFactory partitionIdFactory = new MockPartitionIdFactory();

//        final int i = 233; { // fixed value
        for (int i = 2; i < entryCount; i++) { //stress test

            final int nsplits = i;

            if (log.isInfoEnabled()) {

                log.info("Will split " + entryCount + " tuples into " + nsplits
                        + " splits");
                
            }
            
            final DefaultSplitHandler adjustedSplitHandler;
            try {

                // adjust the split handler to create that many splits.
                adjustedSplitHandler = splitHandler
                        .getAdjustedSplitHandlerForEqualSplits(nsplits,
                                entryCount);
                
            } catch (Throwable t) {

                fail("entryCount=" + entryCount + ", nsplits=" + nsplits, t);

                return;
                
            }

            try {

                // compute the splits.
                final Split[] splits = adjustedSplitHandler.getSplits(
                        partitionIdFactory, btree);
                
                // validate the computed splits.
                SplitUtility.validateSplits(btree, splits);

            } catch (Throwable t) {

                fail("entryCount=" + entryCount + ", nsplits=" + nsplits
                        + ", adjustedSplitHandler=" + adjustedSplitHandler, t);

            }

        }

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
     * FIXME write unit tests for split handler adjustments made using an
     * {@link ITupleCursor} for the {@link SparseRowStore} where the split
     * handler must obey the additional constraint that separator keys lie on
     * logical row boundaries.
     */
    public void test_logicalRowSplits() {

        fail("write tests");
        
    }

}
