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
     * Unit tests validate that the adjusted split handler embodies constraints
     * such that an index partition would be split into more or less equal sized
     * key ranges.
     * <p>
     * Note: There are several ways to make the adjusted split handler do its
     * job. However, they do not all work out equally well. This test is written
     * to verify that the adjustments preserve the underCapacityMultiplier while
     * making
     * 
     * <pre>
     * rangeCount := nsplits * entryCountPerSplit * underCapacityMultiplier
     * </pre>
     * 
     * true within the constraints of rounding.
     * <p>
     * The adjusted split handler should also adjust the minimumEntryCount
     * (smallest split size before a join would be triggered) such that
     * 
     * <pre>
     * ratio := minimumEntryCount / entryCountPerSplit
     * </pre>
     * 
     * remains constant before and after the adjustment within the constraints
     * of rounding.
     */
    public void test_adjustedForEqualsSplits() {

        final int minimumEntryCount = 500 * Bytes.kilobyte32;
        final int entryCountPerSplit = 1 * Bytes.megabyte32;
        final double overCapacityMultiplier = 1.5;
        final double underCapacityMultiplier = .75;
        final int sampleRate = 20;

        final DefaultSplitHandler splitHandler = new DefaultSplitHandler(
                minimumEntryCount, entryCountPerSplit, overCapacityMultiplier,
                underCapacityMultiplier, sampleRate);

        final int nsplits = 2;
        final long rangeCount = 2000;

        final double expectedRatio = minimumEntryCount
                / (double) entryCountPerSplit;

        final DefaultSplitHandler adjustedSplitHandler = splitHandler
                .getAdjustedSplitHandlerForEqualSplits(nsplits, rangeCount);

        final double actualRatio = adjustedSplitHandler.getMinimumEntryCount()
                / (double) adjustedSplitHandler.getEntryCountPerSplit();

        final double actualProduct = nsplits
                * adjustedSplitHandler.getEntryCountPerSplit()
                * adjustedSplitHandler.getUnderCapacityMultiplier();

        if (log.isInfoEnabled())
            log.info("nsplits=" + nsplits + ", rangeCount=" + rangeCount
                    + ", product=" + actualProduct + ", expectedRatio="
                    + expectedRatio + ", actualRatio=" + actualRatio);
        
        assertEquals("product", rangeCount, Math.round(actualProduct));

        if (actualRatio / expectedRatio < .99
                || actualRatio / expectedRatio > 1.01) {

            /*
             * The ratios are not close to being the same (not near 1:1).
             */
            
            fail("ratio: expected=" + expectedRatio + ", actual=" + actualRatio);

        }

        /*
         * This value should not have been changed.
         * 
         * Note: this one is important as it effects how the split points are
         * choosen and how many splits actually get choosen.
         */
        assertEquals("underCapacityMultiplier", underCapacityMultiplier,
                adjustedSplitHandler.getUnderCapacityMultiplier());

        // should not have been changed (not critical)
        assertEquals("overCapacityMultiplier", overCapacityMultiplier,
                adjustedSplitHandler.getOverCapacityMultiplier());

        // should not have been changed (not critical).
        assertEquals("sampleRate", sampleRate, adjustedSplitHandler
                .getSampleRate());

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

//        final int i = 3; { // fixed value
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

                if (splits.length != nsplits) {

                    /*
                     * This is not necessarily an error. We do check for error
                     * conditions below (a split which is too small or too
                     * large).
                     */
                    
                    log.warn("nsplits: expected=" + nsplits + ", actual="
                            + splits.length);

                }

                // #of tuples samples that will be coverged per sample. 
                final double tuplesPerSample = entryCount
                        / adjustedSplitHandler.getSampleRate(); 
                
                /*
                 * This is the ideal #of tuples in a split. Reality is permitted
                 * to vary somewhat. For the purposes of this test we allow some
                 * variation since the point is to verify that the splits are of
                 * approximately the right size and there are cases where we
                 * need to accept more or fewer tuples into a given split.
                 */
                final int targetEntryCountPerSplit = adjustedSplitHandler
                        .getTargetEntryCountPerSplit();

                // The permitted margin of error from the ideal.
                final double margin = .3;
                
                final int minAllowed = (int) Math.min(//
                        targetEntryCountPerSplit - tuplesPerSample / 2, //
                        targetEntryCountPerSplit * (1 - margin));

                final int maxAllowed = (int) Math.max(//
                        targetEntryCountPerSplit + tuplesPerSample / 2,//
                        targetEntryCountPerSplit * (1 + margin));

                for (int j = 0; j < splits.length; j++) {

                    final Split split = splits[j];

                    if (j + 1 == splits.length) {

                        /*
                         * If samples have a bias such that each split is
                         * slightly the target capacity, then the last split can
                         * wind up short. Therefore we give it a greater margin
                         * than the other splits. However, it should not be
                         * under 1/2 of the ideal -- otherwise the tuples really
                         * should have been placed within the previous split.
                         */
                        
                        if (split.ntuples < targetEntryCountPerSplit / 2) {

                            fail("Not enough tuples: split[" + j + "] : "
                                    + split + ", targetEntryCountPerSplit="
                                    + targetEntryCountPerSplit);

                        }

                    } else {
                        
                        if (split.ntuples < minAllowed) {

                            fail("Not enough tuples: split[" + j + "] : "
                                    + split + ", targetEntryCountPerSplit="
                                    + targetEntryCountPerSplit + ", margin="
                                    + margin + ", minAllowed=" + minAllowed);

                        }
                        
                    }
                    
                    if (split.ntuples > maxAllowed) {

                        fail("Too many tuples: split[" + j + "] : " + split
                                + ", targetEntryCountPerSplit="
                                + targetEntryCountPerSplit + ", margin="
                                + margin + ", maxAllowed=" + maxAllowed);

                    }
                    
                }
                
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
