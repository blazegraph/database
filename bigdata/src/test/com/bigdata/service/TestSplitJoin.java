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
 * Created on Feb 26, 2008
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.TestKeyBuilder;
import com.bigdata.btree.proc.BatchInsert.BatchInsertConstructor;
import com.bigdata.btree.proc.BatchRemove.BatchRemoveConstructor;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.rawstore.Bytes;
import com.bigdata.resources.ResourceManager.Options;
import com.bigdata.service.ndx.ClientIndexView;

/**
 * Test suite verifies that inserts eventually split an index and that deletes
 * eventually cause the index partitions to be joined.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSplitJoin extends AbstractEmbeddedFederationTestCase {

    /**
     * 
     */
    public TestSplitJoin() {
        super();
    }

    public TestSplitJoin(String name) {
        super(name);
    }

    /**
     * Overridden to specify the {@link BufferMode#Disk} mode and to lower the
     * threshold at which an overflow operation will be selected.
     */
    public Properties getProperties() {
        
        Properties properties = new Properties( super.getProperties() );
        
        // overrides value set in the superclass.
        properties.setProperty(Options.BUFFER_MODE,BufferMode.Disk.toString());

        // this test does not rely on multiple data services.
        properties.setProperty(EmbeddedClient.Options.NDATA_SERVICES, "1");

        /*
         * Note: disable copy of small index segments to the new journal during
         * overflow so the behavior is more predictable.
         */ 
        properties.setProperty(Options.COPY_INDEX_THRESHOLD, "0");

        // Note: disables index partition moves.
        properties.setProperty(Options.MAXIMUM_MOVES_PER_TARGET, "0");

        // Note: make sure joins are enabled.
        properties.setProperty(Options.JOINS_ENABLED, "true");

        // Note: disable scatter splits
        properties.setProperty(Options.SCATTER_SPLIT_ENABLED, "false");

//        /*
//         * Note: Together these properties disable incremental index builds. We
//         * need to do that since a compacting build is required before the
//         * rangeCount() for the view will drop, which is a precondition for the
//         * JOIN.
//         */
//        properties.setProperty(Options.MAXIMUM_JOURNALS_PER_VIEW, "2");
//        properties.setProperty(Options.MAXIMUM_SEGMENTS_PER_VIEW, "1");
//        properties.setProperty(Options.MAXIMUM_OPTIONAL_MERGES_PER_OVERFLOW, ""+Integer.MAX_VALUE);

        // turn off acceleration features.
        properties.setProperty(Options.ACCELERATE_OVERFLOW_THRESHOLD, "0");
        properties.setProperty(Options.ACCELERATE_SPLIT_THRESHOLD, "0");
        
        // Note: Set a low maximum shard size.
        properties.setProperty(Options.NOMINAL_SHARD_SIZE, ""+Bytes.megabyte);
        
//        properties.setProperty(Options.INITIAL_EXTENT, ""+1*Bytes.megabyte);
        
//        properties.setProperty(Options.MAXIMUM_EXTENT, ""+1*Bytes.megabyte);
        
        return properties;
        
    }

    /**
     * Test registers a scale-out index, writes data onto the initial index
     * partition, forces a split, verifies that the scale-out index has been
     * divided into two index partitions, and verifies that a range scan of the
     * scale-out index agrees with the ground truth. The test then goes on to
     * delete index entries until it forces a join of the index partitions and
     * verifies that the index partitions were in fact joined.
     * 
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_splitJoin() throws IOException, InterruptedException,
            ExecutionException {
        
        /*
         * Register the index.
         */
        final String name = "testIndex";
        final UUID indexUUID = UUID.randomUUID();
        final int batchSize = 5000;
//        final int entryCountPerSplit = 400;
//        final double overCapacityMultiplier = 1.5;
//        final int minimumEntryCountPerSplit = 100;
        {

            final IndexMetadata indexMetadata = new IndexMetadata(name,indexUUID);

//            // The threshold below which we will try to join index partitions.
//            ((DefaultSplitHandler)indexMetadata.getSplitHandler()).setMinimumEntryCount(minimumEntryCountPerSplit);
//            
//            // The target #of index entries per partition.
//            ((DefaultSplitHandler)indexMetadata.getSplitHandler()).setEntryCountPerSplit(entryCountPerSplit);
//
//            // Overcapacity multiplier before an index partition will be split.
//            ((DefaultSplitHandler)indexMetadata.getSplitHandler()).setOverCapacityMultiplier(overCapacityMultiplier);
            
            // must support delete markers
            indexMetadata.setDeleteMarkers(true);

            // register the scale-out index, creating a single index partition.
            fed.registerIndex(indexMetadata, dataService0.getServiceUUID());
            
        }

        /*
         * Verify the initial index partition.
         */
        final PartitionLocator pmd0;
        {
            
            final ClientIndexView ndx = (ClientIndexView) fed.getIndex(name,
                    ITx.UNISOLATED);

            final IMetadataIndex mdi = ndx.getMetadataIndex();
            
            assertEquals("#index partitions", 1, mdi.rangeCount(null, null));

            // This is the initial partition locator metadata record.
            pmd0 = mdi.get(new byte[]{});

            assertEquals("partitionId", 0L, pmd0.getPartitionId());

            assertEquals("dataServiceUUIDs", dataService0.getServiceUUID(),
                    pmd0.getDataServiceUUID());
            
        }
        assertEquals("partitionCount", 1, getPartitionCount(name));
        
        /*
         * Setup the ground truth B+Tree. This is backed by a temporary raw
         * store so that it does not overflow.
         */
        final BTree groundTruth;
        {
        
            final IndexMetadata indexMetadata = new IndexMetadata(indexUUID);

            groundTruth = BTree.create(new TemporaryRawStore(), indexMetadata);

        }

        /*
         * Populate the index with data until the initial the journal for the
         * data service on which the initial partition resides overflows.
         * 
         * Note: Since the keys are random there can be duplicates which means
         * that the resulting rangeCount can be less than the #of tuples written
         * on the index. We handle by looping until a scan of the metadata index
         * shows that an index partition split has occurred.
         * 
         * Note: The index split will occur asynchronously once (a) the index
         * partition has a sufficient #of entries; and (b) a group commit
         * occurs. However, this loop will continue to run, so writes will
         * continue to accumulate on the index partition on the live journal.
         * Once the overflow process completes the client be notified that the
         * index partition which it has been addressing no longer exists on the
         * data service. At that point the client SHOULD re-try the operation.
         * Once the client returns from that retry we will notice that the
         * partition count has increased and exit this loop.
         */
        int nrounds = 0;
        long nwritten = 0L;
//        boolean done = false;
        int npartitions = -1;
        final long overflowCounter0 = dataService0.getAsynchronousOverflowCounter();
        long overflowCounter = overflowCounter0;
        while (npartitions < 2) {

            final int nentries = batchSize;//minimumEntryCountPerSplit;
//            final KV[] data = getRandomKeyValues(nentries);
            final byte[][] keys = new byte[nentries][];
            final byte[][] vals = new byte[nentries][];

            for (int i = 0; i < nentries; i++) {

                keys[i] = TestKeyBuilder.asSortKey(nwritten + i);

                vals[i] = SerializerUtil.serialize(nwritten + i);
                
            }

            // insert the data into the ground truth index.
            groundTruth
                    .submit(0/*fromIndex*/,nentries/*toIndex*/, keys, vals,
                            BatchInsertConstructor.RETURN_NO_VALUES, null/* handler */);

            /*
             * Set flag to force overflow on group commit.
             */
//            if (groundTruth.getEntryCount() >= overCapacityMultiplier * entryCountPerSplit) {
                
                dataService0.forceOverflow(false/*immediate*/,false/*compactingMerge*/);

//                done = true;
//                
//            }

            // insert the data into the scale-out index.
            fed.getIndex(name, ITx.UNISOLATED)
                    .submit(0/*fromIndex*/,nentries/*toIndex*/, keys, vals,
                            BatchInsertConstructor.RETURN_NO_VALUES, null/* handler */);
            
            /*
             * Problem is comparing entryCount on ground truth to rangeCount
             * on scale-out index. Since duplicate keys can be generated the
             * scale-out count can be larger than the ground truth count.
             * Write a utility method based on an iterator that returns the
             * real count for the scale-out index if I care.
             */
//            assertEquals("rangeCount", groundTruth.getEntryCount(), fed
//                    .getIndex(name, ITx.UNISOLATED).rangeCount(null, null));
            
            // wait until asynchronous overflow processing is done.
            overflowCounter = awaitAsynchronousOverflow(dataService0,
                    overflowCounter/* oldValue */);

            nrounds++;

            nwritten += nentries;
            
            // When GTE 2 the initial index partition was split.
            npartitions = getPartitionCount(name);
            
            if (log.isInfoEnabled())
                log.info("Populating the index: overflowCounter="
                        + overflowCounter
                        + ", nrounds="
                        + nrounds
                        + ", nwritten="
                        + nwritten
                        + ", nentries="
                        + groundTruth.getEntryCount()
                        + " ("
                        + fed.getIndex(name, ITx.UNISOLATED).rangeCount() + ")"
                        + ", npartitions=" + npartitions);

            /*
             * Compare the index against ground truth after overflow.
             */

            if (log.isInfoEnabled())
                log.info("Verifying scale-out index against ground truth");

            assertSameEntryIterator(groundTruth, fed.getIndex(name,
                    ITx.UNISOLATED));

        }
        
//        // wait until overflow processing is done.
//        final long overflowCounter1 = awaitAsynchronousOverflow(dataService0,
//                overflowCounter0);
//        
//        assertEquals("partitionCount", 2, getPartitionCount(name));
        
//        /*
//         * Compare the index against ground truth after overflow.
//         */
//        
//        if (log.isInfoEnabled())
//            log.info("Verifying scale-out index against ground truth");
//
//        assertSameEntryIterator(groundTruth, fed.getIndex(name, ITx.UNISOLATED));

        /*
         * Get the key range for the left-most index partition and then delete
         * entries until the index partition underflows.
         */
        nrounds = 0;
        while (npartitions >= 2) {

            final byte[] fromKey = new byte[] {};

            final PartitionLocator locator = fed.getMetadataIndex(name,
                    ITx.READ_COMMITTED).get(fromKey);

            final byte[] toKey = locator.getRightSeparatorKey();

            // ground truth range count for that index partition.
            final int rangeCount = (int) groundTruth.rangeCount(fromKey, toKey);

            assertTrue(rangeCount > 0);
            
            // #of entries to delete (seeking to trigger a join operation).
            final int ndelete = rangeCount <= batchSize ? (rangeCount / 2) + 1
                    : rangeCount - batchSize;
            
            assertTrue("rangeCount=" + rangeCount + ", batchSize=" + batchSize
                    + ", ndelete=" + ndelete, ndelete > 0);
            
            final byte[][] keys = new byte[ndelete][];

            final ITupleIterator<?> itr = groundTruth.rangeIterator(fromKey,
                    toKey, ndelete, IRangeQuery.KEYS, null/* filter */);

            for (int i = 0; i < ndelete; i++) {

                keys[i] = itr.next().getKey();

            }

            if (log.isInfoEnabled())
                log.info("Will delete " + ndelete + " of " + rangeCount
                        + " entries from " + locator + " to trigger underflow");

            groundTruth.submit(0/* fromIndex */, ndelete/* toIndex */, keys,
                    null/* vals */, BatchRemoveConstructor.RETURN_MUTATION_COUNT,
                    null/* handler */);

            // data service will overflow at the next group commit.
            dataService0
                    .forceOverflow(false/* immediate */, false/* compactingMerge */);

            // delete those tuples, triggering overflow.
            fed.getIndex(name, ITx.UNISOLATED).submit(0/* fromIndex */,
                    ndelete/* toIndex */, keys, null/* vals */,
                    BatchRemoveConstructor.RETURN_MUTATION_COUNT, null/* handler */);
            
            // wait until asynchronous overflow processing is done.
            overflowCounter = awaitAsynchronousOverflow(dataService0,
                    overflowCounter/* oldValue */);

            // #of partitions afterwards.
            npartitions = getPartitionCount(name);
            
            if (log.isInfoEnabled())
                log.info("Populating the index: overflowCounter="
                        + overflowCounter
                        + ", nrounds="
                        + nrounds
                        + ", nwritten="
                        + nwritten
                        + ", nentries="
                        + groundTruth.getEntryCount()
                        + " ("
                        + fed.getIndex(name, ITx.UNISOLATED).rangeCount(null,
                                null) + ")" + ", npartitions=" + npartitions);

            /*
             * Compare the index against ground truth after overflow.
             */
            if (log.isInfoEnabled())
                log.info("Verifying scale-out index against ground truth");

            assertSameEntryIterator(groundTruth, fed.getIndex(name,
                    ITx.UNISOLATED));

        }

//        // wait until overflow processing is done.
//        final long overflowCounter2 = awaitAsynchronousOverflow(dataService0,
//                overflowCounter1);
//        
//        /*
//         * Confirm index partitions were NOT joined.
//         * 
//         * Note: Even though we have deleted index entries the #of index entries
//         * in the partition (as reported by rangeCount()) WILL NOT be reduced
//         * until the next build task for that index partition. Therefore our
//         * post-overflow expectation is that an index build was performed and
//         * the #of index partitions WAS NOT changed.
//         */
//        assertEquals("partitionCount", 2, getPartitionCount(name));
//        
//        /*
//         * Compare the index against ground truth after overflow.
//         */
//        
//        if (log.isInfoEnabled())
//            log.info("Verifying scale-out index against ground truth");
//
//        assertSameEntryIterator(groundTruth, fed.getIndex(name, ITx.UNISOLATED));

//        /*
//         * Set the forceOverflow flag and then write another index entry on the
//         * other index partition (not the one that we are trying to underflow).
//         * This should trigger a JOIN operation now that the index partition has
//         * gone through a compacting build.
//         */
//        {
//
//            dataService0
//                    .forceOverflow(false/* immediate */, false/* compactingMerge */);
//
//            // find the locator for the last index partition.
//            final PartitionLocator locator = fed.getMetadataIndex(name,
//                    ITx.READ_COMMITTED).find(null);
//
//            final byte[][] keys = new byte[][] { locator.getLeftSeparatorKey() };
////            final byte[][] vals = new byte[][] { /*empty byte[] */ };
//
//            // overwrite the value (if any) under the left separator key.
//            groundTruth.submit(0/* fromIndex */, 1/* toIndex */, keys,
//                    null/* vals */, BatchRemoveConstructor.RETURN_MUTATION_COUNT,
//                    null/* handler */);
//
//            // overwrite the value (if any) under the left separator key.
//            fed.getIndex(name, ITx.UNISOLATED)
//                    .submit(0/* fromIndex */, 1/* toIndex */, keys, null/*vals*/,
//                            BatchRemoveConstructor.RETURN_MUTATION_COUNT, null/* handler */);
//            
//        }
//
//        // wait until overflow processing is done.
//        final long overflowCounter3 = awaitAsynchronousOverflow(dataService0, overflowCounter2);
                
//        /*
//         * Confirm index partitions were joined.
//         * 
//         * Note: Even though we have deleted index entries the #of index entries
//         * in the partition WILL NOT be reduced until the next build task for
//         * that index partition. Therefore our post-overflow expectation is that
//         * an index build was performed and the #of index partitions WAS NOT
//         * changed.
//         */

        // Confirm the index partitions were joins.
        assertEquals("partitionCount", 1, getPartitionCount(name));
        
        /*
         * Compare the index against ground truth after overflow.
         */
        
        if (log.isInfoEnabled())
            log.info("Verifying scale-out index against ground truth");

        assertSameEntryIterator(groundTruth, fed.getIndex(name,ITx.UNISOLATED));
        
    }
    
}
