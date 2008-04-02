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
 * Created on Mar 11, 2008
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IOverflowHandler;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.KV;
import com.bigdata.btree.BatchInsert.BatchInsertConstructor;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.resources.DefaultSplitHandler;
import com.bigdata.resources.ResourceManager;
import com.bigdata.resources.ResourceManager.Options;

/**
 * Some unit tests for moving an index partition.
 * 
 * @todo test with an {@link IOverflowHandler}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIndexPartitionMove extends AbstractEmbeddedFederationTestCase {

    public TestIndexPartitionMove() {
        super();
    }

    public TestIndexPartitionMove(String name) {
        super(name);
    }

    /**
     * Overriden to specify the {@link BufferMode#Disk} mode and to lower the
     * threshold at which an overflow operation will be selected.
     */
    public Properties getProperties() {
        
        Properties properties = new Properties( super.getProperties() );
        
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk
                .toString());

        // Note: disable copy of small index segments to the new journal during overflow.
        properties.setProperty(Options.COPY_INDEX_THRESHOLD,"0");
        
        // set low minimum #of active partitions per data service.
        properties.setProperty(Options.MINIMUM_ACTIVE_INDEX_PARTITIONS,"1");
        
        // enable moves (one per target).
        properties.setProperty(ResourceManager.Options.MAXIMUM_MOVES_PER_TARGET,"1");

//        properties.setProperty(Options.INITIAL_EXTENT, ""+1*Bytes.megabyte);
        
//        properties.setProperty(Options.MAXIMUM_EXTENT, ""+1*Bytes.megabyte);
        
        return properties;
        
    }

    /**
     * Test forces a move of an index partition and validates the scale-out
     * after the move against ground truth.
     * 
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     * 
     * FIXME This test needs to override the {@link ILoadBalancerService} in
     * order to setup the pre-conditions for the move (the source data service
     * is "highly utilized" and the target data service is "under utilized").
     */
    public void test_move() throws IOException, InterruptedException, ExecutionException {
        
        /*
         * Register the index.
         */
        final String name = "testIndex";
        final UUID indexUUID = UUID.randomUUID();
        final int entryCountPerSplit = 400;
        final double overCapacityMultiplier = 1.5;
        final int minimumEntryCountPerSplit = 100;
        {

            final IndexMetadata indexMetadata = new IndexMetadata(name,indexUUID);

            // The threshold below which we will try to join index partitions.
            ((DefaultSplitHandler)indexMetadata.getSplitHandler()).setMinimumEntryCount(minimumEntryCountPerSplit);
            
            // The target #of index entries per partition.
            ((DefaultSplitHandler)indexMetadata.getSplitHandler()).setEntryCountPerSplit(entryCountPerSplit);

            // Overcapacity multipler before an index partition will be split.
            ((DefaultSplitHandler)indexMetadata.getSplitHandler()).setOverCapacityMultiplier(overCapacityMultiplier);
            
            // must support delete markers
            indexMetadata.setDeleteMarkers(true);

            // register the scale-out index, creating a single index partition.
            fed.registerIndex(indexMetadata,dataService0.getServiceUUID());
            
        }

        /*
         * Verify the initial index partition.
         */
        final PartitionLocator pmd0;
        {
            
            ClientIndexView ndx = (ClientIndexView)fed.getIndex(name,ITx.UNISOLATED);
            
            IMetadataIndex mdi = ndx.getMetadataIndex();
            
            assertEquals("#index partitions", 1, mdi.rangeCount(null, null));

            // This is the initial partition locator metadata record.
            pmd0 = mdi.get(new byte[]{});

            assertEquals("partitionId", 0L, pmd0.getPartitionId());

            assertEquals("dataServiceUUIDs", new UUID[] { dataService0
                    .getServiceUUID() }, pmd0.getDataServices());
            
        }
        assertEquals("partitionCount", 1, getPartitionCount(name));
        
        /*
         * Setup the ground truth B+Tree.
         */
        final BTree groundTruth;
        {
        
            IndexMetadata indexMetadata = new IndexMetadata(indexUUID);
            
            groundTruth = BTree.create(new TemporaryRawStore(),indexMetadata);
        
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
         * Once the client returns from tha retry we will notice that the
         * partition count has increased and exit this loop.
         */
        final long overflowCounter0 = dataService0.getOverflowCounter();
        {

            log.info("Writing on indices to provoke overflow");

            int nrounds = 0;
            long nwritten = 0L;
            boolean done = false;
            while (!done) {

                final int nentries = minimumEntryCountPerSplit;
                final KV[] data = getRandomKeyValues(nentries);
                final byte[][] keys = new byte[nentries][];
                final byte[][] vals = new byte[nentries][];

                for (int i = 0; i < nentries; i++) {

                    keys[i] = data[i].key;

                    vals[i] = data[i].val;

                }

                // insert the data into the ground truth index.
                groundTruth
                        .submit(0/* fromIndex */, nentries/* toIndex */, keys,
                                vals, BatchInsertConstructor.RETURN_NO_VALUES,
                                null/* handler */);

                /*
                 * Set flag to force overflow on group commit if the ground
                 * truth index has reached the split threshold.
                 */
                if (groundTruth.getEntryCount() >= overCapacityMultiplier
                        * entryCountPerSplit) {

                    dataService0.forceOverflow();

                    done = true;

                }

                // insert the data into the scale-out index.
                fed.getIndex(name, ITx.UNISOLATED)
                        .submit(0/* fromIndex */, nentries/* toIndex */, keys,
                                vals, BatchInsertConstructor.RETURN_NO_VALUES,
                                null/* handler */);

                assertEquals("rangeCount", groundTruth.getEntryCount(), fed
                        .getIndex(name, ITx.UNISOLATED).rangeCount(null, null));

                nrounds++;

                nwritten += nentries;

                System.err.println("Populating the index: nrounds="
                        + nrounds
                        + ", nwritten="
                        + nwritten
                        + ", nentries="
                        + groundTruth.getEntryCount()
                        + " ("
                        + fed.getIndex(name, ITx.UNISOLATED).rangeCount(null,
                                null) + ")");

            }
            
        }
        
        // wait until overflow processing is done.
        final long overflowCounter1 = awaitOverflow(dataService0,overflowCounter0);
        
        assertEquals("partitionCount", 2,getPartitionCount(name));
        
        /*
         * Compare the index against ground truth after overflow.
         */
        
        System.err.println("Verifying scale-out index against ground truth");

        assertSameEntryIterator(groundTruth, fed.getIndex(name,ITx.UNISOLATED));

        /*
         * Continue to populate index until we can provoke another overflow.
         * Since there are now 2 index partitions and 2 data services and since
         * we have configured the various thresholds appropriately this overflow
         * should select one of the index partitions to move over to the other
         * data service.
         */
        {

            log.info("Writing on indices to provoke overflow");
            
            int nrounds = 0;
            long nwritten = 0L;
            boolean done = false;
            while (!done) {

                final int nentries = minimumEntryCountPerSplit;
                final KV[] data = getRandomKeyValues(nentries);
                final byte[][] keys = new byte[nentries][];
                final byte[][] vals = new byte[nentries][];

                for (int i = 0; i < nentries; i++) {

                    keys[i] = data[i].key;

                    vals[i] = data[i].val;

                }

                // insert the data into the ground truth index.
                groundTruth
                        .submit(0/* fromIndex */, nentries/* toIndex */, keys,
                                vals, BatchInsertConstructor.RETURN_NO_VALUES,
                                null/* handler */);

                /*
                 * Set flag to force overflow on group commit if the ground
                 * truth index has reached the split threshold.
                 */
                if (groundTruth.getEntryCount() >= overCapacityMultiplier
                        * entryCountPerSplit) {

                    dataService0.forceOverflow();

                    done = true;

                }

                // insert the data into the scale-out index.
                fed.getIndex(name, ITx.UNISOLATED)
                        .submit(0/* fromIndex */, nentries/* toIndex */, keys,
                                vals, BatchInsertConstructor.RETURN_NO_VALUES,
                                null/* handler */);

                /*
                 * Problem is comparing entryCount on ground truth to rangeCount
                 * on scale-out index. Since duplicate keys can be generated the
                 * scale-out count can be larger than the ground truth count.
                 * Write a utility method based on an iterator that returns the
                 * real count for the scale-out index if I care.
                 */
//                assertEquals("rangeCount", groundTruth.getEntryCount(), fed
//                        .getIndex(name, ITx.UNISOLATED).rangeCount(null, null));

                nrounds++;

                nwritten += nentries;

                System.err.println("Populating the index: nrounds="
                        + nrounds
                        + ", nwritten="
                        + nwritten
                        + ", nentries="
                        + groundTruth.getEntryCount()
                        + " ("
                        + fed.getIndex(name, ITx.UNISOLATED).rangeCount(null,
                                null) + ")");

            }
        
        }
        
        // wait until overflow processing is done.
        final long overflowCounter2 = awaitOverflow(dataService0,overflowCounter1);
        
        assertEquals("partitionCount", 2,getPartitionCount(name));

        /*
         * Figure out which index partition was moved and verify that there is
         * now one index partition on each data service.
         */
        {
            
            int ndataService0 = 0;
            int ndataService1 = 0;
            
            final ITupleIterator itr = new RawDataServiceRangeIterator(
                    metadataService,//
                    MetadataService.getMetadataIndexName(name), //
                    ITx.READ_COMMITTED,//
                    true, // readConsistent
                    null, // fromKey
                    null, // toKey
                    0,    // capacity,
                    IRangeQuery.DEFAULT,// flags
                    null // filter
                    );

            int n = 0;

            while (itr.hasNext()) {

                PartitionLocator locator = (PartitionLocator) SerializerUtil
                        .deserialize(itr.next().getValue());

                System.err.println("locators["+n+"]="+locator);
                
                if (locator.getDataServices()[0].equals(dataService0
                        .getServiceUUID())) {

                    ndataService0++;

                } else if (locator.getDataServices()[0].equals(dataService1
                        .getServiceUUID())) {

                    ndataService1++;

                } else {

                    fail("Not expecting partition move to this service: "
                            + locator);
                    
                }
                
                n++;

            }
            
            assertEquals("#dataService0",1,ndataService0);

            assertEquals("#dataService1",1,ndataService1);
            
        }
        
        /*
         * Compare the index against ground truth after overflow.
         */
        
        System.err.println("Verifying scale-out index against ground truth");

        assertSameEntryIterator(groundTruth, fed.getIndex(name,ITx.UNISOLATED));

    }

}
