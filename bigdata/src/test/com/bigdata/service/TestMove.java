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

import org.apache.log4j.Level;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.KV;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.proc.BatchInsert.BatchInsertConstructor;
import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.rawstore.Bytes;
import com.bigdata.resources.ResourceManager;
import com.bigdata.resources.ResourceManager.Options;
import com.bigdata.service.ndx.ClientIndexView;
import com.bigdata.service.ndx.RawDataServiceTupleIterator;

/**
 * Some unit tests for moving an index partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMove extends AbstractEmbeddedFederationTestCase {

    public TestMove() {
        super();
    }

    public TestMove(String name) {
        super(name);
    }

    /**
     * Overridden to specify the {@link BufferMode#Disk} mode and to lower the
     * threshold at which an overflow operation will be selected.
     */
    public Properties getProperties() {
        
        final Properties properties = new Properties(super.getProperties());
        
        // overrides Transient in the base class.
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk
                .toString());

        // this test relies on 2 or more data services.
        properties.setProperty(EmbeddedClient.Options.NDATA_SERVICES, "2");

        // Note: disable copy of small index segments to the new journal during overflow.
        properties.setProperty(Options.COPY_INDEX_THRESHOLD,"0");
        
        // set low minimum #of active partitions per data service.
        properties.setProperty(Options.MINIMUM_ACTIVE_INDEX_PARTITIONS,"1");
        
        // enable moves (one per target).
        properties.setProperty(ResourceManager.Options.MAXIMUM_MOVES_PER_TARGET,"1");

        // allow move of shards which would otherwise be split.
        properties.setProperty(ResourceManager.Options.MAXIMUM_MOVE_PERCENT_OF_SPLIT,"2.0");

        // disable the CPU threshold for moves.
        properties.setProperty(ResourceManager.Options.MOVE_PERCENT_CPU_TIME_THRESHOLD,".0");
        
        // disable scatter split
        properties.setProperty(ResourceManager.Options.SCATTER_SPLIT_ENABLED,"false");

        /*
         * Note: Disables the initial round robin policy for the load balancer
         * service so that it will use our fakes scores.
         */
        properties.setProperty(LoadBalancerService.Options.INITIAL_ROUND_ROBIN_UPDATE_COUNT, "0");

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
     * Test forces a move of an index partition and validates the scale-out
     * index after the move against ground truth.
     * 
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_move() throws IOException, InterruptedException, ExecutionException {
        
        /*
         * Register the index.
         */
        final String name = "testIndex";
        final UUID indexUUID = UUID.randomUUID();
        {

            final IndexMetadata indexMetadata = new IndexMetadata(name,indexUUID);

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
            
            assertEquals("#index partitions", 1, mdi.rangeCount());

            // This is the initial partition locator metadata record.
            pmd0 = mdi.get(new byte[]{});

            assertEquals("partitionId", 0L, pmd0.getPartitionId());

            assertEquals("dataServiceUUID", dataService0
                    .getServiceUUID(), pmd0.getDataServiceUUID());
            
        }
        assertEquals("partitionCount", 1, getPartitionCount(name));
        
        /*
         * Setup the ground truth B+Tree.
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
         * Note: The index split will occur asynchronously once (a) the index
         * partition has a sufficient #of entries; and (b) a group commit
         * occurs. However, this loop will continue to run, so writes will
         * continue to accumulate on the index partition on the live journal.
         * Once the overflow process completes the client be notified that the
         * index partition which it has been addressing no longer exists on the
         * data service. At that point the client SHOULD re-try the operation.
         * Once the client returns from the retry we will notice that the
         * partition count has increased and exit this loop.
         */
        final int batchSize = 5000;
        long overflowCounter = dataService0.getAsynchronousOverflowCounter();
        int npartitions = -1;
        {

            if(log.isInfoEnabled())
                log.info("Writing on indices to provoke overflow");

            int nrounds = 0;
            long nwritten = 0L;
            while (npartitions < 2) {

                final byte[][] keys = new byte[batchSize][];
                final byte[][] vals = new byte[batchSize][];

                for (int i = 0; i < batchSize; i++) {

                    keys[i] = KeyBuilder.asSortKey(nwritten + i);

                    vals[i] = SerializerUtil.serialize(nwritten + i);
                    
                }

                // insert the data into the ground truth index.
                groundTruth
                        .submit(0/* fromIndex */, batchSize/* toIndex */, keys,
                                vals, BatchInsertConstructor.RETURN_NO_VALUES,
                                null/* handler */);

                // Set flag to force overflow on group commit.
                dataService0
                        .forceOverflow(false/* immediate */, false/* compactingMerge */);

                // insert the data into the scale-out index.
                fed.getIndex(name, ITx.UNISOLATED)
                        .submit(0/* fromIndex */, batchSize/* toIndex */, keys,
                                vals, BatchInsertConstructor.RETURN_NO_VALUES,
                                null/* handler */);

                overflowCounter = awaitAsynchronousOverflow(dataService0,
                        overflowCounter);
                
                assertEquals("rangeCount", groundTruth.getEntryCount(), fed
                        .getIndex(name, ITx.UNISOLATED).rangeCount());

                nrounds++;

                nwritten += batchSize;

                npartitions = getPartitionCount(name);

//                if (log.isInfoEnabled())
//                    log.info
                System.err.println
                    ("Populating the index: overflowCounter="
                        + overflowCounter + ", nrounds=" + nrounds
                        + ", nwritten=" + nwritten + ", nentries="
                        + groundTruth.getEntryCount() + " ("
                        + fed.getIndex(name, ITx.UNISOLATED).rangeCount()
                        + "), npartitions=" + npartitions);

                /*
                 * Compare the index against ground truth after overflow.
                 */
                if(log.isInfoEnabled())
                    log.info("Verifying scale-out index against ground truth");

                assertSameEntryIterator(groundTruth, fed.getIndex(name,
                        ITx.UNISOLATED));

            }

        }

        npartitions = getPartitionCount(name);

        // Verify at least 2 partitions.
        assertTrue("partitionCount=" + npartitions, npartitions >= 2);
        
        /*
         * Fake out the load balancer so that it will report the source data
         * service (dataService0) is "highly utilized" and the target data
         * service (dataService1) is "under utilized".
         */
        {

            System.err.println("Setting up LBS for move.");

            // explicitly set the log level for the load balancer.
            LoadBalancerService.log.setLevel(Level.INFO);
            
            final AbstractEmbeddedLoadBalancerService lbs = ((AbstractEmbeddedLoadBalancerService) ((EmbeddedFederation) fed)
                    .getLoadBalancerService());

            final ServiceScore[] fakeServiceScores = new ServiceScore[2];

            fakeServiceScores[0] = new ServiceScore(
                    AbstractStatisticsCollector.fullyQualifiedHostName,
                    dataService0.getServiceUUID(), "dataService0", 1.0/* rawScore */);

            fakeServiceScores[1] = new ServiceScore(
                    AbstractStatisticsCollector.fullyQualifiedHostName,
                    dataService1.getServiceUUID(), "dataService1", 0.0/* rawScore */);

            // set the fake scores on the load balancer.
            lbs.setServiceScores(fakeServiceScores);
            
        }

        /*
         * Continue to populate index until we can provoke another overflow.
         * 
         * Since we have configured the various thresholds appropriately this
         * overflow should select one of the index partitions to move over to
         * the other data service.
         */
        {

            if (log.isInfoEnabled())
                log.info("Writing on indices to provoke overflow");
            
//            int nrounds = 0;
//            long nwritten = 0L;
//            boolean done = false;
//            while (!done) 
            {

                /*
                 * Just a little random data.
                 * 
                 * Note: We have to write enough data so that the new updates
                 * are not just copied onto the new journal in order for the
                 * index partition(s) on which we write to be eligible for a
                 * move. 
                 */
                final int nentries = 5000;
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
                 * Set flag to force overflow on group commit.
                 */
                dataService0
                        .forceOverflow(false/* immediate */, false/* compactingMerge */);

                // insert the data into the scale-out index.
                fed.getIndex(name, ITx.UNISOLATED)
                        .submit(0/* fromIndex */, nentries/* toIndex */, keys,
                                vals, BatchInsertConstructor.RETURN_NO_VALUES,
                                null/* handler */);

                // wait until overflow processing is done.
                overflowCounter = awaitAsynchronousOverflow(dataService0,
                        overflowCounter);

                /*
                 * Compare the index against ground truth after overflow.
                 */

                if (log.isInfoEnabled())
                    log.info("Verifying scale-out index against ground truth");

                assertSameEntryIterator(groundTruth, fed.getIndex(name,
                        ITx.UNISOLATED));

            }
        
        }

        /*
         * Figure out which index partition was moved and verify that there is
         * now (at least) one index partition on each data service.
         */
        {

            int ndataService0 = 0;// #of index partitions on data service 0.
            int ndataService1 = 0;// #of index partitions on data service 1.
            
            final ITupleIterator itr = new RawDataServiceTupleIterator(
                    fed.getMetadataService(),//
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

                final PartitionLocator locator = (PartitionLocator) SerializerUtil
                        .deserialize(itr.next().getValue());

                System.err.println("locators["+n+"]="+locator);
                
                if (locator.getDataServiceUUID().equals(dataService0
                        .getServiceUUID())) {

                    ndataService0++;

                } else if (locator.getDataServiceUUID().equals(
                        dataService1.getServiceUUID())) {

                    ndataService1++;

                } else {

                    fail("Not expecting partition move to this service: "
                            + locator);

                }

                n++;

            }

            System.err.println("npartitions=" + getPartitionCount(name));
            System.err.println("npartitions(ds0)=" + ndataService0);
            System.err.println("npartitions(ds1)=" + ndataService1);
            assertEquals("#dataService0=" + ndataService0, 1, ndataService0);
            assertEquals("#dataService1=" + ndataService0, 1, ndataService1);

        }

    }

}
