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
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ISplitHandler;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.BatchInsert.BatchInsertConstructor;
import com.bigdata.btree.BatchRemove.BatchRemoveConstructor;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.repo.BigdataRepository.Options;
import com.bigdata.resources.DefaultSplitHandler;
import com.bigdata.resources.ResourceManager;
import com.bigdata.sparse.SparseRowStore;

/**
 * Tests for various scenarios that result in overflow of the live journal
 * backing a data service.
 * 
 * @todo This test suite really needs to be re-run against the distributed
 *       federation as well. This is especially important since the distributed
 *       federation may have different behavior arising from client-side caching
 *       of the metadata index and RMI marshalling is only performed when
 *       running against the distributed federation.
 *       <p>
 *       It would be nice to get these tests to run as a proxy test suite, but
 *       some of them rely on access to the data service objects so we would
 *       have to provide for that access differently, e.g., by resolving the
 *       service vs having the object available locally. Likewise, we can't just
 *       reach in and inspect or tweak the objects running on the data service,
 *       including the {@link ResourceManager} so the tests would have to be
 *       re-written around that.
 * 
 * @todo write test where the {@link ISplitHandler} applies additional
 *       constraints when choosen the split points, e.g., for the
 *       {@link SparseRowStore}.
 * 
 * @todo test where we write enough data that we cause the index partition to be
 *       split into more than one index partition on overflow.
 * 
 * @todo factor out a test that does continuous writes, including both inserts
 *       and removes, and verify that the scale-out index tracks ground truth.
 *       Periodically delete keys in an index partition until it would underflow
 *       and add keys into an index partition until it would overflow.
 *       <p>
 *       It is important that this test write ahead of overflow processing for
 *       two reasons. First, it will cause writes to be outstanding on the data
 *       service based on the locators as of the time that the clients submitted
 *       the writes. Those locators will be invalidated if a split/join/move
 *       occurs, forcing the client to resolve the new index partition and
 *       re-issue the request.
 *       <p>
 *       If the test merely writes and removes index entries under random keys
 *       then it will fairely quickly converge on a stable set of index
 *       partitions. Therefor this should be a "torture" test in the sense that
 *       it should focus on causing splits, joins, and moves by its actions.
 *       E.g., write so much data on an index partition that it will be split on
 *       overflow or delete most of the data from an index partition. The client
 *       should execute a few threads in parallel, each of which tackles some
 *       part of the scale-out index so that we trigger overflows in which more
 *       than one thing is happening.
 * 
 * @todo tests scenarios leading to simple overflow processing (index segment
 *       builds), to index partition splits, to index partition moves, etc.
 * 
 * FIXME test continued splits of the index by continuing to write data on the
 * scale-out index and verify that the view remains consistent with the ground
 * truth.
 * 
 * FIXME test when index would be copied to the new journal rather than
 * resulting in an index segment build.
 * 
 * FIXME test index partition move based on utilization. We will have to
 * satisify the various pre-conditions for a move. A mock (or extended) service
 * can be used to specify the move target.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestOverflow extends AbstractEmbeddedBigdataFederationTestCase {

    /**
     * 
     */
    public TestOverflow() {
        super();
    }

    public TestOverflow(String name) {
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
        // @todo also test with copy enabled.
        properties.setProperty(com.bigdata.resources.ResourceManager.Options.COPY_INDEX_THRESHOLD,"0");
        
        // Note: disables index partition moves.
        properties.setProperty(com.bigdata.resources.ResourceManager.Options.MAXIMUM_MOVES_PER_TARGET,"0");
        
//        properties.setProperty(Options.INITIAL_EXTENT, ""+1*Bytes.megabyte);
        
//        properties.setProperty(Options.MAXIMUM_EXTENT, ""+1*Bytes.megabyte);
        
        return properties;
        
    }

    /**
     * Sets the forceOverflow flag and then registers a scale-out index. The
     * test verifies that overflow occurred and that the index is still
     * available after the overflow operation.
     * 
     * @throws IOException
     */
    public void test_register1ThenOverflow() throws IOException {

        /*
         * Register the index.
         */
        final String name = "testIndex";
        final UUID indexUUID = UUID.randomUUID();
        final long overflowCounter0;
        final long overflowCounter1;
        {

            final IndexMetadata indexMetadata = new IndexMetadata(name,indexUUID);
            
            // must support delete markers
            indexMetadata.setDeleteMarkers(true);

            overflowCounter0 = dataService0.getOverflowCounter();
            
            assertEquals(0,overflowCounter0);
            
            dataService0.forceOverflow();
            
            // register the scale-out index, creating a single index partition.
            fed.registerIndex(indexMetadata,dataService0.getServiceUUID());

            overflowCounter1 = dataService0.getOverflowCounter();
            
            assertEquals(1,overflowCounter1);

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
        
        assertEquals(0L,fed.getIndex(name,ITx.UNISOLATED).rangeCount(null,null));

    }
    
    /**
     * Test registers a scale-out index, writes data onto the initial index
     * partition, forces a split, verifies that the scale-out index has been
     * divided into two index partitions, and verifies that a range scan of the
     * scale-out index agrees with the ground truth.
     * 
     * @throws IOException
     */
    public void test_splitJoin() throws IOException {
        
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
        int nrounds = 0;
        long nwritten = 0L;
        boolean done = false;
        final long overflowCounter0 = dataService0.getOverflowCounter();
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
                    .submit(0/*fromIndex*/,nentries/*toIndex*/, keys, vals,
                            BatchInsertConstructor.RETURN_NO_VALUES, null/* handler */);

            /*
             * Set flag to force overflow on group commit if the ground truth index has
             * reached the split threshold.
             */
            if (groundTruth.getEntryCount() >= overCapacityMultiplier * entryCountPerSplit) {
                
                dataService0.forceOverflow();

                done = true;
                
            }

            // insert the data into the scale-out index.
            fed.getIndex(name, ITx.UNISOLATED)
                    .submit(0/*fromIndex*/,nentries/*toIndex*/, keys, vals,
                            BatchInsertConstructor.RETURN_NO_VALUES, null/* handler */);
            
            assertEquals("rangeCount", groundTruth.getEntryCount(), fed
                    .getIndex(name, ITx.UNISOLATED).rangeCount(null, null));
            
            nrounds++;

            nwritten += nentries;
            
            System.err.println("Populating the index: nrounds=" + nrounds
                    + ", nwritten=" + nwritten + ", nentries="
                    + groundTruth.getEntryCount() + " ("
                    + fed.getIndex(name, ITx.UNISOLATED).rangeCount(null, null)
                    + ")");

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
         * Get the key range for the left-most index partition and then delete
         * entries until the index partition underflows.
         */
        {
                        
            final byte[] fromKey = new byte[]{};
            
            PartitionLocator locator = fed.getMetadataIndex(name,
                    ITx.READ_COMMITTED).get(fromKey);
            
            final byte[] toKey = locator.getRightSeparatorKey();
            
            // ground truth range count for that index partition.
            final int rangeCount = (int) groundTruth.rangeCount(fromKey, toKey);
            
            // #of entries to delete to trigger a join operation.
            final int ndelete = rangeCount - minimumEntryCountPerSplit;
            
            final byte[][] keys = new byte[ndelete][];

            final ITupleIterator itr = groundTruth.rangeIterator(fromKey,
                    toKey, ndelete, IRangeQuery.KEYS, null/* filter */);
            
            for(int i=0; i<ndelete; i++) {
                
                keys[i] = itr.next().getKey();
                
            }
            
            System.err.println("Will delete " + ndelete + " entries from "
                    + locator + " to trigger underflow");
            
            groundTruth.submit(0/*fromIndex*/,ndelete/*toIndex*/, keys, null/* vals */,
                    BatchRemoveConstructor.RETURN_NO_VALUES, null/*handler*/);

            dataService0.forceOverflow();
            
            fed.getIndex(name, ITx.UNISOLATED).submit(0/*fromIndex*/,ndelete/*toIndex*/, keys,
                    null/* vals */, BatchRemoveConstructor.RETURN_NO_VALUES,
                    null/*handler*/);
            
        }

        // wait until overflow processing is done.
        final long overflowCounter2 = awaitOverflow(dataService0, overflowCounter1);
        
        /*
         * Confirm index partitions were not joined.
         * 
         * Note: Even though we have deleted index entries the #of index entries
         * in the partition WILL NOT be reduced until the next build task for
         * that index partition. Therefore our post-overflow expectation is that
         * an index build was performed and the #of index partitions WAS NOT
         * changed.
         */
        assertEquals("partitionCount", 2, getPartitionCount(name));
        
        /*
         * Compare the index against ground truth after overflow.
         */
        
        System.err.println("Verifying scale-out index against ground truth");

        assertSameEntryIterator(groundTruth, fed.getIndex(name,ITx.UNISOLATED));

        /*
         * Set the forceOverflow flag and then write another index entry on the
         * other index partition (not the one that we are trying to underflow).
         * This should trigger a JOIN operation now that the index partition has
         * gone through a compacting build.
         */
        {
            
            dataService0.forceOverflow();
            
            // find the locator for the last index partition.
            PartitionLocator locator = fed.getMetadataIndex(name,
                    ITx.READ_COMMITTED).find(null);

            final byte[][] keys = new byte[][] { locator.getLeftSeparatorKey() };
//            final byte[][] vals = new byte[][] { /*empty byte[] */ };

            // overwrite the value (if any) under the left separator key.
            groundTruth.submit(0/*fromIndex*/,1/*toIndex*/, keys, null/*vals*/,
                    BatchRemoveConstructor.RETURN_NO_VALUES, null/* handler */);

            // overwrite the value (if any) under the left separator key.
            fed.getIndex(name, ITx.UNISOLATED).submit(0/*fromIndex*/,1/*toIndex*/, keys, null/*vals*/,
                    BatchRemoveConstructor.RETURN_NO_VALUES, null/* handler */);
            
        }

        // wait until overflow processing is done.
//        final long overflowCounter3 = 
            awaitOverflow(dataService0, overflowCounter2);
        
        /*
         * Confirm index partitions were joined.
         * 
         * Note: Even though we have deleted index entries the #of index entries
         * in the partition WILL NOT be reduced until the next build task for
         * that index partition. Therefore our post-overflow expectation is that
         * an index build was performed and the #of index partitions WAS NOT
         * changed.
         */
        assertEquals("partitionCount", 1, getPartitionCount(name));
        
        /*
         * Compare the index against ground truth after overflow.
         */
        
        System.err.println("Verifying scale-out index against ground truth");

        assertSameEntryIterator(groundTruth, fed.getIndex(name,ITx.UNISOLATED));
        
    }
    
    /**
     * Waits until the overflow counter has been incremented, indicating that
     * overflow processing has occurred and that post-processing for the
     * overflow event is complete.
     * <p>
     * Note: Normally you use bring the data service to the brink of the desired
     * overflow event, note the current overflow counter using
     * {@link IDataService#getOverflowCounter()}, use
     * {@link IDataService#forceOverflow()} to set the forceOverflow flag, do
     * one more write to trigger group commit and overflow processing, and then
     * invoke this method to await the end of overflow post-processing.
     * 
     * @param dataService
     *            The data service.
     * 
     * @param priorOverflowCounter
     * 
     * @return The new value of the overflow counter.
     * 
     * @throws IOException
     */
    protected long awaitOverflow(IDataService dataService,long priorOverflowCounter) throws IOException {
        
        log.info("Awaiting overflow: " + dataService);

        // // pre-condition.
        // assert dataService.getForceOverflow() : "Expecting forceOverflow to
        // be set: "+dataService;

        final long begin = System.currentTimeMillis();

        long newOverflowCounter;

        while ((newOverflowCounter = dataService.getOverflowCounter()) == priorOverflowCounter) {

            try {
                Thread.sleep(250/* ms */);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            System.err.println("Awaiting overflow: " + dataService);

        }

        final long elapsed = System.currentTimeMillis() - begin;

        System.err.println("Overflow complete: elapsed=" + elapsed
                + " ms : priorOverflowCounter=" + priorOverflowCounter
                + ", newOverflowCounter=" + newOverflowCounter);

        assertTrue(newOverflowCounter > priorOverflowCounter);

        return newOverflowCounter;
        
    }
    
    /**
     * Return the #of index partitions in a scale-out index.
     * <p>
     * Note: This uses an key range scan to count only the non-deleted index
     * partition entries.
     * 
     * @param name
     *            The name of the scale-out index.
     * 
     * @return The #of index partitions.
     * 
     * @todo note that the {@link MetadataIndex} does not use delete markers so
     *       a range count would be exact.
     */
    protected int getPartitionCount(String name) {
        
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
        
        while(itr.hasNext()) {
            
            n++;
         
            log.info(SerializerUtil.deserialize(itr.next().getValue()));
            
        }
        
        return n;
        
    }
    
    /**
     * Verifies the data in the two indices using a batch-oriented key range
     * scan. Only the keys and values of non-deleted index entries are
     * inspected.
     * 
     * @param expected
     * @param actual
     */
    protected void assertSameEntryIterator(IIndex expected, IIndex actual) {
        
        ITupleIterator expectedItr = expected.rangeIterator(null,null);

        ITupleIterator actualItr = actual.rangeIterator(null,null);
        
        long nvisited = 0L;
        
        while(expectedItr.hasNext()) {

            assertTrue("Expecting another index entry: nvisited="+nvisited, actualItr.hasNext());
            
            ITuple expectedTuple = expectedItr.next();

            ITuple actualTuple = actualItr.next();
            
            nvisited++;

            if(!BytesUtil.bytesEqual(expectedTuple.getKey(), actualTuple.getKey())) {
                
                fail("Wrong key: nvisited=" + nvisited + ", expecting="
                        + BytesUtil.toString(expectedTuple.getKey())
                        + ", actual="
                        + BytesUtil.toString(actualTuple.getKey()));
                        
            }
            
            if(!BytesUtil.bytesEqual(expectedTuple.getValue(), actualTuple.getValue())) {
                
                fail("Wrong value: nvisited=" + nvisited + ", expecting="
                        + BytesUtil.toString(expectedTuple.getValue())
                        + ", actual="
                        + BytesUtil.toString(actualTuple.getValue()));
                        
            }
            
        }
        
        assertFalse("Not expecting more tuples", actualItr.hasNext());
        
    }

    /**
     * A key-value pair.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class KV implements Comparable<KV>{
        
        final byte[] key;
        final byte[] val;
        
        public KV(byte[] key, byte[] val) {
            
            this.key = key;
            
            this.val = val;
            
        }

        public int compareTo(KV arg0) {

            return BytesUtil.compareBytes(key, arg0.key);
            
        }
        
    }
    
    /**
     * Generate random key-value data in key order.
     * <p>
     * Note: The auto-split feature of the scale-out indices depends on the
     * assumption that the data are presented in key order.
     * <p>
     * Note: This method MAY return entries having duplicate keys.
     * 
     * @param N
     *            The #of key-value pairs to generate.
     * 
     * @return The random key-value data, sorted in ascending order by key.
     * 
     * @see ClientIndexView#submit(int, byte[][], byte[][],
     *      com.bigdata.btree.IIndexProcedure.IIndexProcedureConstructor,
     *      com.bigdata.btree.IResultHandler)
     * 
     * @todo parameter for random deletes, in which case we need to reframe the
     *       batch operation since a batch insert won't work. Perhaps a
     *       BatchWrite would be the thing.
     * 
     * @todo use null values ~5% of the time.
     */
    protected KV[] getRandomKeyValues(int N) {

        final Random r = new Random();

        final KV[] data = new KV[N];

        for (int i = 0; i < N; i++) {

            // @todo param governs chance of a key collision and maximum #of distinct keys.
            final byte[] key = KeyBuilder.asSortKey(r.nextInt(100000));

            // Note: #of bytes effects very little that we want to test so we keep it small.
            final byte[] val = new byte[4];

            r.nextBytes(val);
            
            data[i] = new KV(key,val);

        }

        Arrays.sort(data);
        
        return data;
        
    }

}
