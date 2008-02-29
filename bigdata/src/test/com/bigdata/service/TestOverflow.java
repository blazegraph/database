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

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.BatchInsert.BatchInsertConstructor;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.rawstore.Bytes;
import com.bigdata.repo.BigdataRepository.Options;
import com.bigdata.resources.DefaultSplitHandler;
import com.bigdata.resources.ResourceManager;

/**
 * Tests for various scenarios where the live journal backing a data service
 * overflows.
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
 * FIXME This test suite really needs to be re-run against the distributed
 * federation. This is especially important since the distributed federation
 * will have different behavior arising from client-side caching of the metadata
 * index. It would be nice to get these tests to run as a proxy test suite, but
 * some of them rely on access to the data service objects so we would have to
 * provide for that access differently, e.g., by resolving the service vs having
 * the object available locally. Likewise, we can't just reach in and inspect or
 * tweak the objects running on the data service, including the
 * {@link ResourceManager} so the tests would have to be re-written around that.
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
        
        properties.setProperty(Options.INITIAL_EXTENT, ""+1*Bytes.megabyte);
        
        properties.setProperty(Options.MAXIMUM_EXTENT, ""+1*Bytes.megabyte);
        
        return properties;
        
    }

    /**
     * Test generation of N index splits based on an index partition that has
     * been pre-populated with index entries and a specified target #of index
     * entries per index partition.
     * 
     * @todo test overflow without splits by creating many indices and only
     *       writing less than the entryCountPerSplit on each one.
     * 
     * @todo test overflow with split by creating a single index and writing
     *       more than entryCountPerSplit on that index.
     * 
     * @todo in both cases we also have to trigger the overflow event itself.
     *       That could be done directly using the data service on which the
     *       index was registered or indirectly by forcing the amount of data to
     *       exceed the maximum extent for the journal.
     * 
     * @throws Exception
     */
    public void test_splitIndexPartition() throws Exception {
        
        /*
         * Register the index.
         */
        final String name = "testIndex";
        final UUID indexUUID = UUID.randomUUID();
        final int entryCountPerSplit = 4000;
        {

            final IndexMetadata indexMetadata = new IndexMetadata(name,indexUUID);

            // The target #of index entries per partition.
            ((DefaultSplitHandler)indexMetadata.getSplitHandler()).setEntryCountPerSplit(entryCountPerSplit);
            
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
        
        /*
         * The resource manager on dataService0, which is where we register the
         * initial index partition.
         */
        final ResourceManager resourceManager = (ResourceManager) ((DataService) dataService0)
                .getResourceManager();

        /*
         * Verify the #of journal resources for the data service on which the
         * index was registered.
         */
        assertEquals(1, resourceManager.getJournalCount());

        /*
         * Populate the index with data until the initial the journal for the
         * data service on which the initial partition resides overflows.
         */
        final BTree groundTruth;
        {
        
            IndexMetadata indexMetadata = new IndexMetadata(indexUUID);
            
            groundTruth = BTree.create(new TemporaryRawStore(),indexMetadata);
        
        }

        int nrounds = 0;
        long nwritten = 0L;
        while (resourceManager.getJournalCount() == 1) {
        
            // @todo add random deletes here so that we can verify correct maintenance of delete markers.
            // @todo use null values 5% of the time.
            
            final int nentries = 10000;
            final byte[][] keys = new byte[nentries][];
            final byte[][] vals = new byte[nentries][];
            
            Random r = new Random();

            for (int i = 0; i < nentries; i++) {

                keys[i] = KeyBuilder.asSortKey(r.nextInt(100000));

                vals[i] = new byte[4];

                r.nextBytes(vals[i]);

                groundTruth.insert(keys[i],vals[i]);
                                
            }

            IIndex ndx = fed.getIndex(name,ITx.UNISOLATED);
            
            // insert the data into the scale-out index.
            ndx.submit(nentries, keys, vals,
                    BatchInsertConstructor.RETURN_NO_VALUES, null/* handler */);
            
            nrounds++;

            nwritten += nentries;
            
            System.err.println("Populating the index: nrounds=" + nrounds
                    + ", nwritten=" + nwritten + ", nentries="
                    + groundTruth.getEntryCount());

        }

        while (!resourceManager.isOverflowAllowed()) {

            System.err.println("Waiting for overflow processing to complete");

            Thread.sleep(1000/* ms */);

        }
         
        /*
         * @todo Verify that the index was split during overflow post-processing
         * and that the new index partition definitions are each formed from a
         * live btree and an index segment.  Verify that the #of index partitions
         * is appropriate given the entryCountPerSplit and the #of index entries
         * in the ground truth btree.
         */
        {
            
            ClientIndexView ndx = (ClientIndexView)fed.getIndex(name,ITx.UNISOLATED);
            
            IMetadataIndex mdi = ndx.getMetadataIndex();

            int expectedPartitionCount = (int) Math.floor(nwritten
                    / entryCountPerSplit);

            // Note: MDI does not use delete markers so rangeCount is exact.
            assertNotSame("#index partitions", expectedPartitionCount, mdi
                    .rangeCount(null, null));
            
        }
        
        /*
         * Compare the index against ground truth after overflow.
         */
        
        System.err.println("Verifying scale-out index against ground truth");

        assertSameEntryIterator(groundTruth, fed.getIndex(name,ITx.UNISOLATED));
        
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

        ITupleIterator actualItr = expected.rangeIterator(null,null);
        
        while(expectedItr.hasNext()) {

            assertTrue(actualItr.hasNext());
            
            ITuple expectedTuple = expectedItr.next();

            ITuple actualTuple = actualItr.next();
            
            assertEquals(expectedTuple.getKey(), actualTuple.getKey());
            
            assertEquals(expectedTuple.getValue(), actualTuple.getValue());
            
        }
        
        assertFalse(actualItr.hasNext());
        
    }
    
}
