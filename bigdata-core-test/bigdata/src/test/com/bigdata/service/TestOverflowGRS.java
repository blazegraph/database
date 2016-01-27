/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexSegment;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.resources.ResourceManager.Options;
import com.bigdata.sparse.GlobalRowStoreHelper;
import com.bigdata.sparse.GlobalRowStoreSchema;
import com.bigdata.sparse.ITPS;
import com.bigdata.sparse.SparseRowStore;

/**
 * Test drives inserts on the GRS index partition until the data service is
 * forced to go through an overflow such that an index build is performed for
 * the GRS index (rather than copying the index into the new live journal). We
 * verify that we can scan the GRS index before and after the asynchronous
 * overflow event, and that we are in fact reading on a complex view (both a
 * {@link Journal} and an {@link IndexSegment}) after the overflow event.
 * 
 * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/682">
 *      AtomicRowFilter UnsupportedOperationException </a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestOverflowGRS extends AbstractEmbeddedFederationTestCase {

    /**
     * 
     */
    public TestOverflowGRS() {
        super();
    }

    public TestOverflowGRS(String name) {
        super(name);
    }

    /**
     * Use a very low threshold for an index build.
     * 
     * @see EmbeddedClient.Options#COPY_INDEX_THRESHOLD
     */
    private final static int copyIndexThreshold = 10;
    
    /**
     * Overridden to specify the {@link BufferMode#Disk} mode.
     */
    public Properties getProperties() {
        
        final Properties properties = new Properties(super.getProperties());
        
        // overrides value set in the superclass.
        properties.setProperty(Options.BUFFER_MODE,BufferMode.Disk.toString());
        
        // restrict the test to one data service [dataService0].
        properties.setProperty(EmbeddedClient.Options.NDATA_SERVICES,"1");

        // use a very low threshold for an index build.
        properties.setProperty(EmbeddedClient.Options.COPY_INDEX_THRESHOLD,
                Integer.toString(copyIndexThreshold));

        // pre-size the journal to the mimumum extent.
        properties.setProperty(EmbeddedClient.Options.INITIAL_EXTENT,
                Long.toString(Options.minimumInitialExtent));

        // overflow as soon as we exceed that minimum extent.
        properties.setProperty(EmbeddedClient.Options.MAXIMUM_EXTENT,
                Long.toString(Options.minimumInitialExtent));

        return properties;
        
    }

    /**
     * Sets the forceOverflow flag and then registers a scale-out index. The
     * test verifies that overflow occurred and that the index is still
     * available after the overflow operation.
     * 
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void test_GRS_Overflow_Scan() throws IOException,
            InterruptedException, ExecutionException {

        /*
         * This test depends on there being ONE data service so it knows on
         * which data service the index has been registered.
         */
        assertEquals("dataServiceCount", 1, ((EmbeddedFederation<?>) fed)
                .getDataServiceCount());

        /*
         * Obtain a view of the GRS index
         */
        final SparseRowStore rowStore = fed.getGlobalRowStore();

        /*
         * Do a GRS scan. Should be empty.
         */
        {

            long nrows = 0;
            
            final Iterator<? extends ITPS> itr = rowStore
                    .rangeIterator(GlobalRowStoreSchema.INSTANCE);
            
            while (itr.hasNext()) {
             
                final ITPS tps = itr.next();
                
                if (log.isInfoEnabled())
                    log.info(tps);
            
                nrows++;
                
            }
            
            assertEquals("nrows", 0L, nrows);
            
        }

        final int N = copyIndexThreshold;

        final int M = N * 2;
        
        /*
         * Insert some rows into the GRS.
         * 
         * Note: While we insert up to [copyIndexThreshold] rows, each row is
         * very small and the total bytes on the disk for the Journal is not
         * enough to trigger an overflow.
         */
        final Map<String, Object> propertySet = new LinkedHashMap<String, Object>();
        {

            for (int i = 0; i < N; i++) {

                propertySet.put(GlobalRowStoreSchema.NAME, "index" + i);

                propertySet.put(GlobalRowStoreSchema.VALUE, Integer.valueOf(i));

                rowStore.write(GlobalRowStoreSchema.INSTANCE, propertySet);

                final long c = dataService0.getAsynchronousOverflowCounter();

                if (c > 0)
                    fail("Asynchronous overflow: i=" + i
                            + ", asynchronousOverflowCounter=" + c);
                
            }

        }

        /*
         * Do another GRS scan. Should be N-1 rows.
         */
        {

            long nrows = 0;

            final Iterator<? extends ITPS> itr = rowStore
                    .rangeIterator(GlobalRowStoreSchema.INSTANCE);

            while (itr.hasNext()) {

                final ITPS tps = itr.next();

                if (log.isInfoEnabled())
                    log.info(tps);

                nrows++;

            }

            assertEquals("nrows", N, nrows);

        }

        /*
         * Force overflow of the data service.
         */
        final long overflowCounter0;
        final long overflowCounter1;
        {

            /*
             * Should not have triggered an overflow.
             */
            overflowCounter0 = dataService0.getAsynchronousOverflowCounter();

            assertEquals(0, overflowCounter0);

            // trigger overflow
            dataService0
                    .forceOverflow(true/* immediate */, false/* compactingMerge */);

            // write some more on the GRS index.
            for (int i = N; i < M; i++) {

                propertySet.put(GlobalRowStoreSchema.NAME, "index" + i);

                propertySet.put(GlobalRowStoreSchema.VALUE, Integer.valueOf(i));

                rowStore.write(GlobalRowStoreSchema.INSTANCE, propertySet);

            }

            // wait until overflow processing is done.
            overflowCounter1 = awaitAsynchronousOverflow(dataService0,
                    overflowCounter0);
            
            assertEquals(1, overflowCounter1);

        }
        
        /*
         * Verify the GRS is now a complex view (journal + index segement).
         */
        {
            
            final String shardName = DataService
                    .getIndexPartitionName(
                            GlobalRowStoreHelper.GLOBAL_ROW_STORE_INDEX, 0/* partitionId */);

            final IndexMetadata md = dataService0.getIndexMetadata(shardName,
                    ITx.UNISOLATED);

            final LocalPartitionMetadata lpmd = md.getPartitionMetadata();

            if (log.isInfoEnabled())
                log.info("GRS PMD on DS: " + lpmd);

            if (lpmd.getResources().length < 2) {

                fail("Expecting at least two resources in the GRS shard view: "
                        + lpmd);

            }

        }

        /*
         * Do final GRS scan. Should be N rows.
         */
        {

            long nrows = 0;

            final Iterator<? extends ITPS> itr = rowStore
                    .rangeIterator(GlobalRowStoreSchema.INSTANCE);

            while (itr.hasNext()) {

                final ITPS tps = itr.next();

                if (log.isInfoEnabled())
                    log.info(tps);

                nrows++;

            }

            assertEquals("nrows", M, nrows);

        }

    }

}
