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
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.IMetadataIndex;
import com.bigdata.mdi.PartitionLocator;
import com.bigdata.resources.ResourceManager.Options;
import com.bigdata.service.ndx.ClientIndexView;

/**
 * Simple test verifies that a scale-out index is preserved across both
 * synchronous and asynchronous overflow events.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestOverflow extends AbstractEmbeddedFederationTestCase {

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
     * Overridden to specify the {@link BufferMode#Disk} mode.
     */
    public Properties getProperties() {
        
        final Properties properties = new Properties(super.getProperties());
        
        // overrides value set in the superclass.
        properties.setProperty(Options.BUFFER_MODE,BufferMode.Disk.toString());
        
        // restrict the test to one data service [dataService0].
        properties.setProperty(EmbeddedClient.Options.NDATA_SERVICES,"1");
        
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
    public void test_register1ThenOverflow() throws IOException,
            InterruptedException, ExecutionException {

        /*
         * This test depends on there being ONE data service so it knows on
         * which data service the index has been registered.
         */
        assertEquals("dataServiceCount", 1, ((EmbeddedFederation<?>) fed)
                .getDataServiceCount());

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

            overflowCounter0 = dataService0.getAsynchronousOverflowCounter();
            
            assertEquals(0,overflowCounter0);
            
            dataService0.forceOverflow(false/*immediate*/,false/*compactingMerge*/);
            
            // register the scale-out index, creating a single index partition.
            fed.registerIndex(indexMetadata,dataService0.getServiceUUID());

            // wait until overflow processing is done.
            overflowCounter1 = awaitAsynchronousOverflow(dataService0,
                    overflowCounter0);
            
//            {
//                long counter = 0;
//
//                for (int i = 0; i < 10 && counter == 0; i++) {
//
//                    counter = dataService0.getAsynchronousOverflowCounter();
//
//                    if (counter == 0) {
//
//                        Thread.sleep(500/* ms */);
//
//                    }
//
//                }
//
//                overflowCounter1 = counter;
//
//            }

            assertEquals(1, overflowCounter1);

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

            assertEquals("dataServiceUUID", dataService0.getServiceUUID(), pmd0
                    .getDataServiceUUID());
            
        }

        assertEquals("partitionCount", 1, getPartitionCount(name));
        
        assertEquals(0L, fed.getIndex(name, ITx.UNISOLATED).rangeCount());

    }
    
}
