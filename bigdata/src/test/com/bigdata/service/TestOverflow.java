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

            overflowCounter1 = dataService0.getAsynchronousOverflowCounter();
            
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

            assertEquals("dataServiceUUID", dataService0.getServiceUUID(), pmd0
                    .getDataServiceUUID());
            
        }

        assertEquals("partitionCount", 1, getPartitionCount(name));
        
        assertEquals(0L, fed.getIndex(name, ITx.UNISOLATED).rangeCount());

    }
    
}
