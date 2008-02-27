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
 * Created on Feb 19, 2008
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.BatchInsert.BatchInsertConstructor;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.repo.BigdataRepository.Options;

/**
 * Test suite for the ability to re-open an {@link EmbeddedBigdataFederation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRestartSafe extends AbstractEmbeddedBigdataFederationTestCase {

    /**
     * 
     */
    public TestRestartSafe() {
    }

    /**
     * @param arg0
     */
    public TestRestartSafe(String arg0) {
        super(arg0);
    }

    /**
     * Overriden to specify the {@link BufferMode#Disk} mode.
     */
    public Properties getProperties() {
        
        Properties properties = new Properties( super.getProperties() );
        
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk
                .toString());
        
        return properties;
        
    }
    
    /**
     * Test creates a new embedded federation (this is done in setUp() by the
     * super class), registers a scale-out index with the metadata service
     * having an index partition on each of the data services and then writes
     * some data such that there is data for that scale-out index on each of the
     * data services. The federation is then closed and a new instance of the
     * federation is opened and we verify that the metadata and data services
     * were discovered, that the index is still registered, and that the data is
     * still in the index.
     * 
     * @throws IOException 
     */
    public void test_restartSafe() throws IOException {

        /*
         * Verify the #of data services and note the UUID for the data and
         * metadata services.
         */
        assertEquals("#dataServices", 2,
                ((EmbeddedBigdataFederation) fed).ndataServices);
        
        final UUID metadataServiceUUID = fed.getMetadataService()
                .getServiceUUID();

        final UUID dataService0UUID = ((EmbeddedBigdataFederation) fed)
                .getDataService(0).getServiceUUID();
        
        final UUID dataService1UUID = ((EmbeddedBigdataFederation) fed)
                .getDataService(1).getServiceUUID();

        /*
         * Register a scale-out index with data on each of the data services.
         */
        
        final String name = "testIndex";
        
        final IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());

        metadata.setDeleteMarkers(true);

        final UUID indexUUID = fed.registerIndex(metadata, new byte[][]{//
                new byte[]{},
                new byte[]{5}
        }, new UUID[]{//
                dataService0.getServiceUUID(),
                dataService1.getServiceUUID() });

        /*
         * Setup the data to write on the scale-out index. The keys are choosen
         * so that 1/2 of the data will show up on each of the data services.
         */

        final int nentries = 10;

        final byte[][] keys = new byte[nentries][];
        final byte[][] vals = new byte[nentries][];

        for (int i = 0; i < nentries; i++) {

            keys[i] = KeyBuilder.asSortKey(i);

            vals[i] = new byte[4];

            r.nextBytes(vals[i]);

        }

        /*
         * Write data on the index.
         */
        {

            IIndex ndx = fed.getIndex(name,ITx.UNISOLATED);

            ndx.submit(nentries, keys, vals,
                    BatchInsertConstructor.RETURN_NO_VALUES, null/*handler*/);

        }

        /*
         * Verify read-back of the data on the index.
         */
        {

            IIndex ndx = fed.getIndex(name,ITx.UNISOLATED);

            assertEquals(nentries, ndx.rangeCount(null, null));

            IEntryIterator itr = ndx.rangeIterator(null, null);
            
            int i = 0;
            
            while(itr.hasNext()) {
                
                ITuple tuple = itr.next();
                
                assertEquals(keys[i],tuple.getKey());

                assertEquals(vals[i],tuple.getValue());
                
                i++;
                
            }
            
            assertEquals(nentries, i);
            
        }

        /*
         * Close down the embedded federation.
         * 
         * See setUp() in the parent class.
         */
        
        client.terminate();
        
        client = null;
        
        fed = null;
        
        dataService0 = null;
        
        dataService1 = null;

        /*
         * Open the embedded federation again.
         *
         * See setUp() in the parent class.
         */
        
        client = new EmbeddedBigdataClient(getProperties());
        
        fed = client.connect();

        assertEquals("#dataServices", 2,
                ((EmbeddedBigdataFederation) fed).ndataServices);
        
        dataService0 = ((EmbeddedBigdataFederation)fed).getDataService(0);

        dataService1 = ((EmbeddedBigdataFederation)fed).getDataService(1);

        /*
         * Verify the data and metadata service UUIDs.
         * 
         * Note: there is only one metadata service so there is no uncertainty
         * about which UUID it must have.
         * 
         * However, there are two data services. On restart either data service
         * could be assigned to either index ZERO or ONE in the dataServices[].
         * Therefore we check to make sure that the service UUIDs for the data
         * services are (a) distinct; and (b) each one is the service UUID for
         * one of the expected data services.
         */
        
        assertEquals("metadataService UUID", metadataServiceUUID, fed
                .getMetadataService().getServiceUUID());

        // verify services have distinct UUIDs.
        assertNotSame(dataService0.getServiceUUID(), dataService1
                .getServiceUUID());
        
        // verify dataService[0] has one of the expected data service UUIDs.
        if (!dataService0.getServiceUUID().equals(dataService0UUID)
                && !dataService0.getServiceUUID().equals(dataService1UUID)) {

            fail("Not expecting data service with UUID: "
                    + dataService0.getServiceUUID());

        }
        
        // verify dataService[1] has one of the expected data service UUIDs.
        if (!dataService1.getServiceUUID().equals(dataService0UUID)
                && !dataService1.getServiceUUID().equals(dataService1UUID)) {

            fail("Not expecting data service with UUID: "
                    + dataService1.getServiceUUID());

        }

        /*
         * Verify the scale-out index is registered.
         */
        
        assertNotNull(fed.getIndex(name,ITx.UNISOLATED));

        assertEquals(indexUUID, fed.getIndex(name,ITx.UNISOLATED)
                .getIndexMetadata().getIndexUUID());
        
        /*
         * Verify read-back of the data on the index.
         */
        {

            IIndex ndx = fed.getIndex(name,ITx.UNISOLATED);

            assertEquals(nentries, ndx.rangeCount(null, null));

            IEntryIterator itr = ndx.rangeIterator(null, null);
            
            int i = 0;
            
            while(itr.hasNext()) {
                
                ITuple tuple = itr.next();
                
                assertEquals(keys[i],tuple.getKey());

                assertEquals(vals[i],tuple.getValue());
                
                i++;
                
            }
            
            assertEquals(nentries, i);
            
        }

    }

}
