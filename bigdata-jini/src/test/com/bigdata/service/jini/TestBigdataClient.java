/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Apr 23, 2007
 */

package com.bigdata.service.jini;

import java.io.Serializable;
import java.util.Random;
import java.util.UUID;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.proc.BatchInsert.BatchInsertConstructor;
import com.bigdata.journal.ITx;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.jini.util.JiniServicesHelper;

/**
 * Test suite for the {@link JiniClient}.
 * <p>
 * Note: The core test suite has already verified the basic semantics of the
 * {@link IDataService} interface and partitioned indices so all we have to
 * focus on here is the jini integration and verifying that the serialization
 * imposed by RMI goes off without a hitch (e.g., that everything implements
 * {@link Serializable} and that those {@link Serializable} implementations can
 * correctly round trip the data).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBigdataClient extends AbstractServerTestCase {

    public TestBigdataClient() {
    }

    public TestBigdataClient(String name) {
        super(name);
    }

    /**
     * Starts a {@link DataServer} ({@link #dataServer1}) and then a
     * {@link MetadataServer} ({@link #metadataServer0}). Each runs in its own
     * thread.
     */
    public void setUp() throws Exception {

        super.setUp();

        helper = new JiniServicesHelper();

        helper.start();
        
    }
    protected JiniServicesHelper helper = null;

    /**
     * Destroy the test services.
     */
    public void tearDown() throws Exception {

        if (helper != null) {

            helper.destroy();
            
        }

        super.tearDown();

    }

    /**
     * Test ability to registers a scale-out index on one of the
     * {@link DataService}s.
     * 
     * @throws Exception
     */
    public void test_registerIndex1() throws Exception {

        final IBigdataFederation<?> fed = helper.client.connect();

        final String name = "testIndex";

        final IndexMetadata metadata = new IndexMetadata(name, UUID
                .randomUUID());

        metadata.setDeleteMarkers(true);

        fed.registerIndex(metadata);

        final IIndex ndx = fed.getIndex(name, ITx.UNISOLATED);

        assertEquals("indexUUID", metadata.getIndexUUID(), ndx
                .getIndexMetadata().getIndexUUID());

        doBasicIndexTests(ndx);
        
    }

    /**
     * Test ability to registers a scale-out index on both of the
     * {@link DataService}s.
     * 
     * @throws Exception
     */
    public void test_registerIndex2() throws Exception {

        final IBigdataFederation<?> fed = helper.client.connect();
        
        final String name = "testIndex";
        
        final IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());
        
        metadata.setDeleteMarkers(true);

        final UUID indexUUID = fed.registerIndex( metadata, //
                // separator keys.
                new byte[][] {
                    new byte[]{},
                    KeyBuilder.asSortKey(500)
                },//
                // data service assignments.
                new UUID[] { //
                    helper.getDataService0().getServiceUUID(),//
                    helper.getDataService1().getServiceUUID() //
                });

        final IIndex ndx = fed.getIndex(name, ITx.UNISOLATED);

        assertEquals("indexUUID", indexUUID, ndx.getIndexMetadata()
                .getIndexUUID());

        // verify partition 0 on dataService0
        assertNotNull(helper.getDataService0().getIndexMetadata(
                DataService.getIndexPartitionName(name, 0), ITx.UNISOLATED));

        // verify partition 1 on dataService1
        assertNotNull(helper.getDataService1().getIndexMetadata(
                DataService.getIndexPartitionName(name, 1), ITx.UNISOLATED));

        doBasicIndexTests(ndx);

    }

    /**
     * Test helper reads and writes some data on the index in order to verify
     * that these operations can be performed without serialization errors
     * arising from the RPC calls.
     * 
     * @param ndx
     */
    protected void doBasicIndexTests(final IIndex ndx) {
        
        final int limit = 1000;
        
        final byte[][] keys = new byte[limit][];
        final byte[][] vals = new byte[limit][];
        
        final Random r = new Random();

        for (int i = 0; i < limit; i++) {

            keys[i] = KeyBuilder.asSortKey(i);
    
            final byte[] val = new byte[10];
            
            r.nextBytes(val);
            
            vals[i] = val;
            
        }

        // batch insert.
        ndx.submit(0/* fromIndex */, limit/* toIndex */, keys, vals,
                BatchInsertConstructor.RETURN_NO_VALUES, null);

        // verify #of index entries.
        assertEquals(limit, ndx.rangeCount(null, null));

        // verify data.
        {

            final ITupleIterator<?> itr = ndx.rangeIterator(null, null);

            int i = 0;

            while (itr.hasNext()) {

                final ITuple<?> tuple = itr.next();

                assertEquals(keys[i], tuple.getKey());

                assertEquals(vals[i], tuple.getValue());

                i++;

            }

            assertEquals(limit, i);

        }

    }
    
}
