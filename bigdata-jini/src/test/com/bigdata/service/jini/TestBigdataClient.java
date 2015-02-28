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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import com.bigdata.bop.ap.R;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.TestKeyBuilder;
import com.bigdata.btree.proc.BatchInsert.BatchInsertConstructor;
import com.bigdata.journal.ITx;
import com.bigdata.journal.NoSuchIndexException;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.relation.RelationSchema;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.jini.util.JiniServicesHelper;
import com.bigdata.sparse.ITPS;
import com.bigdata.sparse.SparseRowStore;

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
    @Override
    public void setUp() throws Exception {

        super.setUp();

        helper = new JiniServicesHelper();

        helper.start();
        
    }
    private JiniServicesHelper helper = null;

    /**
     * Destroy the test services.
     */
    @Override
    public void tearDown() throws Exception {

        if (helper != null) {

            helper.destroy();
            
            helper = null;
            
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
                    TestKeyBuilder.asSortKey(500)
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

            keys[i] = TestKeyBuilder.asSortKey(i);
    
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

    /**
     * Unit test of ability to list out the registered locatable resources on a
     * federation.
     */
    public void test_listNamespaces() {
        
        final IBigdataFederation<?> fed = helper.client.connect();
    
        final String namespace = "testRelation";
        
        final Properties properties = new Properties();
        
        /*
         * Verify behavior before any writes on the federation.
         */
        final long lastCommitTime0;
        {

            lastCommitTime0 = fed.getLastCommitTime();

            assertEquals(ITx.UNISOLATED, lastCommitTime0);

            final List<String> namespaces = getNamespaces(fed, lastCommitTime0);

            assertEquals(0, namespaces.size());
            
        }

        /*
         * Now create an index. This will cause a commit point to appear on the
         * federation. However, the GRS will still be empty since we have not
         * registered a locatable resource, just an index.
         */
        final long lastCommitTime1;
        {

            final String name = "index1";

            final IndexMetadata metadata = new IndexMetadata(name,
                    UUID.randomUUID());

            metadata.setDeleteMarkers(true);

            fed.registerIndex(metadata);

            lastCommitTime1 = fed.getLastCommitTime();

            assertTrue(lastCommitTime1 > 0);

            final List<String> namespaces = getNamespaces(fed, lastCommitTime1);

            assertEquals(0, namespaces.size());

        }

        /*
         * Now register a locatable resource and verify that it is correctly
         * discovered by a GRS scan.
         */
        final long lastCommitTime2;
        {

            final R rel = new R(fed, namespace, ITx.UNISOLATED, properties);
         
            // Verify nothing was registered yet.
            assertTrue(getNamespaces(fed, ITx.UNISOLATED).isEmpty());

            rel.create();

            lastCommitTime2 = fed.getLastCommitTime();
            
            assertTrue(lastCommitTime2 > lastCommitTime1);
            
            final List<String> namespaces = getNamespaces(fed, lastCommitTime2);

            assertEquals(1, namespaces.size());

            assertEquals(namespace, namespaces.get(0));

        }

        /*
         * Now destroy the locatable resource.
         */
        final long lastCommitTime3;
        {

            final R rel = (R) fed.getResourceLocator().locate(namespace,
                    ITx.UNISOLATED);

            assertNotNull(rel);
            
            rel.destroy();

            lastCommitTime3 = fed.getLastCommitTime();

            assertTrue(lastCommitTime3 > lastCommitTime2);
            
        }

        /*
         * Verify no longer locatable (for the unisolated view).
         */
        {

            final R rel = (R) fed.getResourceLocator().locate(namespace,
                    ITx.UNISOLATED);

            assertNull(rel);

        }

        /*
         * Now repeat the scan and verify that it is empty once again.
         */
        {

            final List<String> namespaces = getNamespaces(fed, lastCommitTime3);

            assertEquals(0, namespaces.size());

        }

    }

    /**
     * Return a list of the namespaces for the resources registered against the
     * bigdata instance.
     */
    private List<String> getNamespaces(final IBigdataFederation<?> fed,
            final long timestamp) {

        // the triple store namespaces.
        final List<String> namespaces = new LinkedList<String>();

        final SparseRowStore grs = fed.getGlobalRowStore(timestamp);

        if (grs == null) {

            log.warn("No GRS @ timestamp="
                    + TimestampUtility.toString(timestamp));

            // Empty.
            return namespaces;

        }

        // scan the relation schema in the global row store.
        @SuppressWarnings("unchecked")
        final Iterator<ITPS> itr = (Iterator<ITPS>) grs
                .rangeIterator(RelationSchema.INSTANCE);

        while (itr.hasNext()) {

            // A timestamped property value set is a logical row with
            // timestamped property values.
            final ITPS tps = itr.next();

            // Log out the TPS.
            if (log.isInfoEnabled())
                log.info(tps.toString());

            // The namespace is the primary key of the logical row for the
            // relation schema.
            final String namespace = (String) tps.getPrimaryKey();

            // Get the name of the implementation class
            final String className = (String) tps.get(RelationSchema.CLASS)
                    .getValue();

            if (className == null) {
                // Skip deleted triple store entry.
                continue;
            }

            // Found something.
            namespaces.add(namespace);

        }

        return namespaces;

    }

    /**
     * Test for correct throw of {@link NoSuchIndexException} if the index does
     * not exist.
     */
    public void test_dropIndex_notFound() throws Exception {

        final IBigdataFederation<?> fed = helper.client.connect();

        try {
            fed.dropIndex(getName());
            fail("Expecting: "+NoSuchIndexException.class);
        } catch (NoSuchIndexException ex) {
            log.info("Ignoring expected exception: " + ex);
        }
        
    }

    /**
     * Test for correct drop of a registered index.
     */
    public void test_dropIndex_found() throws Exception {

        final IBigdataFederation<?> fed = helper.client.connect();

        final String name = "testIndex";

        /*
         * Register index.
         */
        final UUID uuid = UUID.randomUUID();
        {
            final IndexMetadata metadata = new IndexMetadata(name,
                    uuid);

            metadata.setDeleteMarkers(true);

            fed.registerIndex(metadata);

        }

        /*
         * Verify exists.
         */
        {

            final IIndex ndx = fed.getIndex(name, ITx.UNISOLATED);

            assertEquals("indexUUID", uuid, ndx.getIndexMetadata()
                    .getIndexUUID());
        }
        
        /*
         * Drop index.
         */
        {
            
            fed.dropIndex(name);
            
        }

        /*
         * Verify gone.
         */
        {
        
            final IIndex ndx = fed.getIndex(name, ITx.UNISOLATED);

            assertNull(ndx);

        }

        /*
         * Verify 2nd drop throws expected exception.
         */
        try {
            fed.dropIndex(getName());
            fail("Expecting: " + NoSuchIndexException.class);
        } catch (NoSuchIndexException ex) {
            log.info("Ignoring expected exception: " + ex);
        }

    }

}
