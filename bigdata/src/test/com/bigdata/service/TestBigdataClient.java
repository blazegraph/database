/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Apr 23, 2007
 */

package com.bigdata.service;

import java.util.UUID;

import com.bigdata.btree.IIndex;
import com.bigdata.service.BigdataClient.IBigdataFederation;

/**
 * Test suite for the {@link BigdataClient}.
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
     * Starts in {@link #setUp()}.
     */
    MetadataServer metadataServer0;
    /**
     * Starts in {@link #setUp()}.
     */
    DataServer dataServer1;
    /**
     * Starts in {@link #setUp()}.
     */
    DataServer dataServer0;
    /**
     * Starts in {@link #setUp()}.
     */
    BigdataClient client;
    
    /**
     * Starts a {@link DataServer} ({@link #dataServer1}) and then a
     * {@link MetadataServer} ({@link #metadataServer0}). Each runs in its own
     * thread.
     */
    public void setUp() throws Exception {
        
        /*
         * Start up a data server before the metadata server so that we can make
         * sure that it is detected by the metadata server once it starts up.
         */
        dataServer1 = new DataServer(
                new String[] { "src/resources/config/standalone/DataServer1.config" });

        new Thread() {

            public void run() {
                
                dataServer1.run();
                
            }
            
        }.start();

        /*
         * Start the metadata server.
         */
        metadataServer0 = new MetadataServer(
                new String[] { "src/resources/config/standalone/MetadataServer0.config" });
        
        new Thread() {

            public void run() {
                
                metadataServer0.run();
                
            }
            
        }.start();

        /*
         * Start up a data server after the metadata server so that we can make
         * sure that it is detected by the metadata server once it starts up.
         */
        dataServer0 = new DataServer(
                new String[] { "src/resources/config/standalone/DataServer0.config" });

        new Thread() {

            public void run() {
                
                dataServer0.run();
                
            }
            
        }.start();

        client = new BigdataClient(
                new String[] { "src/resources/config/standalone/Client.config" });
        
    }
    
    /**
     * Destroy the test services.
     */
    public void tearDown() throws Exception {
        
        if(metadataServer0!=null) {

            metadataServer0.destroy();
        
            metadataServer0 = null;

        }

        if(dataServer0!=null) {

            dataServer0.destroy();
        
            dataServer0 = null;

        }
        
        if (dataServer1 != null) {
            
            dataServer1.destroy();

            dataServer1 = null;
            
        }
        
        if(client!=null) {

            client.terminate();

            client = null;
            
        }
        
    }
    
    public void test_federationRunning() throws Exception {
        
        assertNotNull("metadataService", client.getMetadataService());

        IBigdataFederation fed = client.connect();
        
        final String name = "testIndex";
        
        UUID indexUUID = fed.registerIndex(name);
        
        IIndex ndx = fed.getIndex(IBigdataFederation.UNISOLATED,name);
        
        // fetches the index UUID from the metadata service.
        assertEquals("indexUUID",indexUUID,ndx.getIndexUUID());

        // uses the cached copy of the index UUID.
        assertEquals("indexUUID",indexUUID,ndx.getIndexUUID());

        // the index is empty.
        assertFalse(ndx.contains(new byte[]{1}));
     
        // the key is not in the index.
        assertEquals(null,(byte[])ndx.lookup(new byte[]{1}));
        
        // insert a key-value pair.
        assertNull(ndx.insert(new byte[]{1}, new byte[]{1}));

        // verify index reports value exists for the key.
        assertTrue(ndx.contains(new byte[]{1}));

        // verify correct value in the index.
        assertEquals(new byte[]{1},(byte[])ndx.lookup(new byte[]{1}));

        // verify some range counts.
        assertEquals(0,ndx.rangeCount(new byte[]{}, new byte[]{1}));
        assertEquals(1,ndx.rangeCount(new byte[]{}, new byte[]{2}));
        assertEquals(1,ndx.rangeCount(new byte[]{1}, new byte[]{2}));
        assertEquals(0,ndx.rangeCount(null, new byte[]{1}));
        assertEquals(1,ndx.rangeCount(new byte[]{1},null));
        assertEquals(1,ndx.rangeCount(null,new byte[]{2}));
        assertEquals(1,ndx.rangeCount(null,null));
        
        // remove the index entry.
        assertEquals(new byte[]{1},(byte[])ndx.remove(new byte[]{1}));

        // the index is empty.
        assertFalse(ndx.contains(new byte[]{1}));

        // the key is not in the index.
        assertEquals(null,(byte[])ndx.lookup(new byte[]{1}));
        
        /*
         * verify some range counts.
         * 
         * Note: the range counts do NOT immediately adjust when keys are
         * removed since deletion markers are written into those entries. The
         * relevant index partition(s) need to be compacted for those deletion
         * markers to be removed and the range counts adjusted to match.
         */
        assertEquals(0,ndx.rangeCount(new byte[]{}, new byte[]{1}));
        assertEquals(1,ndx.rangeCount(new byte[]{}, new byte[]{2}));
        assertEquals(1,ndx.rangeCount(new byte[]{1}, new byte[]{2}));
        assertEquals(0,ndx.rangeCount(null, new byte[]{1}));
        assertEquals(1,ndx.rangeCount(new byte[]{1},null));
        assertEquals(1,ndx.rangeCount(null,new byte[]{2}));
        assertEquals(1,ndx.rangeCount(null,null));

    }
    
}
