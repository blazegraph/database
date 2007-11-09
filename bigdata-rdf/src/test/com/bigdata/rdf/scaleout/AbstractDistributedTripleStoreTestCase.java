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
 * Created on May 19, 2007
 */

package com.bigdata.rdf.scaleout;

import com.bigdata.btree.BTree;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.service.BigdataClient;
import com.bigdata.service.IBigdataFederation;

/**
 * Abstract test case that sets up and connects to a bigdata federation and
 * establishes an RDF database on that federation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractDistributedTripleStoreTestCase extends AbstractDistributedBigdataFederationTestCase {

    /**
     * 
     */
    public AbstractDistributedTripleStoreTestCase() {
    }

    /**
     * @param arg0
     */
    public AbstractDistributedTripleStoreTestCase(String arg0) {
        super(arg0);
    }

    protected final long NULL = IRawTripleStore.NULL;
    
    /**
     * The triple store under test.
     */
    ScaleOutTripleStore store;
    
    /**
     * FIXME Work the setup, teardown, and APIs until I can use either an
     * embedded database or a client-service divide with equal ease -- and note
     * that there is also a distinction at this time between an intrinsically
     * local rdf database and one that is network capable but running locally
     * (e.g., whether or not it was written using the {@link BigdataClient}
     * class or direct use of a {@link BTree}).
     * <p>
     * This will be especially important for the {@link TempTripleStore}, which
     * normally uses only local resources. The best way is to defined an
     * interface IBigdataClient and then provide both embedded and data-server
     * implementations.
     */
    public void setUp() throws Exception {

        super.setUp();
        
        // Connect to the federation.
        IBigdataFederation fed = client.connect();

        // Register indices.
        fed.registerIndex(ScaleOutTripleStore.name_termId);
        fed.registerIndex(ScaleOutTripleStore.name_idTerm);
        fed.registerIndex(ScaleOutTripleStore.name_spo);
        fed.registerIndex(ScaleOutTripleStore.name_pos);
        fed.registerIndex(ScaleOutTripleStore.name_osp);

        // start the database.
        store = new ScaleOutTripleStore(fed);

    }

    public void tearDown() throws Exception {
        
        super.tearDown();
        
    }
    
}
