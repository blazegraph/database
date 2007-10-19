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
 * Created on May 19, 2007
 */

package com.bigdata.rdf.scaleout;

import com.bigdata.btree.BTree;
import com.bigdata.rdf.store.ITripleStore;
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

    protected final long NULL = ITripleStore.NULL;
    
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
