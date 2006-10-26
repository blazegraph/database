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
 * Created on Oct 26, 2006
 */

package com.bigdata.istore;

import java.util.Properties;

import junit.framework.TestCase;


import com.bigdata.journal.Journal;

/**
 * Rudiments of a test suite for the bigdata client API.
 * 
 * @todo The test currently configures an embedded database using a journal. It
 *       should be modified to test with a journal + read-optimized database and
 *       with a client-server configuration, and finally with a distributed
 *       database configuration.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBasics extends TestCase {

    /**
     * 
     */
    public TestBasics() {
    }

    /**
     * @param arg0
     */
    public TestBasics(String arg0) {
        super(arg0);
    }

    JournalStore store;
    
    public void setUp() throws Exception {

        Properties properties = new Properties();
        
        properties.setProperty("bufferMode","transient");
//        properties.setProperty("segmentId","0");
        
        Journal journal = new Journal(properties);

        store = new JournalStore( journal );
        
    }

    public void tearDown() throws Exception {

        // NOP since the journal is transient.
        
    }

    /**
     * @todo modify to test for "not found" and "deleted" semantics. Those
     *       depend on whether or not transactions have been GC'd.  The store
     *       API needs a transaction service that is responsible for notifying
     *       the segments when transactions can be GC'd.
     */
    public void test_crud() {
        
        final Object expected0 = "expected0";
        final Object expected1 = "expected1";
        final Object expected2 = "expected2";
        
        // insert.
        final long id0 = store.insert(expected0);

        assertEquals(expected0,store.read(id0));

        // update.
        store.update(id0, expected1);
        
        assertEquals(expected1,store.read(id0));
        
        // update.
        store.update(id0, expected2);

        assertEquals(expected2,store.read(id0));
        
        // delete.
        store.delete(id0);
        
        store.close();

    }

    /**
     * @todo expand to verify isolation.
     * @todo expand to test read after commit.
     * @todo expand to test restart.
     */
    public void test_crudTx() {
        
        final Object expected0 = "expected0";
        final Object expected1 = "expected1";
        final Object expected2 = "expected2";
        
        ITx tx = store.startTx();
        
        // insert.
        final long id0 = tx.insert(expected0);

        assertEquals(expected0,tx.read(id0));

        // update.
        tx.update(id0, expected1);
        
        assertEquals(expected1,tx.read(id0));
        
        // update.
        tx.update(id0, expected2);

        assertEquals(expected2,tx.read(id0));
        
        // delete.
        tx.delete(id0);
        
        tx.prepare();
        
        tx.commit();
        
        store.close();

    }

}
