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
 * Created on Oct 26, 2006
 */

package com.bigdata.istore;

import java.util.Properties;

import junit.framework.TestCase;

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

    IStore store;
    
    public void setUp() throws Exception {

        Properties properties = new Properties();
        
        properties.setProperty("bufferMode","transient");
//        properties.setProperty("segmentId","0");
        
        // FIXME initialize.
//        store = new JournalStore( properties );
        
    }

    public void tearDown() throws Exception {

        store.close();
        
    }

    /**
     * Basic CRUD without transactional isolation.
     * 
     * @todo modify to test for "not found" and "deleted" semantics. Those
     *       depend on whether or not transactions have been GC'd.  The store
     *       API needs a transaction service that is responsible for notifying
     *       the segments when transactions can be GC'd.
     */
    public void test_crud() {

        final Object expected0 = "expected0";
        final Object expected1 = "expected1";
        final Object expected2 = "expected2";

        IOM om = store.getObjectManager();
        
        // insert.
        final long id0 = om.insert(expected0);

        assertEquals(expected0,om.read(id0));

        // update.
        om.update(id0, expected1);
        
        assertEquals(expected1,om.read(id0));
        
        // update.
        om.update(id0, expected2);

        assertEquals(expected2,om.read(id0));
        
        // delete.
        om.delete(id0);

    }

    /**
     * Basic CRUD with transactional isolation.
     * 
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
        
        tx.commit();

    }

}
