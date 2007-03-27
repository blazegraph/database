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
 * Created on Feb 16, 2007
 */

package com.bigdata.journal;

import java.util.UUID;

import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.objndx.IIndex;

/**
 * Test suite for read-committed transactions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestReadCommittedTx extends ProxyTestCase {

    /**
     * 
     */
    public TestReadCommittedTx() {
    }

    /**
     * @param name
     */
    public TestReadCommittedTx(String name) {
        super(name);
    }

    /**
     * Test verifies that you can not write on a read-only transaction.
     */
    public void test_isReadOnly() {

        Journal journal = new Journal(getProperties());
        
        String name = "abc";
        
        final byte[] k1 = new byte[]{1};

        final byte[] v1 = new byte[]{1};

        {
            
            /*
             * register an index, write on the index, and commit the journal.
             */
            IIndex ndx = journal.registerIndex(name, new UnisolatedBTree(
                    journal, UUID.randomUUID()));
            
            ndx.insert(k1, v1);

            journal.commit();
            
        }
        
        {
            
            /*
             * create a read-only transaction, verify that we can read the
             * value written on the index but that we can not write on the
             * index.
             */
            
            final long tx1 = journal.newTx(IsolationEnum.ReadOnly);
            
            IIndex ndx = journal.getIndex(name,tx1);

            assertNotNull(ndx);
            
            assertEquals((byte[])v1,(byte[])ndx.lookup(k1));
         
            try {
                ndx.insert(k1,new byte[]{1,2,3});
                fail("Expecting: "+UnsupportedOperationException.class);
                } catch( UnsupportedOperationException ex) {
                System.err.println("Ignoring expected exception: "+ex);
            }
            
            journal.commit(tx1);
            
        }
        
        {
            
            /*
             * do it again, but this time we will abort the read-only
             * transaction.
             */
            
            final long tx1 = journal.newTx(IsolationEnum.ReadOnly);
            
            IIndex ndx = journal.getIndex(name,tx1);

            assertNotNull(ndx);
            
            assertEquals((byte[])v1,(byte[])ndx.lookup(k1));
         
            try {
                ndx.insert(k1,new byte[]{1,2,3});
                fail("Expecting: "+UnsupportedOperationException.class);
                } catch( UnsupportedOperationException ex) {
                System.err.println("Ignoring expected exception: "+ex);
            }
            
            journal.abort(tx1);
            
        }

        journal.closeAndDelete();
        
    }

    /**
     * @todo test that the transaction begins reading from the most recently
     *       committed state, that unisolated writes without commits are not
     *       visible, that newly committed state shows up in the next index view
     *       requested by the tx (so this is either a different index view
     *       object or a delegation mechanism that indirects to the current
     *       view), and that the same index view object is returned if there
     *       have been no intervening commits.
     */
    public void test_readComittedIsolation() {

        Journal journal = new Journal(getProperties());

        String name = "abc";
        
        final byte[] k1 = new byte[]{1};

        final byte[] v1 = new byte[]{1};

        // create a new read-committed transaction.
        final long ts0 = journal.newTx(IsolationEnum.ReadCommitted);

        {
            /*
             * verify that the index is not accessible since it has not been
             * registered.
             */
            assertNull(journal.getIndex(name,ts0));
            
        }
        
        {
            
            // register an index and commit the journal.

            journal.registerIndex(name, new UnisolatedBTree(journal,
                    UUID.randomUUID()));
            
            journal.commit();
            
        }
        
        {

            /*
             * verify that the index is now accessible but that it does not 
             * hold any data.
             */
            
            IIndex ts0_ndx = journal.getIndex(name, ts0);

            assertFalse(ts0_ndx.contains(k1));
            assertNull(ts0_ndx.lookup(k1));
            assertEquals(0,ts0_ndx.rangeCount(null, null));
            
        }
        
        {
            // obtain the unisolated index.
            IIndex ndx = journal.getIndex(name);
            
            // write on the index.
            ndx.insert(k1, v1);

        }
        
        {
            
            /*
             * verify that the write is not visible since the journal has not
             * been committed.
             */

            IIndex ts0_ndx = journal.getIndex(name, ts0);

            assertFalse(ts0_ndx.contains(k1));
            assertNull(ts0_ndx.lookup(k1));
            assertEquals(0,ts0_ndx.rangeCount(null, null));
            
        }
        
        {
            /*
             * commit the journal and verify that the write is now visible to
             * the read-committed transaction.
             */

            journal.commit();
            
            IIndex ts0_ndx = journal.getIndex(name, ts0);

            assertTrue(ts0_ndx.contains(k1));
            assertEquals(v1,(byte[])ts0_ndx.lookup(k1));
            assertEquals(1,ts0_ndx.rangeCount(null, null));
            
        }
        
        {
            /*
             * verify that the write is also visible in a new read-committed
             * transaction.
             */
            
            long ts1 = journal.newTx(IsolationEnum.ReadCommitted);

            IIndex ts1_ndx = journal.getIndex(name, ts1);

            assertTrue(ts1_ndx.contains(k1));
            assertEquals(v1,(byte[])ts1_ndx.lookup(k1));
            assertEquals(1,ts1_ndx.rangeCount(null, null));

            // should be a nop.
            assertEquals(0,journal.commit(ts1));

        }
        
        // should be a nop.
        journal.abort(ts0);

        // close and delete the database.
        journal.closeAndDelete();
        
    }
    
    /**
     * @todo test protocol for closing index views and releasing holds on commit
     *       points.
     */
    public void test_releaseViews() {
    
        fail("write test");
        
    }
    
}
