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
 * Created on Feb 16, 2007
 */

package com.bigdata.journal;

import java.util.UUID;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;

/**
 * Test suite for fully isolated read-only transactions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestReadOnlyTx extends ProxyTestCase {

    /**
     * 
     */
    public TestReadOnlyTx() {
    }

    /**
     * @param name
     */
    public TestReadOnlyTx(String name) {
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
            IndexMetadata md = new IndexMetadata(name,UUID.randomUUID());
            
            md.setIsolatable(true);
            
            journal.registerIndex(md);

            IIndex ndx = journal.getIndex(name);

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

}
