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

import java.nio.ByteBuffer;

import com.bigdata.btree.BTree;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Test suite for {@link CommitRecordIndex}. Tests focus on get (exact match)
 * vs find (most recent match less than or equal to) and restart safety.
 * 
 * @see TestCommitHistory, which tests the {@link CommitRecordIndex} in situ.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestCommitRecordIndex extends AbstractCommitRecordTestCase {

    /**
     * 
     */
    public TestCommitRecordIndex() {
    }

    /**
     * @param name
     */
    public TestCommitRecordIndex(String name) {
        super(name);
    }

    /**
     * Tests the ability to perform an exact match on a commit time and return
     * the address for the associated commit record. This also tests fence posts
     * for find and restart-safety for one entry.
     */
    public void test_basics() {
        
        IRawStore store = new SimpleMemoryRawStore();
        
        CommitRecordIndex ndx = CommitRecordIndex.create(store);
        
        final ICommitRecord cr1 = getRandomCommitRecord(); 

        final long addr1 = store.write(ByteBuffer
                .wrap(CommitRecordSerializer.INSTANCE.serialize(cr1)));

        // verify existence test (not found).
        assertFalse(ndx.hasTimestamp(cr1.getTimestamp()));

        // verify get (exact match).
        assertNull(ndx.get(cr1.getTimestamp()));

        // verify find (record with largest timestamp less than or equal to).
        assertNull(ndx.find(cr1.getTimestamp()));

        // add commit record.
        ndx.add(addr1, cr1);
        
        try {
            /*
             * correct rejection of another entry with the same commit time.
             */
            ndx.add(addr1, cr1);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        // verify existence test.
        assertTrue(ndx.hasTimestamp(cr1.getTimestamp()));

        // verify get (exact match).
        assertEquals(cr1,ndx.get(cr1.getTimestamp()));
        
        // verify find (exact match).
        assertEquals(cr1,ndx.find(cr1.getTimestamp()));

        // verify find (no match given timestamp less than any available).
        assertNull(ndx.find(cr1.getTimestamp()-1));

        // verify find (match given timestamp greater than any available).
        assertEquals(cr1,ndx.find(cr1.getTimestamp()+1));

        // write index on store.
        long ndx_addr = ndx.writeCheckpoint();
        
        {
            
            // reload the index from the store.
            ndx = (CommitRecordIndex)BTree.load(store, ndx_addr);

            // verify existence test.
            assertTrue(ndx.hasTimestamp(cr1.getTimestamp()));

            // verify get (exact match).
            assertEquals(cr1,ndx.get(cr1.getTimestamp()));
            
            // verify find (exact match).
            assertEquals(cr1,ndx.find(cr1.getTimestamp()));

            // verify find (no match given timestamp less than any available).
            assertNull(ndx.find(cr1.getTimestamp()-1));

            // verify find (match given timestamp greater than any available).
            assertEquals(cr1,ndx.find(cr1.getTimestamp()+1));

        }
        
    }
    
}
