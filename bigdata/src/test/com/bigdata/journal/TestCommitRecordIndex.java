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
        
        CommitRecordIndex ndx = new CommitRecordIndex(store);
        
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
        long ndx_addr = ndx.write();
        
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
