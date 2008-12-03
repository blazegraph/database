/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on May 20, 2008
 */

package com.bigdata.btree;

import org.apache.log4j.Level;

import com.bigdata.btree.IndexSegment.ImmutableLeafCursor;
import com.bigdata.btree.IndexSegment.ImmutableNodeFactory.ImmutableLeaf;

/**
 * Adds some methods for testing an {@link IndexSegment} for consistency.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractIndexSegmentTestCase extends AbstractBTreeTestCase {

    /**
     * 
     */
    public AbstractIndexSegmentTestCase() {
    }

    /**
     * @param name
     */
    public AbstractIndexSegmentTestCase(String name) {
        super(name);
    }

    /**
     * apply dump() as a structural validation of the tree.  note that we
     * have to materialize the leaves in the generated index segment since
     * dump does not materialize a child from its Addr if it is not already
     * resident.
     */
    protected void dumpIndexSegment(IndexSegment seg) {

        // materialize the leaves.
        
        final ILeafCursor cursor = seg.newLeafCursor(SeekEnum.First);
        
        int n = 0;
        while(cursor.next()!=null) {
            n++;
        }

        cursor.last();
        while(cursor.prior()!=null) {
            n--;
        }
        
        /*
         * Note: n will be zero if the same number of leaves were visited in
         * each direction.
         */
        assertEquals(0, n);
        
        /*
         * Dump the tree to validate it.
         * 
         * Note: This will only dump the root node since the [cursor] for an
         * IndexSegment does not materialize the intermediate nodes and dump()
         * only dumps those nodes and leaves which are already materialized.
         */

        System.err.println("dumping index segment");

        assert seg.dump(Level.DEBUG, System.err);

        // dump the leaves in forward order.
        cursor.first().dump(Level.DEBUG, System.err);
        while(cursor.next()!=null) {
            cursor.leaf().dump(Level.DEBUG, System.err);
        }
    }
    
    /**
     * Test forward leaf scan.
     */
    public void testForwardScan(IndexSegment seg)
    {

        final int nleaves = seg.getStore().getCheckpoint().nleaves;
        
        final ImmutableLeaf firstLeaf = seg.readLeaf(seg.getStore().getCheckpoint().addrFirstLeaf);
        assertEquals("priorAddr", 0L, firstLeaf.priorAddr);

        final ImmutableLeaf lastLeaf = seg.readLeaf(seg.getStore().getCheckpoint().addrLastLeaf);
        assertEquals("nextAddr", 0L, lastLeaf.nextAddr);

        final ImmutableLeafCursor itr = seg.newLeafCursor(SeekEnum.First);
        
        assertTrue(firstLeaf==itr.leaf()); // Note: test depends on cache!
        
        ImmutableLeaf priorLeaf = itr.leaf();
        
        int n = 1;
        
        for(int i=1; i<seg.getLeafCount(); i++) {
            
            ImmutableLeaf current = itr.next();
            
            assertEquals("priorAddr",priorLeaf.getIdentity(),current.priorAddr);

            priorLeaf = current;

            if (current == lastLeaf) {

                // last leaf.
                assertNull(itr.next());
                
            }
            
            n++;
            
        }
        
        assertEquals("#leaves",nleaves,n);
        
    }
    
    /**
     * Test reverse leaf scan.
     * 
     * Note: the scan starts with the last leaf in the key order and then
     * proceeds in reverse key order.
     */
    public void testReverseScan(IndexSegment seg) {

        final int nleaves = seg.getStore().getCheckpoint().nleaves;

        final ImmutableLeaf firstLeaf = seg.readLeaf(seg.getStore().getCheckpoint().addrFirstLeaf);
        assertEquals("priorAddr", 0L, firstLeaf.priorAddr);

        final ImmutableLeaf lastLeaf = seg.readLeaf(seg.getStore().getCheckpoint().addrLastLeaf);
        assertEquals("nextAddr", 0L, lastLeaf.nextAddr);

        final ImmutableLeafCursor itr = seg.newLeafCursor(SeekEnum.Last);
        
        assertTrue(lastLeaf==itr.leaf()); // Note: test depends on cache!
        
        ImmutableLeaf nextLeaf = itr.leaf();
        
        int n = 1;
        
        for(int i=1; i<seg.getLeafCount(); i++) {
            
            ImmutableLeaf current = itr.prior();
            
            assertEquals("nextAddr", nextLeaf.getIdentity(), current.nextAddr);

            nextLeaf = current;

            if (current == firstLeaf) {

                // last leaf.
                assertNull(itr.prior());
                
            }
            
            n++;
            
        }
        
        assertEquals("#leaves",nleaves,n);

    }

}
