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
 * Created on Jun 12, 2008
 */

package com.bigdata.btree;

import java.util.UUID;

import junit.framework.TestCase2;

import com.bigdata.btree.BTree.Stack;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Test suite for the {@link BTree}'s {@link ILeafCursor} implementation. The
 * most critical thing about this test suite is that it validates the cursor's
 * ability to correctly maintain the {@link Stack} of {@link Node}s over the
 * current cursor position, especially for {@link ILeafCursor#prior()} and
 * {@link ILeafCursor#next()}. These tests have to be conducted at a
 * {@link BTree} depth of 2 (a root node, two nodes beneath that, and then a
 * layer of leaves under those nodes) in order to test the recursive handling of
 * the node stack by prior() and next(). {@link #getProblem1()} is used for this
 * purpose.
 * <p>
 * Note: The leaves are verified by comparing the first key in the leaf against
 * the expected key for that position of that leaf.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBTreeLeafCursors extends TestCase2 {

    /**
     * 
     */
    public TestBTreeLeafCursors() {
    }

    /**
     * @param name
     */
    public TestBTreeLeafCursors(String name) {
        super(name);
    }

    final BTree btree = getProblem1();

    /**
     * Unit tests for the node stack impl.
     */
    public void test_stack() {

        Stack s = new Stack(10);
        
        assertEquals(10,s.capacity());
        
        assertEquals(0,s.size());
        
        try {
            s.peek();
            fail("Expecting: " + IllegalStateException.class);
        } catch (IllegalStateException ex) {
            log.info("Ingoring expected exception: " + ex);
        }
        
        try {
            s.pop();
            fail("Expecting: " + IllegalStateException.class);
        } catch (IllegalStateException ex) {
            log.info("Ingoring expected exception: " + ex);
        }
        
        /*
         * Note: These nodes are named per the problem definition. See the
         * worksheet.
         */
        final Node g = (Node)btree.getRoot();
        final Node c = (Node)g.getChild(0);
        final Node f = (Node)g.getChild(1);
        
        s.push(g);
        
        assertEquals(1,s.size());
        
        assertEquals(g,s.peek());
        
        assertEquals(g,s.pop());

        assertEquals(0,s.size());

        /*
         * @todo more tests covering clear(), copy() and resize of backing
         * array.
         */
        
    }
    
    public void test_firstLast() {
        
        ILeafCursor<Leaf> cursor = btree.newLeafCursor(SeekEnum.First);

        // verify first leaf since that is where we positioned the cursor.
        assertEquals(KeyBuilder.asSortKey(1), cursor.leaf().getKeys().getKey(0));

        // first().
        assertEquals(KeyBuilder.asSortKey(1), cursor.first().getKeys().getKey(0));

        // last().
        assertEquals(KeyBuilder.asSortKey(9), cursor.last().getKeys().getKey(0));
        
    }
    
    public void test_seek() {

        ILeafCursor<Leaf> cursor = btree.newLeafCursor(KeyBuilder.asSortKey(5));

        // verify initial seek.
        assertEquals(KeyBuilder.asSortKey(5), cursor.leaf().getKeys().getKey(0));

        // verify seek to each key found in the B+Tree.
        assertEquals(KeyBuilder.asSortKey(1), cursor.seek(
                KeyBuilder.asSortKey(1)).getKeys().getKey(0));

        assertEquals(KeyBuilder.asSortKey(1), cursor.seek(
                KeyBuilder.asSortKey(2)).getKeys().getKey(0));

        assertEquals(KeyBuilder.asSortKey(3), cursor.seek(
                KeyBuilder.asSortKey(3)).getKeys().getKey(0));

        assertEquals(KeyBuilder.asSortKey(3), cursor.seek(
                KeyBuilder.asSortKey(4)).getKeys().getKey(0));

        assertEquals(KeyBuilder.asSortKey(5), cursor.seek(
                KeyBuilder.asSortKey(5)).getKeys().getKey(0));

        assertEquals(KeyBuilder.asSortKey(5), cursor.seek(
                KeyBuilder.asSortKey(6)).getKeys().getKey(0));

        assertEquals(KeyBuilder.asSortKey(7), cursor.seek(
                KeyBuilder.asSortKey(7)).getKeys().getKey(0));

        assertEquals(KeyBuilder.asSortKey(7), cursor.seek(
                KeyBuilder.asSortKey(8)).getKeys().getKey(0));

        assertEquals(KeyBuilder.asSortKey(9), cursor.seek(
                KeyBuilder.asSortKey(9)).getKeys().getKey(0));

        assertEquals(KeyBuilder.asSortKey(9), cursor.seek(
                KeyBuilder.asSortKey(10)).getKeys().getKey(0));

        // verify seek to key that would be in the last leaf but is not actually in the B+Tree.
        assertEquals(KeyBuilder.asSortKey(9),cursor.seek(KeyBuilder.asSortKey(12)).getKeys().getKey(0));

    }

    // FIXME explictly verify the stack!
    public void test_forwardScan() {
        
        ILeafCursor<Leaf> cursor = btree.newLeafCursor(SeekEnum.First);

        // verify first leaf since that is where we positioned the cursor.
        assertEquals(KeyBuilder.asSortKey(1), cursor.leaf().getKeys().getKey(0));
        
        // next().
        assertEquals(KeyBuilder.asSortKey(3), cursor.next().getKeys().getKey(0));

        // next().
        assertEquals(KeyBuilder.asSortKey(5), cursor.next().getKeys().getKey(0));
        
        // next().
        assertEquals(KeyBuilder.asSortKey(7), cursor.next().getKeys().getKey(0));

        // next().
        assertEquals(KeyBuilder.asSortKey(9), cursor.next().getKeys().getKey(0));
        
    }

    public void test_reverseScan() {
        
        ILeafCursor<Leaf> cursor = btree.newLeafCursor(SeekEnum.Last);

        // verify last leaf since that is where we positioned the cursor.
        assertEquals(KeyBuilder.asSortKey(9), cursor.leaf().getKeys().getKey(0));
        
        // next().
        assertEquals(KeyBuilder.asSortKey(7), cursor.prior().getKeys().getKey(0));

        // next().
        assertEquals(KeyBuilder.asSortKey(5), cursor.prior().getKeys().getKey(0));
        
        // next().
        assertEquals(KeyBuilder.asSortKey(3), cursor.prior().getKeys().getKey(0));

        // next().
        assertEquals(KeyBuilder.asSortKey(1), cursor.prior().getKeys().getKey(0));
        
    }
    
    /**
     * Create, populate, and return a btree with a branching factor of (3) and
     * ten sequential keys [1:10]. The values are {@link String}s objects
     * formed using "v+"+i, where i is the integer from which the key was
     * formed.
     * 
     * @return The btree.
     * 
     * @see src/architecture/btree.xls, which details this input tree and a
     *      series of output trees with various branching factors.
     */
    public BTree getProblem1() {

        final BTree btree;
        {
            
            IndexMetadata md = new IndexMetadata(UUID.randomUUID());

            md.setBranchingFactor(3);
            
            btree = BTree.create(new SimpleMemoryRawStore(), md);
            
        }

        for (int i = 1; i <= 10; i++) {

            btree.insert(KeyBuilder.asSortKey(i), "v"+i);

        }
        
        return btree;

    }

}
