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

import com.bigdata.btree.AbstractTupleFilterator.Removerator;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Test suite for the {@link Removerator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRemoverator extends AbstractCursorTestCase {

    /**
     * 
     */
    public TestRemoverator() {
    }

    /**
     * @param name
     */
    public TestRemoverator(String name) {
        super(name);
    }

    /**
     * Test verifies that we can remove each tuple as we visit it and that the
     * tuple returned to the caller is not invalidated by that remove().
     */
    public void test() {
    
        BTree btree = BTree.create(new SimpleMemoryRawStore(), new IndexMetadata(
                UUID.randomUUID()));

        btree.insert(10, "Bryan");
        btree.insert(20, "Mike");
        btree.insert(30, "James");

        ITupleIterator<String> itr = new Removerator(btree.rangeIterator());
        
        assertTrue(itr.hasNext());

        assertTrue(btree.contains(10));

        assertEquals(new TestTuple<String>(10,"Bryan"),itr.next());
        
        assertFalse(btree.contains(10));
        
        assertTrue(itr.hasNext());

        assertTrue(btree.contains(20));

        assertEquals(new TestTuple<String>(20,"Mike"),itr.next());

        assertFalse(btree.contains(20));

        assertTrue(itr.hasNext());

        assertTrue(btree.contains(30));

        assertEquals(new TestTuple<String>(30,"James"),itr.next());

        assertFalse(itr.hasNext());

        assertFalse(btree.contains(30));

        assertEquals(0,btree.getEntryCount());
        
    }

    @Override
    protected ITupleCursor<String> newCursor(AbstractBTree btree, int flags, byte[] fromKey, byte[] toKey) {
        // TODO Auto-generated method stub
        return null;
    }
    
}
