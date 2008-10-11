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
 * Created on Jun 10, 2008
 */

package com.bigdata.btree;

import java.util.UUID;

import com.bigdata.btree.AbstractBTreeTupleCursor.ReadOnlyBTreeTupleCursor;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Unit tests for {@link ITupleCursor} for a read-only {@link BTree}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestReadOnlyBTreeCursors extends AbstractBTreeCursorTestCase {

    /**
     * 
     */
    public TestReadOnlyBTreeCursors() {
    }

    /**
     * @param arg0
     */
    public TestReadOnlyBTreeCursors(String arg0) {
        super(arg0);
    }


    @Override
    protected boolean isReadOnly() {
    
        return true;
        
    }
    
    @Override
    protected ITupleCursor2<String> newCursor(AbstractBTree btree, int flags,
            byte[] fromKey, byte[] toKey) {

        assert btree.isReadOnly();
        
        return new ReadOnlyBTreeTupleCursor<String>((BTree) btree,
                new Tuple<String>(btree, IRangeQuery.DEFAULT),
                fromKey, toKey);
        
    }
    
    /**
     * Verify that {@link ITupleCursor#remove()} will thrown an exception if
     * the source {@link BTree} does not allow writes.
     */
    public void test_remove_not_allowed() {
        
        final BTree btree = BTree.create(new SimpleMemoryRawStore(),
                new IndexMetadata(UUID.randomUUID()));

        btree.insert(10, "Bryan");
        
        btree.setReadOnly(true);
        
        final ITupleCursor<String> cursor = newCursor(btree);
        
        assertEquals(new TestTuple<String>(10,"Bryan"),cursor.next());
        
        try {
            cursor.remove();
            fail("Expecting: "+UnsupportedOperationException.class);
        } catch(UnsupportedOperationException ex) {
            log.info("Ignoring expected exception: "+ex);
        }
        
    }
    
}
