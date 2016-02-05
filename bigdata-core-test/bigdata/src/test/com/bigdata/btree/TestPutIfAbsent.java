/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Feb 13, 2008
 */

package com.bigdata.btree;

import java.util.UUID;

import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Test of basic btree operations when delete markers are maintained.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @see BLZG-1539 (putIfAbsent)
 */
public class TestPutIfAbsent extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestPutIfAbsent() {
    }

    /**
     * @param name
     */
    public TestPutIfAbsent(String name) {
        super(name);
    }

    /**
	 * Basic putIfAbsent() test when delete markers are not enabled.
	 */
	public void test_putIfAbsent_01() {

		final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

        // create index.
        final BTree btree = BTree.create(new SimpleMemoryRawStore(), metadata);
        
        // verify delete markers are NOT in use.
        assertFalse(btree.getIndexMetadata().getDeleteMarkers());
        
        final byte[] k1 = new byte[] { 1 };
        final byte[] v1 = new byte[] { 1 };
        final byte[] v2 = new byte[] { 2 };
        final byte[] v3 = null;
        
		// entry not found under the key.
		assertFalse(btree.contains(k1));

		// conditional mutation when no entry under the key should return null
		// and modify the index.
		assertNull(btree.putIfAbsent(k1, v1));

		// entry now found under the key.
		assertTrue(btree.contains(k1));

		assertEquals(v1, btree.lookup(k1));

		// conditional mutation when entry under key exists should return old
		// value and not modify the index.
		assertEquals(v1, btree.putIfAbsent(k1, v2));

		assertEquals(v1, btree.lookup(k1));
		
		/*
		 * Now delete out the tuple. Verify that it is gone.
		 */
		assertEquals(v1, btree.remove(k1));
		assertFalse(btree.contains(k1));
		assertEquals(null, btree.lookup(k1));

		/*
		 * Now use conditional insert again. The tuple was deleted so we should
		 * be able to use putIfAbsent() for the same key and a different value
		 * and find the new value in the index.
		 */
		// conditional insert with new value.
		assertEquals(null, btree.putIfAbsent(k1, v2));
		assertTrue(btree.contains(k1));
		assertEquals(v2, btree.lookup(k1));

		/*
		 * Now examine what happens when we have a null value in the tuple.
		 */

		// remove the current entry under the key.
		assertEquals(v2, btree.remove(k1));
		assertFalse(btree.contains(k1));
		assertEquals(null, btree.lookup(k1));
		
		// conditional insert of a null under the key.
		assertEquals(null, btree.putIfAbsent(k1, v3));
		assertTrue(btree.contains(k1)); // index reports key exists.
		assertEquals(null, btree.lookup(k1)); // null value is returned by lookup.

		/*
		 * Conditional insert of a different (non-null) value should fail since
		 * there is an entry under that key.
		 */
		assertEquals(null, btree.putIfAbsent(k1, v1)); // should fail!
		assertTrue(btree.contains(k1)); // index reports key exists.
		assertEquals(null, btree.lookup(k1)); // null value is returned by lookup.
		
	}

    /**
     * Variant test when delete markers are enabled.
     */
    public void test_putIfAbsent_01_deleteMarkers() {
        
    	final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        
        metadata.setDeleteMarkers(true);

        // create index.
        final BTree btree = BTree.create(new SimpleMemoryRawStore(), metadata);
        
        // verify delete markers are in use.
        assertTrue(btree.getIndexMetadata().getDeleteMarkers());
        
        final byte[] k1 = new byte[] { 1 };
        final byte[] v1 = new byte[] { 1 };
        final byte[] v2 = new byte[] { 2 };
        final byte[] v3 = null;
        
		// entry not found under the key.
		assertFalse(btree.contains(k1));

		// conditional mutation when no entry under the key should return null
		// and modify the index.
		assertNull(btree.putIfAbsent(k1, v1));

		// entry now found under the key.
		assertTrue(btree.contains(k1));

		assertEquals(v1, btree.lookup(k1));

		// conditional mutation when entry under key exists should return old
		// value and not modify the index.
		assertEquals(v1, btree.putIfAbsent(k1, v2));

		assertEquals(v1, btree.lookup(k1));
		
		/*
		 * Now delete out the tuple. Verify that it is gone.
		 */
		assertEquals(v1, btree.remove(k1));
		assertFalse(btree.contains(k1));
		assertEquals(null, btree.lookup(k1));

		/*
		 * Now use conditional insert again. The tuple was deleted so we should
		 * be able to use putIfAbsent() for the same key and a different value
		 * and find the new value in the index.
		 */
		// conditional insert with new value.
		assertEquals(null, btree.putIfAbsent(k1, v2));
		assertTrue(btree.contains(k1));
		assertEquals(v2, btree.lookup(k1));

		/*
		 * Now examine what happens when we have a null value in the tuple.
		 */

		// remove the current entry under the key.
		assertEquals(v2, btree.remove(k1));
		assertFalse(btree.contains(k1));
		assertEquals(null, btree.lookup(k1));
		
		// conditional insert of a null under the key.
		assertEquals(null, btree.putIfAbsent(k1, v3));
		assertTrue(btree.contains(k1)); // index reports key exists.
		assertEquals(null, btree.lookup(k1)); // null value is returned by lookup.

		/*
		 * Conditional insert of a different (non-null) value should fail since
		 * there is an entry under that key.
		 */
		assertEquals(null, btree.putIfAbsent(k1, v1)); // should fail!
		assertTrue(btree.contains(k1)); // index reports key exists.
		assertEquals(null, btree.lookup(k1)); // null value is returned by lookup.

    }

}
