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
 * Created on Feb 28, 2008
 */

package com.bigdata.btree;

import java.util.UUID;

import com.bigdata.mdi.LocalPartitionMetadata;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Tests for some fence posts for an index supporting delete markers and having
 * a {@link LocalPartitionMetadata} that declares its legal key range.
 * 
 * @todo test with keys that are outside of and on the partition boundaries.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIndexPartitionFencePosts extends AbstractBTreeTestCase {

    /**
     * 
     */
    public TestIndexPartitionFencePosts() {
        super();
    }

    /**
     * @param name
     */
    public TestIndexPartitionFencePosts(String name) {
        super(name);
    }

    /**
     * Tests basics with an index partition.
     * 
     * @throws Exception
     */
    public void test_onePartition() throws Exception {

        final String name = "testIndex";

        final IndexMetadata metadata = new IndexMetadata(name, UUID
                .randomUUID());

        metadata.setDeleteMarkers(true);

        metadata.setPartitionMetadata(new LocalPartitionMetadata(//
                0,// partitionId,
                new byte[]{}, // leftSeparator
                null, // rightSeparator
                null, // no resource descriptions.
                "" // history
                ));
        
        BTree ndx = BTree.create(new SimpleMemoryRawStore(),metadata);

        // the index is empty.
        assertFalse(ndx.contains(new byte[]{1}));
     
        // the key is not in the index.
        assertEquals(null, ndx.lookup(new byte[]{1}));
        
        // insert a key-value pair.
        assertNull(ndx.insert(new byte[]{1}, new byte[]{1}));

        // verify index reports value exists for the key.
        assertTrue(ndx.contains(new byte[]{1}));

        // verify correct value in the index.
        assertEquals(new byte[]{1}, ndx.lookup(new byte[]{1}));

        // verify some range counts.
        assertEquals(0, ndx.rangeCount(new byte[] {}, new byte[] { 1 }));
        assertEquals(1, ndx.rangeCount(new byte[] {}, new byte[] { 2 }));
        assertEquals(1, ndx.rangeCount(new byte[] { 1 }, new byte[] { 2 }));
        assertEquals(0, ndx.rangeCount(null, new byte[] { 1 }));
        assertEquals(1, ndx.rangeCount(new byte[] { 1 }, null));
        assertEquals(1, ndx.rangeCount(null, new byte[] { 2 }));
        assertEquals(1, ndx.rangeCount(null, null));

        // verify iterator for the same queries.
        assertSameIterator(new byte[][]{}, ndx.rangeIterator(new byte[] {}, new byte[] { 1 }));
        assertSameIterator(new byte[][]{new byte[]{1}}, ndx.rangeIterator(new byte[] {}, new byte[] { 2 }));
        assertSameIterator(new byte[][]{new byte[]{1}}, ndx.rangeIterator(new byte[] { 1 }, new byte[] { 2 }));
        assertSameIterator(new byte[][]{}, ndx.rangeIterator(null, new byte[] { 1 }));
        assertSameIterator(new byte[][]{new byte[]{1}}, ndx.rangeIterator(new byte[] { 1 }, null));
        assertSameIterator(new byte[][]{new byte[]{1}}, ndx.rangeIterator(null, new byte[] { 2 }));
        assertSameIterator(new byte[][]{new byte[]{1}}, ndx.rangeIterator(null, null));
        
        // remove the index entry.
        assertEquals(new byte[] { 1 }, ndx.remove(new byte[] { 1 }));

        // the index is empty.
        assertFalse(ndx.contains(new byte[] { 1 }));

        // the key is not in the index.
        assertEquals(null, ndx.lookup(new byte[] { 1 }));
        
        /*
         * verify some range counts.
         * 
         * Note: the range counts do NOT immediately adjust when keys are
         * removed since deletion markers are written into those entries. The
         * relevant index partition(s) need to be compacted for those deletion
         * markers to be removed and the range counts adjusted to match.
         */
        assertEquals(0, ndx.rangeCount(new byte[] {}, new byte[] { 1 }));
        assertEquals(1, ndx.rangeCount(new byte[] {}, new byte[] { 2 }));
        assertEquals(1, ndx.rangeCount(new byte[] { 1 }, new byte[] { 2 }));
        assertEquals(0, ndx.rangeCount(null, new byte[] { 1 }));
        assertEquals(1, ndx.rangeCount(new byte[] { 1 }, null));
        assertEquals(1, ndx.rangeCount(null, new byte[] { 2 }));
        assertEquals(1, ndx.rangeCount(null, null));

        // verify iterator for the same queries.
        assertSameIterator(new byte[][]{}, ndx.rangeIterator(new byte[] {}, new byte[] { 1 }));
        assertSameIterator(new byte[][]{}, ndx.rangeIterator(new byte[] {}, new byte[] { 2 }));
        assertSameIterator(new byte[][]{}, ndx.rangeIterator(new byte[] { 1 }, new byte[] { 2 }));
        assertSameIterator(new byte[][]{}, ndx.rangeIterator(null, new byte[] { 1 }));
        assertSameIterator(new byte[][]{}, ndx.rangeIterator(new byte[] { 1 }, null));
        assertSameIterator(new byte[][]{}, ndx.rangeIterator(null, new byte[] { 2 }));
        assertSameIterator(new byte[][]{}, ndx.rangeIterator(null, null));

    }

}
