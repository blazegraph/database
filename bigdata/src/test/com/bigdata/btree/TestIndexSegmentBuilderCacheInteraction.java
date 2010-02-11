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
package com.bigdata.btree;

import java.io.File;
import java.util.UUID;

import com.bigdata.LRUNexus;
import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.data.INodeData;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.cache.IGlobalLRU;
import com.bigdata.cache.IGlobalLRU.ILRUCache;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;

/**
 * Test suite examines the interaction of the {@link IndexSegmentBuilder} with
 * an {@link IGlobalLRU}.
 */
public class TestIndexSegmentBuilderCacheInteraction extends
        AbstractIndexSegmentTestCase {

    File outFile;

    File tmpDir;

    public TestIndexSegmentBuilderCacheInteraction() {
    }

    public TestIndexSegmentBuilderCacheInteraction(String name) {
        super(name);
    }

    final boolean bufferNodes = true;
    
    public void setUp() throws Exception {

        super.setUp();
        
        outFile = new File(getName() + ".seg");

        if (outFile.exists() && !outFile.delete()) {

            throw new RuntimeException("Could not delete file: " + outFile);

        }
        
        tmpDir = outFile.getAbsoluteFile().getParentFile();

    }

    public void tearDown() throws Exception {

        if (outFile != null && outFile.exists() && !outFile.delete()) {

            log.warn("Could not delete file: " + outFile);

        }

        super.tearDown();
        
    }

    /**
     * Return a btree backed by a journal with the indicated branching factor.
     * The serializer requires that values in leaves are {@link SimpleEntry}
     * objects.
     * 
     * @param branchingFactor
     *            The branching factor.
     * 
     * @return The btree.
     */
    public BTree getBTree(final int branchingFactor,
            final BloomFilterFactory bloomFilterFactory) {

        final IRawStore store = new SimpleMemoryRawStore(); 

        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        
        metadata.setBranchingFactor(branchingFactor);
        
        metadata.setBloomFilterFactory(bloomFilterFactory);
        
        return BTree.create(store, metadata);

    }
    
    /**
     * Create, populate, and return a btree with a branching factor of (3) and
     * ten sequential keys [1:10]. The values are {@link SimpleEntry} objects
     * whose state is the same as the corresponding key.
     * 
     * @return The btree.
     * 
     * @see src/architecture/btree.xls, which details this input tree and a
     *      series of output trees with various branching factors.
     */
    public BTree getProblem1() {

        final BTree btree = getBTree(3, BloomFilterFactory.DEFAULT);

        for (int i = 1; i <= 10; i++) {

            btree.insert(KeyBuilder.asSortKey(i), new SimpleEntry(i));

        }
        
        return btree;

    }
    
    public void test_buildOrder3() throws Exception {

        if (LRUNexus.INSTANCE == null) {
            
            log.warn("The LRUNexus is disabled");
            
            return;
            
        }
        
        final BTree btree = getProblem1();

        final long commitTime = System.currentTimeMillis();

        final IndexSegmentBuilder builder = IndexSegmentBuilder.newInstance(
                outFile, tmpDir, btree.getEntryCount(), btree.rangeIterator(),
                3/* m */, btree.getIndexMetadata(), commitTime,
                true/* compactingMerge */, bufferNodes);

        final IndexSegmentCheckpoint checkpoint = builder.call();

        final ILRUCache<Long, Object> cache = LRUNexus.INSTANCE.getCache(
                checkpoint.segmentUUID, null/* am */);
        
        assertNotNull(cache);
        
        // verify records already in this cache.
        assertTrue(cache.size() > 0);

        assertEquals(commitTime, checkpoint.commitTime);
        assertEquals(2, checkpoint.height);
        assertEquals(4, checkpoint.nleaves);
        assertEquals(3, checkpoint.nnodes);
        assertEquals(10, checkpoint.nentries);

        // verify index metadata object is in the cache.
        assertNotNull(cache.get(checkpoint.addrMetadata));
        assertTrue(cache.get(checkpoint.addrMetadata) == builder.metadata);
        
        // verify the bloom filter is in the cache (must be requested).
        assertTrue(checkpoint.addrBloom!=0L);
        assertNotNull(cache.get(checkpoint.addrBloom));
        assertTrue(cache.get(checkpoint.addrBloom) == builder.bloomFilter);
        
        /*
         * Check the nodes.
         */
        
        // root node is in the cache.
        assertNotNull(cache.get(checkpoint.addrRoot));

        final INodeData nodeC = (INodeData) cache.get(checkpoint.addrRoot);
        assertNotNull(nodeC);
        assertEquals(2, nodeC.getChildCount());
        final long addrA = nodeC.getChildAddr(0);
        final long addrB = nodeC.getChildAddr(1);

        final INodeData nodeA = (INodeData) cache.get(addrA);
        assertNotNull(nodeA);
        assertEquals(2, nodeA.getChildCount());

        final INodeData nodeB = (INodeData) cache.get(addrB);
        assertNotNull(nodeB);
        assertEquals(2, nodeB.getChildCount());
       
        /*
         * Check the leaves.
         * 
         * Note: There are four leaves for the generated index segment.
         */
        
        // first leaf is in the cache.
        assertNotNull(cache.get(checkpoint.addrFirstLeaf));

        final ILeafData leaf1 = (ILeafData) cache.get(checkpoint.addrFirstLeaf);

        assertTrue(leaf1.isDoubleLinked());

        assertEquals(0L, leaf1.getPriorAddr());

        assertTrue(leaf1.getNextAddr() != 0L);

        // 2nd leaf.
        final long addr2 = leaf1.getNextAddr();
        
        final ILeafData leaf2 = (ILeafData) cache.get(addr2);

        assertTrue(leaf2.isDoubleLinked());

        assertEquals(checkpoint.addrFirstLeaf, leaf2.getPriorAddr());

        assertTrue(leaf1.getNextAddr() != 0L);

        // 3rd leaf.
        final long addr3 = leaf2.getNextAddr();
        
        final ILeafData leaf3 = (ILeafData) cache.get(addr3);

        assertTrue(leaf3.isDoubleLinked());

        assertEquals(addr2, leaf3.getPriorAddr());

        assertTrue(leaf3.getNextAddr() != 0L);

        assertEquals(checkpoint.addrLastLeaf, leaf3.getNextAddr());

        // 4th and last leaf is in the cache.
        assertNotNull(cache.get(checkpoint.addrLastLeaf));

        final ILeafData leaf4 = (ILeafData) cache.get(checkpoint.addrLastLeaf);

        assertTrue(leaf4.isDoubleLinked());

        assertTrue(leaf4.getPriorAddr() != 0L);

        assertEquals(0L, leaf4.getNextAddr());

        /*
         * Verify the index segment is Ok and using the same objects that were
         * pushed into the cache during the index segment build.
         */
        
        final IndexSegmentStore segStore = new IndexSegmentStore(outFile);

        try {

            final IndexSegment seg = segStore.loadIndexSegment();

            // verify root node's data record is same object as in the cache.
            assertTrue(nodeC == seg.getRoot().getDelegate());

            // verify the intermediate nodes.
            assertTrue(nodeA == ((Node) seg.getRoot()).getChild(0)
                    .getDelegate());

            assertTrue(nodeB == ((Node) seg.getRoot()).getChild(1)
                    .getDelegate());

            // verify 1st leaf has same data record object as in cache.
            assertTrue(leaf1 == seg.readLeaf(checkpoint.addrFirstLeaf)
                    .getDelegate());

            // verify last leaf has same data record object as in cache.
            assertTrue(leaf4 == seg.readLeaf(checkpoint.addrLastLeaf)
                    .getDelegate());
            
            /*
             * Verify the total index order.
             */
            assertSameBTree(btree, seg);

        } finally {

            segStore.destroy();
            
        }

    }

}
