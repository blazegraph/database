/**

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
 * Created on Jun 1, 2011
 */

package com.bigdata.btree;

import java.io.File;
import java.io.IOException;

/**
 * Test suite for building an {@link IndexSegment} from an empty {@link BTree}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIndexSegmentBuilder_EmptyIndex extends
        AbstractIndexSegmentTestCase {

    /**
     * 
     */
    public TestIndexSegmentBuilder_EmptyIndex() {
    }

    /**
     * @param name
     */
    public TestIndexSegmentBuilder_EmptyIndex(String name) {
        super(name);
    }

    private File outFile;

    private File tmpDir;

    private boolean bufferNodes;

    public void setUp() throws Exception {

        super.setUp();
        
        // random choice.
        bufferNodes = r.nextBoolean();
        
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
        
        // clear references.
        outFile = null;
        tmpDir = null;
        
    }

    /**
     * Test ability to build an index segment from an empty {@link BTree}.
     */
    public void test_buildOrder3_emptyIndex() throws Exception {

        final BTree btree = getBTree(3);

        final IndexSegmentCheckpoint checkpoint = doBuildAndDiscardCache(btree,
                3/* m */);

        /*
         * Verify can load the index file and that the metadata associated with
         * the index file is correct (we are only checking those aspects that
         * are easily defined by the test case and not, for example, those
         * aspects that depend on the specifics of the length of serialized
         * nodes or leaves).
         */

        final IndexSegmentStore segStore = new IndexSegmentStore(outFile);

        assertEquals(checkpoint.commitTime, segStore.getCheckpoint().commitTime);
        assertEquals(0, segStore.getCheckpoint().height);
        assertEquals(1, segStore.getCheckpoint().nleaves);
        assertEquals(0, segStore.getCheckpoint().nnodes);
        assertEquals(0, segStore.getCheckpoint().nentries);

        final IndexSegment seg = segStore.loadIndexSegment();

        try {

            assertEquals(3, seg.getBranchingFactor());
            assertEquals(0, seg.getHeight());
            assertEquals(1, seg.getLeafCount());
            assertEquals(0, seg.getNodeCount());
            assertEquals(0, seg.getEntryCount());

            testForwardScan(seg);
            testReverseScan(seg);

            // test index segment structure.
            dumpIndexSegment(seg);

            /*
             * Test the tree in detail.
             */
            {

                final Leaf a = (Leaf) seg.getRoot();

                assertKeys(new int[] {}, a);

                // Note: values are verified by testing the total order.

            }

            /*
             * Verify the total index order.
             */
            assertSameBTree(btree, seg);

        } finally {

            // close so we can delete the backing store.
            seg.close();

        }

    }

    protected IndexSegmentCheckpoint doBuildAndDiscardCache(final BTree btree,
            final int m) throws IOException, Exception {

        final long commitTime = System.currentTimeMillis();

        final IndexSegmentCheckpoint checkpoint = IndexSegmentBuilder
                .newInstance(outFile, tmpDir, btree.getEntryCount(),
                        btree.rangeIterator(), m, btree.getIndexMetadata(),
                        commitTime, true/* compactingMerge */, bufferNodes)
                .call();

//        @see BLZG-1501 (remove LRUNexus)
//        if (LRUNexus.INSTANCE != null) {
//
//            /*
//             * Clear the records for the index segment from the cache so we will
//             * read directly from the file. This is necessary to ensure that the
//             * data on the file is good rather than just the data in the cache.
//             */
//            
//            LRUNexus.INSTANCE.deleteCache(checkpoint.segmentUUID);
//
//        }
        
        return checkpoint;

    }

}
