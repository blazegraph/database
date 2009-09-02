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
 * Created on Jun 9, 2008
 */

package com.bigdata.btree;

import java.io.File;
import java.io.IOException;

import com.bigdata.btree.IndexSegment.IndexSegmentTupleCursor;

/**
 * Test suite for {@link IndexSegmentTupleCursor}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo test variant using delete markers. note that delete markers can be
 *       present in an index segment (unless a compacting merge was performed)
 *       but that they have to be pre-populated in the index in order for them
 *       to be visible (vs just appearing as a result of mutation).
 *       <p>
 *       Note: Delete markers are already tested by
 *       {@link TestMutableBTreeCursors} and there is little reason to think
 *       that they would not work for an {@link IndexSegment}.
 */
public class TestIndexSegmentCursors extends AbstractTupleCursorTestCase {

    /**
     * 
     */
    public TestIndexSegmentCursors() {
    }

    /**
     * @param arg0
     */
    public TestIndexSegmentCursors(String arg0) {

        super(arg0);

    }

    File outFile, tmpDir;
    
    protected void setUp() throws Exception {
        
        super.setUp();
        
        outFile = new File(getName() + ".seg");

        if (outFile.exists() && !outFile.delete()) {

            throw new RuntimeException("Could not delete file: " + outFile);

        }

        tmpDir = outFile.getAbsoluteFile().getParentFile();

    }

    protected void tearDown() throws Exception {
        
        super.tearDown();

        if (outFile != null && outFile.exists() && !outFile.delete()) {

            log.warn("Could not delete file: " + outFile);

        }

    }

    /**
     * Builds an {@link IndexSegment} from a {@link BTree}.
     * 
     * @param btree
     * 
     * @return
     * 
     * @throws Exception
     */
    protected IndexSegment buildIndexSegment(final BTree btree)
            throws Exception {

        new IndexSegmentBuilder(outFile, tmpDir, btree.getEntryCount(), btree
                .rangeIterator(), 30/* m */, btree.getIndexMetadata(), System
                .currentTimeMillis()/* commitTime */, true/* compactingMerge */)
                .call();

        IndexSegmentStore segStore = new IndexSegmentStore(outFile);

        return segStore.loadIndexSegment();
        
    }
    
    protected ITupleCursor2<String> newCursor(final AbstractBTree btree,
            final int flags, final byte[] fromKey, final byte[] toKey) {

        return new IndexSegmentTupleCursor<String>((IndexSegment) btree,
                new Tuple<String>(btree, IRangeQuery.DEFAULT),
                fromKey, toKey);

    }

    public void test_oneTuple() throws IOException, Exception {

        final BTree btree = getOneTupleBTree();
 
        final IndexSegment seg = buildIndexSegment(btree);

        try {

            doOneTupleTest(seg);

            /*
             * Verify that {@link ITupleCursor#remove()} will thrown an
             * exception if the source {@link BTree} does not allow writes.
             */
            {
                ITupleCursor2<String> cursor = newCursor(seg);

                assertEquals(new TestTuple<String>(10, "Bryan"), cursor.next());

                try {
                    cursor.remove();
                    fail("Expecting: " + UnsupportedOperationException.class);
                } catch (UnsupportedOperationException ex) {
                    log.info("Ignoring expected exception: " + ex);
                }

            }

        } finally {

            // close so it can be deleted by tearDown().
            seg.close();
            
        }
        
    }

    /**
     * A test for first(), last(), next(), prior(), and seek() given a B+Tree
     * that has been pre-populated with a few tuples.
     * 
     * @throws Exception
     * @throws IOException
     */
    public void test_baseCase() throws IOException, Exception {

        final BTree btree = getBaseCaseBTree();

        final IndexSegment seg = buildIndexSegment(btree);

        try {

            doBaseCaseTest(seg);

        } finally {

            // close so it can be deleted by tearDown().
            seg.close();

        }

    }

}
