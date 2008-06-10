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
 */
public class TestIndexSegmentCursors extends AbstractCursorTestCase {

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

    protected ITupleCursor<String> newCursor(AbstractBTree btree, int flags,
            byte[] fromKey, byte[] toKey) {

        return new IndexSegmentTupleCursor<String>((IndexSegment) btree,
                new Tuple<String>(btree, IRangeQuery.DEFAULT),
                null/* fromKey */, null/* toKey */);

    }

    /**
     * A test for first(), last(), next(), prior(), and seek() given a B+Tree
     * that has been pre-popluated with a few tuples.
     * 
     * @throws Exception
     * @throws IOException
     */
    public void test_baseCase() throws IOException, Exception {

        File outFile = new File(getName() + ".seg");

        if (outFile.exists() && !outFile.delete()) {

            throw new RuntimeException("Could not delete file: " + outFile);

        }

        File tmpDir = outFile.getAbsoluteFile().getParentFile();

        try {

            final IndexSegment seg;
            {

                BTree btree = getBaseCaseBTree();

                new IndexSegmentBuilder(outFile, tmpDir, btree.getEntryCount(),
                        btree.entryIterator(), 30/* m */, btree
                                .getIndexMetadata(), System.currentTimeMillis()/* commitTime */)
                        .call();

                IndexSegmentStore segStore = new IndexSegmentStore(outFile);

                seg = segStore.loadIndexSegment();

            }

            doBaseCaseTest(seg);

        } finally {

            if (outFile != null && outFile.exists() && !outFile.delete()) {

                log.warn("Could not delete file: " + outFile);

            }

        }

    }

}
