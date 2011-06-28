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
 * Created on Dec 20, 2009
 */

package com.bigdata.resources;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import junit.framework.TestCase2;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.ISimpleSplitHandler;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentBuilder;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;

/**
 * Unit tests for splitting an index segment based on its size on the disk, the
 * nominal size of an index partition, and an optional application level
 * constraint on the choice of the separator keys. This approach presumes a
 * compacting merge has been performed such that all history other than the
 * buffered writes is on a single index segment. The buffered writes are not
 * considered when choosing the #of splits to make and the separator keys for
 * those splits. They are simply copied afterwards onto the new index partition
 * which covers their key-range.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see src/architecture/SplitMath.xls
 */
public class AbstractTestSegSplitter extends TestCase2 {

    /**
     * 
     */
    public AbstractTestSegSplitter() {
    }

    /**
     * @param name
     */
    public AbstractTestSegSplitter(String name) {
        super(name);
    }

    protected IJournal getStore() throws IOException {
        
        Properties p = new Properties();
        
        p.setProperty(Options.FILE, File.createTempFile(getName(), Options.JNL).toString());
        
        return new Journal(p);
        
    }
    
    protected IPartitionIdFactory pidFactory = new MockPartitionIdFactory();
    
    /**
     * Always accepts the recommended separator key.
     */
    protected static final ISimpleSplitHandler acceptAllSplits = new ISimpleSplitHandler() {
        
        public byte[] getSeparatorKey(IndexSegment seg, int fromIndex, int toIndex,
                int splitAt) {

            return seg.keyAt(splitAt);
            
        }
    };
    
    /**
     * Always returns <code>null</code> (never accepts any splits).
     */
    protected static final ISimpleSplitHandler rejectAllSplits = new ISimpleSplitHandler() {
        
        public byte[] getSeparatorKey(IndexSegment seg, int fromIndex, int toIndex,
                int splitAt) {

            return null;
            
        }
    };

    /**
     * Accepts the recommended separator key unless it is GTE the key given to
     * the constructor, in which case it refuses to accept any splits.
     */
    protected static class RejectSplitsAfterKey implements ISimpleSplitHandler {

        private final byte[] rejectKey;

        public RejectSplitsAfterKey(final byte[] rejectKey) {

            if (rejectKey == null)
                throw new IllegalArgumentException();

            this.rejectKey = rejectKey;
            
        }
        
        public byte[] getSeparatorKey(final IndexSegment seg,
                final int fromIndex, final int toIndex, final int splitAt) {

            final byte[] a = seg.keyAt(splitAt);

            if (BytesUtil.compareBytes(a, rejectKey) >= 0) {

                // reject any split GTE the key specified to the ctor.
                return null;

            }

            // Otherwise accept the recommended separator key.
            return a;
            
        }
    };

    /**
     * Generate an {@link IndexSegment} from the given BTree.
     * 
     * @param src
     *            The source {@link BTree}.
     * 
     * @return The {@link IndexSegmentBuilder}.
     */
    static protected IndexSegmentBuilder doBuild(final String name,
            final ILocalBTreeView src, final long commitTime,
            final byte[] fromKey, final byte[] toKey) throws Exception {

        if (src == null)
            throw new IllegalArgumentException();

        // final String name = getName();

        final File tmpDir = new File(System.getProperty("java.io.tmpdir"));

        File outFile = null;
        final IndexSegmentBuilder builder;
        try {

            // the file to be generated.
            outFile = File.createTempFile(name, Options.SEG, tmpDir);

            // new builder.
            builder = IndexSegmentBuilder.newInstance(/*name, */src, outFile,
                    tmpDir, true/* compactingMerge */, commitTime, fromKey,
                    toKey);

            // build the index segment.
            builder.call();

            return builder;
        
        } catch (Throwable t) {

            if (outFile != null && outFile.exists()) {

                try {

                    outFile.delete();

                } catch (Throwable t2) {

                    log.warn(t2.getLocalizedMessage(), t2);

                }

            }

            if (t instanceof Exception)
                throw (Exception) t;

            throw new RuntimeException(t);

        }

    }

    /**
     * Mock implementation assigns index partitions from a counter beginning
     * with ZERO (0), which is the first legal index partition identifier. The
     * name parameter is ignored.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class MockPartitionIdFactory implements IPartitionIdFactory {

        private int i = 0;

        public int nextPartitionId(String nameIsIgnored) {

            return i++;

        }

    }

}
