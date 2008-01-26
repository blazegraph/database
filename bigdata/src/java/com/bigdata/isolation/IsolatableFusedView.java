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
/*
 * Created on Feb 12, 2007
 */

package com.bigdata.isolation;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IFusedView;
import com.bigdata.btree.ReadOnlyFusedView;

/**
 * An read-only {@link IFusedView} that supports transactions and deletion
 * markers.
 * <p>
 * Processing deletion markers requires that the source(s) for an index
 * partition view are read in order from the most recent to the earliest
 * historical resource. The first entry for the key in any source is the value
 * that will be reported on a read. If the entry is deleted, then the read will
 * report that no entry exists for that key.
 * <p>
 * Note that deletion markers can exist in both historical journals and index
 * segments having data for the view. Deletion markers are expunged from index
 * segments only by a full compacting merge of all index segments having life
 * data for the partition.
 * 
 * FIXME implement; support processing of delete markers (including handling of
 * the merge rule) - basically they have to be processed on read so that a
 * delete on the mutable btree overrides an historical value, and a deletion
 * marker in a more recent index segment overrides a deletion marker in an
 * earlier index segment. Deletion markers can exist in the both mutable btree
 * and in index segments that are not either a clean first eviction or a full
 * compacting merge (e.g., they can still exist in a compacting merge if there
 * are other index segments or btrees that are part of a partition but are not
 * partitipating in the compacting merge).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IsolatableFusedView extends ReadOnlyFusedView implements IIsolatableIndex {

    /**
     * @param src1
     * @param src2
     */
    public IsolatableFusedView(AbstractBTree src1, AbstractBTree src2) {
        super(src1, src2);
    }

    /**
     * @param srcs
     */
    public IsolatableFusedView(AbstractBTree[] srcs) {
        super(srcs);
    }

    public boolean contains(byte[] key) {

        throw new UnsupportedOperationException();

    }

    public Object lookup(Object key) {
        
        throw new UnsupportedOperationException();

    }

    public long rangeCount(byte[] fromKey, byte[] toKey) {

        throw new UnsupportedOperationException();
        
    }

    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey) {
        
        throw new UnsupportedOperationException();
        
    }

}
