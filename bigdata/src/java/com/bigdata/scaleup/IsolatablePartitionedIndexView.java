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

package com.bigdata.scaleup;

import com.bigdata.btree.IBatchBTree;
import com.bigdata.btree.ReadOnlyFusedView;
import com.bigdata.isolation.IIsolatableIndex;
import com.bigdata.isolation.IsolatableFusedView;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.mdi.MetadataIndex;

/**
 * A {@link PartitionedIndexView} that supports transactions and deletion
 * markers. Write operations are passed through to the base class, which in turn
 * delegates them to the {@link UnisolatedBTree} identified to the constructor.
 * Read operations understand deletion markers. Processing deletion markers
 * requires that the source(s) for an index partition view are read in order
 * from the most recent (the mutable btree that is absorbing writes for the
 * index partition) to the earliest historical resource. The first entry for the
 * key in any source is the value that will be reported on a read. If the entry
 * is deleted, then the read will report that no entry exists for that key.
 * <p>
 * Note that deletion markers can exist in both the mutable btree absorbing
 * writes and in historical journals and index segments having data for the
 * partition view. Deletion markers are expunged from index segments only by a
 * full compacting merge of all index segments having life data for the
 * partition.
 * <p>
 * Implementation note: both the write operations and the {@link IBatchBTree}
 * operations are inherited from the base class. Only non-batch read operations
 * are overriden by this class.
 * 
 * FIXME implement; support processing of delete markers - basically they have
 * to be processed on read so that a delete on the mutable btree overrides an
 * historical value, and a deletion marker in a more recent index segment
 * overrides a deletion marker in an earlier index segment. Deletion markers can
 * exist in the both mutable btree and in index segments that are not either a
 * clean first eviction or a full compacting merge (e.g., they can still exist
 * in a compacting merge if there are other index segments or btrees that are
 * part of a partition but are not partitipating in the compacting merge).
 * 
 * @see IsolatableFusedView
 * @see ReadOnlyFusedView
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IsolatablePartitionedIndexView extends PartitionedIndexView implements IIsolatableIndex {

    /**
     * @param btree
     *            The btree that will absorb writes for the index partitions.
     * @param mdi
     *            The metadata index.
     */
    public IsolatablePartitionedIndexView(UnisolatedBTree btree, MetadataIndex mdi) {
        
        super(btree, mdi);

    }

}
