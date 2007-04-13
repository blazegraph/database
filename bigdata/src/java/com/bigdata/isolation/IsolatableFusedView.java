/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Feb 12, 2007
 */

package com.bigdata.isolation;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.IBatchBTree;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.ReadOnlyFusedView;
import com.bigdata.scaleup.PartitionedIndexView;

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
 * <p>
 * Implementation note: the {@link IBatchBTree} operations are inherited from
 * the base class. Only non-batch read operations are overriden by this class.
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

    public int rangeCount(byte[] fromKey, byte[] toKey) {

        throw new UnsupportedOperationException();
        
    }

    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey) {
        
        throw new UnsupportedOperationException();
        
    }

}
