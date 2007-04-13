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
import com.bigdata.btree.BTree;
import com.bigdata.btree.IBatchBTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentMerger;
import com.bigdata.btree.Leaf;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Tx;
import com.bigdata.journal.Name2Addr.ValueSerializer;
import com.bigdata.scaleup.IsolatablePartitionedIndexView;

/**
 * <p>
 * This is a marker interface for an index that can be isolated by a
 * transaction. Implementations of this interface understand and maintain both
 * version counters and deletion markers and constrain application data to
 * variable length byte[]s. Both unisolated and isolated indicies MUST use this
 * interface in order to support transactions since version counters MUST be
 * maintained in the unisolated indices as well as the isolated indices.
 * </p>
 * <p>
 * The basic design for isolation requires that reads are performed against a
 * historical committed state of the store (the ground state, which is typically
 * the last committed state of the store at the time that the transaction
 * begins) while writes are isolated (they are not visible outside of the
 * transaction). The basic mechanism for isolation is a isolated btree that
 * reads through to a read-only btree loaded from a historical metadata record
 * while writes go into the isolated btree. The isolated btree is used by the
 * transaction and never by another transaction.
 * </p>
 * <p>
 * In order to commit, the transaction must validate the write set on the
 * isolated btree against the then current committed state of the btree. If
 * there have been no intervening commits then validation is a NOP since the
 * read-only btree that the isolated btree reads through to is the current
 * committed state. If there have been intervening commits, then validation may
 * identify write-write conflicts (read-write conflicts are obviated by the
 * basic design). A write-write conflict exists when a concurrent transaction
 * wrote a record for the same key as the transaction that is being validated
 * and has already committed (conflicts are not visible until a writer has
 * committed). Write-write conflicts may be resolved by data type specific merge
 * rules. Examples include debits and credits on a bank account or conflicts on
 * record metadata but the basic state of the record. If a conflict can not be
 * validated then the transaction is aborted and may be retried.
 * </p>
 * <p>
 * Once a transaction has validated it is merged down onto the globally visible
 * state of the btree. This process consists simply of applying the changes to
 * the globally visible btree, including both inserts of key-value pairs and
 * removal of keys that were deleted during the transaction.
 * </p>
 * <p>
 * If a transaction is reading from or writing on more than one btree, then it
 * must validate the write set for each btree during its validation stage and
 * merge down the write set for each btree during its merge stage. Once this
 * merge process is complete, the btree is flushed to the backing store which
 * results in a new metadata record. The mapping from btree identifier to btree
 * metadata record is then updated on the backing store. Finally, an atomic
 * commit is then performed on the backing store. At this point the transaction
 * has successfully completed.
 * </p>
 * <p>
 * The use of this interface is NOT required if neither transactions nor
 * partitioned indicies will be used for an index. For example, a temporary
 * index used by some transactions and then discarded rather than participating
 * in a commit need not use this index (e.g., temporary tables or the answer set
 * for some fixed point operation or query).
 * </p>
 * <p>
 * Note: Since each batch operation is atomic within an index partition, the
 * {@link IBatchBTree batch api} provides an alternative to transactional
 * isolatation that is suitable for some applications.
 * </p>
 * <p>
 * Note: Another alternative to transactional isolation is to append a timestamp
 * or version identifier to the application keys and then use a version
 * expiration policy based on either age or the maximum #of versions to be
 * retained. This approach can be combined with the use of the batch api
 * mentioned above.
 * </p>
 * 
 * @todo each value paired with a key must have a versionCounter and deletion
 *       flag in addition to the value itself. I think that it is best to change
 *       the value to a byte[] since this will place the serialization burden on
 *       the clients, where it belongs, and will further prepare us for a
 *       network api.
 * 
 * @todo The read-only view must be against the partitioned index, not just the
 *       btree on the journal. the UnisolatedBTree is different in that it reads
 *       against a view and writes on the UnisolatedBTree. In fact, this is
 *       precisely how the PartitionedIndexView works already. All that we have to
 *       do is to construct the view from the historical committed state from
 *       which the transaction emerges. In order to do this, we need to be able
 *       to recover prior historical states of btrees which we could do by
 *       chaining their metadata records or by maintaining the timestamp in the
 *       key in the Name2BTree mapping (much like a column store). We also must
 *       keep old journals and index segments around even after their data has
 *       been evicted onto index segments until there are no longer any active
 *       transactions which could read from a ground state found on a given
 *       journal + index segments.
 * 
 * @todo handle journal overflow smoothly with respect to transactional
 *       isolation, including how to handle both short and long-lived readers
 *       and writers. draw some boundaries on what is supported and what is not.
 *       a long-lived transaction can evict data onto the journal and have it
 *       overflow onto index segments. that presents a scalable approach, but we
 *       must then delete the index segments for the isolated transaction when
 *       it commmits. this requires a persistent index to keep track of the
 *       transactional indices. the simplest thing to do is register an index
 *       specifically for this purpose. the minimum information is to enter a tx
 *       into the index when a journal overflows and the tx has data evicted
 *       into an index segment. When the tx completes, its indexs segments are
 *       deleted and its entry in the transactions index is removed. On restart
 *       any index segments for transactions are deleted since any transaction
 *       which was running must have failed.
 * 
 * @todo is double-deletion of a deleted key on an isolated btree is an error or
 *       should it be silently ignored?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME review all implementations of this interface, plus the
 * {@link IndexSegmentMerger} and {@link IndexSegment} for correct detection and
 * handling of deletion markers. Review applications for correct use of
 * {@link BTree} vs {@link UnisolatedBTree}.
 * 
 * @todo support transactions {@link Tx} on the {@link Journal} and bring back
 *       the test suite for transactional isolation or just write a new one.
 * 
 * @todo update the basic merge rule in {@link IndexSegmentMerger} so that it
 *       will process delete markers.
 * 
 * @todo update {@link Leaf} to understand delete markers and define a btree
 *       mode in which delete markers are used and then write tests of that mode
 *       since the behavior differs significantly, e.g., when a key is deleted
 *       the #of entries in the tree does not change and the #of spanned keys
 *       does not change, and we do not actually remove the entry from the leaf.
 *       Note: the _use_ of the btree needs to change when supporting
 *       transactions since all application values MUST be wrapped up as Value
 *       objects. This means that methods such as
 *       {@link AbstractBTree#contains(byte[])} must be overriden so as to
 *       correctly process delete markers (this applies to the batch api as
 *       well). The place to do this is probably where the btrees are created
 *       using an IsolatedBtree class (extends BTree or perhaps just implements
 *       {@link IIndex} so that we can use it in combination with the ReadOnlyFusedView
 *       to support partitioned indices). ReadOnlyFusedView will also need to be
 *       modified to support delete markers and could thrown an exception if the
 *       version counters were out of the expected order (so could the leaf
 *       merge iterator). The UnisolatedBTree would always use the
 *       {@link ValueSerializer} and applications would become responsible for
 *       serializing values before insert (we could serialize them on the index
 *       side but then we have much more network IO).
 *       <p>
 *       The only time to de-serialize the application values is when there is a
 *       write-write conflict and a conflict resolver registered on the btree to
 *       handle that conflict).
 * 
 * @see UnisolatedBTree
 * @see IsolatableFusedView
 * @see IsolatablePartitionedIndexView
 * @see IndexSegmentMerger
 * @see Journal
 * @see Tx
 */
public interface IIsolatableIndex extends IIndex {

}
