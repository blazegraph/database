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
package com.bigdata.isolation;

import com.bigdata.objndx.AbstractBTree;

/**
 * Interface for values in an {@link AbstractBTree} supporting deletion markers
 * and version counters.
 * <p>
 * Deletion markers are required both by transactions (so that the entry may be
 * removed from the unisolated tree when the transaction commits) and by
 * partitioned indices (so that a key deleted in an unisolated index may be
 * removed during a compacting merge with existing index segments).
 * <p>
 * Version counters are required in order to support transactional isolation of
 * an index. The version counter is incremented on each write on the unisolated
 * tree. When a transaction isolates an index it does so by creating a fused
 * view that reads from a historical committed state of the corresponding
 * unisolated index and writes on an isolated index. The first time a value is
 * written on the isolated index for which there is a pre-existing value in the
 * unisolated index, the version counter from the unisolated index is copied
 * into the isolated index. When the transaction commits, it validates writes by
 * testing the version counter in the <em>then current</em> unisolated index.
 * If the version counter for an entry in the isolated index does not agree with
 * the version counter on the unisolated index then an intervening commit has
 * already overwritten that entry and a write-write conflict exists. Either the
 * write-write conflict can be resolved or the transaction must abort. In the
 * special case where there are no intervening commits since the transaction
 * began validation is unecessary and should be skipped.
 * <p>
 * A delete is handled as a write that sets a "deleted" flag. Eventually the
 * keys for deleted entries are removed from the index. When a partitioned index
 * is used, delete markers are retained until the next compacting merge. When
 * partitioned indices are not used, the key is simply removed from the
 * unisolated index when transaction merges its writes down onto the unisolated
 * index. In either case, note that the next write on an unisolated index after
 * the delete marker has been processed (and the corresponding key has been
 * removed from the index) will once again assign a version counter of ONE (1).
 * The reuse of version counters will not cause a problem since transactions are
 * always isolated with respect to a specific historical committed state (the
 * most recent committed state before the transaction starts).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IValue {

    /**
     * The value of the version counter that is used the first time a data
     * version is written for a key in an unisolated index: ONE (1).
     * <p>
     * Note: The first version counter written on the unisolated index (1) is
     * larger than the {@link #FIRST_VERSION_ISOLATED first version counter}
     * written on an isolated index (0). The reason is that this approach
     * essentially presumes a version counter of zero (0) if a key does not
     * exist in the unisolated index, which is then copied into the isolated
     * index as a zero(0) version counter.
     * <p>
     * If the transaction validates and the isolated write is copied onto the
     * unisolated index then the version counter is handled in one of the
     * following ways.
     * <ol>
     * <li> If there is still no key for the entry, then the
     * {@link #FIRST_VERSION_UNISOLATED} version counter (1) is used and we have
     * effectively incremented from a zero (0) version counter for a
     * non-existing key to an one (1) version counter on the first write.</li>
     * <li> If another transaction has committed, then the version counter in
     * the global scope will now be GTE one (1). This will cause a write-write
     * conflict to be detected during validation. If the write-write conflict is
     * resolved, then the copy down onto the global scope will assign the next
     * version counter, e.g., the new version counter will be two (2) (if there
     * was only one intervening commit) or greater (if there was more than one
     * intervening commit).</li>
     * </ol>
     */
    public static short FIRST_VERSION_UNISOLATED = (short) 1;

    /**
     * The value of the version counter that is used the first time a data
     * version is written in an insolated index for which there was no
     * pre-existing version in the unisolated index: ZERO(0).
     */
    public static short FIRST_VERSION_ISOLATED = (short) 0;

    /**
     * The value of the version counter that is used when the version counter
     * overflows: TWO(2).
     */
    public static short ROLLOVER_VERSION_COUNTER = (short) 2;

    /**
     * A counter that is updated to detect write-write conflicts.
     * <p>
     * The counter is always incremented when a new version is written in an
     * unisolated index (the first write on the unisolated index outside of any
     * transaction must set the counter to ONE (1) in order to correctly detect
     * a conflict with a concurrent transaction). When an index is isolated by a
     * transaction, the version counter is copied into the isolated index the
     * first time a value for a given key is written on the isolated index (the
     * counter is set to ZERO(0) if the key was not found in the unisolated
     * index, which will turn into a counter of ONE (1) iff the transaction
     * successfully validates and commits).
     * <p>
     * When the transaction validates, a write-write conflict exists iff the
     * value of the counter in the then current unisolated index is not equal to
     * the value in the within the insolated index. This always indicates that a
     * newer version has been committed. If the conflict can not be validated,
     * e.g., by a merge rule, then validation will fail and the transaction will
     * abort. The counter is always incremented when an entry in an isolated
     * index is merged down onto the then current unisolated index.
     * <p>
     * Note that version counters do rollover. A version counter is only a short
     * integer and one bit is reserved to flag a deleted key. When the counter
     * overflows it is reset to {@link #ROLLOVER_VERSION_COUNTER} (2) since the
     * value ZERO (0) is reserved to indicate a key that has never been written
     * on the unisolated index and the value ONE (1) is only used the first time
     * a version is written. However, since the rule for detecting a write-write
     * conflict is that the version counters do not agree it would require
     * exactly 32k-2 ( 32,766 ) <em>concurrent</em> overwrites of a value to
     * create a situation in which a write-write conflict would be missed. It is
     * not envisioned that any transaction will live long enough for there to be
     * at least this many intervening concurrent transactions. However, to be
     * absolutely safe, a transaction should be aborted if at least this many
     * intervening transactions have successfully committed. This seems a
     * reasonable limit on long-running transactions.
     * 
     * @return The version counter.
     */
    public short getVersionCounter();

    /**
     * True iff the entry has been deleted.
     * 
     * @return True iff the entry has been deleted.
     */
    public boolean isDeleted();

//    /**
//     * True if there is no earlier version of the value (eg, if the version
//     * counter is ZERO(no earlier version in an isolated index) or ONE( no
//     * earlier version in an unisolated index)). When the value is in the
//     * unisolated index, this is true if the first committed write for the
//     * corresponding key. When the value is in an isolated index, this means
//     * that there was no key for this value on the unisolated index when the
//     * transaction began.
//     * 
//     * @deprecated This is not always true since rollover of the version counter
//     *             can cause a version counter of ONE to not mean that this is
//     *             the first version. That could be dealt with by rolling over
//     *             the version counter to TWO(2) rather than ONE(1), but I am
//     *             not sure that we need this bit of information in any case.
//     *             Rather, it seems that version counters should be processed
//     *             strictly in terms of whether or not they agree (and indicate
//     *             a write-write conflict when they do not agree).
//     */
//    public boolean isFirstVersion();

    /**
     * The value assigned to the key. This is always <code>null</code> for a
     * deleted value but MAY be <code>null</code> for a non-deleted value as
     * well (keys are permitted that are not associated with a value in the
     * index).
     * 
     * @return The value assigned to the key.
     */
    public byte[] getValue();

}
