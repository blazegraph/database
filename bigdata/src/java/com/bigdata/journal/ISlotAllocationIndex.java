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
 * Created on Oct 18, 2006
 */

package com.bigdata.journal;

import java.util.BitSet;

/**
 * <p>
 * Interface for managing the free and allocated slots in a {@link Journal}.
 * Logically the slot allocation index is random access bit set with one bit for
 * each slot in the journal. The primary operations are getting or setting the
 * bit corresponding to a single slot and scanning for the next clear bit.
 * </p>
 * <p>
 * The journal is logically an append only data structure. Each data version
 * written on the journal fills the next N slots (depending on the length of the
 * data version) whose bits in the allocation index are clear. When data
 * versions are no longer readable by any active transaction, the slots on which
 * those versions were written are cleared. The {@link IObjectIndex} stores the
 * first slot on which a given data version was written. The per-slot header
 * stores a double-linked list of the prior and next slots. When the journal
 * overflows, it wraps around back to its head and continues to search for free
 * slots.
 * </p>
 * <p>
 * Slot allocation index operations tend to have good locality. Efficient
 * forward scans for the next clear slot must be supported. Scans must wrap
 * around and must not search further than necessary. Either the journal itself
 * or the slot allocation index MUST track the index of the
 * <em>next free slot</em> in the journal so that slots may be assigned
 * efficiently as data versions are written on the journal. In addition, the
 * slot allocation index should track the index of the <em>first free slot</em>
 * and the <em>last free slot</em> in order to bound scans for the next free
 * slot when a new slot must be allocated.
 * </p>
 * 
 * @todo decide on requirements for incremental or all at once storage.
 *       incremental storage would break down the slot allocation index into
 *       chunks and write only those chunks that are dirty (as part of the
 *       commit protocol). incremental storage therefore needs a mechanism to
 *       decide which chunks and in what order form the total slot allocation
 *       index. This could be done with slot header that chain together smaller
 *       bit sets stored as slot data (a persistence capable linked list) or as
 *       some sort of a tree or hash map data structure. The slot allocation
 *       index should be wired into a memory on restart or simply cached as we
 *       go, but should also write through to disk. If we store each bit set
 *       chunk as a single slot, then it seems likely that we can avoid any
 *       recursion problems.
 * 
 * @todo decide on whether we need to store one version of the slot allocation
 *       index; one per tx; tx isolated changes that get merged; or two versions -
 *       one for each of the root blocks. <br>
 *       The global context MUST be consistent with the allocations made across
 *       all active and committed transactions. This can be achieved either by
 *       marking the change in a global bit set and then backing it out on abort
 *       (undo log); by testing the global index and then all per-tx indices if
 *       there is a miss in the global index (this seems very inefficient given
 *       the need for both fast bit test and fast scan for the next free bit and
 *       the per-tx changes would have to be merged down onto the global index
 *       at commit);<br>
 *       The "undo log" handles isolation by keeping a record of the current set
 *       of slots that were allocated to a transaction. We already know the
 *       first slot in each current version from the object index. We could scan
 *       the data versions from each first slot to find the current set of slots
 *       allocated to the transaction, but this could be very slow in the disk
 *       only mode. The alternative is to record the current set of slots that
 *       were allocated within the transaction scope. (De-)allocations write
 *       through to the global index, including the special case when a data
 *       version overwrites a prior data version written earlier by the same
 *       transaction (and hence the prior version is no longer visible to any
 *       transaction and its slots are immediately deallocated). On any commit,
 *       dirty chunks of the global slot index are flushed (this implies that we
 *       can defer writes for the special case of the slots that code a chunk in
 *       the slot index) and the per-transaction slot index information is
 *       simply discarded. On abort, the slots allocated to the transaction are
 *       backed out of the global slot index, causing those slots to be marked
 *       as free. While both commit and abort are cheap, on restart there can be
 *       slots that appear to be allocated (since dirty chunks of the slot index
 *       map are always flushed) but which in fact do not contain any committed
 *       data - those slots would have to be discovered and released.<br>
 *       The above appears to be an efficient design except with regard to
 *       restart. Since restart is a relatively common event, even if it is
 *       restart from a clean shutdown, we must also optimize restart. On
 *       approach is to use two bits per slot. One bit codes whether the slot is
 *       allocated. The 2nd bit codes whether the allocation belongs to a
 *       committed transaction. On restart, we would then scan the slot index
 *       and deallocate any slots that were not marked as belonging to a
 *       committed transaction. This process should be extremely fast.<br>
 *       There is going to be an interesting problem bootstrapping the slot
 *       index. With a new store, we can simply allocate the first N slots to
 *       the index, where N is computed based on the initial extent and the #of
 *       slots that can be indexed by the data in a single slot (dataSize/4
 *       since we need two bits per slot). The allocations that comprise the
 *       slot index itself will in all likelyhood fit onto the first slot. Those
 *       slots can be marked as allocated and we can then execute a native
 *       commit, at which point the will be marked as allocated + committed and
 *       the dirty slot index chunks will be flushed to the disk. The store is
 *       now ready for use.<br>
 *       On restart, the head of the slot index chain is read from the
 *       appropriate root block. The chain is then read into an indexed
 *       transient data structure in order to (a) ensure that the slot
 *       allocation index is cached; and (b) provide fast random access to the
 *       chunks in the slot allocation index.<br>
 * 
 * @todo handle extension, which requires bootstrapping additional chunks for
 *       the slot index. Extension should be easy since we again have slots in
 *       the new extent that we know a-priori to be free. We stitch those slots
 *       into the chain, mark the slots allocated to the chain as allocated and
 *       do a native commit.<br>
 *       If performing a native commit during extension is undesirable, e.g.,
 *       because it would break serialization of transactions, then on restart
 *       we simply need to check the logical length of the slot index against
 *       the extent. If the logical length is less than the extent, then we need
 *       to bootstrap the chain for the new extent. This situation can only
 *       arise when the journal crashes before the first commit on the new
 *       extent.
 * 
 * @todo handle compaction in preparation for truncation. There are probably
 *       choices here. One choice is to ensure that we can release N slots
 *       starting at the head of the journal, where N is the length of the slot
 *       allocation chain. The chain is then bootstapped in its new location and
 *       the old chain is released. By handling the slot allocation chain first
 *       we can guarentee that other compaction operations will not effect any
 *       slot that is part of that chain. This is important since the chain is
 *       NOT recorded in the object index - it can only be discovered from the
 *       current root block.<br>
 *       Compaction probably requires a commit in order to write the new root
 *       block. Likewise, we can NOT truncate the journal until after we have
 *       performed a successful commit. Thus, once the journal is prepared for
 *       truncation, it needs to wait until after the next commit before
 *       actually chopping off the tail.<br>
 *       There is, of course, an edge case when the journal is empty. E.g., all
 *       data has been successfully migrated to the read-optimized database and
 *       no active transaction can read any historical versions on the journal.
 *       In this case, we can simple drop the journal.
 * 
 * @todo add methods to this API and create a SimpleSlotAllocationIndex class
 *       that implements those methods using a {@link BitSet}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ISlotAllocationIndex {

    /**
     * Release slots on the journal beginning at the specified slot index by
     * logical deletion of historical data versions no longer accessable to any
     * active transaction. If necessary, triggers the extension of the journal.
     * 
     * @param fromSlot
     *            The index of the slot where the release operation will begin.
     * 
     * @param minSlots
     *            The minimum #of slots that must be made available by this
     *            operation.
     * 
     * @return The index of the next free slot.
     */
    public int release(int fromSlot, int minSlots );    

    /**
     * Ensure that at least this many slots are available for writing data in
     * this transaction. If necessary, triggers the logical deletion of
     * historical data versions no longer accessable to any active transaction.
     * If necessary, triggers the extension of the journal.
     * 
     * @param tx
     *            The transaction. Note that slots written in the same
     *            transaction whose data have since been re-written or deleted
     *            are available for overwrite in that transaction (but not in
     *            any other transaction).
     * 
     * @param nslots
     *            The #of slots required.
     * 
     * @return The first free slot. The slot is NOT mark as allocated by this
     *         call.
     * 
     * @see #releaseNextSlot(long)
     */
    public int releaseSlots(int nslots);

    /**
     * Marks the current slot as "in use" and return the next slot free in the
     * journal.
     * 
     * @param tx
     *            The transaction.
     * 
     * @return The next slot.
     * 
     * @exception IllegalStateException
     *                if there are no free slots. Note that
     *                {@link #releaseSlots(int)} MUST be invoked as a
     *                pre-condition to guarentee that the necessary free slots
     *                have been released on the journal.
     */
    public int releaseNextSlot();
 
    /**
     * Mark the slot as allocated but not committed.
     * 
     * @param slot
     *            The slot.
     * 
     * @exception IllegalStateException
     *                if the slot is already marked as allocated.
     * 
     * @see #setCommitted(int)
     */
    public void setAllocated(int slot);
    
    /**
     * True iff the slot is currently allocated.
     * 
     * @param slot
     *            The slot.
     * 
     * @see #isCommitted()
     */
    public boolean isAllocated(int slot);
    
    /**
     * Mark the slot as committed.
     * 
     * @param slot
     *            The slot.
     * 
     * @exception IllegalStateException
     *                if the slot is not already marked as allocated.
     */
    public void setCommitted(int slot);
    
    /**
     * True iff the slot is allocated and the allocation has been committed.
     * 
     * @see #isAllocated()
     */
    public boolean isCommitted(int slot);

    /**
     * Clears the slot (it will be marked as de-allocated and its commit flag
     * will also be cleared).
     * 
     * @param slot
     *            The slot.
     */
    public void clear(int slot);

    /**
     * The #of allocated slots (does NOT guarentee that all allocated slots
     * are also committed).
     */
    public int getAllocatedSlotCount();

}
