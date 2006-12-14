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
 * Created on Oct 16, 2006
 */

package com.bigdata.journal;

import java.util.Iterator;
import java.util.TreeMap;

import com.bigdata.cache.HardReferenceQueue;
import com.bigdata.objndx.ArrayType;
import com.bigdata.objndx.BTree;
import com.bigdata.objndx.DefaultEvictionListener;
import com.bigdata.objndx.IndexEntrySerializer;
import com.bigdata.objndx.Int32OIdKeySerializer;
import com.bigdata.objndx.KeyValueIterator;
import com.bigdata.objndx.PO;

/**
 * A persistent object index supporting full transactional isolation.
 * 
 * FIXME This needs to implement the object index semantics.
 * 
 * @todo convert from int32 to int64 so that we can multiplex object index
 *       segments on the same journal or create one index per segment on the
 *       journal?
 * 
 * @todo eventually refactor {@link IObjectIndexEntry} so that it does not
 *       require us to store objects at random locations in the journal but
 *       rather allows us to store them inline within the leaves of the object
 *       index. This change might break a lot of the existing tests.
 * 
 * @todo We could just hide the btree as a field rather than extending it. The
 *       easy way to get the object index semantics is using a one for one
 *       substitution of the btree for a {@link TreeMap} in
 *       {@link SimpleObjectIndex}. We also need to write and apply a test
 *       suite and test the journal using the persistence capable object index
 *       and not just the {@link SimpleObjectIndex}.
 * 
 * @todo Review the logic that isolations mutations on the object index. For
 *       example, a delete of an object does not remove an entry from the base
 *       object index but rather records that the object is marked as deleted in
 *       that entry. When the isolated object index is merged down onto the
 *       global object index entries for objects that are marked as deleted are
 *       then deleted on the global index. When the transaction commits, the
 *       state change in the global index is atomically committed.
 * 
 * @todo Verify that we cause the storage allocated to the object index to be
 *       released when it is not longer accessible, e.g., the transaction
 *       commits or objects have been deleted and are no longer visible so we
 *       now remove their entries from the object index -- this can cause leaves
 *       and nodes in the object index to become empty, at which point they are
 *       deleted.
 * 
 * @todo Consider whether a call back "IValueSetter" would make it possible to
 *       update the value on the leaf in a more sophisticated manner. E.g.,
 *       rather than having to lookup to see if a value exists and then insert
 *       the value, we could pass in a lamba expression that gets evaluated in
 *       context and is able to distinguish between a value that did not exist
 *       and one that does exist. Ideally we could offer three options at that
 *       point : test, set, and delete, but just test/set is fine for most
 *       purposes since remove(key) has the same effect.
 * 
 * @todo The btree support provides copy-on-write, which means that we do not
 *       need to close the base object index when starting a new transaction.
 * 
 * @todo Make the objects inline within the index (clustered object index) vs
 *       managing allocated slots on the {@link IRawStore}. This would result
 *       in a change to the {@link IObjectIndexEntry} API.
 * 
 * @todo Any kind of btree can be used with transactional isolation and multiple
 *       btrees can be used at the same time with transactional isolation
 * 
 * @todo exploring opportunities for further efficiencies by moving some of the
 *       update logic into a callback or behavior extension for the btree
 *       insert/update method.
 * 
 * FIXME Write lots of tests for this interface as well as for transactional
 * isolation at the {@link Journal} and {@link Tx} API level. We will reuse
 * those for the persistence capable object index implementation.
 * 
 * @todo Write a persistence capable version that is efficient for all the
 *       things that we actually use the object index for. We need benchmarks
 *       that drive all of those activities (including migration to the
 *       read-optimized database and deletion of versions that are no longer
 *       accessible) in order to drive the algorithm, implementation, and
 *       performance tuning.<br>
 *       Some of the complex points are handling transaction isolation
 *       efficiently with minimal duplication of data and high performance and,
 *       in interaction with the slot allocation index, providing fast
 *       deallocation of slots no longer used by any active transaction - the
 *       current scheme essentially forces visitation of the slots to be
 *       deleted.
 * 
 * @todo A larger branching factor in the object index will result in fewer
 *       accesses to resolve a given identifier. This does not matter much when
 *       the journal is fully buffered, but it is critical when a journal uses a
 *       page buffer (unless the base object index and the per transaction
 *       object indices can be wired into memory).<br>
 *       A large branching factor is naturally in opposition to a smaller slot
 *       size. If larger nodes are used with smaller slot sizes then each index
 *       node will occupy multiple slots. It is possible to force allocation of
 *       those slots such that they are contiguous - this approach has the
 *       advantage that the journal remains an append-only store, but introduces
 *       complexity in allocation of index nodes. My expectation is that most
 *       allocations will be much smaller than index node allocations, but that
 *       many allocations will be made together since writes are buffered on a
 *       per tx basis before being applied to the journal. If it is also true
 *       that we tend to release the allocations for entire transactions at
 *       once, then this reduces the likelyhood that we will exhaust an extent
 *       for index node allocations through fragmentation. <br>
 *       Another approach would partition the journal (either within one file or
 *       into two files) so that part was reserved for object index nodes and
 *       part was reserved for slots. This would, pragmatically, result in two
 *       memory spaces each having fixed length slots - one for the objects and
 *       the slot allocation index blocks and one for the object index nodes.
 *       However, partitioning also goes directly against the requirement for an
 *       append-only store and would result in, essentially, twice as many
 *       append only data structures - and hence twice the disk head movement as
 *       a design that does not rely on either within file partition or a two
 *       file scheme.<br>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ObjectIndex implements IObjectIndex {

    static class MyIndex extends BTree {
        
        /**
         * Create a new index.
         * 
         * @param store
         */
        public MyIndex(IRawStore store) {

            super(store,
                    ArrayType.INT,
                    DEFAULT_BRANCHING_FACTOR,
                    new HardReferenceQueue<PO>(new DefaultEvictionListener(),
                            DEFAULT_LEAF_QUEUE_CAPACITY,
                            DEFAULT_LEAF_QUEUE_SCAN),
                    Integer.valueOf(0),
                    null, // no comparator for primitive key type.
                    Int32OIdKeySerializer.INSTANCE,
                    new IndexEntrySerializer(store.getSlotMath()));

        }

        /**
         * Load an existing index.
         *  
         * @param store
         * @param metadataId
         */
        public MyIndex(IRawStore store, long metadataId) {
            
            super(store,metadataId,
                    new HardReferenceQueue<PO>(new DefaultEvictionListener(),
                            DEFAULT_LEAF_QUEUE_CAPACITY,
                            DEFAULT_LEAF_QUEUE_SCAN),
                    Integer.valueOf(0),
                    null, // no comparator for primitive key type.
                    Int32OIdKeySerializer.INSTANCE,
                    new IndexEntrySerializer(store.getSlotMath()));
            
        }

        /**
         * Makes {@link BTree#metadataId} visible.
         */
        public long getMetadataId() {
            
            return metadataId;
            
        }

    }
    
    /**
     * Interface for an entry (aka value) in the {@link IObjectIndex}. The
     * entry stores either the slots on which the data version for the
     * corresponding persistent identifier was written or notes that the
     * persistent identifier has been deleted. When there is an overwrite of a
     * pre-existing version (one that exists in the base object index scope),
     * then the slots allocated to that pre-existing version are copied into the
     * entry so that they may be efficiently processed later when the
     * transaction is garbage collected.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface IObjectIndexEntry /*extends Cloneable*/ {

        /**
         * A counter that is updated each time a new version is committed for
         * the persistent associated with this entry. This is used to detect
         * write-write conflicts. The counter is copied on write together with
         * the rest of the entry when pre-existing version is overwritten within
         * a transaction. When the transaction validates, a write-write conflict
         * exists iff the value of the counter in the global object index scope
         * is greater than the value in the within transaction object index
         * entry. This case always indicates that a newer version has been
         * committed. If the conflict can not be validated, then validation will
         * fail. Whether or not a conflict is detected, the counter is always
         * incremented when the modified entries in the transaction scope are
         * merged down onto the global object index.
         * 
         * @return The counter.
         * 
         * @todo What about when the overwrite is outside of a transaction
         *       context? Just increment the counter and get on with life?
         * 
         * FIXME If the counter is zero and preExistingVersionSlots is non-null
         * then this indicates a rollover of the counter, right? This case needs
         * to be handled and tests need to be written for this case.
         * 
         * @see SimpleObjectIndex#mapIdToSlots(int, ISlotAllocation,
         *      ISlotAllocationIndex)
         */
        public short getVersionCounter();
        
        /**
         * True iff the persistent identifier for this entry has been deleted.
         * 
         * @return True if the persistent identifier has been deleted.
         */
        public boolean isDeleted();
        
        /**
         * True iff a pre-existing version has been overwritten (or deleted). If
         * this transaction commits, then the slots allocated to that
         * pre-existing version will eventually be garbage collected.
         * 
         * @return True if a pre-existing version has been overwritten.
         */
        public boolean isPreExistingVersionOverwritten();
        
        /**
         * Non-null iff there is a current version for the persistent identifier
         * that indexes this entry and null iff the persistent version for the
         * entry has been deleted within the current transactional scope.
         * 
         * @see #isDeleted()
         */
        public ISlotAllocation getCurrentVersionSlots();
        
        /**
         * <p>
         * When non-null, the slots containing a pre-existing version that was
         * overwritten during a transaction. This is used to support garbage
         * collection of pre-existing versions overwritten within a transaction
         * once they are no longer accessible to any active transaction.
         * </p>
         * <p>
         * Note: If there is a pre-existing version that is overwritten multiple
         * times within a transactions, then this field is set the first time to
         * the slots for the pre-existing version and is thereafter immutable.
         * This is because versions created within a transaction may be
         * overwritten immediately while a restart safe record of overwritten
         * pre-existing versions must be retained until we can GC the
         * transaction in which the overwrite was performed.
         * </p>
         * <p>
         * Note: If no version was pre-existing when the transaction began, then
         * writes and overwrites within that transaction do NOT cause this field
         * to be set and the slots for the overwritten versions are
         * synchronously deallocated.
         * </p>
         * <p>
         * Note: For the purposes of garbage collection, we treat a delete as an
         * overwrite. Therefore, if the delete was for a pre-existing version,
         * then this field contains the slots for that pre-existing version. If
         * the delete was for a version created within this transaction, then
         * the slots for that version are synchronously deallocated and this
         * field will be <code>null</code>.
         * </p>
         */
        public ISlotAllocation getPreExistingVersionSlots();
        
    }

    /**
     * A non-persistence capable implementation of {@link IObjectIndexEntry}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class SimpleEntry implements IObjectIndexEntry {

        private short versionCounter;
        private ISlotAllocation currentVersionSlots;
        private ISlotAllocation preExistingVersionSlots;

        SimpleEntry() {

            // NOP
            
        }
        
        public short getVersionCounter() {
            
            return versionCounter;
            
        }
        
        public boolean isDeleted() {
            
            return currentVersionSlots == null;
            
        }
        
        public boolean isPreExistingVersionOverwritten() {
            
            return preExistingVersionSlots != null;
            
        }
        
        public ISlotAllocation getCurrentVersionSlots() {

            return currentVersionSlots;
            
        }
        
        public ISlotAllocation getPreExistingVersionSlots() {
            
            return preExistingVersionSlots;
            
        }
        
        /**
         * Dumps the state of the entry.
         */
        public String toString() {
            return "{versionCounter=" + versionCounter + ", currentVersion="
                    + currentVersionSlots + ", preExistingVersion="
                    + preExistingVersionSlots + "}";
        }

//        public IObjectIndexEntry clone() {
//            
//            SimpleEntry clone = new SimpleEntry();
//            
//            clone.currentVersionSlots = currentVersionSlots;
//            
//            clone.preExistingVersionSlots = preExistingVersionSlots;
//            
//            return clone;
//            
//        }
        
    }

    /**
     * Map from the int32 within segment persistent identifier to
     * {@link IObjectIndexEntry} for that identifier.
     * 
     * @see IObjectIndexEntry
     */
    final MyIndex objectIndex;

    /**
     * When non-null, this is the base (or inner) object index that represents
     * the committed object index state as of the time that a transaction began.
     */
    final MyIndex baseObjectIndex;

    /**
     * The slot allocation index.
     * 
     * @todo do we really need this?
     */
    final ISlotAllocationIndex allocationIndex;
    
    /**
     * Disallowed constructor.
     */
    private ObjectIndex() {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Constructor used for the base object index (outside of any transactional
     * scope).
     * 
     * @param store
     *            The backing store.
     * @param allocationIndex
     *            The slot allocation index.
     */
    public ObjectIndex(IRawStore store, ISlotAllocationIndex allocationIndex) {

        if( allocationIndex == null ) throw new IllegalArgumentException();
        
        this.objectIndex = new MyIndex(store);
        
        /*
         * FIXME This commit is required to write out a metadata record so that
         * we can re-load the initial (empty) btree. However, this fails to
         * perform a store level commit so it is not restart safe.
         */ 
        this.objectIndex.write();

        this.baseObjectIndex = null;
        
        this.allocationIndex = allocationIndex;

    }

//    /**
//     * Private constructor creates a read-only (unmodifiable) deep-copy of the
//     * supplied object index state. This is used to provide a strong guarentee
//     * that object index modifications can not propagate through to the inner
//     * layer using this API.
//     * 
//     * @param objectIndex
//     * 
//     * FIXME The requirement to deep-copy the base object index arises because
//     * the transaction needs a view that is consistent with the start time for
//     * that transaction. When using a persistence capable object index based on
//     * a btree, we will use copy-on-write semantics to prevent the view with
//     * which the tx starts from being modified either by unisolated writes or by
//     * concurrent transactions that commit.
//     */
//    private ObjectIndex(ISlotAllocationIndex allocationIndex, long metadataId ) {
//
//        if( allocationIndex == null ) throw new IllegalArgumentException();
//        
//        if( objectIndex == null ) throw new IllegalArgumentException();
//
//        this.objectIndex = new MyIndex( store, metadataId );
//
//        this.baseObjectIndex = null;
//        
//        this.allocationIndex = allocationIndex; 
//
//    }
    
    /**
     * Constructor used to isolate a transaction by a read-only read-through
     * view of some committed object index state.
     * 
     * @param baseObjectIndex
     *            Misses on the primary object index read through to this object
     *            index. Writes are ONLY performed on the primary object index.
     *            The base object index is always read-only.
     * 
     * @todo This makes an eager deep copy of the current object index. This
     *       provides isolation against changes to the object index on the
     *       journal. An efficient implementation MUST NOT make an eager deep
     *       copy. Instead, {@link #mergeWithGlobalObjectIndex(Journal)} MUST
     *       use copy on write semantics can cause lazy cloning of the global
     *       object index structure so that changes do not get forced into
     *       concurrent transactions but are only visible within new
     *       transactions that begin from the post-commit state of the isolated
     *       transaction.
     */
    public ObjectIndex(ObjectIndex baseObjectIndex) {

        assert baseObjectIndex != null;

        /*
         * FIXME There is concurrent modification under traversal during
         * state-based validation. However, there is never more than a single
         * thread accessing the index so the concurrency level is ONE (1).
         * Either we need to support concurrent modification for the btree or we
         * need to buffer the changes in a transient data structure.
         */
        
        final IRawStore store = baseObjectIndex.objectIndex.getStore();
        
        /*
         * Create a new writable index that will isolate writes.
         */
        this.objectIndex = new MyIndex(store);

        /*
         * A read-only view of the now current object index. This is loaded from
         * the store so that modifications made to the global object index state
         * are isolated from this transaction.
         * 
         * FIXME It would be very nice to be able to share immutable nodes and
         * leaves between the base index for this transaction (a committed
         * historical state for the global index) and the current global base
         * index.  However, doing this will require some sophistication.
         */
        this.baseObjectIndex = new MyIndex(store,
                baseObjectIndex.objectIndex.getMetadataId());

        this.allocationIndex = baseObjectIndex.allocationIndex;
        
    }

    /**
     * Read an entry from the object index. If there is a miss on the outer
     * index and an inner index is defined then try a read on the inner index.
     * 
     * @param id
     *            The int32 within segment persistent identifier.
     * 
     * @return The entry for that persistent identifier or null if no entry was
     *         found.
     * 
     * @see #hitOnOuterIndex
     */
    private IObjectIndexEntry getEntry(int id) {
        
        Integer t = Integer.valueOf(id);
        
        IObjectIndexEntry entry = (IObjectIndexEntry) objectIndex.lookup( t );
        
        if( entry == null ) {
            
            hitOnOuterIndex = false;

            if( baseObjectIndex != null ) {

                entry = (IObjectIndexEntry) baseObjectIndex.lookup( t );
            
            }

            // MAY be null.
            return entry;
            
        }   
        
        hitOnOuterIndex = true;
        
        return entry;
        
    }
    
    /**
     * This field is set by {@link #getEntry(int)} each time it is invoked. The field
     * will be true iff there was a hit on the object index and the hit occurred
     * on the outer layer - that is, the layer to which the request was
     * directed. When true, this flag may be interpreted as indicating that a
     * version already exists within the transaction scope IFF this object index
     * is providing transactional isolation (vs the journal's native level
     * object index). In this situation the slots allocated to the version that
     * is being overwritten MUST be immediately released on the journal since it
     * is impossible for any transaction to read the version stored on those
     * slots.
     */
    private boolean hitOnOuterIndex = false;
    
    public ISlotAllocation get( int id ) {

        IObjectIndexEntry entry = getEntry(id);
        
        if( entry == null ) return null;
        
        if( entry.isDeleted() ) {

            throw new DataDeletedException(id);

        }
    
        ISlotAllocation slots = entry.getCurrentVersionSlots(); 
        
        assert slots != null;
        
        return slots; 
        
    }
    
    public void put( int id, ISlotAllocation slots ) {
        
        if( slots == null ) throw new IllegalArgumentException();
        
        if( allocationIndex == null ) throw new IllegalArgumentException();

        // Integer variant since we use it several times.
        final Integer id2 = Integer.valueOf( id );
        
        /* 
         * Get the object index entry.  This can read through into the base
         * object index.
         * 
         * Note: [hitOnOuterIndex] is set as a side effect.
         */

        IObjectIndexEntry entry = getEntry(id2);

        if( entry == null ) {
            
            /*
             * This is the first write of a version for the persistent
             * identifier. We create a new entry and insert it into the outer
             * map.
             */
            
            SimpleEntry newEntry = new SimpleEntry();
            
            newEntry.versionCounter = 0;

            newEntry.currentVersionSlots = slots;
            
            newEntry.preExistingVersionSlots = null;
            
            objectIndex.insert( id2, newEntry );
            
            return;
            
        }
        
        if( entry.isDeleted() ) {

            /*
             * You can not write on a persistent identifier that has been
             * deleted.
             */
            
            throw new DataDeletedException(id);
            
        }
        
        if( hitOnOuterIndex ) {

            /*
             * If we hit on the outer index, then we can immediately deallocate
             * the slots for the prior version since they MUST have been
             * allocated within this transaction.
             */

            // deallocate slots for the prior version.
            allocationIndex.clear(entry.getCurrentVersionSlots());
            
            // save the slots allocated for the new version.
            ((SimpleEntry)entry).currentVersionSlots = slots;
            
            // @todo mark index node as dirty!
            
        } else {

            /*
             * If we hit on the inner index then we create a new entry (we NEVER
             * modify the inner index directly while a transaction is running).
             * The currentVersion is set to the provided slots. The
             * preExistingVersion is set to the slots found on the current
             * version in the _inner_ index. Those slots will get GC'd
             * eventually if this transaction commits.
             */
            
            SimpleEntry newEntry = new SimpleEntry();
            
            newEntry.versionCounter = entry.getVersionCounter();
            
            // save the slots allocated for the new version.
            newEntry.currentVersionSlots = slots;

            // copy the slots for the pre-existing version for later GC of tx.
            newEntry.preExistingVersionSlots = entry.getCurrentVersionSlots(); 
            
            // add new entry into the outer index.
            objectIndex.insert(id2, newEntry);
            
        }

    }

    public void delete(int id ) {

        /* 
         * Get the object index entry.  This can read through into the base
         * object index.
         */

        IObjectIndexEntry entry = getEntry(id);

        if( entry == null ) {
            
            throw new IllegalArgumentException("Not found: id="+id);
            
        }

        if( entry.isDeleted() ) {

            /*
             * It is an error to double-delete an object.
             */
            
            throw new DataDeletedException(id);
            
        }
        
        if( hitOnOuterIndex ) {

            /*
             * If we hit on the outer index, then we can immediately deallocate
             * the slots for the current versions since they MUST have been
             * allocated within this transaction.
             */

            // deallocate slots for the current version.
            allocationIndex.clear(entry.getCurrentVersionSlots());
            
            // mark the current version as deleted.
            ((SimpleEntry)entry).currentVersionSlots = null;
            
            // @todo mark index node as dirty!
            
        } else {

            /*
             * If we hit on the inner index then we create a new entry. The
             * currentVersion is set to null since the persistent identifier is
             * deleted. The preExistingVersion is set to the slots found on the
             * current version in the _inner_ index.  Those slots will get GC'd
             * eventually if this transaction commits.
             */
            
            SimpleEntry newEntry = new SimpleEntry();
            
            // copy the version counter.
            newEntry.versionCounter = entry.getVersionCounter();

            // mark version as deleted.
            newEntry.currentVersionSlots = null;
            
            // copy the slots for the pre-existing version for later GC of tx.
            newEntry.preExistingVersionSlots = entry.getCurrentVersionSlots(); 
            
            // add new entry into the outer index.
            objectIndex.insert(id, newEntry);
            
        }
        
    }

    /**
     * 
     * @todo For a persistence capable implementation of the object index we
     *       could clear currentVersionSlots during this operation since there
     *       should be no further access to that field. The only time that we
     *       will go re-visit the committed object index for the transaction is
     *       when we GC the pre-existing historical versions overwritten during
     *       that transaction. Given that, we do not even need to store the
     *       object index root for a committed transaction (unless we want to
     *       provide a feature for reading historical states, which is NOT part
     *       of the journal design). So another option is to just write a chain
     *       of {@link ISlotAllocation} objects. (Note, per the item below GC
     *       also needs to remove entries from the global object index so this
     *       optimization may not be practical). This could be a single long
     *       run-encoded slot allocation spit out onto a series of slots during
     *       PREPARE. When we GC the transaction, we just read the chain,
     *       deallocate the slots found on that chain, and then release the
     *       chain itself (it could have its own slots added to the end so that
     *       it is self-consuming). Just pay attention to ACID deallocation so
     *       that a partial operation does not have side-effects (at least, side
     *       effects that we do not want). This might require a 3-bit slot
     *       allocation index so that we can encode the conditional transition
     *       from (allocated + committed) to (deallocated + uncommitted) and
     *       know that on restart the state should be reset to (allocated +
     *       committed).
     * 
     * @todo GC should remove the 'deleted' entries from the global object index
     *       so that the index size does not grow without limit simply due to
     *       deleted versions. This makes it theoretically possible to reuse a
     *       persistent identifier once it has been deleted, is no longer
     *       visible to any active transaction, and has had the slots
     *       deallocated for its last valid version. However, in practice this
     *       would require that the logic minting new persistent identifiers
     *       received notice as old identifiers were expired and available for
     *       reuse. (Note that applications SHOULD use names to recover root
     *       objects from the store rather than their persistent identifiers.)
     * 
     * FIXME Validation of the object index MUST specifically treat the case
     * when no version for a persistent identifier exists in the ground state
     * for a tx, another tx begins and commits having written a version for that
     * identifier, and then this tx attempts to commit having written (or
     * written and deleted) a version for that identifier. Failure to treat this
     * case will cause problems during the merge since there will be an entry in
     * the global scope that was NOT visible to this transaction (which executed
     * against a distinct historical global scope). My take is the persistent
     * identifier assignment does not tend to have semantics (they are not
     * primary keys, but opaque identifiers) therefore we MUST NOT consider them
     * to be the same "object" and an unreconcilable write-write conflict MUST
     * be reported during validation. (Essentially, two transactions were handed
     * the same identifier for new objects.)
     * 
     * FIXME Think up sneaky test cases for this method and verify its operation
     * in some detail.
     */
    public void mergeWithGlobalObjectIndex(Journal journal) {
        
        // Verify that this is a transaction scope object index.
        assert baseObjectIndex != null;
        
        final KeyValueIterator itr = objectIndex.getRoot().entryIterator();
        
        while( itr.hasNext() ) {
            
            // The value for that persistent identifier.
            IObjectIndexEntry entry = (IObjectIndexEntry) itr.next();
            
            // The persistent identifier.
            final Integer id = (Integer) itr.getKey();
            
            if( entry.isDeleted() ) {

                /*
                 * IFF there was a pre-existing version in the global scope then
                 * we clear the 'currentVersionSlots' in the entry in the global
                 * scope and mark the index entry as dirty. The global scope
                 * will now recognized the persistent identifier as 'deleted'.
                 */
                
                if( entry.isPreExistingVersionOverwritten() ) {

                    /*
                     * Update the entry in the global object index.
                     * 
                     * Note: the same post-conditions could be satisified by
                     * getting the entry in the global scope, clearing its
                     * [currentVersionSlots] field, settting its
                     * [preExistingVersionSlots] field and marking the entry as
                     * dirty -- that may be more effective with a persistence
                     * capable implementation. It is also more "obvious" and
                     * safer since there is no reference sharing.
                     */
                    ((ObjectIndex)journal.objectIndex).objectIndex.insert(id,entry);
                    
                } else {
                    
                    /*
                     * The deleted version never existed in the global scope.
                     */
                    
                }

            } else {

                /*
                 * Copy the entry down onto the global scope.
                 */
                ((ObjectIndex)journal.objectIndex).objectIndex.insert(id, entry);

                /*
                 * Mark the slots for the current version as committed.
                 * 
                 * @todo This MUST be atomic. (It probably will be once it is
                 * modified for a persistence capable index since we do not
                 * record the new root of the object index on the journal until
                 * the moment of the commit, so while dirty index nodes may be
                 * evicted onto the journal, they are not accessible in case of
                 * a transaction restart. This does suggest a recursive twist
                 * with whether or not the slots for the index nodes themsevles
                 * are marked as committed on the journal -- all stuff that
                 * needs tests!)
                 */
                journal.allocationIndex.setCommitted(entry.getCurrentVersionSlots());
                
            }
            
            /*
             * The slots allocated to the pre-existing version are retained in
             * the index entry for this transaction until the garbage collection
             * is run for the transaction. This is true regardless of whether
             * new version(s) were written in this transaction, if the
             * pre-existing version was simply deleted, or if the most recent
             * versions written by this transaction was finally deleted. If the
             * entry is holding the slots for a pre-existing version that was
             * overwritten then we MUST NOT remove it from the transaction's
             * object index. That information is required later to GC the
             * pre-existing versions.
             */
            
            if( ! entry.isPreExistingVersionOverwritten() ) {

                // Remove the index entry in the transaction scope.
                
                itr.remove();

            }

        }

    }
    
    /**
     * @param journal The journal.
     * 
     * @param tx The transaction being validated.
     * 
     * @return True iff validation succeeds.
     * 
     * FIXME As a trivial case, if no intervening commits have occurred on the
     * journal then this transaction MUST be valid regardless of its write (or
     * delete) set.  This test probably needs to examine the current root block
     * and the transaction to determine if there has been an intervening commit. 
     * 
     * FIXME Make validation efficient by a streaming pass over the write set of
     * this transaction that detects when the transaction identifier for the
     * global object index has been modified since the transaction identifier
     * that serves as the basis for this transaction (the committed state whose
     * object index this transaction uses as its inner read-only context).
     */
    public boolean validate(Journal journal,IStore tx) {
        
        /*
         * This MUST be the journal's object index. The journals' object index
         * is NOT always the same as the inner object index map used by normal
         * the transaction since other transactions MAY have committed on the
         * journal since the transaction started. If you use the inner object
         * index for the transaction by mistake then interleaved transactions
         * will NOT be visible and write-write conflicts will NOT be detected.
         */
        final ObjectIndex globalScope = (ObjectIndex)journal.objectIndex;
        
        /*
         * Note: Write-write conflicts can be validated iff a conflict resolver
         * was declared when the Journal object was instantiated.
         */
        final IConflictResolver conflictResolver = journal.getConflictResolver();
        
        // Verify that this is a transaction scope object index.
        assert baseObjectIndex != null;
        
        /*
         * A read-only transaction whose ground state is the current committed
         * state of the journal. This will be exposed to the conflict resolver
         * so that it can read the current state of objects committed on the
         * journal.
         * 
         * @todo Extract ITx and refactor Tx to write this class. What is the
         * timestamp concept for this transaction or does it simply fail to
         * register itself with the journal?
         */
        IStore readOnlyTx = null; // new ReadOnlyTx(journal);
        
        final KeyValueIterator itr = objectIndex.getRoot().entryIterator();
        
        while( itr.hasNext() ) {
            
            // The value for that persistent identifier in the transaction.
            final SimpleEntry txEntry = (SimpleEntry) itr.next();
            
            // The persistent identifier.
            final Integer id = (Integer) itr.getKey();
            
            // Lookup the entry in the global scope.
            IObjectIndexEntry baseEntry = (IObjectIndexEntry)globalScope.objectIndex.lookup(id);
            
            /*
             * If there is an entry in the global scope, then we MUST compare the
             * version counters.
             */
            if( baseEntry != null ) {

                /*
                 * If the version counters do not agree then we need to perform
                 * write-write conflict resolution.
                 */
                if( baseEntry.getVersionCounter() != txEntry.getVersionCounter() ) {

                    if( conflictResolver == null ) {
                        
                        System.err.println("Could not validate write-write conflict: id="+id);
                        
                        // validation failed.
                        
                        return false;
                        
                    } else {
                        
                        try {
                            
                            conflictResolver.resolveConflict(id,readOnlyTx,tx);
                            
                        } catch( Throwable t ) {
                            
                            System.err.println("Could not resolve write-write conflict: id="+id+" : "+t);
                            
                            return false;
                            
                        }

                        /*
                         * FIXME We need to write the resolved version on the
                         * journal. However, we have to take care since this can
                         * result in a concurrent modification of the
                         * transaction's object index, which we are currently
                         * traversing.
                         * 
                         * The simple way to handle this is to accumulate
                         * updates from conflict resolution during validation
                         * and write them afterwards when we are no longer
                         * traversing the transaction's object index.
                         * 
                         * A better way would operate at a lower level and avoid
                         * the memory allocation and heap overhead for those
                         * temporary structures - this works well if we know
                         * that only the current entry will be updated by
                         * conflict resolution.
                         * 
                         * Finally, if more than one entry can be updated when
                         * we MUST use an object index data structure for the
                         * transaction that is safe for concurrent modification
                         * and we MUST track whether each entry has been
                         * resolved and scan until all entries resolve or a
                         * conflict is reported. Ideally cycles will be small
                         * and terminate quickly (ideally validation itself will
                         * terminate quickly), in which case we could use a
                         * transient data structure to buffer concurrent
                         * modifications to the object index. In that case, we
                         * only need to buffer records that are actually
                         * overwritten during validation - but that change would
                         * need to be manifest throughout the object index
                         * support since it is essentially stateful (or by
                         * further wrapping of the transaction's object index
                         * with a buffer!).
                         */
                        
                    }
                    
                }
                
                if( baseEntry.getVersionCounter() == Long.MAX_VALUE ) {
                    
                    /*
                     * @todo There may be ways to handle this, but that is
                     * really a LOT of overwrites. For example, we could just
                     * transparently promote the field to a BigInteger, which
                     * would require storing it as a Number rather than a
                     * [long]. Another approach is to only rely on "same or
                     * different". With that approach we could use a [short] for
                     * the version counter, wrap to zero on overflow, and there
                     * would not be a problem unless there were 32k new versions
                     * of this entry written while the transaction was running
                     * (pretty unlikely, and you can always use a packed int or
                     * long if you are worried :-) We could also just use a
                     * random number and accept rollback if the random values
                     * happened to collide.
                     */
                    
                    throw new RuntimeException("Too many overwrites: id="+id);
                    
                }

                /*
                 * Increment the version counter. We add one to the current
                 * version counter in the _global_ scope since that was the
                 * current version at the time that the write-write conflict was
                 * detected.
                 * 
                 * Note: We MUST bump the version counter even if the "WRITE"
                 * was a "DELETE" otherwise we will fail to notice a write-write
                 * conflict where an intervening transaction deletes the version
                 * and commits before an overwrite of the version by a concurrent
                 * transaction.
                 */
                txEntry.versionCounter = (short) (baseEntry.getVersionCounter() + 1);
                                
            }
            
        }

        // validation suceeded.
        
        return true;
        
    }
    
    /**
     * This implementation simply scans the object index. After a commit, the
     * only entries that we expect to find in the transaction's object index are
     * those where a pre-existing version was overwritten by the transaction. We
     * just deallocate the slots for those pre-existing versions.
     * 
     * @param allocationIndex
     *            The index on which slot allocations are maintained.
     * 
     * FIXME The transaction's object index SHOULD be deallocated on the journal
     * after garbage collection since it no longer holds any usable information.
     * 
     * FIXME Garbage collection probably MUST be atomic (it is Ok if it is both
     * incremental and atomic, but it needs a distinct commit point, it must be
     * restart safe, etc.).
     */
    public void gc(ISlotAllocationIndex allocationIndex) {
        
        // Verify that this is a transaction scope object index.
        assert baseObjectIndex != null;
        
        final Iterator itr = objectIndex.getRoot().entryIterator();
        
        while( itr.hasNext() ) {
            
            // The value for that persistent identifier.
            final IObjectIndexEntry entry = (IObjectIndexEntry)itr.next();
            
            // The slots on which the pre-existing version was written.
            ISlotAllocation preExistingVersionSlots = entry
                    .getPreExistingVersionSlots();

            // Deallocate those slots.
            allocationIndex.clear(preExistingVersionSlots);
            
            /*
             * Note: This removes the entry to avoid possible problems with
             * double-gc. However, this issue really needs to be resolved by an
             * ACID GC operation.
             */
            itr.remove();
                
        }

    }
    
}
