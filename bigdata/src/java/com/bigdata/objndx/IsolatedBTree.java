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

package com.bigdata.objndx;

import com.bigdata.journal.IConflictResolver;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Tx;
import com.bigdata.rawstore.IRawStore;

/**
 * <p>
 * A B+-Tree that has been isolated by a transaction.
 * </p>
 * Writes will be applied to the isolated btree. Reads will read first on the
 * isolated btree and then read through to the immutable {@link UnisolatedBTree}
 * specified in the constructor if no entry is found under the key. Deletes
 * leave "delete markers".
 * <p>
 * In order to commit the changes on the {@link UnisolatedBTree} the writes must
 * be {@link #validate(UnisolatedBTree) validated},
 * {@link #mergeDown(UnisolatedBTree) merged down}, and the and dirty nodes and
 * leaves in the {@link UnisolatedBTree} must be flushed onto the store.
 * Finally, the store must record the new metadata record for the source btree
 * in a root block and commit. This protocol can be generalized to allow
 * multiple btrees to be isolated and commit atomically within the same
 * transaction. On abort, it is possible that nodes and leaves were written on
 * the store for the isolated btree. Those data are unreachable and MAY be
 * recovered depending on the nature of the store and its abort protocol.
 * <p>
 * The {@link #rangeCount(byte[], byte[])} method MAY report more entries in a
 * key range that would actually be visited - this is due to both the presence
 * of "delete marker" entries in the {@link IsolatedBTree} and the requirement
 * to read on a fused view of the writes on the {@link IsolatedBTree} and the
 * read-only {@link UnisolatedBTree}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IsolatedBTree extends BTree implements IIsolatableIndex, IsolatedIndex {

    private final UnisolatedBTree src;

    /**
     * Returns a fully isolated btree suitable for use within a transaction.
     * 
     * @param store
     *            The backing store.
     * @param src
     *            The unisolated btree that the isolated btree will use when a
     *            read on a key is a miss. This btree MUST be choosen
     *            consistently for all indices isolated by a transaction.
     *            Typically, it is the unisolated btree from the last committed
     *            state on the {@link Journal} before the {@link Tx} starts.
     * 
     * @todo we need a means to have active transactions span instances of a
     *       journal (as each journal fills up it is eventually frozen, a new
     *       journal is opened, and the indices from the old journal are rebuilt
     *       in perfect read-only index segments on disk; those segments are
     *       periodically compacted and segments that grow too large are split).
     *       when we use a transient map to isolate writes then a journal
     *       contains only committed state.
     * 
     * @todo The isolated btree needs to start from the committed stable state
     *       of another btree (a possible exception is the first transaction to
     *       create a given btree). In order to verify that the source btree
     *       meets those requirements we need to know that it was loaded from a
     *       historical metadata record, e.g., as found in a root block or a
     *       read-only root names index found in a root block. Merely having a
     *       persistent root is NOT enough since just writing the tree onto the
     *       store does not make it restart safe.
     * 
     * @todo It would be very nice if we could reuse immutable nodes from the
     *       last committed state of a given btree. However, we can not simply
     *       use the BTree instance from the global state since intervening
     *       writes will show up inside of its node set and the view MUST be of
     *       a historical ground state.
     * 
     * @todo We need to be able to validate against and write on the then
     *       current {@link UnisolatedBTree}. Therefore, in order to support
     *       transactions for unnamed btrees we need to pass that into the
     *       validate() and mergeDown() operations as a parameter.
     */
    public IsolatedBTree(IRawStore store, UnisolatedBTree src) {

        super(store, src.branchingFactor, Value.Serializer.INSTANCE);
        
        this.src = src;
        
    }

    /**
     * @param store
     * @param metadata
     * @param src
     */
    public IsolatedBTree(IRawStore store, BTreeMetadata metadata, UnisolatedBTree src) {
        super(store, metadata);
        
        assert src != null;
        
        this.src = src;
    }
    
    /**
     * True iff the key does not exist or if it exists but is marked as
     * {@link IValue#isDeleted()}.
     * 
     * @param key
     *            The search key.
     * 
     * @return True iff there is an non-deleted entry for the search key.
     */
    public boolean contains(byte[] key) {
        
        Value value = (Value)lookup(key);
        
        if(value==null) {
            
            return src.contains(key);
            
        }
        
        if (value.deleted)
            return false;
        
        return true;
        
    }

    /**
     * Return the {@link IValue#getValue()} associated with the key or
     * <code>null</code> if the key is not found or if the key was found by
     * the entry is flagged as {@link IValue#isDeleted()}.
     * 
     * @param key
     *            The search key.
     * 
     * @return The application value stored under that search key (may be null)
     *         or null if the key was not found or if they entry was marked as
     *         deleted.
     */
    public Object lookup(byte[] key) {
        
        Value value = (Value)lookup(key);
        
        if(value==null) {
            
            return src.lookup(key);
            
        }

        if(value.deleted) return null;
        
        return value.value;
        
    }

    /**
     * Remove the 
     * 
     * @param key
     * @return
     * 
     * FIXME re-write this and write insert().
     */
    public Object remove(byte[] key) {

        // try
        Value value = (Value)remove(key);
        
        if(value==null) {
            
            value = (Value) src.lookup(key);
            
            if(value.deleted) return null;
            
            if(value==null) {
                
                return null;
                
            }
            
            super.insert(key, new Value(value.versionCounter, true, null));
            
        }

        if(value.deleted) return null;
        
        return value.value;
        
    }

    /**
     * <p>
     * Validate changes made to the index within a transaction against the last
     * committed state of the index in the global scope. In general there are
     * two kinds of conflicts: read-write conflicts and write-write conflicts.
     * Read-write conflicts are handled by NEVER overwriting an existing version
     * (an MVCC style strategy). Write-write conflicts are detected by backward
     * validation against the last committed state of the journal. A write-write
     * conflict exists IFF the version counter on the transaction index entry
     * differs from the version counter in the global index scope.
     * </p>
     * <p>
     * Validation occurs as part of the prepare/commit protocol. Concurrent
     * transactions MAY continue to run without limitation. A concurrent commit
     * (if permitted) would force re-validation since the transaction MUST now
     * be validated against the new baseline. (It is possible that this
     * validation could be optimized.)
     * </p>
     * 
     * @param current
     *            This MUST be the current {@link UnisolatedBTree}. This object
     *            is NOT always the same as the {@link UnisolatedBTree}
     *            specified to the constructor and used by read and write
     *            operations up until validation. It is critical that the
     *            current {@link UnisolatedBTree} be used here since other
     *            transactions MAY have committed since the transaction started.
     *            Otherwise interleaved transactions will NOT be visible and
     *            write-write conflicts will NOT be detected.
     * 
     * @return True iff validation succeeds.
     * 
     * @todo Once detected, the resolution of a write-write conflict is
     *       delegated to a {@link IConflictResolver conflict resolver}. If a
     *       write-write conflict can not be validated, then validation will
     *       fail and the transaction must abort. The version counters are
     *       incremented during commit as part of the {@link #mergeDown()} of
     *       the transaction scope index onto the global scope index. (The
     *       commit resolver should be part of the {@link UnisolatedBTree}
     *       metadata record.)
     * 
     * FIXME As a trivial case, if no intervening commits have occurred on the
     * journal then this transaction MUST be valid regardless of its write (or
     * delete) set. This test probably needs to examine the current root block
     * and the transaction to determine if there has been an intervening commit.
     * (This should be handled by the {@link Tx} since no writes need to be
     * validated under this condition.
     * 
     * FIXME Make validation efficient by a streaming pass over the write set of
     * this transaction that detects when the transaction identifier for the
     * global object index has been modified since the transaction identifier
     * that serves as the basis for this transaction (the committed state whose
     * object index this transaction uses as its inner read-only context).
     */
    public boolean validate(UnisolatedBTree globalScope /*current*/) {

        /*
         * Note: Write-write conflicts can be validated iff a conflict resolver
         * was declared when the Journal object was instantiated.
         */
        final IConflictResolver conflictResolver = null; /*@todo globalScope.getConflictResolver();*/

        /*
         * Scan the write set of the transaction.
         * 
         * Note: Both indices have the same total ordering so we are essentially
         * scanning both indices in order.
         */

        final IEntryIterator itr = entryIterator();

        while (itr.hasNext()) {

            // The value for that persistent identifier in the transaction.
            final Value txEntry = (Value) itr.next();

            // The key for that value.
            final byte[] key = (byte[]) itr.getKey();

            // Lookup the entry in the global scope.
            Value baseEntry = (Value) globalScope.lookup(key);

            /*
             * If there is an entry in the global scope, then we MUST compare the
             * version counters.
             */
            if (baseEntry != null) {

                /*
                 * If the version counters do not agree then we need to perform
                 * write-write conflict resolution.
                 */
                if (baseEntry.versionCounter != txEntry.versionCounter) {

                    if (conflictResolver == null) {

                        System.err
                                .println("Could not validate write-write conflict: id="
                                        + key);

                        // validation failed.

                        return false;

                    } else {

                        try {

                            throw new UnsupportedOperationException(
                                    "FIXME conflict resolution");
                            //                          conflictResolver.resolveConflict(id,readOnlyTx,tx);

                        } catch (Throwable t) {

                            System.err
                                    .println("Could not resolve write-write conflict: key="
                                            + key);

                            return false;

                        }

                        /*
                         * FIXME (Actually, I believe that this has become a
                         * non-issue.) We need to write the resolved version on
                         * the journal. However, we have to take care since this
                         * can result in a concurrent modification of the
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

            }

        }

        // validation suceeded.

        return true;

    }

    /**
     * <p>
     * Merge the transaction scope index onto its global scope index.
     * </p>
     * <p>
     * Note: This method is invoked by a transaction during commit processing to
     * merge the write set of an index into the global scope. This operation
     * does NOT check for conflicts. The pre-condition is that the transaction
     * has already been validated (hence, there will be no conflicts). This
     * method is also responsible for incrementing the version counters in the
     * {@link UnisolatedBTree} that are used to detect write-write conflicts
     * during validation.
     * </p>
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
    public void mergeDown(UnisolatedBTree globalScope) {

        final IEntryIterator itr = entryIterator();

        while (itr.hasNext()) {

            // The value for that persistent identifier.
            Value entry = (Value) itr.next();

            // The corresponding key.
            final byte[] id = (byte[]) itr.getKey();

            if (entry.deleted) {

                /*
                 * IFF there was a pre-existing version in the global scope then
                 * we clear the 'currentVersionSlots' in the entry in the global
                 * scope and mark the index entry as dirty. The global scope
                 * will now recognized the persistent identifier as 'deleted'.
                 */

                if (globalScope.contains(id)) {

                    /*
                     * Mark the entry in the unisolated index as deleted.
                     */
                    globalScope.insert(id, new Value(
                            entry.nextVersionCounter(), true, null));

                } else {

                    /*
                     * The deleted version never existed in the unisolated index
                     * so we do not need to record the entry.
                     */

                }

            } else {

                /*
                 * Copy the entry down onto the global scope.
                 */
                globalScope.insert(id, new Value(entry.nextVersionCounter(),
                        false, entry.value));

            }

        }

    }

}
