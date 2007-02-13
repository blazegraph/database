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

import com.bigdata.journal.Journal;
import com.bigdata.journal.Tx;
import com.bigdata.objndx.AbstractBTree;
import com.bigdata.objndx.BTree;
import com.bigdata.objndx.BTreeMetadata;
import com.bigdata.objndx.IEntryIterator;
import com.bigdata.rawstore.IRawStore;

/**
 * <p>
 * A B+-Tree that has been isolated by a transaction.
 * </p>
 * <p>
 * Writes will be applied to the isolated btree. Reads will read first on the
 * isolated btree and then read through to the immutable {@link UnisolatedBTree}
 * specified in the constructor if no entry is found under the key. Deletes
 * leave "delete markers".
 * </p>
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
 * </p>
 * <p>
 * The {@link #rangeCount(byte[], byte[])} method MAY report more entries in a
 * key range that would actually be visited - this is due to both the presence
 * of "delete marker" entries in the {@link IsolatedBTree} and the requirement
 * to read on a fused view of the writes on the {@link IsolatedBTree} and the
 * read-only {@link UnisolatedBTree}.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IsolatedBTree extends BTree implements IIsolatableIndex, IIsolatedIndex {

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

        super(store, src.getBranchingFactor(), Value.Serializer.INSTANCE);
        
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
    
    public void addAll(AbstractBTree src) {

        if (!(src instanceof UnisolatedBTree)) {

            throw new IllegalArgumentException("Expecting: "
                    + UnisolatedBTree.class);

        }

        super.addAll(src);
        
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
     * differs from the version counter in the global index scope. Once
     * detected, the resolution of a write-write conflict is delegated to a
     * {@link IConflictResolver conflict resolver}. If a write-write conflict
     * can not be validated, then validation will fail and the transaction must
     * abort.
     * </p>
     * <p>
     * Validation occurs as part of the prepare/commit protocol. Concurrent
     * transactions MAY continue to run without limitation. A concurrent commit
     * (if permitted) would force re-validation since the transaction MUST now
     * be validated against the new baseline. (It is possible that this
     * validation could be optimized.)
     * </p>
     * <p>
     * The version counters used to detect write-write conflicts are incremented
     * during the commit as part of the {@link #mergeDown()} of the
     * {@link IsolatedBTree} onto the corresponding {@link UnisolatedBTree} in
     * the global scope.
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
     * @todo Validation of an object index MUST specifically disallow any scheme
     *       which generates opaque object identifiers that are NOT primary keys
     *       MUST NOT allow concurrent transactions to assign the same object
     *       identifier. If this case is handled then all is well.
     */
    public boolean validate(UnisolatedBTree globalScope /*current*/) {

        /*
         * Note: Write-write conflicts can be validated iff a conflict resolver
         * was declared when the Journal object was instantiated.
         */
        final IConflictResolver conflictResolver = globalScope.getConflictResolver();

        /*
         * The versions returned by the conflict resolver must be written on the
         * isolated index so that they will overwrite the committed version when
         * we mergeDown() onto the unisolated index. However, we have to take
         * care since this can result in a concurrent modification of the
         * IsolatedBTree that we are currently traversing.
         * 
         * We handle this by inserting the results from the conflict resolver
         * into an temporary UnisolatedBTree and then writing them on the
         * IsolatedBTree once we finish the validation pass.
         * 
         * @todo Once we create this temporary IsolatedBTree we need to read
         * from a fused view of it and the primary IsolatedBTree if we are going
         * to support conflict resolution that spans more than a single
         * key-value at a time.
         * 
         * @todo It is NOT safe to update the value on the IsolatedBTree during
         * traversal since it might trigger copy-on-write which would cause 
         * structural modifications that would break the iterator.
         */
        UnisolatedBTree tmp = null;
        
        /*
         * Scan the write set of the transaction.
         * 
         * Note: Both indices have the same total ordering so we are essentially
         * scanning both indices in order.
         * 
         * Note: we must use the implementation of this method on the super
         * class in order to visit the IValue objects and see both deleted and
         * undeleted entries.
         */
        final IEntryIterator itr = super.entryIterator();

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

                        // no conflict resolver - validation fails.

                        return false;

                    }

                    /*
                     * Apply the conflict resolver in an attempt to resolve the
                     * conflict.
                     */

                    Value newValue = conflictResolver.resolveConflict(key,
                            baseEntry, txEntry);

                    if (newValue == null) {

                        // could not validate the conflict.
                        return false;

                    }

                    if (newValue == baseEntry) {

                        /*
                         * In this case the version written by the transaction
                         * will take precedence and we do not have to do
                         * anything.
                         */
                        continue;

                    } else {

                        /*
                         * Otherwise, the version returned by the conflict
                         * resolver must be written on the isolated index so
                         * that it will overwrite the committed version when we
                         * mergeDown() onto the unisolated index.
                         */

                    }

                }

            }

        }

        if(tmp!=null) {
            
            /*
             * Copy in any updates resulting from conflict validation. It is
             * safe to apply those updates now that we are no longer traversing
             * the index.
             */
            
            addAll(tmp);
            
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
     * merge the write set of an {@link IsolatedBTree} into the global scope.
     * This operation does NOT check for conflicts. The pre-condition is that
     * the transaction has already been validated (hence, there will be no
     * conflicts). This method is also responsible for incrementing the version
     * counters in the {@link UnisolatedBTree} that are used to detect
     * write-write conflicts during validation.
     * </p>
     */
    public void mergeDown(UnisolatedBTree globalScope) {

        /*
         * Note: we must use the implementation of this method on the super
         * class in order to visit the IValue objects and see both deleted
         * and undeleted entries.
         */
        final IEntryIterator itr = super.entryIterator();

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
