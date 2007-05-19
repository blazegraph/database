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

import com.bigdata.btree.BTree;
import com.bigdata.btree.BTreeMetadata;
import com.bigdata.btree.ByteArrayValueSerializer;
import com.bigdata.btree.IBatchBTree;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.ILinearList;
import com.bigdata.btree.ISimpleBTree;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Tx;
import com.bigdata.rawstore.IRawStore;

/**
 * <p>
 * A B+-Tree that has been isolated by a transaction (a write set for an index
 * isolated by that transactions). In general, this class inherits the
 * observable semantics of an {@link UnisolatedBTree} with the addition of
 * read-through on a miss to the backing btree and the interpretation of delete
 * markers in the write set.
 * </p>
 * <p>
 * {@link ISimpleBTree#insert(Object, Object) Writes} will be applied to this
 * {@link IsolatedBtree}. Reads ({@link ISimpleBTree#contains(byte[])) or {@link ISimpleBTree#lookup(Object)}
 * will read first on the {@link IsolatedBtree} and then read through to the
 * immutable {@link UnisolatedBTree} specified in the constructor iff no entry
 * is found under the key. {@link ISimpleBTree#remove(Object) Deletes} leave
 * "delete markers" in the write set.
 * </p>
 * <p>
 * The {@link IBatchBTree} interface provides a convenience for iterative
 * operations using the corresponding {@link ISimpleBTree} method and shares the
 * semantics for that interface.
 * </p>
 * <p>
 * The {@link #rangeCount(byte[], byte[])} method MAY report more entries in a
 * key range that would actually be visited by
 * {@link #rangeIterator(byte[], byte[])} - this is due to both the presence of
 * "delete marker" entries in the {@link IsolatedBTree} and the requirement to
 * read on a fused view of the writes on the {@link IsolatedBTree} and the
 * read-only {@link UnisolatedBTree}.
 * </p>
 * <p>
 * The {@link ILinearList} interface implemented by this class applies solely to
 * the entries in the write set. The interface specifically does NOT support a
 * fused view on the entry set of both the write set and the isolated index.
 * </p>
 * <p>
 * In order to commit the changes in an {@link IsolatedBTree} onto the backing
 * {@link UnisolatedBTree} the writes must be
 * {@link #validate(UnisolatedBTree) validated},
 * {@link #mergeDown(UnisolatedBTree) merged down}, and the dirty nodes and
 * leaves in the {@link UnisolatedBTree} must be {@link BTree#write() written}
 * onto the store. Finally, the store must record the new metadata record for
 * the source btree in a root block and commit. This protocol can be generalized
 * to allow multiple btrees to be isolated and commit atomically within the same
 * transaction. On abort, it is possible that nodes and leaves were written on
 * the store for the {@link IsolatedBtree}. Those data are unreachable and the
 * storage allocated to them MAY be reclaimed as part of the abort processing.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo consider disabling the {@link ILinearList} interface for this class
 *       since its behavior might surprise people. Alternatively, is there some
 *       slick way to realize the proper semantics for this interface on a fused
 *       view of the write set and the immutable index isolated by this class?
 * 
 * FIXME I have not finished working through the fused view support for this
 * class.
 */
public class IsolatedBTree extends UnisolatedBTree implements IIsolatedIndex {

    /**
     * The index that is being isolated by the {@link IsolatedBTree}.
     */
    private final UnisolatedBTree src;
    
    /**
     * The index that is being isolated by the {@link IsolatedBTree}. The use
     * of this method is <em>strongly</em> discouraged as it requires the
     * caller to maintain isolation. It is exposed so that methods on classes
     * derived from {@link UnisolatedBTree} may be accessed within a
     * transaction.
     */
    public UnisolatedBTree getUnisolatedBTree() {
        
        return src;
        
    }

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
     */
    public IsolatedBTree(IRawStore store, UnisolatedBTree src) {

        super(store, src.getBranchingFactor(), src.getIndexUUID(), src
                .getConflictResolver());
        
        this.src = src;
        
    }

    /**
     * The constructor will re-open the {@link IsolatedBTree} isolating some
     * index, however there is probably no use case for this constructor since
     * any failure during transaction processing typically leads to an abort of
     * the transaction and the discarding of any {@link IsolatedBTree}s that
     * were used by that transaction. Further, there is no expectation that an
     * {@link IsolatedBTree} would ever have a metadata record written since
     * there is never any reason to call {@link #write()} since we never need to
     * recover the isolated write set from persistent storage - it is either
     * applied and discarded during the commit or simply discarded by an abort.
     * <p>
     * A possible use case would be in support of a restart-safe 2-phase commit
     * protocol.
     * 
     * @param store
     * @param metadata
     * @param src
     * 
     * @todo This constructor is somewhat different since it requires access to
     *       a persistence capable parameter in order to reconstruct the view.
     *       Consider whether or not this can be refactored per
     *       {@link BTree#load(IRawStore, long)}.
     */
    public IsolatedBTree(IRawStore store, BTreeMetadata metadata, UnisolatedBTree src) {

        super(store, metadata);
        
        assert src != null;
        
        this.src = src;
        
    }

    /**
     * True iff there are no writes on this isolated index.
     */
    public boolean isEmptyWriteSet() {
        
        return super.nentries == 0;
        
    }
    
    /**
     * If the key is not in the write set, then delegate to
     * {@link UnisolatedBTree#contains(byte[])} on the isolated index. If the
     * key is in the write set but the entry is deleted then return
     * <code>false</code>. Otherwise return <code>true</code>.
     */
    public boolean contains(byte[] key) {
        
        if (key == null)
            throw new IllegalArgumentException();

        Value value = super.getValue(key);

        if (value == null) {

            return src.contains(key);

        }

        if (value.deleted) {

            return false;
            
        }

        return true;
        
    }

    /**
     * If the key is not in the write set, then delegate to
     * {@link UnisolatedBTree#lookup(Object)} on the isolated index. If the key
     * is in the write set but the entry is deleted then return
     * <code>null</code>. Otherwise return the
     * {@link IValue#getValue() application value} for that key.
     */
    public Object lookup(Object key) {
        
        if (key == null)
            throw new IllegalArgumentException();

        Value value = super.getValue((byte[])key);

        if (value == null) {

            return src.lookup(key);

        }

        if (value.deleted) {

            return null;
            
        }

        return value.datum;
        
    }

    /**
     * Remove the key from the write set (does not write through to the
     * unisolated index). If the key is not in the write set, then we check the
     * unisolated index. If the key is found there, then we add a delete marker
     * to the isolated index.
     */
    public Object remove(Object key) {

        if (key == null)
            throw new IllegalArgumentException();

        // check the isolated index.
        Value value = super.getValue((byte[])key);

        if (value == null) {

            /*
             * The key does not exist in the isolated index, so now we test to
             * see if the key exists in the unisolated index. if it does then we
             * need to write a delete marker in the isolated index.
             */
            value = src.getValue((byte[]) key);
            
            if(value==null||value.deleted) return null;

            super.remove(key);
            
            /*
             * return the value from the unisolated index since that is what was
             * deleted.
             */
            return value.datum;

        }

        if (value.deleted) {

            return null;
            
        }

        return super.remove(key);
        
    }

    /**
     * Adds an entry for the key under the value to the write set (does not
     * write through to the isolated index).
     */
    public Object insert(Object key, Object val) {
        return super.insert(key,val);
    }

    /**
     * Returns the entry index in the write set at which the key occurs (does
     * NOT read through to a fused view containing the write set and the
     * isolated index).
     */
    public int indexOf(byte[] key) {
        return super.indexOf(key);
    }

    /**
     * Returns the key at the specified entry index in the write set (does NOT
     * read through to a fused view containing the write set and the isolated
     * index).
     */
    public byte[] keyAt(int index) {
        return super.keyAt(index);
    }

    /**
     * Returns the value at the specified entry index in the write set (does NOT
     * read through to a fused view containing the write set and the isolated
     * index).
     */
    public Object valueAt(int index) {
        return super.valueAt(index);
    }

    /**
     * Returns the sum of the entries in the key range in this write set plus
     * those in the key range in the isolated index.
     */
    public int rangeCount(byte[] fromKey, byte[] toKey) {

        return super.rangeCount(fromKey, toKey)
                + src.rangeCount(fromKey, toKey);
        
    }

    /**
     * Returns an ordered fused view of the entries in the key range in this
     * write set merged with those in the key range in the isolated index.
     */
    public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey) {

        /*
         * This uses a version of ReadOnlyFusedView that is aware of delete markers and
         * that applies them such that a key deleted in the write set will not
         * have a value reported from the isolated index.
         */
        
        return new IsolatableFusedView(this,src).rangeIterator(fromKey, toKey);
        
    }

    /**
     * Returns an ordered fused view of the entries in the key range in this
     * write set merged with those in the key range in the isolated index.
     */
    public IEntryIterator entryIterator() {

        return rangeIterator(null,null);
        
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
         * into an temporary tree and then writing them on the IsolatedBTree
         * once we finish the validation pass.
         * 
         * Note: It is NOT safe to update the value on the IsolatedBTree during
         * traversal since it might trigger copy-on-write which would cause
         * structural modifications that would break the iterator.
         * 
         * @todo Once we create this temporary tree we need to read from a fused
         * view of it and the primary IsolatedBTree if we are going to support
         * conflict resolution that spans more than a single key-value at a
         * time.  However, we also need to expose the Tx to the conflict resolver
         * for that to work (which is why we have not exposed the tx yet).
         */
        BTree tmp = null;
        
        /*
         * Scan the write set of the transaction.
         * 
         * Note: Both indices have the same total ordering so we are essentially
         * scanning both indices in order.
         * 
         * Note: the iterator is chosen carefully in order to visit the IValue
         * objects and see both deleted and undeleted entries.
         */
        final IEntryIterator itr = getRoot().rangeIterator(null, null, null);

        while (itr.hasNext()) {

            // The value for that persistent identifier in the transaction.
            final Value txEntry = (Value) itr.next();

            // The key for that value.
            final byte[] key = (byte[]) itr.getKey();

            // Lookup the entry in the global scope.
            Value baseEntry = (Value) globalScope.getValue(key);

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

                    byte[] newValue;
                    
                    try {
                        
                        newValue = conflictResolver.resolveConflict(key,
                            baseEntry, txEntry);
                        
                    } catch(WriteWriteConflictException ex){

                        return false;
                        
                    }

                    /*
                     * Otherwise, the version returned by the conflict resolver
                     * must be written on the isolated index so that it will
                     * overwrite the committed version when we mergeDown() onto
                     * the unisolated index.
                     * 
                     * Note: This uses the same store as the IsolatedBTree. This
                     * makes it easy to leverage whatever strategy the
                     * transaction is using to buffer its write set.
                     */

                    if (tmp == null) {

                        tmp = new BTree(getStore(), // same store.
                                getBranchingFactor(), // same branching factor
                                src.getIndexUUID(), // same indexUUID.
                                ByteArrayValueSerializer.INSTANCE // byte[] values.
                                );

                        tmp.insert(key, newValue);

                    }

                }

            }

        }

        if (tmp != null) {
            
            /*
             * Copy in any updates resulting from conflict validation. It is
             * safe to apply those updates now that we are no longer traversing
             * the index.
             * 
             * Note that this will increment version numbers as we add in the
             * resolved key/value pairs.
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
         * Note: the iterator is chosen carefully in order to visit the IValue
         * objects and see both deleted and undeleted entries.
         */
        final IEntryIterator itr = getRoot().rangeIterator(null, null, null);

        while (itr.hasNext()) {

            // The value for that persistent identifier.
            Value entry = (Value) itr.next();

            // The corresponding key.
            final byte[] key = (byte[]) itr.getKey();

            if (entry.deleted) {

                /*
                 * IFF there was a pre-existing version in the global scope then
                 * we remove the key from the global scope so that it will now
                 * have a "delete marker" for this key.
                 */

                if (globalScope.contains(key)) {

                    globalScope.remove(key);

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

                globalScope.insert(key,entry.datum);

            }

        }

    }
    
}
