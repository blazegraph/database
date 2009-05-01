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

import java.util.concurrent.atomic.AtomicInteger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.FusedView;
import com.bigdata.btree.ICounter;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ILocalBTreeView;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.Tuple;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.service.IBigdataFederation;

/**
 * <p>
 * An index (or index partition) that has been isolated by a transaction.
 * Isolation is achieved by the following mechanisms:
 * <ol>
 * <li>The writeSet of the transaction on the index is isolated on a
 * {@link BTree} visible only to that transaction.</li>
 * <li>Version timestamps are maintained for index entries in both the isolated
 * write set and groundState from which the transaction is reading.</li>
 * <li>The groundState is defined as the view of the index (partition) as of
 * the abs(startTime) of the transaction.</li>
 * <li>Reads are performed against an ordered view defined by the writeSet
 * followed by the ordered set of indices defining the groundState of the index.</li>
 * <li>Writes first read through the ordered view to locate the most recent
 * version for an index entry. If the index entry is located in the isolated
 * writeSet then it is overwritten and its timestamp is unchanged. If the index
 * entry is located in the groundState then the timestamp is copied from that
 * index entry and written on the new version in the isolated writeSet.</li>
 * <li>During validation, version timestamps in the isolated writeSet are
 * compared against the then current view of the corresponding unisolated index.
 * If the timestamp in the unisolated view differs from that in the writeSet
 * then there is a write-write conflict. Write-write conflicts MAY be validated
 * if the index has a registered {@link IConflictResolver}.</li>
 * <li>If the writeSet is validated then it is mergedDown (copied onto) the
 * then current unisolated index view. During the mergeDown phase the commit
 * timestamp of the transaction is applied to all index entries copied from the
 * write set. Transactions that later try to commit will recognize write-write
 * conflicts based on those updated timestamps.</li>
 * </ol>
 * </p>
 * <p>
 * Note: The commit time of the transaction is a bit of an illusion. What it
 * does is provide a strictly increasing timestamp on the tuples updated by that
 * transaction in the indices on which it has written. Write-write conflicts are
 * detected on the basis of that timestamp. Thus it is really a "revision
 * timestamp" for the transaction commit point. The timestamp from which the
 * post-commit state of the transaction may be read IS NOT defined for an
 * {@link IBigdataFederation}. It is not possible to define this timestamp
 * without requiring concurrent commit processing to be paused on all data
 * services on which the transaction has written, which is viewed as too high
 * a cost.
 * </p>
 * Note: The process of validating, merging down changes, and committing those
 * changes MUST be atomic. Therefore no other operations may be permitted access
 * to the unisolated indices corresponding to the isolated indices on which the
 * transaction during this process. This constraint is generally achieved by
 * holding a write lock on the unisolated indices corresponding to the indices
 * isolated by the transaction, e.g., by declaring those indices to an
 * {@link ITx#UNISOLATED} {@link AbstractTask} which handles this process.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IsolatedFusedView extends FusedView {

    /**
     * The transaction identifier (aka transaction start time).
     */
    private final long startTime;
    
    /**
     * The isolated write set (the place where we record the intention of the
     * transaction). This is just a reference to the mutable {@link BTree} at
     * index zero(0) of sources in the view.
     */
    private final BTree writeSet;
   
    /**
     * The isolated write set (the place where we record the intention of the
     * transaction). This is just a reference to the mutable {@link BTree} at
     * index zero(0) of sources in the view.
     * 
     * @see FusedView#getMutableBTree()
     */
    public BTree getWriteSet() {

        return writeSet;
        
    }
    
    /**
     * Constructor may be used either for a fully isolated transaction or an
     * unisolated operation. In each case the <i>groundState</i> is the ordered
     * set of read-only resources corresponding to the <i>timestamp</i>.
     * <p>
     * Reads will read through the <i>writeSet</i> and then the resource(s) in
     * the <i>groundState</i> in the order in which they are given. A read is
     * satisified by the first resource containing an index entry for the search
     * key.
     * <p>
     * Writes will first read through looking for a @todo javadoc
     * 
     * @param timestamp
     *            The timestamp associated with the <i>groundState</i>.
     * @param sources
     *            An ordered array of sources comprised of the {@link BTree}
     *            that will absorb writes and the historical ground state.
     */
    public IsolatedFusedView(final long timestamp, final AbstractBTree[] sources ) {
        
        super(sources);

        if (!TimestampUtility.isCommitTime(timestamp))
            throw new IllegalStateException();

        this.startTime = timestamp;

        if (sources.length < 2)
            throw new IllegalArgumentException();

        if( !( sources[0] instanceof BTree ) ) {
            
            throw new IllegalArgumentException();
            
        }
        
        writeSet = (BTree) sources[0];

        // verify all sources support timestamps.
        for (int i = 0; i < sources.length; i++) {

            if (!sources[i].getIndexMetadata().getVersionTimestamps()) {

                throw new IllegalArgumentException();

            }
            
        }

    }

    /**
     * True iff there are no writes on this isolated index.
     */
    public boolean isEmptyWriteSet() {

        return writeSet.getEntryCount() == 0;

    }

    /**
     * Counters are disallowed for isolated view. The reason is that counters
     * are typically used to create one-up distinct values assigned to keys. If
     * the counter is stored on the write set then different transactions could
     * easily assign the same counter value under different keys, leading to an
     * undetectable write conflict.
     * 
     * @todo counters could probably be enabled within transactions if we used
     *       the counter from the then current mutable btree. This would have to
     *       be passed into the constructor. In addition, the counter logic
     *       would have to be carefully checked to make sure that counter
     *       assignments remain consistent. The counter itself is an
     *       {@link AtomicInteger}. However additional care needs to be taken
     *       to ensure that the counter value is persisted if it is changed (by
     *       updating the {@link BTree} {@link Checkpoint} record). The cases
     *       where the tx bumps the counter need to be carefully examined since
     *       it could force the write of the unisolated btree when we actually
     *       do not want to commit the btree - some kind of locking may be
     *       required. So, for now, this is disabled.
     * 
     * @throws UnsupportedOperationException
     *             always
     */
    final public ICounter getCounter() {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Write an entry for the key on the write set.
     */
    public byte[] insert(final byte[] key, final byte[] val) {

        final Tuple tuple = lookup(key, getMutableBTree().getLookupTuple());

        if (tuple == null) {
            
            /*
             * There is no entry under that key in the view, not even a deleted
             * entry. Therefore we insert a new entry using the startTime of the
             * transaction. We return [null] since there was no value under that
             * key.
             */
            
//            srcs[0]
            getMutableBTree().insert(key, val, false/*delete*/, startTime, null/*tuple*/);
            
            return null;
            
        } else {
            
            /*
             * There is an (potentially deleted) entry under that key and we are
             * going to overwrite it. We will use the timestamp from that entry.
             * If the entry is NOT in the write set then the timestamp will be
             * the [revisionTime] of the last write on that key before this
             * transaction's start time and the timestamp will be copied into
             * the write set. If the entry is in the write set then the
             * timestamp will either have been copied already into the write set
             * previously by this code branch or it will be the startTime of
             * this transaction (the code branch above).
             */
            
            final long timestamp = tuple.getVersionTimestamp();
            
//            srcs[0]
            getMutableBTree().insert(key, val, false/*delete*/, timestamp, null/*tuple*/);

            return tuple.isNull() || tuple.isDeletedVersion() ? null : tuple
                    .getValue();
            
        }
        
    }

    /**
     * Write a deleted entry for the key on the write set.
     */
    public byte[] remove(byte[] key) {

        final Tuple tuple = lookup(key, getMutableBTree().getLookupTuple());
        
        if (tuple == null) {
            
            /*
             * There is no entry under that key in the view, not even a deleted
             * entry. Therefore we insert a new entry using the startTime of the
             * transaction. We return [null] since there was no value under that
             * key.
             */
            
//            srcs[0]
            getMutableBTree().insert(key, null, true/* delete */, startTime, null/*tuple*/);
            
            return null;
            
        } else {
            
            /*
             * There is an (potentially deleted) entry under that key and we are
             * going to overwrite it. We will use the timestamp from that entry.
             * If the entry is NOT in the write set then the timestamp will be
             * the [revisionTime] of the last write on that key before this
             * transaction's start time and the timestamp will be copied into
             * the write set. If the entry is in the write set then the
             * timestamp will either have been copied into the write set
             * previously by this code branch or it will be the startTime of
             * this transaction (the code branch above).
             */
            
            final long timestamp = tuple.getVersionTimestamp();
            
            if (tuple.isDeletedVersion() && timestamp == this.startTime) {

                /*
                 * Note: Avoid double-delete when the delete was performed by
                 * this transaction.
                 */

            } else {

                /*
                 * Write a delete marker whose timestamp is copied from the
                 * groundState.
                 */
                
//                srcs[0]
                 getMutableBTree().insert(key, null, true/* delete */, timestamp, null/* tuple */);
                
            }

            return tuple.isNull() || tuple.isDeletedVersion() ? null : tuple
                    .getValue();
            
        }

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
     * {@link IsolatedFusedView} onto the corresponding unisolated indices in
     * the global scope.
     * </p>
     * 
     * @param groundStateSources
     *            The ordered view of the unisolated index. This MUST be the
     *            current view of the ground state as of when the transaction is
     *            validated (NOT when it was created). This view WILL NOT the
     *            same as the groundState specified to the constructor if
     *            intervening transactions have committed on the index.
     * 
     * @return True iff validation succeeds.
     */
    public boolean validate(final AbstractBTree[] groundStateSources) {

        if (isEmptyWriteSet()) {

            // Nothing written on this isolated index.

            return true;

        }

        /*
         * Do not validate this index unless the groundState has been modified
         * since the readState for the transaction.
         * 
         * @todo the code below presumed that the source was a single BTree
         * rather than an ordered view of AbstractBTrees. It would be better to
         * compare a timestamp for the view definition, e.g., in the Resource[]
         * defining an index partition.
         */
//        {
//
//            // Note: This is the state from which the transaction read.
//            ReadOnlyFusedView readState = this.groundState;
//
//            if (!currentGroundState.modifiedSince(readState.getMetadata()
//                    .getMetadataAddr())) {
//
//                // No changes to the unisolated index since the readState.
//
//                return true;
//
//            }
//
//        }

        /*
         * Note: Write-write conflicts can be validated iff a conflict resolver
         * was declared when the Journal object was instantiated.
         */
        final IConflictResolver conflictResolver = writeSet.getIndexMetadata()
                .getConflictResolver();

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
         * structural modifications that would break the iterator. [@todo
         * actually, that is fine now if we specify the CURSOR flag].
         * 
         * @todo Once we create this temporary tree we need to read from a fused
         * view of it and the primary IsolatedFusedView if we are going to
         * support conflict resolution that spans more than a single key-value
         * at a time. However, we also need to expose the Tx to the conflict
         * resolver for that to work (which is why we have not exposed the tx
         * yet).
         */
        BTree tmp = null;

        /*
         * A view onto the consistent state of the current global scope for that
         * index. We use this ONLY for reading (clearly).
         */
        final IIndex groundState = (groundStateSources.length == 1 ? groundStateSources[0]
                : new FusedView(groundStateSources));
        
//        /*
//         * The btree that is absorbing writes for the index. We need this as a
//         * BTree and not a FusedView or IIndex in order to handle all of the
//         * cases as cleanly as possible - what matters is having access to the
//         * core lookup() method on AbstractBTree.
//         */
//        final BTree groundStateWriteSet = (BTree) groundStateSources[0];
        
        /*
         * Scan the write set of the transaction.
         * 
         * Note: Both indices have the same total ordering so we are essentially
         * scanning both indices in order.
         * 
         * Note: the iterator is chosen carefully in order to visit the IValue
         * objects and see both deleted and undeleted entries.
         */
        final ITupleIterator itr = writeSet.rangeIterator(null, null,
                0/* capacity */, ALL /* flags */, null);

        // tuple for reading from the groundState index.
        final Tuple groundStateTuple = new Tuple(
                ((ILocalBTreeView)groundState).getMutableBTree(),
//                (groundState instanceof AbstractBTree ? (AbstractBTree) groundState
//                        : ((FusedView) groundState).getSources()[0]), 
                        KEYS | VALS);

        while (itr.hasNext()) {

            // The index entry in the transaction's write set.
            final ITuple txEntry = itr.next();

            // The key for that index entry.
            final byte[] key = txEntry.getKey();

            // Lookup the entry in the global scope.
            final ITuple baseEntry; //= groundState.lookup(key, groundStateTuple);
            
            if(groundState instanceof AbstractBTree) {
                
                 baseEntry = ((AbstractBTree) groundState).lookup(key,
                        groundStateTuple);
                 
            } else {

                baseEntry = ((FusedView) groundState).lookup(key,
                        groundStateTuple);

            }

            /*
             * If there is an entry in the global scope, then we MUST compare
             * the version counters.
             */

            if (baseEntry != null) {

                /*
                 * If the version counters do not agree then we need to perform
                 * write-write conflict resolution.
                 */

                if (baseEntry.getVersionTimestamp() != txEntry
                        .getVersionTimestamp()) {

                    if (conflictResolver == null) {

                        // no conflict resolver - validation fails.

                        log.warn("Write-write conflict - no conflict resolver");

                        return false;

                    }

                    /*
                     * Create a temporary index to buffer the conflict
                     * resolver's decisions. Once the write set has been
                     * validated the versions written (or deleted) by the
                     * conflict resolver must be written on the isolated index
                     * so that it will overwrite the committed version when we
                     * mergeDown() onto the unisolated index.
                     * 
                     * Note: This uses the same store as the writeSet, which is
                     * supposed to be a temporary store dedicated to a specific
                     * transaction.
                     */

                    if (tmp == null) {

                        tmp = BTree.create(//
                                writeSet.getStore(), // same store.
                                writeSet.getIndexMetadata().clone() // same metadata
                                );                        

                    }

                    /*
                     * Apply the conflict resolver in an attempt to resolve the
                     * conflict.
                     */

                    try {

                        if (!conflictResolver.resolveConflict(tmp, txEntry,
                                baseEntry)) {

                            log.warn("Write-write conflict NOT resolved.");

                            // Not validated.
                            return false;

                        }

                    } catch (Exception ex) {

                        log.error("Write-write conflict", ex);

                        // Not validated.
                        return false;

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
             * Note: We visit deleted entries in case conflict resolution
             * decided to delete an index entry.
             * 
             * Note: This sets the timestamp to the startTime of the
             * transaction, but that it an arbitrary choice. Since we have
             * already validated the transaction the timestamps in the write set
             * will be ignored during the mergeDown() operation so the assigned
             * value does NOT matter.
             */

            final ITupleIterator tmpItr = tmp.rangeIterator(null, null,
                    0/* capacity */,
                    IRangeQuery.DEFAULT | IRangeQuery.DELETED, null/* filter */);

            while (tmpItr.hasNext()) {

                final ITuple tuple = tmpItr.next();
                
                if(tuple.isDeletedVersion()) {
                
                    writeSet.insert(tuple.getKey(), null, true/* deleted */,
                            startTime, null/*tuple*/);
                    
                } else {

                    writeSet.insert(tuple.getKey(), tuple.getValue(),
                            false/* deleted */, startTime, null/* tuple */);
                    
                }
                
            }

        }

        // validation suceeded.

        return true;

    }

    /**
     * <p>
     * Merge the transaction scope index onto the then current unisolated index.
     * </p>
     * <p>
     * Note: This method is invoked by a transaction during commit processing to
     * merge the write set of an {@link IsolatedFusedView} into the global
     * scope. This operation does NOT check for conflicts. The pre-condition is
     * that the transaction has already been validated (hence, there will be no
     * conflicts).
     * </p>
     * <p>
     * Note: This method is also responsible for updating the version timestamps
     * that are used to detect write-write conflicts during validation - they
     * are set to the <i>revisionTime</i>.
     * </p>
     * 
     * @param revisionTime
     *            The revision timestamp assigned to the commit point of the
     *            transaction.
     * 
     * @param groundStateSources
     *            The ordered view of the unisolated index. This MUST be the
     *            current view of the ground state as of when the transaction is
     *            validated (NOT when it was created). This view WILL NOT the
     *            same as the groundState specified to the constructor if
     *            intervening transactions have committed on the index.
     */
    public void mergeDown(final long revisionTime,
            final AbstractBTree[] groundStateSources) {

        /*
         * A read-only view onto the consistent state of the current global
         * scope for that index. We use this ONLY for reading (clearly).
         */
//        final IIndex groundStateScope = new ReadOnlyFusedView(groundStateSources);
        /*
         * A view onto the consistent state of the current global scope for that
         * index. We use this ONLY for reading (clearly).
         */
        final IIndex groundState = (groundStateSources.length == 1 ? groundStateSources[0]
                : new FusedView(groundStateSources));
        
        /*
         * The btree that is absorbing writes for the index. We need this as a
         * BTree and not a FusedView or IIndex in order to handle all of the
         * cases as cleanly as possible - what matters is having access to the
         * core insert method on AbstractBTree.
         */
        final BTree groundStateWriteSet = (BTree) groundStateSources[0];
        
        /*
         * Note: the iterator is chosen carefully in order to visit the IValue
         * objects and see both deleted and undeleted entries.
         */
        final ITupleIterator itr = writeSet.rangeIterator(null, null,
                0/* capacity */, ALL/* flags */, null);

        while (itr.hasNext()) {

            // The index entry in the isolated write set.
            final ITuple entry = itr.next();

            // The corresponding key.
            final byte[] key = entry.getKey();

            if (entry.isDeletedVersion()) {

                /*
                 * IFF there was a pre-existing version in the global scope then
                 * we remove the key from the global scope so that it will now
                 * have a "delete marker" for this key.
                 */

                if (groundState.contains(key)) {

//                    globalScope.remove(key);
                    groundStateWriteSet.insert(key, null/* val */, true/* delete */,
                            revisionTime, null/*tuple*/);

                } else {

                    /*
                     * The deleted version never existed in the unisolated index
                     * so we do not need to record the entry.
                     */

                }

            } else {

                /*
                 * Copy the entry down onto the global scope.
                 * 
                 * Note: This writes the [revisionTime] of the transaction on
                 * the unisolated index entry.
                 */

                groundStateWriteSet.insert(key, entry.getValue(),
                        false/* delete */, revisionTime, null/* tuple */);

            }

        }

    }

}
