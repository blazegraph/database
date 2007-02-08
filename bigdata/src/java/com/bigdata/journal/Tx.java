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
 * Created on Oct 13, 2006
 */

package com.bigdata.journal;

import java.util.HashMap;
import java.util.Map;

import com.bigdata.objndx.BTree;

/**
 * <p>
 * A transaction. An instance of this class corresponds to a transaction.
 * </p>
 * <p>
 * A transaction is a context in which the application can access named btrees.
 * btrees returned within the transaction will be isolated according to the
 * isolation level of the transaction. transactions may be requested that are
 * read-only for some historical timestamp, that are read-committed (data
 * committed by _other_ transactions during the transaction will be visible
 * within that transaction), or that are fully isolated (changes made in other
 * transactions are not visible within the transaction).
 * 
 * When using an isolated transaction, changes are accumulated in an isolated
 * btree. The update set must then be validated and finally merged down onto the
 * global state when the transaction commits.
 * </p>
 * <p>
 * Transaction isolation is accomplished as follows. Within a transaction, the
 * most recently written data version is visible. This is accomplished using
 * copy on write semantics for the object index nodes. Originally the
 * transaction executes with a clean view of the last committed object index at
 * of the time that the transaction began. Each time an update is made within
 * the transaction to the object index, a test is performed to see whether the
 * target object index node is clean (same state as when the transaction
 * started). If it is clean, then the node and its ancenstors are cloned (copy
 * on write) and the change is applied to the copy. WRITE operations simply
 * update the first slot on which the current version (within the transaction
 * scope) is written. DELETE operations write a flag into the object index, but
 * do NOT remove the entry for the data from the index. READ operations are
 * performed against this view of the object index, and therefore always read
 * the most recent version (but note that an object not found in the journal
 * MUST be resolved against the corresponding database segment).
 * </p>
 * <p>
 * In a PREPARE operation, dirty index nodes in the transaction scope are merged
 * with the most recent committed state of the object index (backward
 * validation). This merge is used to detect write-write conflicts, which are
 * then resolved by state-based conflict resolution (e.g., merge rules). All
 * dirty object index nodes are flushed to disk during the prepare, but the root
 * block is not updated until the COMMIT.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo update javadoc.
 * 
 * @todo The various public methods on this API that have {@link RunState}
 *       constraints all eagerly force an abort when invoked from an illegal
 *       state. This is, perhaps, excessive. Futher, since this is used in a
 *       single-threaded server context, we are better off testing for illegal
 *       conditions and notifying clients without out generating expensive stack
 *       traces. This could be done by return flags or by the server checking
 *       pre-conditions itself and exceptions being thrown from here if the
 *       server failed to test the pre-conditions and they were not met
 * 
 * @todo Is it possible to have more than one transaction PREPARE must
 *       concurrent PREPARE operations be serialized?
 */
public class Tx implements IStore, ITx {

    /*
     * Text for error messages.
     */
    final static String NOT_ACTIVE = "Not active";
    final static String NOT_PREPARED = "Transaction is not prepared";
    final static String NOT_COMMITTED = "Transaction is not committed";
    final static String IS_COMPLETE = "Transaction is complete";
    
    final private Journal journal;
    final private long timestamp;

    private RunState runState;

    /**
     * BTrees isolated by this transactions.
     */
    private Map<String,BTree> btrees = new HashMap<String,BTree>();
    
    /**
     * Create a transaction starting the last committed state of the journal as
     * of the specified timestamp.
     * 
     * @param journal
     *            The journal.
     * 
     * @param timestamp
     *            The timestamp.
     * 
     * @exception IllegalStateException
     *                if the transaction state has been garbage collected.
     */
    public Tx(Journal journal, long timestamp ) {
        
        if( journal == null ) throw new IllegalArgumentException();
        
        this.journal = journal;
        
        this.timestamp = timestamp;

        journal.activateTx(this);
        
        this.runState = RunState.ACTIVE;

    }
    
    /**
     * The transaction identifier (aka timestamp).
     * 
     * @return The transaction identifier (aka timestamp).
     */
    public long getId() {
        
        return timestamp;
        
    }
    
    public String toString() {
        
        return ""+timestamp;
        
    }
    
    /**
     * Prepare the transaction for a {@link #commit()}.
     * 
     * @exception IllegalStateException
     *                if the transaction is not active.  If the transaction is
     *                not complete, then it will be aborted.
     */
    public void prepare() {

        if( ! isActive() ) {
            
            if( ! isComplete() ) {
                
                abort();
                
            }
            
            throw new IllegalStateException(NOT_ACTIVE);
            
        }

        try {

            /*
             * Validate against the current state of the journal's object index.
             */

            if( ! validate() ) {
                
                abort();
                
                throw new RuntimeException("Validation failed: write-write conflict");
                
            }
            
        } catch( Throwable t ) {
            
            abort();
            
            throw new RuntimeException("Unexpected error: "+t, t);
            
        }

        journal.prepared(this);

        runState = RunState.PREPARED;
        
    }
    
    /**
     * Commit the transaction.
     * 
     * @exception IllegalStateException
     *                if the transaction has not been
     *                {@link #prepare() prepared}. If the transaction is not
     *                already complete, then it is aborted.
     */
    public void commit() {

        if( ! isPrepared() ) {
            
            if( ! isComplete() ) {
                
                abort();
                
            }
            
            throw new IllegalStateException(NOT_PREPARED);
            
        }

        journal.completedTx(this);

        /*
         * Merge the object index into the global scope. This also marks the
         * slots used by the versions written by the transaction as 'committed'.
         * This operation MUST succeed since we have already validated.
         * 
         * Note: non-transactional operations on the global scope should be
         * disallowed when using transactions since they (a) could invalidate
         * the pre-condition for the merge; and (b) uncommitted changes would be
         * discarded if the merge operation fails.
         */
        try {

            mergeOntoGlobalState();
            
            // Atomic commit.
            journal.commit();
            
        } catch( Throwable t) {

            /*
             * If the operation fails then we need to discard any changes that
             * have been merged down into the global state. Failure to do this
             * will result in those changes becoming restart-safe when the next
             * transaction commits!
             * 
             * This will be legal if we are observing serializability; that is,
             * in no one writes on the global state for a restart-safe btree
             * except mergeOntoGlobalState(). When this constraint is observed
             * it is impossible for there to be uncommitted changes when we
             * begin to merge down onto the store and any changes may simply be
             * discarded.
             * 
             * Note: we can not simply reload the current root block (or reset
             * the nextOffset to be assigned) since concurrent transactions may
             * be writing non-restart safe data on the store in their own
             * isolated btrees.
             */
            journal._discardCommitters(); 

            releaseBTrees();
            
            throw new RuntimeException( t );
            
        }
        
        runState = RunState.COMMITTED;
        
    }

    /**
     * Abort the transaction.
     * 
     * @exception IllegalStateException
     *                if the transaction is already complete.
     */
    public void abort() {

        if (isComplete())
            throw new IllegalStateException(IS_COMPLETE);

        releaseBTrees();
        
        journal.completedTx(this);

        runState = RunState.ABORTED;
        
    }

    /**
     * A transaction is "active" when it is created and remains active until it
     * prepares or aborts.  An active transaction accepts READ, WRITE, DELETE,
     * PREPARE and ABORT requests.
     * 
     * @return True iff the transaction is active.
     */
    public boolean isActive() {
        
        return runState == RunState.ACTIVE;
        
    }
    
    /**
     * A transaction is "prepared" once it has been successfully validated and
     * has fulfilled its pre-commit contract for a multi-stage commit protocol.
     * An prepared transaction accepts COMMIT and ABORT requests.
     * 
     * @return True iff the transaction is prepared to commit.
     */
    public boolean isPrepared() {
        
        return runState == RunState.PREPARED;
        
    }
    
    /**
     * A transaction is "complete" once has either committed or aborted. A
     * completed transaction does not accept any requests.
     * 
     * @return True iff the transaction is completed.
     */
    public boolean isComplete() {
        
        return runState == RunState.COMMITTED || runState == RunState.ABORTED;
        
    }

    /**
     * A transaction is "committed" iff it has successfully committed. A
     * committed transaction does not accept any requests.
     * 
     * @return True iff the transaction is committed.
     */
    public boolean isCommitted() {
        
        return runState == RunState.COMMITTED;
        
    }
 
    /**
     * A transaction is "aborted" iff it has successfully aborted. An aborted
     * transaction does not accept any requests.
     * 
     * @return True iff the transaction is aborted.
     */
    public boolean isAborted() {
        
        return runState == RunState.ABORTED;
        
    }

    /**
     * Validate all isolated btrees written on by this transaction.
     */
    private boolean validate() {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Merge down the write set from all isolated btrees written on by this
     * transactions into the corresponding btrees in the global state.
     */
    private void mergeOntoGlobalState() {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Return a named btree. The btree will be isolated at the same level as
     * this transaction. Changes on the btree will be made restart-safe iff the
     * transaction successfully commits.
     * 
     * @param name The name of the btree.
     * 
     * @return The named btree or <code>null</code> if no btree was registered
     *         under that name.
     */
    public BTree getIndex(String name) {

        if(name==null) throw new IllegalArgumentException();
        
        /*
         * store the btrees in hash map so that we can recover the same instance
         * on each call within the same transaction.
         */
        BTree btree = btrees.get(name);
        
        if(btree == null) {
            
            /*
             * FIXME isolate the same named btree in the global scope and store
             * it in the map.
             */
            throw new UnsupportedOperationException();
            
        }
        
        return btree;
        
    }

    /**
     * This method must be invoked any time a transaction completes (aborts or
     * commits) in order to release the hard references to any named btrees
     * isolated within this transaction so that the JVM may reclaim the space
     * allocated to them on the heap.
     */
    private void releaseBTrees() {

        btrees.clear();
        
    }
    
}
