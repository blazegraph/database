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
import java.util.Iterator;
import java.util.Map;

import com.bigdata.isolation.IsolatedBTree;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.objndx.BTree;
import com.bigdata.objndx.IIndex;
import com.bigdata.objndx.IndexSegment;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.SimpleMemoryRawStore;
import com.bigdata.scaleup.MetadataIndex;
import com.bigdata.scaleup.PartitionedIndex;

/**
 * <p>
 * A transaction. A transaction is a context in which the application can access
 * named indices, and perform operations on those indices, and the operations
 * will be isolated according to the isolation level of the transaction. When
 * using an isolated transaction, writes are accumulated in an
 * {@link IsolatedBTree}. The write set is validated when the transaction
 * {@link #prepare()}s and finally merged down onto the global state when the
 * transaction commits.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Transactions may be requested that are read-only for some historical
 *       timestamp, that are read-committed (data committed by _other_
 *       transactions during the transaction will be visible within that
 *       transaction), or that are fully isolated (changes made in other
 *       transactions are not visible within the transaction).
 * 
 * @todo The write set of a transaction is currently written onto an independent
 *       store. This means that concurrent transactions can actually execute
 *       concurrently. We do not even need a read-lock on the indices isolated
 *       by the transaction since they are read-only. This might prove to be a
 *       nice way to leverage multiple processors / cores on a data server.
 * 
 * @todo support {@link PartitionedIndex}es.
 * 
 * @todo Make the {@link IsolatedBTree}s safe across {@link Journal#overflow()}
 *       events. When {@link PartitionedIndex}es are used this adds a
 *       requirement for tracking which {@link IndexSegment}s and
 *       {@link Journal}s are required to support the {@link IsolatedBTree}.
 *       Deletes of old journals and index segments must be deferred until no
 *       transaction remains which can read those data. This metadata must be
 *       restart-safe so that resources are eventually deleted. On restart,
 *       active transactions will abort and their resources may be released.
 *       There is also a requirement for quickly locating the specific journal
 *       and index segments required to support isolation of an index. This
 *       probably means an index into the history of the {@link MetadataIndex}
 *       (so we don't throw it away until no transactions can reach back that
 *       far) as well as an index into the named indices index -- perhaps simply
 *       an index by timestamp into the root addresses (or whole root block
 *       views).
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
    
    /*
     * 
     */
    final private Journal journal;
    
    /**
     * The timestamp assigned to this transaction. 
     */
    final private long timestamp;
    
    /**
     * The commit counter on the journal as of the time that this transaction
     * object was created.
     */
    final private long commitCounter;

    private RunState runState;

    /**
     * A store used to hold write sets for the transaction.  The same store can
     * be used to buffer resolved write-write conflicts.
     * 
     * @todo This uses a memory-based store to avoid issues with maintaining
     *       transactions across journal boundaries. This could be improved on
     *       trivially by transparently promoting the store from memory-based to
     *       disk-based if it overflows some set maximum capacity. In such a
     *       scenario, the file backing the on disk store would be flagged for
     *       deletion on exit of the JVM and allocated in a temporary directory.<br>
     *       A further improvement would allow transactions to use partitioned
     *       indices.
     */
    final private IRawStore tmpStore = new SimpleMemoryRawStore();
    
    /**
     * BTrees isolated by this transactions.
     * 
     * @todo in order to survive overflow this mapping must be persistent.
     */
    private Map<String,IsolatedBTree> btrees = new HashMap<String,IsolatedBTree>();
    
    /**
     * Return a named index. The index will be isolated at the same level as
     * this transaction. Changes on the index will be made restart-safe iff the
     * transaction successfully commits.
     * 
     * @param name
     *            The name of the index.
     * 
     * @return The named index or <code>null</code> if no index is registered
     *         under that name.
     */
    public IIndex getIndex(String name) {

        if(name==null) throw new IllegalArgumentException();
        
        /*
         * store the btrees in hash map so that we can recover the same instance
         * on each call within the same transaction.
         */
        BTree btree = btrees.get(name);
        
        if(btree == null) {
            
            /*
             * see if the index was registered.
             * 
             * FIXME this gets the last committed unisolated index. We need to
             * add a timestamp parameter and look up the appropriate metadata
             * record based on both the name and the timestamp (first metadata
             * record for the named index having a timestamp LT the transaction
             * timestamp).  since this is always a request for historical and
             * read-only data, this method should not register a committer and
             * the returned btree should not participate in the commit protocol.
             */
            UnisolatedBTree src = (UnisolatedBTree)journal.getIndex(name);
            
            // the named index was never registered.
            if(name==null) return null;
            
            /*
             * Isolate the named btree.
             */
            return new IsolatedBTree(tmpStore,src);
            
            
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
        
        if (journal == null)
            throw new IllegalArgumentException();
        
        this.journal = journal;
        
        this.timestamp = timestamp;

        journal.activateTx(this);

        /*
         * Stash the commit counter so that we can figure out if there were
         * concurrent transactions and therefore whether or not we need to
         * validate the write set.
         */
        this.commitCounter = journal.getRootBlockView().getCommitCounter();
        
        this.runState = RunState.ACTIVE;

    }
    
    /**
     * The transaction identifier (aka timestamp).
     * 
     * @return The transaction identifier (aka timestamp).
     */
    final public long getId() {
        
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
     * <p>
     * Commit the transaction.
     * </p>
     * <p>
     * Note: You MUST {@link #prepare()} a transaction before you
     * {@link #commit()} that transaction. This requirement exists as a
     * placeholder for a 2-phase commit protocol for use with distributed
     * transactions.
     * </p>
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

        /*
         * Merge each isolated index into the global scope. This also marks the
         * slots used by the versions written by the transaction as 'committed'.
         * This operation MUST succeed (at a logical level) since we have
         * already validated (neither read-write nor write-write conflicts
         * exist).
         * 
         * @todo Non-transactional operations on the global scope should be
         * either disallowed entirely or locked out during the prepare-commit
         * protocol when using transactions since they (a) could invalidate the
         * pre-condition for the merge; and (b) uncommitted changes would be
         * discarded if the merge operation fails.  One solution is to use
         * batch operations or group commit mechanism to dynamically create
         * transactions from unisolated operations.
         */
        try {

            mergeOntoGlobalState();
            
            // Atomic commit.
            journal.commit(this);
            
            runState = RunState.COMMITTED;

            journal.completedTx(this);
            
        } catch( Throwable t) {

            /*
             * If the operation fails then we need to discard any changes that
             * have been merged down into the global state. Failure to do this
             * will result in those changes becoming restart-safe when the next
             * transaction commits!
             * 
             * Discarding registered committers is legal if we are observing
             * serializability; that is, if no one writes on the global state
             * for a restart-safe btree except mergeOntoGlobalState(). When this
             * constraint is observed it is impossible for there to be
             * uncommitted changes when we begin to merge down onto the store
             * and any changes may simply be discarded.
             * 
             * Note: we can not simply reload the current root block (or reset
             * the nextOffset to be assigned) since concurrent transactions may
             * be writing non-restart safe data on the store in their own
             * isolated btrees.
             */

            journal.abort(); 

            abort();
            
            throw new RuntimeException( t );
            
        } finally {

            releaseBTrees();
            
        }
        
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

        try {

            runState = RunState.ABORTED;

            journal.completedTx(this);
            
        } finally {
            
            releaseBTrees();

        }

    }

    /**
     * A transaction is "active" when it is created and remains active until it
     * prepares or aborts.  An active transaction accepts READ, WRITE, DELETE,
     * PREPARE and ABORT requests.
     * 
     * @return True iff the transaction is active.
     */
    final public boolean isActive() {
        
        return runState == RunState.ACTIVE;
        
    }
    
    /**
     * A transaction is "prepared" once it has been successfully validated and
     * has fulfilled its pre-commit contract for a multi-stage commit protocol.
     * An prepared transaction accepts COMMIT and ABORT requests.
     * 
     * @return True iff the transaction is prepared to commit.
     */
    final public boolean isPrepared() {
        
        return runState == RunState.PREPARED;
        
    }
    
    /**
     * A transaction is "complete" once has either committed or aborted. A
     * completed transaction does not accept any requests.
     * 
     * @return True iff the transaction is completed.
     */
    final public boolean isComplete() {
        
        return runState == RunState.COMMITTED || runState == RunState.ABORTED;
        
    }

    /**
     * A transaction is "committed" iff it has successfully committed. A
     * committed transaction does not accept any requests.
     * 
     * @return True iff the transaction is committed.
     */
    final public boolean isCommitted() {
        
        return runState == RunState.COMMITTED;
        
    }
 
    /**
     * A transaction is "aborted" iff it has successfully aborted. An aborted
     * transaction does not accept any requests.
     * 
     * @return True iff the transaction is aborted.
     */
    final public boolean isAborted() {
        
        return runState == RunState.ABORTED;
        
    }

    /**
     * Validate all isolated btrees written on by this transaction.
     */
    private boolean validate() {
        
        if(journal.getRootBlockView().getCommitCounter() != commitCounter) {
            
            /*
             * This compares the timestamp of the last transaction committed on
             * the journal with the timestamp of the last transaction committed
             * on the journal as of the time that this transaction object was
             * created. In a centralized database archicture this is a sufficient
             * test to determine that no intevening commits have occured.
             */
            
            return true;
            
        }
        
        /*
         * for all isolated btrees, if(!validate()) return false;
         */
        Iterator<Map.Entry<String,IsolatedBTree>> itr = btrees.entrySet().iterator();
        
        while(itr.hasNext()) {
            
            Map.Entry<String, IsolatedBTree> entry = itr.next();
            
            String name = entry.getKey();
            
            IsolatedBTree isolated = entry.getValue();
            
            UnisolatedBTree groundState = (UnisolatedBTree)journal.getIndex(name);
            
            if(!isolated.validate(groundState)) return false;
            
        }
         
        return true;

    }
    
    /**
     * Merge down the write set from all isolated btrees written on by this
     * transactions into the corresponding btrees in the global state.
     */
    private void mergeOntoGlobalState() {
    
        Iterator<Map.Entry<String,IsolatedBTree>> itr = btrees.entrySet().iterator();
        
        while(itr.hasNext()) {
            
            Map.Entry<String, IsolatedBTree> entry = itr.next();
            
            String name = entry.getKey();
            
            IsolatedBTree isolated = entry.getValue();
            
            UnisolatedBTree groundState = (UnisolatedBTree)journal.getIndex(name);
            
            isolated.mergeDown(groundState);
            
        }

    }

}
