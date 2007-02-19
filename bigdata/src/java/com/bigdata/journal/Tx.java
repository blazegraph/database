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
import com.bigdata.objndx.IIndex;
import com.bigdata.objndx.IndexSegment;
import com.bigdata.objndx.ReadOnlyIndex;
import com.bigdata.scaleup.MetadataIndex;
import com.bigdata.scaleup.PartitionedIndex;

/**
 * <p>
 * A transaction. A transaction is a context in which the application can access
 * named indices, and perform operations on those indices, and the operations
 * will be isolated according to the isolation level of the transaction. When
 * using a writable isolated transaction, writes are accumulated in an
 * {@link IsolatedBTree}. The write set is validated when the transaction
 * {@link #prepare()}s and finally merged down onto the global state when the
 * transaction commits. When the transaction is read-only, writes will be
 * rejected and {@link #prepare()} and {@link #commit()} are NOPs.
 * </p>
 * <p>
 * The write set of a transaction is written onto a {@link TemporaryStore}.
 * Therefore the size limit on the transaction write set is currently 2G, but
 * the transaction will run in memory up to 100M. The {@link TemporaryStore} is
 * closed and any backing file is deleted as soon as the transaction completes.
 * </p>
 * <p>
 * Each {@link IsolatedBTree} is local to a transaction and is backed by its own
 * store. This means that concurrent transactions can execute without
 * synchronization (real concurrency) up to the point where they
 * {@link #prepare()}. We do not need a read-lock on the indices isolated by
 * the transaction since they are <em>historical</em> states that will not
 * receive concurrent updates. This might prove to be a nice way to leverage
 * multiple processors / cores on a data server.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Support read-committed transactions (data committed by _other_
 *       transactions during the transaction will be visible within that
 *       transaction). Read-committed transactions do NOT permit writes (they
 *       are read-only). Prepare and commit are NOPs. This might be a distinct
 *       implementation sharing a common base class for handling the run state
 *       stuff, or just a distinct implementation altogether. The read-committed
 *       transaction is implemented by just reading against the named indices on
 *       the journal. However, since commits or overflows of the journal might
 *       invalidate the index objects we may have to setup a delegation
 *       mechanism that resolves the named index either on each operation or
 *       whenever the transaction receives notice that the index must be
 *       discarded. In order to NOT see in-progress writes, the read-committed
 *       transaction actually needs to dynamically resolve the most recent
 *       {@link ICommitRecord} and then use the named indices resolved from
 *       that. This suggests an event notification mechanism for commits so that
 *       we can get the commit record more cheaply.
 * 
 * @todo Support transactions where the indices isolated by the transactions are
 *       {@link PartitionedIndex}es.
 * 
 * @todo Track which {@link IndexSegment}s and {@link Journal}s are required
 *       to support the {@link IsolatedBTree}s in use by a {@link Tx}. Deletes
 *       of old journals and index segments MUST be deferred until no
 *       transaction remains which can read those data. This metadata must be
 *       restart-safe so that resources are eventually deleted. On restart,
 *       active transactions will have been discarded abort and their resources
 *       released. (Do we need a restart-safe means to indicate the set of
 *       running transactions?)<br>
 *       There is also a requirement for quickly locating the specific journal
 *       and index segments required to support isolation of an index. This
 *       probably means an index into the history of the {@link MetadataIndex}
 *       (so we don't throw it away until no transactions can reach back that
 *       far) as well as an index into the named indices index -- perhaps simply
 *       an index by startTimestamp into the root addresses (or whole root block
 *       views, or moving the root addresses out of the root block and into the
 *       store with only the address of the root addresses in the root block).
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
 * @todo PREPARE operations must be serialized unless they are provably
 *       non-overlapping. This will require a handshake with either the
 *       {@link Journal} or (more likely) the {@link TransactionServer}.
 */
public class Tx implements IStore, ITx {

    /*
     * Text for error messages.
     */
    final static String NOT_ACTIVE = "Not active";
    final static String NOT_PREPARED = "Transaction is not prepared";
    final static String NOT_COMMITTED = "Transaction is not committed";
    final static String IS_COMPLETE = "Transaction is complete";
    
    /**
     * The transaction uses the {@link Journal} for some handshaking in the
     * commit protocol and to locate the named indices that it isolates.
     */
    final protected Journal journal;
    
    /**
     * The start startTimestamp assigned to this transaction.
     * <p>
     * Note: Transaction {@link #startTimestamp} and {@link #commitTimestamp}s
     * are assigned by a global time service. The time service must provide
     * unique times for transaction start and commit timestamps and for commit
     * times for unisolated {@link Journal#commit()}s.
     */
    final protected long startTimestamp;
    
    /**
     * The commit startTimestamp assigned to this transaction.
     */
    private long commitTimestamp;
    
    /**
     * True iff the transaction is read only and will reject writes.
     */
    final protected boolean readOnly;
    
    /**
     * The commit counter on the journal as of the time that this transaction
     * object was created.
     */
    final protected long commitCounter;

    /**
     * The historical {@link ICommitRecord} choosen as the ground state for this
     * transaction. All indices isolated by this transaction are isolated as of
     * the discoverable root address based on this commit record.
     */
    final private ICommitRecord commitRecord;
    
    private RunState runState;

    /**
     * A store used to hold write sets for the transaction. The same store can
     * be used to buffer resolved write-write conflicts. This uses a
     * {@link TemporaryStore} to avoid issues with maintaining transactions
     * across journal boundaries. This places a limit on transactions of 2G in
     * their serialized write set. Since the indices use a copy-on-write model,
     * the amount of user data can be significantly less due to multiple
     * versions of the same btree nodes. Using smaller branching factors in the
     * isolated index helps significantly to increase the effective utilization
     * of the store since copy-on-write causes fewer bytes to be copied each
     * time it is invoked.
     */
    final protected TemporaryStore tmpStore = new TemporaryStore();
    
    /**
     * Indices isolated by this transactions.
     */
    private Map<String, IIndex> btrees = new HashMap<String, IIndex>();
    
    /**
     * Create a fully isolated read-write transaction.
     */
    public Tx(Journal journal,long timestamp) {
        
        this(journal,timestamp,false);
        
    }
    
    /**
     * Create a transaction starting the last committed state of the journal as
     * of the specified startTimestamp.
     * 
     * @param journal
     *            The journal.
     * 
     * @param startTimestamp
     *            The startTimestamp, which MUST be assigned consistently based on a
     *            {@link ITimestampService}. Note that a transaction does not
     *            start up on all {@link Journal}s at the same time. Instead,
     *            the transaction start startTimestamp is assigned by a centralized
     *            time service and then provided each time a transaction object
     *            must be created for isolated on some {@link Journal}.
     * 
     * @param readOnly
     *            When true the transaction will reject writes and
     *            {@link #prepare()} and {@link #commit()} will be NOPs.
     * 
     * @exception IllegalStateException
     *                if the transaction state has been garbage collected.
     */
    public Tx(Journal journal, long timestamp, boolean readOnly) {
        
        if (journal == null)
            throw new IllegalArgumentException();
        
        this.journal = journal;
        
        this.startTimestamp = timestamp;

        this.readOnly = readOnly;
        
        journal.activateTx(this);

        /*
         * Stash the commit counter so that we can figure out if there were
         * concurrent transactions and therefore whether or not we need to
         * validate the write set.
         */
        this.commitCounter = journal.getRootBlockView().getCommitCounter();

        /*
         * The commit record serving as the ground state for the indices
         * isolated by this transaction (MAY be null, in which case the
         * transaction will be unable to isolate any indices).
         */
        this.commitRecord = journal.getCommitRecord(timestamp);
        
        this.runState = RunState.ACTIVE;

    }

    /**
     * The hash code is based on the {@link #getStartTimestamp()}.
     * 
     * @todo pre-compute this value if it is used much.
     */
    final public int hashCode() {
        
        return Long.valueOf(startTimestamp).hashCode();
        
    }

    /**
     * True iff they are the same object or have the same start timestamp.
     * 
     * @param o
     *            Another transaction object.
     */
    final public boolean equals(ITx o) {
        
        return this == o || startTimestamp == o.getStartTimestamp();
        
    }
    
    /**
     * The transaction identifier.
     * 
     * @return The transaction identifier.
     * 
     * @todo verify that this has the semantics of the transaction start time
     *       and that the startTimestamp is (must be) assigned by the same service
     *       that assigns the {@link #getCommitTimestamp()}.
     */
    final public long getStartTimestamp() {
        
        return startTimestamp;
        
    }
    
    /**
     * Return the commit timestamp assigned to this transaction.
     * 
     * @return The commit timestamp assigned to this transaction. 
     * 
     * @exception IllegalStateException
     *                unless the transaction writable and {@link #isPrepared()}
     *                or {@link #isCommitted()}.
     */
    final public long getCommitTimestamp() {

        if(readOnly) {

            throw new IllegalStateException();

        }
        
        switch(runState) {
        case ACTIVE:
        case ABORTED:
            throw new IllegalStateException();
        case PREPARED:
        case COMMITTED:
            if(commitTimestamp == 0L) throw new AssertionError();
            return commitTimestamp;
        }
        
        throw new AssertionError();
        
    }
    
    public String toString() {
        
        return ""+startTimestamp;
        
    }
    
    /**
     * This method must be invoked any time a transaction completes ({@link #abort()}s
     * or {@link #commit()}s) in order to release resources held by that
     * transaction.
     */
    protected void releaseResources() {

        /*
         * Release hard references to any named btrees isolated within this
         * transaction so that the JVM may reclaim the space allocated to them
         * on the heap.
         */
        btrees.clear();
        
        /*
         * Close and delete the TemporaryStore.
         */
        if (tmpStore.isOpen()) {

            tmpStore.close();
            
        }
        
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

        if (!readOnly) {

            /*
             * The commit startTimestamp is assigned when we prepare the transaction
             * since the the commit protocol does not permit unisolated writes
             * once a transaction begins to prepar until the transaction has
             * either committed or aborted (if such writes were allowed then we
             * would have to re-validate the transaction in order to enforce
             * serializability).
             * 
             * @todo resolve this against a service in a manner that will
             * support a distributed database commit protocol.
             */
            commitTimestamp = journal.timestampFactory.nextTimestamp();

            try {

                /*
                 * Validate against the current state of the various indices
                 * on write the transaction has written.
                 */

                if (!validate()) {

                    abort();

                    throw new ValidationError();

                }

            } catch( ValidationError ex) {
                
                throw ex;
                
            } catch (Throwable t) {

                abort();

                throw new RuntimeException("Unexpected error: " + t, t);

            }

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
     * @return The commit timestamp or <code>0L</code> if the transaction was
     *         read-only.
     * 
     * @exception IllegalStateException
     *                if the transaction has not been
     *                {@link #prepare() prepared}. If the transaction is not
     *                already complete, then it is aborted.
     */
    public long commit() {

        if( ! isPrepared() ) {
            
            if( ! isComplete() ) {
                
                abort();
                
            }
            
            throw new IllegalStateException(NOT_PREPARED);
            
        }

        try {

            if(!readOnly) {

                /*
                 * Merge each isolated index into the global scope. This also
                 * marks the slots used by the versions written by the
                 * transaction as 'committed'. This operation MUST succeed (at a
                 * logical level) since we have already validated (neither
                 * read-write nor write-write conflicts exist).
                 * 
                 * @todo Non-transactional operations on the global scope should
                 * be either disallowed entirely or locked out during the
                 * prepare-commit protocol when using transactions since they
                 * (a) could invalidate the pre-condition for the merge; and (b)
                 * uncommitted changes would be discarded if the merge operation
                 * fails. One solution is to use batch operations or group
                 * commit mechanism to dynamically create transactions from
                 * unisolated operations.
                 */

                mergeOntoGlobalState();
            
                // Atomic commit.
                journal.commit(this);
                
            }
            
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

            releaseResources();
            
        }

        return readOnly ? 0L : getCommitTimestamp();
        
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
            
            releaseResources();

        }

    }

    final public boolean isReadOnly() {
        
        return readOnly;
        
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
 
        assert ! readOnly;
        
        /*
         * This compares the current commit counter on the journal with the
         * commit counter at the time that this transaction was started. If they
         * are the same, then no intervening commits have occurred on the
         * journal and there is nothing to validate.
         */
        
        if(journal.getRootBlockView().getCommitCounter() == commitCounter) {
            
            return true;
            
        }
        
        /*
         * for all isolated btrees, if(!validate()) return false;
         */

        Iterator<Map.Entry<String,IIndex>> itr = btrees.entrySet().iterator();
        
        while(itr.hasNext()) {
            
            Map.Entry<String, IIndex> entry = itr.next();
            
            String name = entry.getKey();
            
            IsolatedBTree isolated = (IsolatedBTree) entry.getValue();
            
            /*
             * Note: this is the _current_ committed state for the named index.
             * We need to validate against the current state, not against some
             * historical state.
             */
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

        assert ! readOnly;

        Iterator<Map.Entry<String,IIndex>> itr = btrees.entrySet().iterator();
        
        while(itr.hasNext()) {
            
            Map.Entry<String, IIndex> entry = itr.next();
            
            String name = entry.getKey();
            
            IsolatedBTree isolated = (IsolatedBTree) entry.getValue();
            
            /*
             * Note: this is the _current_ committed state for the named index.
             * We need to merge down onto the current state, not onto some
             * historical state.
             */
            UnisolatedBTree groundState = (UnisolatedBTree)journal.getIndex(name);

            /*
             * Copy the validated write set for this index down onto the
             * corresponding unisolated index, updating version counters, delete
             * markers, and values as necessary in the unisolated index.
             */
            isolated.mergeDown(groundState);
            
        }

    }

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
     * 
     * @exception IllegalStateException
     *                if the transaction is not active.
     */
    public IIndex getIndex(String name) {

        if (name == null)
            throw new IllegalArgumentException();

        if (!isActive()) {

            throw new IllegalStateException();

        }

        /*
         * Store the btrees in hash map so that we can recover the same instance
         * on each call within the same transaction.
         */
        IIndex index = btrees.get(name);
        
        if(commitRecord==null) {
            
            /*
             * This occurs when there are either no commit records or no
             * commit records before the start time for the transaction.
             */
            
            return null;
            
        }
        
        if(index == null) {
            
            /*
             * See if the index was registered as of the ground state used by
             * this transaction to isolated indices.
             */
            UnisolatedBTree src = (UnisolatedBTree) journal.getIndex(name,
                    commitRecord);
            
            // the named index was never registered.
            if(name==null) return null;
            
            /*
             * Isolate the named btree.
             */
            if(readOnly) {

                index = new ReadOnlyIndex(src);
                
            } else {
                
                index = new IsolatedBTree(tmpStore,src);
                
            }

            btrees.put(name, index);
            
        }
        
        return index;
        
    }

}
