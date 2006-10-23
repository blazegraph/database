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

import java.nio.ByteBuffer;

/**
 * <p>
 * A transaction. An instance of this class corresponds to a transaction.
 * Transactions are fully isolated.
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
 * FIXME The various public methods on this API that have {@link RunState}
 * constraints all eagerly force an abort when invoked from an illegal state.
 * This is, perhaps, excessive. Futher, since this is used in a single-threaded
 * server context, we are better off testing for illegal conditions and
 * notifying clients without out generating expensive stack traces. This could
 * be done by return flags or by the server checking pre-conditions itself and
 * exceptions being thrown from here if the server failed to test the
 * pre-conditions and they were not met
 * 
 * @todo Define isolation for the allocation index. We can actually ignore the
 *       problem and only sacrifice the ability to reuse slots allocated to data
 *       versions within the transaction that are subsequently updated or
 *       deleted within the same transaction. This is a special case and
 *       doubtless of benefit, but we can get by with a flag bit set if we
 *       ignore that case.
 * 
 * @todo Define the commit procotol.
 * 
 * @todo Is it possible to have more than one transaction PREPARE must
 *       concurrent PREPARE operations be serialized?
 * 
 * @todo Define the abort protocol. On abort, the root block is simply not
 *       updated, all current data versions in the transaction scope are marked
 *       as available, and the slots dedicated to the transaction scope object
 *       index nodes are themselves marked as available. (This could be done
 *       asynchronously but we have not yet defined a slot reaper. This will
 *       have to be interleaved with the thread performing operations on the
 *       journal. So, either they are the same thread that somehow switches
 *       tasks, or the journal is handed off between threads based on workload
 *       (sounds good), or we have to make the journal thread safe.)
 * 
 * @todo Define a mechanism for generating transaction identifiers. They could
 *       be UUIDs, which would make it very easy to create them but we would
 *       have to transmit transaction metadata for the concurrency control (CC)
 *       strategy, especially the timestamp. Typically the transaction
 *       identifier is simply a timestamp, but that can be overly constraining
 *       depending on the CC strategy. Since we probably need better than millis
 *       resolution, we could shift out the high bytes and bring in some nanos.
 *       We also need to bring in host information or have a centralized server
 *       that generates transaction identifiers that are guarenteed to be unique
 *       (pay a little in latency, but easy to do).
 * 
 * @todo This implementation actually uses a two level transient hash map for
 *       the object index (this is not restart safe). The first level is the
 *       transaction scope object index. All changes are made to that index. If
 *       an object is not found in that index, then we read the entry from the
 *       base object index. If it is not found there, then it is not found
 *       period.
 */

public class Tx {

    /*
     * Text for error messages.
     */
    final static String NOT_ACTIVE = "Not active";
    final static String NOT_PREPARED = "Transaction is not prepared";
    final static String NOT_COMMITTED = "Transaction is not committed";
    final static String IS_COMPLETE = "Transaction is complete";
    
    private enum RunState {
        ACTIVE("active"),
        PREPARED("prepared"),
        COMMITTED("committed"),
        ABORTED("aborted");
//        ACTIVE("active",0,true),
//        PREPARING("preparing",1,false),
//        PREPARED("prepared",1,true),
//        COMMITTING("committing",2,false),
//        COMMITTED("committed",2,true),
//        ABORTING("aborting",2,false),
//        ABORTED("aborted",2,false);
        private final String name;
//        private final int order;
//        private final boolean stable;
        RunState(String name/*,int order,boolean stable*/) {
            this.name = name;
//            this.order = order;
//            this.stable = stable;
        }
        public String toString() {
            return name;
        }
//        public int getOrder() {
//            return order;
//        }
//        public boolean isStable() {
//            return stable;
//        }
    }
    
    final Journal journal;
    final private long timestamp;
    final SimpleObjectIndex objectIndex;
    
    private RunState runState;

    public Tx(Journal journal, long timestamp ) {
        
        if( journal == null ) throw new IllegalArgumentException();
        
        this.journal = journal;
        
        this.timestamp = timestamp;

        /* FIXME This can work, but a COMMIT MUST replace the object
         * index on the journal so that this transaction continues to
         * resolve objects against the object index for the last committed
         * state at the time that the transaction was created.
         */
        this.objectIndex = new SimpleObjectIndex(
                (SimpleObjectIndex) journal.objectIndex);

        journal.activatingTx(this);
        
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
     * Read the current version of the data from the store.
     * 
     * @param id
     *            The int32 within-segment persistent identifier.
     * @param dst
     *            When non-null and having sufficient bytes remaining, the data
     *            version will be read into this buffer. If null or if the
     *            buffer does not have sufficient bytes remaining, then a new
     *            (non-direct) buffer will be allocated that is right-sized for
     *            the data version, the data will be read into that buffer, and
     *            the buffer will be returned to the caller.
     * 
     * @return The data. The position will always be zero if a new buffer was
     *         allocated. Otherwise, the position will be invariant across this
     *         method. The limit - position will be the #of bytes read into the
     *         buffer, starting at the position. A <code>null</code> return
     *         indicates that the object was not found in the journal, in which
     *         case the application MUST attempt to resolve the object against
     *         the database (i.e., the object MAY have been migrated onto the
     *         database and the version logically deleted on the journal).
     * 
     * @exception DataDeletedException
     *                if the current version of the identifier data has been
     *                deleted within the scope visible to the transaction. The
     *                caller MUST NOT read through to the database if the data
     *                were deleted.
     * 
     * @exception IllegalStateException
     *                if the transaction is not active.  If the transaction is
     *                not complete, then it will be aborted.
     */
    public ByteBuffer read( int id, ByteBuffer dst ) {

        if( ! isActive() ) {
            
            if( ! isComplete() ) {
                
                abort();
                
            }
            
            throw new IllegalStateException(NOT_ACTIVE);
            
        }
        
        return journal.read( this, id, dst );
        
    }

    /**
     * Write a data version. The data version of the data will not be visible
     * outside of this transaction until the transaction is committed.
     * 
     * @param id
     *            The int32 within-segment persistent identifier.
     * @param data
     *            The data to be written. The bytes from
     *            {@link ByteBuffer#position()} to {@link ByteBuffer#limit()}
     *            will be written.
     *            
     * @exception DataDeletedException
     *                if the persistent identifier is deleted.
     *                
     * @exception IllegalStateException
     *                if the transaction is not active.  If the transaction is
     *                not complete, then it will be aborted.
     */
    public void write(int id,ByteBuffer data) {

        if( ! isActive() ) {
            
            if( ! isComplete() ) {
                
                abort();
                
            }
            
            throw new IllegalStateException(NOT_ACTIVE);
            
        }

        journal.write( this, id, data );
    
    }
    
    /**
     * Delete the data from the store.
     * 
     * @param id
     *            The int32 within-segment persistent identifier.
     *            
     * @exception DataDeletedException
     *                if the persistent identifier is already deleted.
     *                
     * @exception IllegalStateException
     *                if the transaction is not active.  If the transaction is
     *                not complete, then it will be aborted.
     */
    public void delete( int id) {

        if( ! isActive() ) {
            
            if( ! isComplete() ) {
                
                abort();
                
            }
            
            throw new IllegalStateException(NOT_ACTIVE);
            
        }

        journal.delete( this, id );
        
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

        journal.preparingTx(this);

        try {

            /*
             * Validate against the current state of the journal's object index.
             * 
             * Note: This is NOT always the same as the inner object index map
             * used by normal the transaction since other transactions MAY have
             * committed on the journal since the transaction started!
             */
            objectIndex.validate(journal.objectIndex);
            
        } catch( ValidationError ex ) {
            
            if( ! isAborted() ) {
                
                abort();
                
            }

            throw ex;
            
        } catch( Throwable t ) {
            
            if( ! isAborted() ) {
                
                abort();
                
            }
            
            throw new ValidationError("Unexpected error: "+t, t);
            
        }
        
        runState = RunState.PREPARED;
        
    }
    
    /**
     * @todo Notion exception thrown for normal validation errors.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ValidationError extends RuntimeException {
        
        private static final long serialVersionUID = 6662456627493976835L;

        public ValidationError(String msg) {super(msg);}
        
        public ValidationError(Throwable cause) {super(cause);}

        public ValidationError(String msg, Throwable cause) {super(msg,cause);}
        
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

        journal.completingTx(this);

        /*
         * Merge the object index into the global scope. This also marks the
         * slots used by the versions written by the transaction as 'committed'.
         * This operation MUST succeed since we have already validated.
         * 
         * FIXME This MUST be ATOMIC.
         * 
         * FIXME Note that non-transactional operations on the global scope
         * should probably be disallowed if they would conflict with a prepared
         * transaction, otherwise this merge operation would not have its
         * pre-conditions satisified.
         */
        objectIndex.mergeWithGlobalObjectIndex(journal);

        writeCommitRecord();
        
        runState = RunState.COMMITTED;
        
    }

    /**
     * FIXME Write commit record, including: the transaction identifier, the
     * location of the object index and the slot allocation index, the location
     * of a run length encoded representation of slots allocated by this
     * transaction, and the identifier and location of the prior transaction
     * serialized on this journal.
     */
    private void writeCommitRecord() {
        
    }
    
    /**
     * Abort the transaction.
     * 
     * @exception IllegalStateException
     *                if the transaction is already complete.
     */
    public void abort() {

        if( isComplete() ) throw new IllegalStateException(IS_COMPLETE);

        journal.completingTx(this);

        /*
         * FIXME Implement abort. There are some deallocation operations that
         * need to be performed for an abort, including slot allocation index
         * and object index nodes in addition to the data version slots.
         */
        
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
     * Garbage collect pre-existing versions that were overwritten or deleted
     * during this transactions. This method MUST NOT be invoked by the
     * application since its pre-conditions require total knowledge of the state
     * of transactions running against the distributed database. That knowledge
     * is available for the journal locally IFF it is running as a standalone /
     * embedded database. Otherwise the knowledge is only available to the
     * distributed transaction server.
     * 
     * @exception IllegalStateException
     *                if the transaction has not committed.
     * 
     * @todo When a pre-existing version is deleted within a transaction scope
     *       and the transaction later commits and is finally GC'd, document
     *       whether or not tge GC will cause the index to report "not found" as
     *       a post-condition rather than "deleted".
     */
    void gc() {

        if( ! isCommitted() ) throw new IllegalStateException(NOT_COMMITTED);
        
        objectIndex.gc(journal.allocationIndex);
        
    }

}
