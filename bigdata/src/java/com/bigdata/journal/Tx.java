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
import java.util.HashMap;
import java.util.Map;

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
 * @todo Define isolation for the allocation index. We can actually ignore the
 *       problem and only sacrifice the ability to reuse slots allocated to data
 *       versions within the transaction that are subsequently updated or
 *       deleted within the same transaction. This is a special case and
 *       doubtless of benefit, but we can get by with a flag bit set if we
 *       ignore that case.
 * 
 * @todo Define the commit procotol.
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

    final Journal journal;
    final private long timestamp;
    final private Map<Integer,Integer> baseObjectIndex;
    final private Map<Integer,Integer> objectIndex;

    public Tx(Journal journal, long timestamp ) {
        
        if( journal == null ) throw new IllegalArgumentException();
        
        this.journal = journal;
        
        this.timestamp = timestamp;

        /* FIXME This can work, but a COMMIT MUST replace the object
         * index on the journal so that this transaction continues to
         * resolve objects against the object index for the last committed
         * state at the time that the transaction was created.
         */
        this.baseObjectIndex = journal.objectIndex;
        
        this.objectIndex = new HashMap<Integer,Integer>();

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
     */
    public ByteBuffer read( int id, ByteBuffer dst ) {
        
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
     */
    public void write(Tx tx,int id,ByteBuffer data) {

        journal.write( this, id, data );
    
    }
    
    /**
     * Delete the data from the store.
     * 
     * @param id
     *            The int32 within-segment persistent identifier.
     *            
     * @exception IllegalArgumentException
     *                if the persistent identifier is bad.
     */
    public void delete( int id) {

        journal.delete( this, id );
        
    }
    
    /**
     * Prepare the transaction for a {@link #commit()}.
     * 
     * @exception IllegalStateException
     *                if the transaction is not active.
     */
    public void prepare() {
        
        throw new UnsupportedOperationException();
        
    }
    
    /**
     * Commit the transaction.
     * 
     * @exception IllegalStateException
     *                if the transaction has not been
     *                {@link #prepare() prepared}.
     */
    public void commit() {
        
        throw new UnsupportedOperationException();
        
    }
    
    /**
     * Abort the transaction.
     * 
     * @exception IllegalStateException
     *                if the transaction is not active.
     */
    public void abort() {
        
        throw new UnsupportedOperationException();
        
    }

}
