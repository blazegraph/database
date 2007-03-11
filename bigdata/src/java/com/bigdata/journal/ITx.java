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
 * Created on Oct 25, 2006
 */

package com.bigdata.journal;

import com.bigdata.isolation.IsolatedBTree;
import com.bigdata.objndx.IIndex;

/**
 * Interface for transactional reading and writing of persistent data.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ITx extends IIndexStore {

    /**
     * The start time for the transaction as assigned by a centralized
     * transaction manager service. Transaction start times are unique and also
     * serve as transaction identifiers. Note that this is NOT the time at which
     * a transaction begins executing on a specific journal as the same
     * transaction may start at different moments on different journals and
     * typically will only start on some journals rather than all.
     * 
     * @return The transaction start time.
     */
    public long getStartTimestamp();

    /**
     * Return the commit timestamp assigned to this transaction by a centralized
     * transaction manager service.
     * 
     * @return The commit timestamp assigned to this transaction.
     * 
     * @exception UnsupportedOperationException
     *                unless the transaction is writable.
     * 
     * @exception IllegalStateException
     *                if the transaction is writable but has not yet prepared (
     *                the commit time is assigned when the transaction is
     *                prepared).
     */
    public long getCommitTimestamp();
    
    /**
     * Prepare the transaction for a {@link #commit()} by validating the write
     * set for each index isolated by the transaction.
     * 
     * @param commitTime
     *            The commit time assigned by a centralized transaction manager
     *            service -or- ZERO (0L) IFF the transaction is read-only.
     * 
     * @exception IllegalStateException
     *                if the transaction is not active. If the transaction is
     *                not complete, then it will be aborted.
     * 
     * @exception ValidationError
     *                If the transaction can not be validated. If this exception
     *                is thrown, then the transaction was aborted.
     */
    public void prepare(long commitTime);

    /**
     * Commit a transaction that has already been {@link #prepare(long)}d.
     * 
     * @return The commit time assigned to the transactions -or- 0L if the
     *         transaction was read-only.
     * 
     * @exception IllegalStateException
     *                If the transaction has not {@link #prepare(long) prepared}.
     *                If the transaction is not already complete, then it is
     *                aborted.
     * 
     * @return The commit timestamp assigned by a centralized transaction
     *         manager service or <code>0L</code> if the transaction was
     *         read-only.
     */
    public long commit();

    /**
     * Abort the transaction.
     * 
     * @exception IllegalStateException
     *                if the transaction is already complete.
     */
    public void abort();

    /**
     * When true, the transaction will reject writes.
     */
    public boolean isReadOnly();
    
    /**
     * A transaction is "active" when it is created and remains active until it
     * prepares or aborts.  An active transaction accepts READ, WRITE, DELETE,
     * PREPARE and ABORT requests.
     * 
     * @return True iff the transaction is active.
     */
    public boolean isActive();

    /**
     * A transaction is "prepared" once it has been successfully validated and
     * has fulfilled its pre-commit contract for a multi-stage commit protocol.
     * An prepared transaction accepts COMMIT and ABORT requests.
     * 
     * @return True iff the transaction is prepared to commit.
     */
    public boolean isPrepared();

    /**
     * A transaction is "complete" once has either committed or aborted. A
     * completed transaction does not accept any requests.
     * 
     * @return True iff the transaction is completed.
     */
    public boolean isComplete();

    /**
     * A transaction is "committed" iff it has successfully committed. A
     * committed transaction does not accept any requests.
     * 
     * @return True iff the transaction is committed.
     */
    public boolean isCommitted();

    /**
     * A transaction is "aborted" iff it has successfully aborted. An aborted
     * transaction does not accept any requests.
     * 
     * @return True iff the transaction is aborted.
     */
    public boolean isAborted();

    /**
     * Return an isolated view onto a named index. Writes on the returned index
     * will be isolated in an {@link IsolatedBTree}. Reads that miss on the
     * {@link IsolatedBTree} will read through named index as of the ground
     * state of this transaction.
     * <p>
     * During {@link #prepare(long)}, the write set of each
     * {@link IsolatedBTree} will be validated against the then current commited
     * state of the named index.
     * <p>
     * During {@link #commit()}, the validated write sets will be merged down
     * onto the then current committed state of the named index.
     */
    public IIndex getIndex(String name);
    
}
