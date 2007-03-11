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
 * Created on Feb 19, 2007
 */

package com.bigdata.journal;

import com.bigdata.isolation.IConflictResolver;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.objndx.IIndex;
import com.bigdata.objndx.IndexSegment;

/**
 * A client-facing interface for managing transaction life cycles.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ITransactionManager extends ITimestampService {

    /**
     * Create a new fully-isolated read-write transaction.
     * 
     * @return The transaction start time, which serves as the unique identifier
     *         for the transaction.
     */
    public long newTx();

    /**
     * Create a new transaction.
     * <p>
     * The concurrency control algorithm is MVCC, so readers never block and
     * only write-write conflicts can arise. It is possible to register an
     * {@link UnisolatedBTree} with an {@link IConflictResolver} in order to
     * present the application with an opportunity to validate write-write
     * conflicts using state-based techniques (i.e., by looking at the records
     * and timestamps and making an informed decision).
     * <p>
     * MVCC requires a strategy to release old versions that are no longer
     * accessible to active transactions. bigdata uses a highly efficient
     * technique in which writes are multiplexed onto append-only
     * {@link Journal}s and then evicted on overflow into {@link IndexSegment}s
     * using a bulk index build mechanism. Old journal and index segment
     * resources are simply deleted from the file system some time after they
     * are no longer accessible to active transactions.
     * 
     * @param level
     *            The isolation level. The following isolation levels are
     *            supported:
     *            <dl>
     *            <dt>{@link IsolationEnum#ReadCommitted}</dt>
     *            <dd>A read-only transaction in which data become visible
     *            within the transaction as concurrent transactions commit. This
     *            is suitable for very long read processes that do not require a
     *            fully consistent view of the data.</dd>
     *            <dt>{@link IsolationEnum#ReadOnly}</dt>
     *            <dd>A fully isolated read-only transaction.</dd>
     *            <dt>{@link IsolationEnum#ReadWrite}</dt>
     *            <dd>A fully isolated read-write transaction.</dd>
     *            </dl>
     * 
     * @return The transaction start time, which serves as the unique identifier
     *         for the transaction.
     */
    public long newTx(IsolationEnum level);
    
//    /**
//     * Create a new fully-isolated transaction.
//     * 
//     * @param readOnly
//     *            When true, the transaction will reject writes.
//     * 
//     * @return The transaction start time, which serves as the unique identifier
//     *         for the transaction.
//     */
//    public long newTx(boolean readOnly);
//
//    /**
//     * Create a new read-committed transaction. The transaction will reject
//     * writes. Any data committed by concurrent transactions will become visible
//     * to indices isolated by this transaction (hence, "read comitted").
//     * <p>
//     * This provides more isolation than "read dirty" since the concurrent
//     * transactions MUST commit before their writes become visible to the a
//     * read-committed transaction.
//     * 
//     * @return The transaction start time, which serves as the unique identifier
//     *         for the transaction.
//     */
//    public long newReadCommittedTx();
    
    /**
     * Abort the transaction.
     * 
     * @param startTime
     *            The transaction start time, which serves as the unique
     *            identifier for the transaction.
     * 
     * @exception IllegalStateException
     *                if there is no active transaction with that timestamp.
     */
    public void abort(long startTime);

    /**
     * Commit the transaction.
     * 
     * @param startTime
     *            The transaction start time, which serves as the unique
     *            identifier for the transaction.
     *            
     * @return The commit timestamp assigned to the transaction.
     * 
     * @exception IllegalStateException
     *                if there is no active transaction with that timestamp.
     */
    public long commit(long startTime);

}
