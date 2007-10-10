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

import com.bigdata.btree.IndexSegment;
import com.bigdata.isolation.IConflictResolver;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.service.DataService;

/**
 * A client-facing interface for managing transaction life cycles. An instance
 * of this class is used to assign both transaction start times (also known as
 * transaction identifiers) and transaction commit times and which are stored in
 * root blocks of the various stored that participate in a given database and
 * reported via {@link #getFirstCommitTime()} and {@link #getLastCommitTime()}).
 * <p>
 * While the transaction "start" and commit "times" these do not strictly
 * speaking have to be "times" they do have to be assigned using the same
 * measure, so this implies either a coordinated time server or a strictly
 * increasing counter. Regardless, we need to know "when" a transaction commits
 * as well as "when" it starts. Also note that we need to assign "commit times"
 * even when the operation is unisolated. This means that we have to coordinate
 * an unisolated commit on a store that is part of a distributed database with
 * the centralized transaction manager. This should be done as part of the group
 * commit since we are waiting at that point anyway to optimize IO by minimizing
 * syncs to disk.
 * <p>
 * A transaction identifier serves to uniquely distinguish transactions. The
 * practice is to use strictly increasing non-ZERO long integer values for
 * transaction identifiers. The strictly increasing requirement is introduced by
 * the MVCC protocol. The non-ZERO requirement is introduced by the APIs since
 * they interpret a 0L transaction identifier as an
 * <code>unisolated operation</code> - that is, not as a transaction at all.
 * <p>
 * It is much simpler to use a unique long integer counter for transaction
 * identifiers than to rely on timestamps. Counters are simpler because there is
 * no reliance on system clocks, which can be reset or adjusted by a variety of
 * mechanisms and which have to be continually synchronized across failover
 * transaction identifier services. In contract, a transaction identifier based
 * solely on a counter is much simpler but is decoupled from the notion of wall
 * clock time. In either case, a store SHOULD refuse a commit where the
 * transaction "commit time" appears to go backward.
 * <p>
 * If a counter is used to assign transaction start and commit "times" then the
 * service SHOULD keep the time at which that transaction identifier was
 * generated persistently on hand so that we are able to roll back the system to
 * the state corresponding to some moment in time (assuming that all relevant
 * history is on hand). When using a counter, there can be multiple transactions
 * that start or end within the resolution of the "moment". E.g., within the
 * same millisecond given a transaction identifier service associated with a
 * clock having millisecond precision (especially if it has less than
 * millisecond accuracy).
 * 
 * FIXME My recommendation based on the above is to go with a centralized time
 * service and to use a heartbeat to drive distributed commits of both isolated
 * and unisolated transactions. the group commit protocol can wait for a
 * suitable heartbeat to make the data restart safe.
 * 
 * @todo The transaction server should make sure that time does not go backwards
 *       when it starts up (with respect to the last time that it issued).
 * 
 * @todo Condider decoupling from the {@link ITimestampService} or folding it
 *       into this class since {@link ITimestampService} may get used to readily
 *       and the timestamp mechanisms of the {@link ITransactionManager} are
 *       quite specialized.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ITransactionManager extends ITimestampService {

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
    public long commit(long startTime) throws ValidationError;

    /**
     * Invoked by tasks executing a transaction to notify the transaction
     * manager that they have written on the named resource(s). This information
     * is used when submitting the task(s) that will handle the validation and
     * commit (or abort) of the transaction.
     * 
     * @param startTime
     *            The transaction identifier.
     * @param resource
     *            The named resources.
     * 
     * @todo pass the {@link DataService} identifier also (or instead) to make
     *       it easier for a distributed transaction manager to locate the
     *       {@link DataService}s holding the write sets for the transaction?
     *       <p>
     *       Note: In a failover situation we need to know the specific
     *       resources on which the transaction wrote on each
     *       {@link DataService} unless the media replication strategy for the
     *       data services also sends along this piece of otherwise transient
     *       state.
     */
    public void wroteOn(long startTime, String[] resource);
    
}
