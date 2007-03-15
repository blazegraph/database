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

import com.bigdata.isolation.IIsolatedIndex;
import com.bigdata.isolation.IsolatedBTree;
import com.bigdata.isolation.ReadOnlyIsolatedIndex;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.objndx.BTree;
import com.bigdata.objndx.IIndex;
import com.bigdata.rawstore.Bytes;
import com.bigdata.scaleup.PartitionedIndexView;

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
 * The write set of a transaction is written onto a {@link TemporaryRawStore}.
 * Therefore the size limit on the transaction write set is currently 2G, but
 * the transaction will run in memory up to 100M. The {@link TemporaryRawStore}
 * is closed and any backing file is deleted as soon as the transaction
 * completes.
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
 * @todo In order to support a distributed transaction commit protocol the write
 *       set of a validated transaction needs to be made restart safe without
 *       making it restart safe on the corresponding unisolated index on the
 *       journal. It may be that the right thing to do is to write the validated
 *       data onto the unisolated indices but not commit the journal and not
 *       permit other unisolated writes until the commit message arives, e.g.,
 *       block in the {@link AbstractJournal#writeService} waiting on the commit
 *       message. A timeout would cause the buffered writes to be discarded (by
 *       an abort).
 * 
 * @todo track whether or not the transaction has written any isolated data (I
 *       currently rangeCount the isolated indices in {@link #isEmptyWriteSet()}).
 *       do this at the same time that I modify the isolated indices use a
 *       delegation strategy so that I can trap attempts to access an isolated
 *       index once the transaction is no longer active. define "active" as up
 *       to the point where a "commit" or "abort" is _requested_ for the tx.
 * 
 * @todo Support transactions where the indices isolated by the transactions are
 *       {@link PartitionedIndexView}es.
 * 
 * @todo The various public methods on this API that have {@link RunState}
 *       constraints all eagerly force an abort when invoked from an illegal
 *       state. This is, perhaps, excessive. Futher, since this is used in a
 *       single-threaded server context, we are better off testing for illegal
 *       conditions and notifying clients without out generating expensive stack
 *       traces. This could be done by return flags or by the server checking
 *       pre-conditions itself and exceptions being thrown from here if the
 *       server failed to test the pre-conditions and they were not met
 */
public class Tx extends AbstractTx implements IIndexStore, ITx {

    /**
     * The historical {@link ICommitRecord} choosen as the ground state for this
     * transaction. All indices isolated by this transaction are isolated as of
     * the discoverable root address based on this commit record.
     */
    final private ICommitRecord commitRecord;
    
    /**
     * A store used to hold write sets for read-write transactions (it is null
     * iff the transaction is read-only). The same store can be used to buffer
     * resolved write-write conflicts. This uses a {@link TemporaryRawStore} to
     * avoid issues with maintaining transactions across journal boundaries.
     * This places a limit on transactions of 2G in their serialized write set.
     * <p>
     * Since the indices use a copy-on-write model, the amount of user data can
     * be significantly less due to multiple versions of the same btree nodes.
     * Using smaller branching factors in the isolated index helps significantly
     * to increase the effective utilization of the store since copy-on-write
     * causes fewer bytes to be copied each time it is invoked.
     */
    final protected TemporaryRawStore tmpStore;
    
    /**
     * Indices isolated by this transactions.
     * 
     * @todo Note that this mapping could use weak value map as long as we
     *       retained the address of the metadata record in a hard reference map
     *       so that we could reload the index from the {@link #tmpStore}. I am
     *       not sure that any performance benefit would be realized by closing
     *       out indices for active transactions. It is possible that this could
     *       benefit or hurt. Many of the same benefits could be realized by
     *       having the {@link BTree} class automatically release large
     *       transient data structures after a period of disuse.
     */
    private Map<String, IIsolatedIndex> btrees = new HashMap<String, IIsolatedIndex>();
    
    /**
     * Create a transaction reading from the most recent committed state not
     * later than the specified startTime.
     * 
     * @param journal
     *            The journal.
     * 
     * @param startTime
     *            The start time assigned to the transaction. Note that a
     *            transaction does not start execution on all {@link Journal}s
     *            at the same moment. Instead, the transaction start startTime
     *            is assigned by a centralized time service and then provided
     *            each time a transaction object must be created for isolatation
     *            of resources accessible on some {@link Journal}.
     * 
     * @param readOnly
     *            When true the transaction will reject writes and
     *            {@link #prepare()} and {@link #commit()} will be NOPs.
     */
    public Tx(AbstractJournal journal, long startTime, boolean readOnly) {

        super(journal, startTime, readOnly ? IsolationEnum.ReadOnly
                : IsolationEnum.ReadWrite);
        
        /*
         * The commit record serving as the ground state for the indices
         * isolated by this transaction (MAY be null, in which case the
         * transaction will be unable to isolate any indices).
         */
        this.commitRecord = journal.getCommitRecord(startTime);
        
        this.tmpStore = readOnly ? null : new TemporaryRawStore(
                Bytes.megabyte * 1, // initial in-memory size.
                Bytes.megabyte * 10, // maximum in-memory size.
                false // do NOT use direct buffers.
                );
        
    }

    /**
     * This method must be invoked any time a transaction completes ({@link #abort()}s
     * or {@link #commit()}s) in order to release resources held by that
     * transaction.
     */
    protected void releaseResources() {

        super.releaseResources();
        
        /*
         * Release hard references to any named btrees isolated within this
         * transaction so that the JVM may reclaim the space allocated to them
         * on the heap.
         */
        btrees.clear();
        
        /*
         * Close and delete the TemporaryRawStore.
         */
        if (tmpStore != null && tmpStore.isOpen()) {

            tmpStore.close();

        }
        
    }
    
    protected boolean validateWriteSets() {
 
        assert ! readOnly;
        
        /*
         * This compares the current commit counter on the journal with the
         * commit counter as of the start time for the transaction. If they are
         * the same, then no intervening commits have occurred on the journal
         * and there is nothing to validate.
         */
        
        if (commitRecord == null
                || (journal.getRootBlockView().getCommitCounter() == commitRecord
                        .getCommitCounter())) {
            
            return true;
            
        }
        
        /*
         * for all isolated btrees, if(!validate()) return false;
         */

        Iterator<Map.Entry<String,IIsolatedIndex>> itr = btrees.entrySet().iterator();
        
        while(itr.hasNext()) {
            
            Map.Entry<String, IIsolatedIndex> entry = itr.next();
            
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
    
    protected void mergeOntoGlobalState() {

        assert ! readOnly;

        super.mergeOntoGlobalState();
        
        Iterator<Map.Entry<String,IIsolatedIndex>> itr = btrees.entrySet().iterator();
        
        while(itr.hasNext()) {
            
            Map.Entry<String, IIsolatedIndex> entry = itr.next();
            
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
            
            throw new IllegalStateException(NOT_ACTIVE);
            
        }

        /*
         * Store the btrees in hash map so that we can recover the same instance
         * on each call within the same transaction.
         */
        IIsolatedIndex index = btrees.get(name);
        
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

                index = new ReadOnlyIsolatedIndex(src);
                
            } else {
                
                // writeable index backed by the temp. store.
                index = new IsolatedBTree(tmpStore,src);

                // report event.
                ResourceManager.isolateIndex(startTime, name);
                
            }

            btrees.put(name, index);
            
        }
        
        return index;
        
    }

    final public boolean isEmptyWriteSet() {
        
        if(isReadOnly()) {
            
            // Read-only transactions always have empty write sets.
            return true;
            
        }

        Iterator<IIsolatedIndex> itr = btrees.values().iterator();

        while(itr.hasNext()) {
            
            IsolatedBTree ndx = (IsolatedBTree) itr.next();
            
            if(!ndx.isEmptyWriteSet()) {
                
                // At least one isolated index was written on.
                
                return false;
                
            }
            
        }
        
        return true;
        
    }

}
