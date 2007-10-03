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
 * Created on Feb 27, 2007
 */

package com.bigdata.journal;

import java.util.UUID;

import com.bigdata.btree.BatchContains;
import com.bigdata.btree.BatchInsert;
import com.bigdata.btree.BatchLookup;
import com.bigdata.btree.BatchRemove;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.isolation.IIsolatableIndex;
import com.bigdata.isolation.IIsolatedIndex;

/**
 * A read-committed transaction provides a read-only view onto the current
 * committed state of the database. Each time a view of an index is requested
 * using {@link #getIndex(String)} the returned view will provide access to the
 * most recent committed state for that index. Unlike a fully isolated
 * transaction, a read-committed transaction does NOT provide a consistent view
 * of the database over time. However, a read-committed transaction imposes
 * fewer constraints on when old resources (historical journals and index
 * segments) may be released. For this reason, a read-committed transaction is a
 * good choice when a very-long running read must be performed on the database.
 * Since a read-committed transaction does not allow writes, the commit and
 * abort protocols are identical.
 * 
 * @todo In order to release the resources associated with a commit point
 *       (historical journals and index segments) we need a protocol by which a
 *       delegate index view is explicitly closed (or collected using a weak
 *       value cache) once it is no longer in use for an operation. The index
 *       views need to be accumulated on a commit point (aka commit record).
 *       When no index views for a given commit record are active, the commit
 *       point is no longer accessible to the read-committed transaction and
 *       should be released. Resources (journals and index segments) required to
 *       present views on that commit point MAY be released once there are no
 *       longer any fully isolated transactions whose start time would select
 *       that commit point as their ground state.
 * 
 * @todo We may not even need a start time for a read-committed transaction
 *       since it always reads from the most recent commit record, in which case
 *       it could be started and finished with lower latency than a
 *       fully-isolated read-only or read-write transaction.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ReadCommittedTx extends AbstractTx implements ITx {

    public ReadCommittedTx(AbstractJournal journal, long startTime ) {
        
        super(journal, startTime, IsolationEnum.ReadCommitted);
        
    }
    
    /**
     * The write set is always empty.
     */
    final public boolean isEmptyWriteSet() {
        
        return true;
        
    }

    /**
     * Return a read-only view of the named index with read-committed isolation.
     * 
     * @return The index or <code>null</code> if the named index is not
     *         registered.
     */
    public IIndex getIndex(String name) {

        if (!isActive()) {
            
            throw new IllegalStateException(NOT_ACTIVE);
            
        }

        if (journal.getIndex(name) == null) {

            /*
             * The named index is not registered at this time.
             */
            return null;
            
        }
        
        return new ReadCommittedIndex(this,name);

    }

    /**
     * Light-weight implementation of a read-committed index view.
     * <p>
     * A delegation model is used since commits or overflows of the journal
     * might invalidate the index objects that actually read on the journal
     * and/or index segments. The delegation strategy checks the commit counters
     * and looks up a new delegate index object each time the commit counter is
     * updated. In this way, newly committed data are always made visible to the
     * next operation on the index. In-progress writes are NOT visible since we
     * only read from a delegate index discovered by resolving the index name
     * against the most recent {@link ICommitRecord}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo if we add an extensible batch index operation then we need to make
     *       sure that it is not possible to use that to circumvent the
     *       read-only contract and write on the unisolated delegate index. For
     *       the same reason, it makes sense to make this class <em>final</em>
     *       and to make {@link #getIndex()} private.
     */
    public static class ReadCommittedIndex implements IIndex, IIsolatedIndex {

        /**
         * The transaction.
         */
        final protected ReadCommittedTx tx;
        
        /**
         * The name of the index.
         */
        final protected String name;
        
        /**
         * The last commit record for which an index was returned.
         */
        private ICommitRecord commitRecord;
        
        /**
         * The last index returned.
         */
        private IIsolatableIndex index;
        
        public ReadCommittedIndex(ReadCommittedTx tx, String name) {
        
            assert tx != null;
            
            assert tx.isActive();
            
            assert name != null;
            
            this.tx = tx;
            
            this.name = name;
            
            this.index = getIndex();
            
        }

        /**
         * Return the current {@link IIsolatableIndex} view. The read-committed
         * view simply exposes this as a read-only {@link IIsolatedIndex}.
         * 
         * @return The current unisolated index on the journal (read-write).
         * 
         * @exception IllegalStateException
         *                if the named index is not registered.
         */
        protected IIsolatableIndex getIndex() {
            
            /*
             * Obtain the most current {@link ICommitRecord} on the journal. All
             * read operations are against the named index as resolved using
             * this commit record.
             */
            ICommitRecord currentCommitRecord = tx.journal.getCommitRecord();
            
            if (commitRecord != null
                    && index != null
                    && commitRecord.getCommitCounter() == currentCommitRecord
                            .getCommitCounter()) {

                /*
                 * the commit record has not changed so we have the correct
                 * index view.
                 */
                return index;
                
            }
            
            // update the commit record.
            this.commitRecord = currentCommitRecord;
            
            /*
             * Lookup the current committed index view against that commit
             * record.
             */
            this.index = (IIsolatableIndex) tx.journal.getIndex(name,
                    commitRecord);
            
            if(index == null) {
                
                throw new IllegalStateException("Index not defined: "+name);
                
            }
            
            return index;
            
        }
        
        public UUID getIndexUUID() {
            return getIndex().getIndexUUID();
        }
        
        public boolean contains(byte[] key) {
            return getIndex().contains(key);
        }

        /**
         * @exception UnsupportedOperationException always.
         */
        public Object insert(Object key, Object value) {
            throw new UnsupportedOperationException();
        }

        public Object lookup(Object key) {
            return getIndex().lookup(key);
        }

        /**
         * @exception UnsupportedOperationException always.
         */
        public Object remove(Object key) {
            throw new UnsupportedOperationException();
        }

        public int rangeCount(byte[] fromKey, byte[] toKey) {
            return getIndex().rangeCount(fromKey, toKey);
        }

        public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey) {
            return getIndex().rangeIterator(fromKey, toKey);
        }

        public void contains(BatchContains op) {
            getIndex().contains(op);
        }

        /**
         * @exception UnsupportedOperationException always.
         */
        public void insert(BatchInsert op) {
            throw new UnsupportedOperationException();
        }

        public void lookup(BatchLookup op) {
            getIndex().lookup(op);
        }

        /**
         * @exception UnsupportedOperationException always.
         */
        public void remove(BatchRemove op) {
            throw new UnsupportedOperationException();
        }
        
    }
    
}
