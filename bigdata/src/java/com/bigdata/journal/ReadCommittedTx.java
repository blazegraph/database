/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Feb 27, 2007
 */

package com.bigdata.journal;

import com.bigdata.btree.IIndex;

/**
 * A read-committed transaction provides a read-only view onto the most recently
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

    public String[] getDirtyResource() {
        
        return EMPTY;
        
    }

    private static transient final String[] EMPTY = new String[0];
    
    /**
     * Return a read-only view of the named index with read-committed isolation.
     * 
     * @return The index or <code>null</code> if the named index is not
     *         registered.
     */
    public IIndex getIndex(String name) {

        lock.lock();

        try {

            if (!isActive()) {

                throw new IllegalStateException(NOT_ACTIVE);

            }

            ICommitRecord commitRecord = journal.getCommitRecord();

            if (commitRecord == null) {

                /*
                 * This happens where there has not yet been a commit on the
                 * store.
                 */

                return null;

            }

            if (journal.getIndex(name, commitRecord) == null) {

                /*
                 * The named index is not registered as of the last commit.
                 */

                return null;

            }

            return new ReadCommittedIndex(journal, name);

        } finally {

            lock.unlock();

        }
        
    }

}
