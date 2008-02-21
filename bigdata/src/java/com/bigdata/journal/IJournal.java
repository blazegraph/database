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
package com.bigdata.journal;

import java.util.Properties;

import com.bigdata.btree.IIndex;
import com.bigdata.rawstore.IMRMW;

/**
 * <p>
 * An append-only persistence capable data structure supporting atomic commit,
 * scalable named indices, and transactions. Writes are logically appended to
 * the journal to minimize disk head movement.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IJournal extends IMRMW, IAtomicStore, IIndexManager {

    /**
     * A copy of the properties used to initialize this journal.
     */
    public Properties getProperties();
    
    /**
     * An overflow condition arises when the journal is within some declared
     * percentage of its maximum capacity during a {@link #commit()}. If this
     * event is not handled then the journal will automatically extent itself
     * until it either runs out of address space (int32) or other resources.
     * 
     * @return true iff the overflow event was handled (e.g., if a new journal
     *         was created to absorb subsequent writes). if a new journal is NOT
     *         opened then this method should return false.
     */
    public boolean overflow();
    
    /**
     * Shutdown the journal politely. Scheduled operations will run to
     * completion, but no new operations will be scheduled.
     */
    public void shutdown();

    /**
     * Return the named index (unisolated). Writes on the returned index will be
     * made restart-safe with the next {@link #commit()} unless discarded by
     * {@link #abort()}.
     * 
     * @param name
     *            The name of the index.
     * 
     * @return The named index or <code>null</code> iff there is no index
     *         registered with that name.
     * 
     * @exception IllegalArgumentException
     *                if <i>name</i> is <code>null</code>
     */
    public IIndex getIndex(String name);

//    /**
//     * Return the named index (isolated). Writes will be allowed iff the
//     * transaction is {@link IsolationEnum#ReadWrite}. Writes on the returned
//     * index will be made restart-safe iff the transaction
//     * {@link ITx#commit() commits}.
//     * 
//     * @param name
//     *            The index name.
//     * @param startTime
//     *            The transaction start time, which serves as the unique
//     *            identifier for the transaction.
//     * 
//     * @return The isolated index or <code>null</code> iff there is no index
//     *         registered with that name.
//     * 
//     * @exception IllegalArgumentException
//     *                if <i>name</i> is <code>null</code>
//     * 
//     * @exception IllegalStateException
//     *                if there is no active transaction with that timestamp.
//     */
//    public IIndex getIndex(String name, long startTime);
//
}
