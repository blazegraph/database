/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.rwstore;

import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.DeleteBlockCommitter;
import com.bigdata.journal.ICommitter;
import com.bigdata.journal.Journal;

/**
 * Interface for glue methods which permit the coordination of the hisory
 * retention and deferred release mechanisms between the {@link Journal}, the
 * {@link IRWStrategy}, and the backing {@link IStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IHistoryManager {

    /**
     * A hook used to support session protection by incrementing and
     * decrementing a transaction counter within the {@link IStore}. As long as
     * a transaction is active we can not release data which is currently marked
     * as freed but was committed at the point the session started.
     */
    public IRawTx newTx();

    /**
     * Saves the current list of delete blocks, returning the address allocated.
     * This can be used later to retrieve the addresses of allocations to be
     * freed.
     * <p>
     * Writes the content of currentTxnFreeList to the store.
     * <p>
     * These are the current buffered frees that have yet been saved into a
     * block referenced from the deferredFreeList
     * 
     * @return the address of the deferred addresses saved on the store, or zero
     *         if none.
     * 
     * @see DeleteBlockCommitter
     */
    public long saveDeferrals();
    
    /**
     * This method is invoked during the commit protocol and gives the backing
     * store an opportunity to check whether storage associated with deferred
     * frees can now be released. The backing store needs to reply the deferred
     * free blocks for the commit points that will be released, mark those
     * addresses as free, and free the logged delete blocks.
     * 
     * @return number of addresses freed
     * 
     * @see AbstractJournal#commitNow()
     */
    public int checkDeferredFrees(AbstractJournal abstractJournal);

    /**
     * Call made from AbstractJournal to register the cache used. This can then
     * be accessed to clear entries when storage is made available for
     * re-cycling.
     * <p>
     * Note: It is not safe to clear at the point of the delete request since
     * the data could still be loaded if the data is retained for a period due
     * to a non-zero retention period or session protection.
     * 
     * @param externalCache
     *            - used by the Journal to cache historical BTree references
     * @param dataSize
     *            - the size of the checkpoint data (fixed for any version)
     */
    public void registerExternalCache(
            ConcurrentWeakValueCache<Long, ICommitter> historicalIndexCache,
            int byteCount);

    /**
     * If history is retained this returns the time for which data was most
     * recently released. No request can be made for data earlier than this.
     * 
     * @return latest data release time
     */
    public long getLastReleaseTime();
    
}
