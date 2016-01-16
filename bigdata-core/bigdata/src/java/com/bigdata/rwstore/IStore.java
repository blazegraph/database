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

import java.io.File;
import java.util.concurrent.locks.Lock;

import com.bigdata.rawstore.IAllocationContext;
import com.bigdata.rawstore.IStreamStore;

/**
 * The {@link IStore} interface provides persistent storage abstraction for
 * fixed size allocations and allocation recycling.
 */
public interface IStore extends IAllocationManager, IStreamStore,
        IHistoryManager {
	
//	/**************************************************************
//	 * called when used as a server, returns whether facility is enabled
//	 **/
//	public boolean preserveSessionData(); 

	/**
	 * Writes data on the store.
	 *
	 * @return the allocated address
	 **/
	public long alloc(byte buf[], int size, IAllocationContext context);

	/**
	 * Frees allocated storage (clears the bit to enable recycling after
	 * the next commit).
	 * 
	 * @param addr
	 *            the storage address to be freed
	 */
	public void free(long addr, int size);

    /**
     * Optionally return a {@link Lock} that must be used (when non-
     * <code>null</code>) to make the {@link #commit()} / {@link #postCommit()}
     * strategy atomic.
     */
    public Lock getCommitLock();

    /**
     * Global commit on the backing store. Previously committed data which has
     * been marked as {@link #free(long, int)} is now available for recycling.
     * However, recycling can not occur if session protection is active.
     */
	public void commit();
	
    /**
     * Hook that supports synchronization with an external commit before which
     * a rollback to "pre-commit" state is supported.
     */
	public void postCommit();
	
//	/**************************************************************
//	 * Odd method needed by PSInputStream to fetch data of unknown
//	 *	size into a buffer
//	 *
//	 * <p>Both RWStore and WOStore store data in either explicit or
//	 *	implicit block sizes.</p>
//	 *
//	 * @param addr the address of the data in the IStore
//	 *				buf the buffer to store the data in
//	 *
//	 * @returns the size of the data copied
//	 **/
//	public int getDataSize(long addr, byte buf[]);

	/**
	 * Read data of a known size from the store.
	 * 
	 * @param addr
	 *            the address of the data
	 * @param buf
	 *            the buffer of the size required!
	 */
	public void getData(long addr, byte buf[]);
	
	/**************************************************************
	 * @param addr - the address
	 * @return the size of the slot associated
	 */
	public int getAssociatedSlotSize(int addr);
	
//	/**************************************************************
//	 * Given a physical address (byte offset on the store), return true
//	 * if that address could be managed by an allocated block.
//	 *
//	 * @param a the storage address to be tested
//	 **/	
//	public boolean verify(long a);

//	/**
//	 * The {@link RWStore} always generates negative address values.
//	 * 
//	 * @return whether the address given is a native IStore address
//	 */
//	public boolean isNativeAddress(long value);
	
//	/**
//	 * useful in debug situations
//	 *
//	 * @return store allocation and usage statistics
//	 */
//	public String getStats(boolean full);
	
	/**
	 * Close the backing storage.
	 */
	public void close();
	
//	/**
//	 * Needed by PSOutputStream for BLOB buffer chaining.
//	 */
//	public int bufferChainOffset();

	/**
	 * Retrieves store file. Can be used to delete the store after the IStore
	 * has been released
	 * 
	 * @return the File object
	 */
	public File getStoreFile();
	
//	/**
//	 * Called by the PSOutputStream to register the header block of a blob. The
//	 * store must return a new address that is used to retrieve the blob header.
//	 * This double indirection is required to be able to manage the blobs, since
//	 * the blob header itself is of variable size and is handled by the standard
//	 * FixedAllocators in the RWStore.
//	 * 
//	 * @param addr
//	 *            The address of the header block of the blob.
//	 * 
//	 * @return The 
//	 */
//	public int registerBlob(int addr);

//    /**
//     * Return an output stream which can be used to write on the backing store.
//     * You can recover the address used to read back the data from the
//     * {@link IPSOutputStream}.
//     * 
//     * @return The output stream.
//     */
//    public IPSOutputStream getOutputStream();
//
//    /**
//     * Return an output stream which can be used to write on the backing store
//     * within the given allocation context. You can recover the address used to
//     * read back the data from the {@link IPSOutputStream}.
//     * 
//     * @param context
//     *            The context within which any allocations are made by the
//     *            returned {@link IPSOutputStream}.
//     *            
//     * @return an output stream to stream data to and to retrieve an address to
//     *         later stream the data back.
//     */
//    public IPSOutputStream getOutputStream(final IAllocationContext context);
//
//    /**
//     * @return an input stream for the data for provided address
//     */
//    public InputStream getInputStream(long addr);

//    /**
//     * Call made from AbstractJournal to register the cache used. This can then
//     * be accessed to clear entries when storage is made available for
//     * re-cycling.
//     * <p>
//     * Note: It is not safe to clear at the point of the delete request since
//     * the data could still be loaded if the data is retained for a period due
//     * to a non-zero retention period or session protection.
//     * 
//     * @param externalCache
//     *            - used by the Journal to cache historical BTree references
//     * @param dataSize
//     *            - the size of the checkpoint data (fixed for any version)
//     */
//    public void registerExternalCache(
//            ConcurrentWeakValueCache<Long, ICommitter> historicalIndexCache,
//            int byteCount);

//    /**
//     * Saves the current list of delete blocks, returning the address allocated.
//     * This can be used later to retrieve the addresses of allocations to be
//     * freed.
//     * 
//     * Writes the content of currentTxnFreeList to the store.
//     * 
//     * These are the current buffered frees that have yet been saved into a
//     * block referenced from the deferredFreeList
//     * 
//     * @return the address of the deferred addresses saved on the store, or zero
//     *         if none.
//     */
//    public long saveDeferrals();
//    
//    /**
//     * Called prior to commit, so check whether storage can be freed and then
//     * whether the deferred header needs to be saved.
//     * <p>
//     * Note: The caller MUST be holding the {@link #m_allocationLock}.
//     * <p>
//     * Note: This method is package private in order to expose it to the unit
//     * tests.
//     * 
//     * returns number of addresses freed
//     */
//    public int checkDeferredFrees(AbstractJournal abstractJournal);

//    /**
//     * A hook used to support session protection by incrementing and
//     * decrementing a transaction counter within the {@link IStore}. As long as
//     * a transaction is active we can not release data which is currently marked
//     * as freed but was committed at the point the session started.
//     */
//    public IRawTx newTx();
//
}
