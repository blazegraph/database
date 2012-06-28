package com.bigdata.rwstore;

import com.bigdata.journal.IBufferStrategy;

/**
 * Defines a marker interface to be used to indicate strategies
 * that share RW semantics such as RWStrategy and MemStrategy
 * 
 * <p>Methods have been added as required to support the clients
 * (mostly AbstractJournal).
 * 
 * @author Martyn Cutcher
 *
 */
public interface IRWStrategy extends IBufferStrategy, IAllocationManager,
        IAllocationManagerStore, IStreamStore, IHistoryManager {

    /**
     * Return the backing {@link IStore}.
     */
    IStore getStore();
    
//	/**
//	 * @return IRawTx to enable activate/deactvate
//	 */
//	IRawTx newTx();

//	void registerContext(IAllocationContext context);
//
//	void detachContext(IAllocationContext context);
//
//	void abortContext(IAllocationContext context);

//	void registerExternalCache(
//			ConcurrentWeakValueCache<Long, ICommitter> historicalIndexCache,
//			int byteCount);

//	long write(ByteBuffer data, long oldAddr, IAllocationContext context);
//
//	long write(ByteBuffer data, IAllocationContext context);
//
//	void delete(long addr, IAllocationContext context);

//	/**
//	 * @return an outputstream to stream data to and to retrieve
//	 * an address to later stream the data back.
//	 */
//	public IPSOutputStream getOutputStream();
//	
//	/**
//	 * @param context within which any allocations are made by the
//	 * returned IPSOutputStream
//	 * @return an outputstream to stream data to and to retrieve
//	 * an address to later stream the data back.
//	 */
//	public IPSOutputStream getOutputStream(IAllocationContext context);
//	
//	/**
//	 * @return an inputstream for the data for provided address
//	 */
//	public InputStream getInputStream(long addr);

//	/**
//	 * Called by DeleteBlockCommitter
//	 * 
//	 * @return the address of the deferred release data
//	 */
//	long saveDeferrals();
//
//	/**
//	 * Called from AbstractJournal commitNow
//	 */
//	int checkDeferredFrees(AbstractJournal abstractJournal);

//    /**
//     * If history is retained this returns the time for which data was most
//     * recently released. No request can be made for data earlier than this.
//     * 
//     * @return latest data release time
//     */
//	long getLastReleaseTime();
		
    /**
     * Return <code>true</code> iff the allocation having that address is
     * flagged as committed. The caller must be holding the allocation lock in
     * order for the result to remain valid outside of the method call.
     * 
     * @param addr
     *            The address.
     * 
     * @return <code>true</code> iff the address is currently committed.
     */
    public boolean isCommitted(long addr);
    
}
