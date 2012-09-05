package com.bigdata.rwstore;

import com.bigdata.journal.IBufferStrategy;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RWStrategy;
import com.bigdata.rwstore.sector.MemStrategy;

/**
 * Defines a marker interface to be used to indicate strategies that share RW
 * semantics such as {@link RWStrategy} and {@link MemStrategy}
 * 
 * @author Martyn Cutcher
 */
public interface IRWStrategy extends IBufferStrategy, IAllocationManager,
        IAllocationManagerStore, IStreamStore, IHistoryManager {

    /**
     * Return the backing {@link IStore}.
     */
    IStore getStore();
    
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

    /**
     * Resets allocators from current rootblock
     */
	void resetFromHARootBlock(IRootBlockView rootBlock);
    
}
