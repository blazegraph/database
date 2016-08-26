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

package com.bigdata.rwstore.sector;

import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.UUID;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.journal.AbstractBufferStrategy;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.rawstore.AbstractRawStore;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rawstore.IAllocationContext;
import com.bigdata.rawstore.IAllocationManagerStore;
import com.bigdata.rawstore.IPSOutputStream;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.TransientResourceMetadata;

/**
 * An {@link IRawStore} backed by an {@link IMemoryManager}.
 * 
 * @author thompsonbry
 */
public class MemStore extends AbstractRawStore implements IAllocationManagerStore {

//	private static final transient Logger log = Logger
//			.getLogger(MemStore.class);

	/**
	 * The backing store implementation.
	 */
	private final MemStrategy m_strategy;

	/**
	 * The {@link UUID} for the store.
	 */
    private final UUID m_uuid;


    /**
     * Create a new instance.
     * 
     * @param pool
     *            The pool from which the backing direct {@link ByteBuffer}s
     *            will be allocated.
     */
    public MemStore(final DirectBufferPool pool) {

        this(pool, Integer.MAX_VALUE);

    }
    
    /**
     * Create a new instance.
     * 
     * @param pool
     *            The pool from which the backing direct {@link ByteBuffer}s
     *            will be allocated.
     * @param bufferCapacity
     *            The maximum #of buffers which may be allocated by this
     *            {@link MemStore} from that pool. This may be
     *            {@link Integer#MAX_VALUE} for an effectively unlimited
     *            capacity.
     */
	public MemStore(final DirectBufferPool pool, final int bufferCapacity) {

		if (pool == null)
			throw new IllegalArgumentException();

		if (bufferCapacity <= 0)
			throw new IllegalArgumentException();

		m_strategy = new MemStrategy(new MemoryManager(pool, bufferCapacity));

		m_uuid = UUID.randomUUID();

	}

	/**
	 * Wrap an existing {@link IMemoryManager}.
	 * 
	 * @param mmgr
	 *            The {@link IMemoryManager}.
	 */
	public MemStore(final IMemoryManager mmgr) {

		this(mmgr, UUID.randomUUID());

	}

	/**
	 * Private constructor used to return a {@link MemStore} backed by a new
	 * {@link IMemoryManager} allocation context.
	 * 
	 * @param mmgr
	 *            The {@link IMemoryManager}.
	 * @param storeId
	 *            The {@link UUID} of the store.
	 */
	private MemStore(final IMemoryManager mmgr, final UUID storeId) {

		if (mmgr == null)
			throw new IllegalArgumentException();

		m_strategy = new MemStrategy(mmgr);

		m_uuid = storeId;

	}
	
	public MemStrategy getStrategy() {
		return m_strategy;
	}

	/**
	 * Return the backing {@link IMemoryManager}.
	 */
    public IMemoryManager getMemoryManager() {

        return m_strategy.getStore();
        
    }
    
	/**
	 * Return a new view of the {@link MemStore} backed by a child
	 * {@link IMemoryManager}. Allocations against the {@link MemStore} may be
	 * released in bulk by {@link #close()}. The life cycle of the returned
	 * {@link MemStore} is bounded by the life cycle of the {@link MemStore}
	 * from which it was allocated.
	 * 
	 * @see IMemoryManager#createAllocationContext()
	 */
	public MemStore createAllocationContext() {

		return new MemStore(m_strategy.getStore().createAllocationContext(), m_uuid);

	}

	public ByteBuffer read(final long addr) {

		return m_strategy.read(addr);

	}

	public long write(final ByteBuffer data) {

		return m_strategy.write(data);

	}
	
    @Override
    public long write(ByteBuffer data, IAllocationContext context) {
    
        return m_strategy.write(data, context);

    }

	public void delete(final long addr) {

		m_strategy.delete(addr);

	}
	
    @Override
    public void delete(long addr, IAllocationContext context) {

        m_strategy.delete(addr, context);
        
    }

	public CounterSet getCounters() {

		final CounterSet root = new CounterSet();

		root.addCounter("UUID", new OneShotInstrument<String>(getUUID()
				.toString()));

		root.attach(m_strategy.getStore().getCounters());
		
		return root;

	}
	
	private volatile boolean open = true;

	private void assertOpen() {

		if (!isOpen())
			throw new IllegalStateException(AbstractBufferStrategy.ERR_NOT_OPEN);

	}

	public boolean isOpen() {

		return open;

	}

	public void close() {

		// throw exception if open per the API.
		assertOpen();

		open = false;
		
		m_strategy.close();

	}

	public void deleteResources() {

		if (open)
			throw new IllegalStateException(AbstractBufferStrategy.ERR_OPEN);

		m_strategy.deleteResources();

	}

	public void destroy() {

		// close w/o exception throw.
		open = false;

		// delete the backing store.
		deleteResources();

	}

	/**
	 * NOP since the {@link MemStore} is not backed by stable media.
	 * <p>
	 * {@inheritDoc}
	 */
	public void force(final boolean metadata) {

		// NOP
		
	}

	/**
	 * This method always returns <code>null</code> since there is no backing
	 * file.
	 * <p>
	 * {@inheritDoc}
	 */
	public File getFile() {

		return null;

	}

	public IResourceMetadata getResourceMetadata() {

		return new TransientResourceMetadata(m_uuid);

	}

	public UUID getUUID() {

		return m_uuid;

	}

	public boolean isFullyBuffered() {

		return true;

	}

	public boolean isReadOnly() {

		return false;

	}

	public boolean isStable() {

		return false;

	}

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the #of bytes in the allocation slots rather than
     * the user bytes. This is because the {@link MemoryManager}, like the
     * {@link RWStore}, does not know the #of bytes of user data in each
     * allocation slot. Therefore it is not able to keep accurate track of the
     * user bytes as allocation slots are cycled.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/212 (Records must
     *      store the as-written length for HA failover reads to be successful.)
     */
	@Override
	public long size() {

		return m_strategy.size();

	}

	/*
	 * IAddressManager
	 */

	public IAddressManager getAddressManager() {
		return m_strategy.getAddressManager();
	}

	public int getByteCount(final long addr) {
		return getAddressManager().getByteCount(addr);
	}

	public long getOffset(final long addr) {
		return getAddressManager().getOffset(addr);
	}

	public long getPhysicalAddress(long addr) {
		return getAddressManager().getPhysicalAddress(addr);
	}

	public long toAddr(final int nbytes, final long offset) {
		return getAddressManager().toAddr(nbytes, offset);
	}

	public String toString(final long addr) {
		return getAddressManager().toString(addr);
	}
	
	/*
	 * Delegate stream handling to MemoryManager
	 */
    public IPSOutputStream getOutputStream() {
    	return m_strategy.getOutputStream();
    }

    /**
     * Return an output stream which can be used to write on the backing store
     * within the given allocation context. You can recover the address used to
     * read back the data from the {@link IPSOutputStream}.
     * 
     * @param context
     *            The context within which any allocations are made by the
     *            returned {@link IPSOutputStream}.
     *            
     * @return an output stream to stream data to and to retrieve an address to
     *         later stream the data back.
     */
    public IPSOutputStream getOutputStream(final IAllocationContext context) {
    	return m_strategy.getOutputStream(context);
    }

    /**
     * Return an input stream from which a previously written stream may be read
     * back.
     * 
     * @param addr
     *            The address at which the stream was written.
     *            
     * @return an input stream for the data for provided address
     */
    public InputStream getInputStream(long addr) {
    	return m_strategy.getInputStream(addr);
    }

}
