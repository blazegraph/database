/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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

package com.bigdata.rwstore.sector;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.UUID;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.journal.AbstractBufferStrategy;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.rawstore.AbstractRawStore;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rawstore.TransientResourceMetadata;

/**
 * An {@link IRawStore} backed by an {@link IMemoryManager}.
 * 
 * @author thompsonbry
 */
public class MemStore extends AbstractRawStore implements IRawStore {

//	private static final transient Logger log = Logger
//			.getLogger(MemStore.class);

	/**
	 * The address manager.
	 */
	private final IAddressManager m_am = new MemStoreAddressManager();

	/**
	 * The backing store implementation.
	 */
	private final IMemoryManager m_store;

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
	 * @param bufferCapacity
	 *            The maximum #of buffers which may be allocated by this
	 *            {@link MemStore} from that pool.
	 */
	public MemStore(final DirectBufferPool pool, final int bufferCapacity) {

		if (pool == null)
			throw new IllegalArgumentException();

		if (bufferCapacity <= 0)
			throw new IllegalArgumentException();

		m_store = new MemoryManager(pool, bufferCapacity);

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

		m_store = mmgr;

		m_uuid = storeId;

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

		return new MemStore(m_store.createAllocationContext(), m_uuid);

	}

	public ByteBuffer read(final long addr) {

		return ByteBuffer.wrap(m_store.read(addr));

	}

	public long write(final ByteBuffer data) {

		return m_store.allocate(data);

	}
	
	public void delete(final long addr) {

		m_store.free(addr);

	}
	
	public CounterSet getCounters() {

		final CounterSet root = new CounterSet();

		root.addCounter("UUID", new OneShotInstrument<String>(getUUID()
				.toString()));

		root.attach(m_store.getCounters());
		
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
		
		m_store.clear();

	}

	public void deleteResources() {

		if (open)
			throw new IllegalStateException(AbstractBufferStrategy.ERR_OPEN);

		m_store.clear();

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

	public long size() {

		return m_store.getUserBytes();

	}

	/*
	 * IAddressManager
	 */

	public IAddressManager getAddressManager() {
		return m_am;
	}

	public int getByteCount(final long addr) {
		return m_am.getByteCount(addr);
	}

	public long getOffset(final long addr) {
		return m_am.getOffset(addr);
	}

	public long toAddr(final int nbytes, final long offset) {
		return m_am.toAddr(nbytes, offset);
	}

	public String toString(final long addr) {
		return m_am.toString(addr);
	}

	private static class MemStoreAddressManager implements IAddressManager {

		public int getByteCount(final long addr) {
			return (int) (addr & 0xFFFFFFFFL);
		}

		public long getOffset(final long addr) {
			return addr >> 32;
		}

		public long toAddr(final int nbytes, final long offset) {
			return (offset << 32) + nbytes;
		}

		public String toString(final long addr) {
			return "{off=" + getOffset(addr) + ",len=" + getByteCount(addr)
					+ "}";
		}

	}

}
