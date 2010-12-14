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

package com.bigdata.io.writecache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicReference;

import com.bigdata.counters.CAT;
import com.bigdata.counters.CounterSet;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.IReopenChannel;
import com.bigdata.rwstore.RWStore;

/**
 * The BufferedWrite merges/elides sorted scattered writes to minimize IO
 * requests and maximize IO rates. This has a net positive effect on SAS, SATA,
 * and SSD.
 * 
 * @author Martyn Cutcher
 * 
 * @todo unit tests (this is used by RWStore and so is in general tested as part
 *       of that class, but it does not have its own test suite and it should not
 *       be all that difficult to write one, especially if we factor out an API
 *       for reporting the slotSize and then use a mock object in place of the
 *       RWStore).
 * 
 * @todo harmonize with {@link CounterSet} for reporting purposes.
 */
public class BufferedWrite {

	/**
	 * Used to determine the size of the allocation slot onto which a record is
	 * being written. This is used to pad the size of the IO out to the size of
	 * the slot. This can improve the IO efficiency When the slots are sized so
	 * as to fall on multiples of sector boundaries.
	 */
	private final RWStore m_store;

	/**
	 * The direct {@link ByteBuffer} used to combine writes which are contiguous
	 * into a single IO.
	 */
//	private final ByteBuffer m_data;
	private final AtomicReference<ByteBuffer> m_data = new AtomicReference<ByteBuffer>();

	/**
	 * The offset on the backing channel at which the data in {@link #m_data}
	 * will be written when it is flushed to the backing channel. This is
	 * <code>-1</code> initially (and when reset) as a flag indicating that
	 * there is no data in {@link #m_data} and that the next record written by
	 * the caller on the buffer will assign the {@link #m_startAddr starting
	 * offset} of the data in the buffer.
	 * <p>
	 * Guarded by synchronized(this) (paranoia)
	 */
	private long m_startAddr = -1;

	/**
	 * The offset of the backing channel at which the next byte would be written
	 * if it were appended to the data already present in {@link #m_data}.
	 * <p>
	 * Guarded by synchronized(this) (paranoia)
	 */
	private long m_endAddr = 0;
	
	/*
	 * Counters.
	 */
	private final CAT m_dataBytes = new CAT();
	private final CAT m_dataWrites = new CAT();
	private final CAT m_fileWrites = new CAT();
	
	public BufferedWrite(final RWStore store) throws InterruptedException {
		
		if (store == null)
			throw new IllegalArgumentException();
		
		m_store = store;
		
		m_data.set( DirectBufferPool.INSTANCE.acquire() );
		
	}

	/**
	 * Release the direct buffer associated with this object.
	 * 
	 * @throws InterruptedException
	 */
//	/*
//	 * Note: Consider adding synchronized(this) here to guard against the
//	 * possibility that the buffer could be released (and hence recycled) while
//	 * a write operation was occurring concurrently, However, this raises the
//	 * specter that a lock ordering problem could cause a deadlock.
//	 */
//	synchronized
	public void release() throws InterruptedException {

		final ByteBuffer tmp = m_data.get();

		if (tmp == null) {

			// Already closed.
			return;

		}

		if (m_data.compareAndSet(tmp/* expected */, null/* update */)) {

			DirectBufferPool.INSTANCE.release(tmp);

		}
		
	}

	/**
	 * Buffer a write.
	 * 
	 * @param offset
	 *            The offset on the backing channel at which the data should be
	 *            written.
	 * @param data
	 *            The data.
	 * @param opener
	 *            The object which knows how to re-open the backing channel.
	 * @return The #of write IOs performed during this method call.
	 * 
	 * @throws IOException
	 */
	synchronized
	public int write(final long offset, final ByteBuffer data,
			final IReopenChannel<FileChannel> opener) throws IOException {
		
		m_dataWrites.increment();
		
		final int data_len = data.remaining();
		final int slot_len = m_store.getSlotSize(data_len);
		
		int nwrites = 0;
		final ByteBuffer m_data = this.m_data.get();
		if (slot_len > m_data.remaining()) {
			/*
			 * There is not enough room in [m_data] to absorb the caller's data
			 * record, so we have to flush first.
			 */
			nwrites += flush(opener);
		}
		
		if (m_startAddr == -1) {
			/*
			 * The buffer will begin to absorb data destined for the [offset]
			 * into the backing channel specified for the caller's data record.
			 */
			m_startAddr = m_endAddr = offset;
		} else if (m_endAddr != offset) {
			/*
			 * If this is NOT a contiguous write then flush existing content.
			 * After the flush, the buffer will begin to absorb data destined
			 * for the [offset] into the backing channel specified for the
			 * caller's data record.
			 */
			nwrites += flush(opener);	
			m_startAddr = m_endAddr = offset;
		}
		// copy the caller's record into the buffer.
		m_data.put(data);
		// update the file offset by the size of the allocation slot
		m_endAddr += slot_len;
		// update the buffer position by the size of the allocation slot.
		final long pos = m_endAddr - m_startAddr;
		m_data.position((int) pos);
		
		return nwrites;
	}
	
	/**
	 * Flush buffered data to the backing channel.
	 * 
	 * @param opener
	 *            The object which knows how to re-open the backing channel.
	 *            
	 * @return The #of write IOs performed during this method call.
	 * 
	 * @throws IOException
	 */
	synchronized
	public int flush(final IReopenChannel<FileChannel> opener)
			throws IOException {

		final ByteBuffer m_data = this.m_data.get();

		if (m_data.remaining() == 0) {
			// NOP.
			return 0;
		}
		
		// increment by the amount of data currently in the buffer.
		m_dataBytes.add( m_data.position() );
		
		// write out the data in the buffer onto the backing channel.
		m_data.flip();
		final int nwrites = FileChannelUtility.writeAll(opener, m_data, m_startAddr);
		m_fileWrites.add(nwrites);

		reset();
		
		return nwrites;
	}

	/**
	 * Reset the buffer position and limit and clear the starting offset on the
	 * file to <code>-1</code>. 
	 */
	synchronized
	public void reset() {
		
		final ByteBuffer m_data = this.m_data.get();

		// reset the buffer state.
		m_data.position(0);
		m_data.limit(m_data.capacity());
		
		m_startAddr = -1;
		m_endAddr = 0;
	}
	
	public String getStats(final StringBuffer buf, final boolean reset) {

		final String ret = "BufferedWrites, data: " + m_dataWrites + ", file: " + m_fileWrites + ", bytes: " + m_dataBytes;
		
		if (buf != null) {
			buf.append(ret + "\n");
		}
		
		if (reset) {
			m_dataBytes.set(0L);
			m_fileWrites.set(0L);
			m_dataWrites.set(0L);
		}
		
		return ret;
	}

}
