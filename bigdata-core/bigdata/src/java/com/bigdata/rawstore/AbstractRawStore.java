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
/*
 * Created on Sep 5, 2007
 */

package com.bigdata.rawstore;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Abstract base class for {@link IRawStore} implementations. This class uses a
 * delegation pattern for the {@link IStoreSerializer} interface and does not
 * implement either the methods defined directly by the {@link IRawStore}
 * interface nor the methods of the {@link IAddressManager} interface. As such
 * it may be used as an abstract base class by any {@link IRawStore}
 * implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractRawStore implements IRawStore {

    /**
     * Return the delegate object that provides the {@link IAddressManager}
     * implementation for this {@link IRawStore}.
     * <p>
     * Note: {@link LRUNexus} depends on the delegation model to retain
     * references to the {@link IAddressManager} without causing the
     * {@link IRawStore} to be retained. It uses the {@link IAddressManager} to
     * decode the address in order to track the bytesOnDisk for the buffered
     * records.
     */
    abstract public IAddressManager getAddressManager();
   
    /**
     * The designated constructor.
     */
    public AbstractRawStore() {

    }

//    /**
//     * The default implementation delegates to {@link #write(ByteBuffer)}.
//     */
//    @Override
//    public long write(ByteBuffer data, long oldAddr) {
//    	return write(data);
//    }

    /**
     * The default implementation is a NOP.
     */
	public void delete(final long addr) {
	    // NOP.
	}

	@Override
	public IPSOutputStream getOutputStream() {
		// TODO: implement an optional pooled object creation
		//	to allow re-use of streams (with their buffers).
		// see ticket:#641
		return new WORMOutputStream();
	}

	@Override
	public InputStream getInputStream(long addr) {
		return new WORMInputStream(addr);
	}
	
	// WORM_STREAM_BUFFER_SIZE as used by Output and Input streams
	static final int WORM_STREAM_BUFFER_SIZE = 16 * 1024;

	/**
	 * WORMOutputStream
	 * <p>
	 * This implements a buffered allocation that may be split
	 * across muliple smaller allocations, thus providing a scalable
	 * approach for concurrent allocations of very large amounts
	 * of data without needing single large buffers for the full
	 * amount.
	 * <p>
	 * The stream address may be interpreted differently from a normal
	 * allocation in that the data size is associated with the total data
	 * stored across all stream allocations.  Thus an address returned by
	 * a stream must only ever be used to retrieve an input stream.
	 * <p>
	 * This is different from the RWStore where the BLOB allocation mechanism
	 * uses streams internally and therefore the addressing mechanism is
	 * compatible with external stream usage.
	 * <p>
	 * Note that an address returned from getAddr() is the negative of the
	 * normal storage address, differentiating a stream address from a
	 * conventional allocation.
	 */
	class WORMOutputStream extends IPSOutputStream {
		final private byte[] m_buffer = new byte[WORM_STREAM_BUFFER_SIZE];
		private int m_cursor = 0;
		
		private int m_bytesWritten = 0;
		
		private boolean m_open = true; // open on creation
		
		private ByteArrayOutputStream m_hdrData = null;
		private DataOutputStream m_header = null;

		@Override
		public long getAddr() {
			if (!m_open)
				throw new IllegalStateException();
			
			m_open = false;
			
			if (m_cursor == 0)
				return 0; // no content is fine for a stream
			
			final ByteBuffer bb = ByteBuffer.wrap(m_buffer, 0, m_cursor);
			
			final long addr = AbstractRawStore.this.write(bb);
			
			m_bytesWritten += m_cursor;
			
			if (m_header == null)
				return addr; // return conventional address since no header is required
			
			// handle blob stream header
			try {
				m_header.writeLong(addr);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			
			// write stream header
			final ByteBuffer hbb = ByteBuffer.wrap(m_hdrData.toByteArray());
			final long hdrAddr = AbstractRawStore.this.write(hbb);
			
			final IAddressManager am = getAddressManager();
			
			// return a stream address indicating the total size of the data and the address
			//	of the header data.
			//
			// the negative address indicates that this is a stream header and cannot be
			//	processed as a contiguous allocation.
			//
			// FIXME: enable standard non-stream read to process streamed data similarly to
			//	RWStore BLOB handling.
			return -am.toAddr(m_bytesWritten, am.getOffset(hdrAddr));
		}

		@Override
		public void write(int b) throws IOException {
			// check threshold write
			if (m_cursor == WORM_STREAM_BUFFER_SIZE) {
				final ByteBuffer bb = ByteBuffer.wrap(m_buffer);
				final long waddr = AbstractRawStore.this.write(bb);
				if (m_header == null) {
					m_hdrData = new ByteArrayOutputStream();
					m_header = new DataOutputStream(m_hdrData);
				}
				m_header.writeLong(waddr);
				m_cursor = 0;
				m_bytesWritten += WORM_STREAM_BUFFER_SIZE;
			}
			
			m_buffer[m_cursor++] = (byte) b;
		}
		
		/****************************************************************
		 * write byte array to the buffer
		 * 
		 * we need to be able to efficiently handle large arrays beyond size
		 * of the blobThreshold, so
		 **/
	  public void write(final byte b[], final int off, final int len) throws IOException {
	  	
	  	if (!m_open) {
	  		throw new IllegalStateException("Already written");
	  	}
	  	
	  	if ((m_cursor + len) > WORM_STREAM_BUFFER_SIZE) {
	  		// not optimal, but this will include a disk write anyhow so who cares
	  		for (int i = 0; i < len; i++) {
	  			write(b[off+i]);
	  		}
	  	} else {
			System.arraycopy(b, off, m_buffer, m_cursor, len);
			
			m_cursor += len;
			
		}
	  }
	}
	
	/**
	 * The WORMInputStream is returned by the getInputStream
	 *	method.
	 *
	 * 
	 */
	class WORMInputStream extends InputStream {
		
		final private ByteBuffer m_hbb;
		private ByteBuffer m_bb;
		
		WORMInputStream(final long stream_addr) {
//			if (stream_addr > 0) {
//				throw new IllegalArgumentException("Address: " + stream_addr + " was not returned from a stream");
//			}
			
			// if negative then is a stream header
			final boolean isStream = stream_addr < 0;
			
			// convert back to original allocation adress
			final long addr = isStream ? -stream_addr : stream_addr;
			
			final IAddressManager am = getAddressManager();
			
			final int nbytes = am.getByteCount(addr);
			
			// Stream could have
			if (isStream && nbytes != 0 && nbytes < WORM_STREAM_BUFFER_SIZE)
				throw new IllegalArgumentException("Stream Address for unexpected data length: " + nbytes);
			
			// check for stream header
			if (isStream) {
				// handle overspill
				final int nblocks = (nbytes + (WORM_STREAM_BUFFER_SIZE-1)) / WORM_STREAM_BUFFER_SIZE;
				
				m_hbb = AbstractRawStore.this.read(am.toAddr(nblocks * 8, am.getOffset(addr))); // 8 bytes size of long
				
				// read first block
				assert m_hbb.hasRemaining();
				m_bb = AbstractRawStore.this.read(m_hbb.getLong());
			} else {
				m_hbb = null;
				
				if (nbytes > 0)				
					m_bb = AbstractRawStore.this.read(addr);	
				else
					m_bb = ByteBuffer.allocate(0);
			}
		}

		@Override
		public int read() throws IOException {
			if (!m_bb.hasRemaining() && m_hbb != null && m_hbb.hasRemaining()) {
				m_bb = AbstractRawStore.this.read(m_hbb.getLong());
			}
			
			if (!m_bb.hasRemaining())
				return -1;
			
			// return unsigned byte as int
			return 0xFF & m_bb.get();
		}
		
		public synchronized int read(final byte dst[], final int off, final int len) throws IOException {
			if (m_bb.remaining() >= len) {
				m_bb.get(dst, off, len);
				return len;
			}
			int cursor = 0;
			
			final int len1 = m_bb.remaining();
			m_bb.get(dst, off, len1);
			cursor += len1;
			
			while (m_hbb != null && m_hbb.hasRemaining()) {
				m_bb = AbstractRawStore.this.read(m_hbb.getLong());
				
				final int len2 = len - cursor;
				if (m_bb.remaining() >= len2) {
					m_bb.get(dst, off+cursor, len2);
					cursor += len2;
					break;
				} else {
					final int len3 = m_bb.remaining();
					m_bb.get(dst, off+cursor, len3);
					cursor += len3;
				}
			}
			
			return cursor;
		}
	}
}
