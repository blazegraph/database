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

import java.io.*;
import java.nio.ByteBuffer;

import com.bigdata.rwstore.sector.IMemoryManager;
import com.bigdata.rwstore.sector.MemoryManager;
import com.bigdata.rwstore.sector.SectorAllocator;

/************************************************************************
 * PSInputStream
 * 
 * Unlike the original PSInputStream this does not incrementally read
 * from the store but rather immediate maps the ByteBuffers to the
 * in-memory storage.
 **/
public class PSInputStream extends InputStream {
	
	final RWStore m_store;
	
	final byte[] m_buffer;
	final int m_maxCursor;
	final DataInputStream m_hdrIn;
	final int m_blobBuffers;
	
	int m_cursor = 0;	
	int m_size = 0;
	
	public PSInputStream(final RWStore store, long addr) {
		m_store = store;
		
		m_size = (int) (addr & 0xFFFFFFFF);
		if (m_size < (store.m_maxFixedAlloc-4)) {
			m_buffer = new byte[m_size+4]; // allow for checksum

			store.getData((addr >> 32), m_buffer);
			m_hdrIn = null;
			m_blobBuffers = 0;
			m_maxCursor = m_size;
		} else {
			m_buffer = new byte[store.m_maxFixedAlloc];
			m_maxCursor = store.m_maxFixedAlloc-4;
		    final int alloc = m_maxCursor;
			final int nblocks = (alloc - 1 + (m_size-4))/alloc;
			final int hdrsize = 4 * (nblocks + 1) + 4; // plus 4 for checksum
			// FIXME: if hdrsize is blob then use recursive stream!
			if (hdrsize > store.m_maxFixedAlloc) {
				throw new UnsupportedOperationException("Header is a blob not yet supported");
			}
            final byte[] hdrbuf = new byte[hdrsize]; // plus 4 bytes for checksum
			store.getData((addr >> 32), hdrbuf);
            m_hdrIn = new DataInputStream(new ByteArrayInputStream(hdrbuf));
            try {
				m_blobBuffers = m_hdrIn.readInt();
			} catch (IOException e) {
				throw new RuntimeException("Unable to initialize blob header", e);
			}
            m_cursor = m_maxCursor; // force lazy read
		}
	}
	
	@Override
	public int read() throws IOException {
		if (m_size == 0) {
			return -1;
		}
		if (m_cursor >= m_maxCursor) {
			loadBuffer();
		}
		m_size--;
		
		return 0xFF & m_buffer[m_cursor++];
	}
	
	private int loadBuffer() throws IOException {
		final int nxtaddr = m_hdrIn.readInt();
		final int rdlen = m_size > m_maxCursor ? m_maxCursor : m_size;
		m_store.getData(nxtaddr, m_buffer, 0, rdlen+4);
		m_cursor = 0;
		
		return rdlen;
	}
	
	public synchronized int read(final byte dst[], final int off, final int len) throws IOException {
		if (m_size == 0) {
			return -1;
		}
		
		if (m_cursor >= m_maxCursor) {
			loadBuffer();
		}

		final int rdlen = len > m_size ? m_size : len;
		int avail = m_maxCursor - m_cursor;
		int delta = 0;
		
		while (avail < (rdlen-delta)) {
			System.arraycopy(m_buffer, m_cursor, dst, off+delta, avail);
			m_cursor += avail;
			delta += avail;
			m_size -= avail;
			avail = loadBuffer();
		}
		
		final int lastRead = (rdlen-delta);
		System.arraycopy(m_buffer, m_cursor, dst, off+delta, lastRead);			
		m_size -= lastRead;
		m_cursor += lastRead;
			
		return rdlen;
	}

 }
