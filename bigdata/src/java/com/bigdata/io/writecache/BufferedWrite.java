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

import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.IReopenChannel;
import com.bigdata.rwstore.RWStore;

/**
 * The BufferedWrite merges/elides sorted scattered writes to minimise
 * IO requests and maximise IO rates.
 * 
 * @author Martyn Cutcher
 *
 */
public class BufferedWrite {
	final RWStore m_store;
	final ByteBuffer m_data;
	long m_startAddr = -1;
	long m_endAddr = 0;
	
	long m_dataBytes = 0;
	long m_dataWrites = 0;
	long m_fileWrites = 0;
	
	public BufferedWrite(final RWStore store) throws InterruptedException {
		m_store = store;
		m_data = DirectBufferPool.INSTANCE.acquire();
	}
	
	public int write(final long offset, final ByteBuffer data, final IReopenChannel<FileChannel> opener) throws IOException {
		int nwrites = 0;
		
		m_dataWrites++;
		
		int data_len = data.remaining();
		int slot_len = m_store.getSlotSize(data_len);
		
		if (slot_len > m_data.remaining()) {
			nwrites += flush(opener);
		}
		
		if (m_startAddr == -1) {
			m_startAddr = m_endAddr = offset;
		} else if (m_endAddr != offset) {
			// if this is NOT a contiguous write then flush existing content
			nwrites += flush(opener);	
			m_startAddr = m_endAddr = offset;
		}
		m_data.put(data);
		m_endAddr += slot_len;
		long pos = m_endAddr - m_startAddr;
		m_data.position((int) pos);
		
		return nwrites;
	}
	
	public int flush(final IReopenChannel<FileChannel> opener) throws IOException {
		m_dataBytes += m_data.position();
		
		m_data.flip();
		final int nwrites = FileChannelUtility.writeAll(opener, m_data, m_startAddr);
		m_fileWrites++;
		
		m_data.position(0);
		m_data.limit(m_data.capacity());
		
		m_startAddr = -1;
		m_endAddr = 0;
		
		return nwrites;
	}
	
	public String getStats(StringBuffer buf, boolean reset) {
		String ret = "BufferedWrites, data: " + m_dataWrites + ", file: " + m_fileWrites + ", bytes: " + m_dataBytes;
		
		if (buf != null) {
			buf.append(ret + "\n");
		}
		
		if (reset) {
			m_dataBytes = m_fileWrites = m_dataWrites = 0;
		}
		
		return ret;
	}
}
