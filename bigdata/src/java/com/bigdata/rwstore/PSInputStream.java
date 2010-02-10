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

package com.bigdata.rwstore;

import java.io.*;

/************************************************************************
 * PSOutputStream
 *
 * Provides stream interface direct to the low-level store.
 *
 * Retrieved from an IObjectStore to enable output to the store.
 *
 * The key idea here is that rather than a call like :
 *	store.realloc(oldAddr, byteOutputStream)=> newAddress
 *
 * instead :
 *	store.allocStream(oldAddr)=>PSOutputStream
 *
 * and then :
 *	stream.save()=> newAddress
 *
 * This will enable large data formats to be streamed to the data store,
 *	where the previous interface would have required that the entire
 *	resource was loaded into a memory structure first, before being
 *	written out in a single block to the store.
 *
 * This new approach will also enable the removal of BLOB allocation
 *	strategy.  Instead, "BLOBS" will be served by linked fixed allocation
 *	blocks, flushed out by the stream.
 *
 * A big advantage of this is that BLOB reallocation is now a lot simpler,
 *	since BLOB storage is simply a potentially large number of 8K blocks.
 *
 * This also opens up the possibility of a Stream oriented data type, that
 *	could be used to serve up a variety of data streams.  By providing
 *	relevant interfaces with the client/server system, a server can then
 *	provide multiple streams to a high number of clients.
 *
 * To this end, the output stream has a fixed buffer size, and they are recycled
 *	from a pool of output streams.
 **/
public class PSInputStream extends InputStream {

	static PSInputStream m_poolHead = null;
	static PSInputStream m_poolTail = null;
	static Integer m_lock = new Integer(42);
	static int m_streamCount = 0;
	
	static int s_allocStreams = 0;
	static int s_returnStreams = 0;
	
	public static PSInputStream getNew(IStore store, int size) {
		synchronized (m_lock) {
			s_allocStreams++;
			
			PSInputStream ret = m_poolHead;
			if (ret != null) {
				m_streamCount--;
				
				m_poolHead = ret.next();
				if (m_poolHead == null) {
					m_poolTail = null;
				}
			} else {
				ret = new PSInputStream();
			}
			
			ret.init(store, size);
			
			return ret;
		}
	}
	
	/*******************************************************************
	 * maintains pool of streams - in a normal situation there will only
	 *	me a single stream continually re-used, but with some patterns
	 *	there could be many streams.  For this reason it is worth checking
	 *	that the pool is not maintained at an unnecessaily large value, so
	 *	 maximum of 10 streams are maintained - adding upto 80K to the
	 *	 garbage collect copy.
	 **/
	static public void returnStream(PSInputStream stream) {
		synchronized (m_lock) {
			s_returnStreams++;
			
			if (m_streamCount > 10) {
				return;
			}
			
			if (m_poolTail != null) {
				m_poolTail.setNext(stream);
			} else {
				m_poolHead = stream;
			}
			
			m_poolTail = stream;
			m_streamCount++;
		}
	}
	
	final int cBufsize = 16 * 1024;
	int m_blobThreshold = 0;
	byte[] m_buf = new byte[cBufsize];
	int m_headAddr = 0;
	int m_count = 0;
	int m_cursor = 0;
	int m_totalBytes = -1;
	int m_totalRead = 0;
	IStore m_store;
	
	private PSInputStream m_next = null;
	
	private PSInputStream next() {
		return m_next;
	}
	
	private void setNext(PSInputStream str) {
		m_next = str;
	}
	
	public void close() {
		returnStream(this);
	}
	
	/****************************************************************
	 * resets private state variables for reuse of stream
	 **/
	void init(IStore store, int size) {
		m_headAddr = 0;
		m_count = size;
		m_store = store;
		m_cursor = 0;
		m_blobThreshold = m_store.bufferChainOffset();
		m_totalBytes = -1;
		m_totalRead = 0;
	}			


	/****************************************************************
	 * Returns buffer for initial read - FIX PROTOCOL LATER
	 **/
	public void setTotalBytes(int totalBytes) {
		m_totalBytes = totalBytes;
	}

	/****************************************************************
	 * Returns buffer for initial read - FIX PROTOCOL LATER
	 **/
	public byte[] getBuffer() {
		return m_buf;
	}

	/************************************************************
	 * util to ensure negatives don't screw things
	 **/
	private int makeInt(byte val) {
		int ret = val;
		
		return ret & 0xFF;
	}
	
	/****************************************************************
	 * Reads next byte - throws EOFException if none more available
	 **/
	public int read() throws IOException {
		
		if (m_totalBytes >=0) {
			if (m_totalRead >= m_totalBytes) {
				return -1;
			}
		}
		
		m_totalRead++;

		if (m_cursor == m_blobThreshold) {
			int nextAddr = makeInt(m_buf[m_cursor++]) << 24;
			nextAddr += makeInt(m_buf[m_cursor++]) << 16;
			nextAddr += makeInt(m_buf[m_cursor++]) << 8;
			nextAddr += makeInt(m_buf[m_cursor]);
			
			m_count = m_store.getDataSize(nextAddr, m_buf);
			
			m_cursor = 0;
		}
		
		if (m_cursor >= m_count) {
			return -1;
		}
		
		int ret = m_buf[m_cursor++];
		return ret & 0xFF;
	}
	
	/****************************************************************
	 * Reads next 4 byte integer value
	 **/
	public int readInt() throws IOException {
		int value = read() << 24;
		value += read() << 16;
		value += read() << 8;
		value += read();
		
		return value;
	}
	
	public long readLong() throws IOException {
		long value = readInt();
		value <<= 32;
		
		value += readInt();
		
		return value;
	}
	
	public synchronized int read(byte b[], int off, int len) throws IOException {
		if (len == 0) {
			return 0;
		}
		
		if (len <= available()) {
			System.arraycopy(m_buf, m_cursor, b, off, len);
			m_cursor += len;
			m_totalRead += len;
		} else {
			for (int i = 0; i < len; i++) {
				int r = read();
				if (r != -1) {
					b[off + i] = (byte) r;
				} else {
					return i == 0 ? -1 : i;
				}
			}
		}
		
		return len;
	}

	/****************************************************************
	 * Space left - until buffer overrun
	 **/
  public int available() throws IOException {
		if (m_count < m_blobThreshold) {
			return m_count - m_cursor;
		} else {
			return m_blobThreshold - m_cursor;
		}
  }

	/****************************************************************
	 * utility method that extracts all data from this stream and
	 *	writes to the output stream
	 **/
  public int read(OutputStream outstr) throws IOException {
  	byte b[] = new byte[512];
  	
  	int retval = 0;
  	
  	int r = read(b);
  	while (r == 512) {
  		outstr.write(b, 0, r);
  		retval += r;
  		
  		r = read(b);
  	}
  	
  	if (r != -1) {
  		outstr.write(b, 0, r);
  		retval += r;
  	}
  	
  	return retval;
  }
 }