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

import java.io.OutputStream;
import java.io.ObjectOutputStream;
import java.io.FilterOutputStream;
import java.io.InputStream;
import java.io.IOException;

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
 *
 * It is ** important **  that output streams are bound to the IStore they
 *	are requested for.
 **/
public class PSOutputStream extends OutputStream {

	protected static java.util.logging.Logger cat = java.util.logging.Logger.getLogger(PSOutputStream.class.getName());

	static PSOutputStream m_poolHead = null;
	static PSOutputStream m_poolTail = null;
	static Integer m_lock = new Integer(42);
	static int m_streamCount = 0;
	
	public static PSOutputStream getNew(IStore store) {
		synchronized (m_lock) {
			PSOutputStream ret = m_poolHead;
			if (ret != null) {
				m_streamCount--;
				
				m_poolHead = ret.next();
				if (m_poolHead == null) {
					m_poolTail = null;
				}
			} else {
				ret = new PSOutputStream();
			}
			
			ret.init(store);
			
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
	static void returnStream(PSOutputStream stream) {
		synchronized (m_lock) {
			if (m_streamCount > 10) {
				return;
			}
			
			stream.m_count = 0; // avoid overflow
			
			if (m_poolTail != null) {
				m_poolTail.setNext(stream);
			} else {
				m_poolHead = stream;
			}
			
			m_poolTail = stream;
			m_streamCount++;
		}
	}
	
	final int cBufsize = 16 * 1024; // ensure big enough for reasonable requested blobThresholds
	byte[] m_buf = new byte[cBufsize];
	boolean m_isSaved = false;
	long m_headAddr = 0;
	long m_prevAddr = 0;
	int m_count = 0;
	int m_bytesWritten = 0;
	int m_blobThreshold = 0;
	IStore m_store;
	
	private PSOutputStream m_next = null;
	
	private PSOutputStream next() {
		return m_next;
	}
	
	private void setNext(PSOutputStream str) {
		m_next = str;
	}
	
	/****************************************************************
	 * resets private state variables for reuse of stream
	 **/
	void init(IStore store) {
		m_isSaved = false;
		flushAttached();
		
		m_headAddr = 0;
		m_prevAddr = 0;
		m_count = 0;
		m_bytesWritten = 0;
		m_store = store;
		m_isSaved = false;
		m_blobThreshold = m_store.bufferChainOffset();
		
		m_store.startTransaction();
	}			

	/****************************************************************
	 * write a single byte
	 *
	 * this is the one place where the blobthreshold is handled
	 *	and its done one byte at a time so should be easy enough,
	 *
	 * The last byte of an 8K buffer is used to indicate whether
	 *	the buffer is extended.  If not, it is set to 0x00.  If
	 *	it is extended, it is set to 0xFF - any other value is an
	 *	error.
	 *
	 * If the buffer is extended, then the previous 4 bytes, hold the
	 *	address of the next buffer, and so on.
	 *
	 * Must store previous address for link forward, and must also
	 *	store head address.
	 **/
  public void write(int b) throws IOException {
  	if (m_store == null) {
  		return;
  	}
  	
  	if (m_isSaved) {
  		throw new Error("What the ?");
  	}
  	
  	if (m_count == m_blobThreshold) {  		
  		long curAddr = m_store.alloc(m_buf, m_count + m_store.getAddressSize()); // allow for continuation address!
  		
  		if (m_prevAddr != 0) {
  			m_store.absoluteWriteAddress(m_prevAddr, m_blobThreshold, curAddr);
  		}
  	
  		m_prevAddr = curAddr;
  		if (m_headAddr == 0) {
  			m_headAddr = m_prevAddr;
  		}
  		
  		m_count = 0;
  	}
  	
  	m_buf[m_count++] = (byte) b;

  	m_bytesWritten++;
  }
  
	/****************************************************************
	 * write a single 4 byte integer
	 **/
  public void writeInt(int b) throws IOException {
  	write((b >>> 24) & 0xFF);
  	write((b >>> 16) & 0xFF);
  	write((b >>> 8) & 0xFF);
  	write(b & 0xFF);
  }
  
  public void writeLong(long b) throws IOException {
  	int hi = (int) (b >> 32);
  	int lo = (int) (b & 0xFFFFFFFF);
   	writeInt(hi);
   	writeInt(lo);
  }
  
	/****************************************************************
	 * write byte array to the buffer
	 **/
  public void write(byte b[], int off, int len) throws IOException {
  	if (m_store == null) {
  		
  		return;
  	}
  	
  	if (m_isSaved) {
  		throw new RuntimeException("PSOutputStream: already been saved");
  	}
  	
  	if ((m_count + len) > m_blobThreshold) {
  		// not optimal, but this will include a disk write anyhow so who cares
  		for (int i = off; i < len; i++) {
  			write(b[i]);
  		}
  	} else {
  		if (len > 4096) {
  			cat.warning("Unexpected large single object: " + len);
  		}
			System.arraycopy(b, off, m_buf, m_count, len);
			
			m_count += len;
			m_bytesWritten += len;
			
  		if (m_count > 8192) {
  			cat.warning("Unexpected large write: " + m_count);
  		}
		}
  }
  
	/****************************************************************
	 * utility method that extracts data from the input stream
	 * @param instr and write to the store.
	 *
	 * This method can be used to stream external files into
	 *	the store.
	 **/
  public void write(InputStream instr) throws IOException {
  	if (m_isSaved) {
  		throw new RuntimeException("PSOutputStream: already been saved");
  	}
  	
  	byte b[] = new byte[512];
  	
  	int r = instr.read(b);
  	while (r == 512) {
  		write(b, 0, r);
  		r = instr.read(b);
  	}
  	
  	if (r != -1) {
  		write(b, 0, r);
  	}
  }
  
  /****************************************************************
   * on save() the current buffer is allocated and written to the
   *	store, and the address of its location returned
   **/
  public long save() {
  	if (m_isSaved) {
  		throw new RuntimeException("PSOutputStream: already been saved");
  	}
  	
  	if (m_store == null) {
  		return 0;
  	}
  	
  	/*
  	if (m_bytesWritten == 0) {
  		cat.info("PSOutputStream: No data has been written before save()");
  	}
  	*/
  	
  	long addr = m_store.alloc(m_buf, m_count);
  	
  	if (m_headAddr == 0) {
  		m_headAddr = addr;
  	} else {
  		m_store.absoluteWriteAddress(m_prevAddr, m_blobThreshold, addr);
  	}
  
  	m_isSaved = true;
  	
  	m_store.commitTransaction(); // What about transaction count?
  		
  	return m_headAddr;
  }
  
  public void close() throws IOException {
  	if (m_store != null) {
  		m_store = null;
  	
			m_isSaved = false;
			
  		returnStream(this);
  	}
  }
  
  /**************************************************************
   * Support method for storing information in GPOMap,
   *	returning the current headAddress and clearing
   *	noting that the stream must be closed
   **/
  public long getAddrAndClear() {
  	if (!m_isSaved) {
  		throw new RuntimeException("The stream has not been saved");
  	}
  	
  	if (m_store == null) {
  		throw new RuntimeException("The stream must not be closed");
  	}
  	
  	long retval = m_headAddr;
  	
  	m_headAddr = 0;
  	m_bytesWritten = 0;
  	m_isSaved = false;
  	
  	return retval;
  }
  
  public int getBytesWritten() {
  	return m_bytesWritten;
  }
  
  ObjectOutputStream m_attachedStream = null;
  
  public void attachStream(ObjectOutputStream outstr) {
  	m_attachedStream = outstr;
  }
  
  public ObjectOutputStream getAttachedStream() {
  	return m_attachedStream;
  }
  
  protected void flushAttached() {
  	if (m_attachedStream != null) {
  		try {
  			m_attachedStream.flush();
  			m_attachedStream.reset();
  		} catch (IOException e) {
  			throw new Error("Problem with attached stream", e);
  		}
  	}
  }
  
  protected void finalize() throws Throwable {
  	close();
  }
  
  public OutputStream getFilterWrapper(final boolean saveBeforeClose) {
  	
		return new FilterOutputStream(this) {
			public void close() throws IOException {
				if (saveBeforeClose) {
					flush();
					
					save();
					
					super.close();
				} else {
					super.close();
					
					save();
				}
			}
		};
	}
			
			
}


/***********************************************
Jython test

from cutthecrap.gpo.client import OMClient;
from cutthecrap.gpo import *;
from java.io import *;

client = OMClient("", "D:/db/test.wo");

om = client.getObjectManager();

g = GPOMap(om);

os = om.createOutputStream();

ds = ObjectOutputStream(os);

ds.writeObject("Hi there World!");

ds.flush();
os.save();

g.set("stream", os);

ds.close();
os.close();

instr = g.get("stream");

ds = ObjectInputStream(instr);

print "read in", ds.readObject();



**/