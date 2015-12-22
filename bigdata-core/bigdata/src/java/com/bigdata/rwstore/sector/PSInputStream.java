package com.bigdata.rwstore.sector;

import java.io.*;
import java.nio.ByteBuffer;

/************************************************************************
 * PSInputStream
 * 
 * Unlike the original PSInputStream this does not incrementally read
 * from the store but rather immediate maps the ByteBuffers to the
 * in-memory storage.
 **/
public class PSInputStream extends InputStream {
	
	final ByteBuffer[] m_buffers;
	
	int m_index = 0;
	int m_cursor = 0;
	
	public PSInputStream(IMemoryManager mm, long addr) {
		int sze = mm.allocationSize(addr);
		
		if (sze > SectorAllocator.BLOB_SIZE) {
			int buffers = 1 + sze/SectorAllocator.BLOB_SIZE;
			m_buffers = new ByteBuffer[buffers];
			// blob header
			ByteBuffer[] hdr = mm.get((addr & 0xFFFFFFFF00000000L) + ((buffers+1)*4));
			
			if (hdr.length > 1)
				throw new IllegalStateException("Check header size assumptions");
			
			int rem = sze;
			int hdrsze = hdr[0].getInt();
			if (hdrsze != buffers) 
				throw new IllegalStateException("Check blob header assumptions");
			
			if (hdr[0].remaining() != (buffers*4))
				throw new IllegalStateException("Check blob header assumptions, remaining: " 
						+ hdr[0].remaining() + " for " + buffers + " buffers");
			
			int index = 0;
			while (rem > SectorAllocator.BLOB_SIZE) {
				long ba = hdr[0].getInt();
				ba <<= 32;
				ByteBuffer[] block = mm.get(ba + SectorAllocator.BLOB_SIZE);
				m_buffers[index++] = block[0];
				
				rem -= SectorAllocator.BLOB_SIZE;
			}
			if (rem > 0) {
				long ba = hdr[0].getInt();
				ba <<= 32;
				ByteBuffer[] block = mm.get(ba + rem);
				m_buffers[index] = block[0];
				
			}
		} else if (sze == 0) {
			m_buffers = new ByteBuffer[0];
		} else {
			m_buffers = mm.get(addr);
		}
	}
	
	@Override
	public int read() throws IOException {
		if (m_index >= m_buffers.length)
			return -1;
		
		final ByteBuffer buf = m_buffers[m_index];
		final int rem = buf.remaining();
		assert rem > 0;
		
		if (rem == 1)
			m_index++;
		
		return 0xFF & buf.get();
	}
	
	public synchronized int read(byte b[], int off, int len) throws IOException {
		if (m_index >= m_buffers.length)
			return -1;

		ByteBuffer buf = m_buffers[m_index];
		int rem = buf.remaining();
		int retlen = 0;
		
		while (len > rem) {
			buf.get(b, off, rem);
			retlen += rem;
			off += rem;
			len -= rem;
			
			if (++m_index == m_buffers.length) 
				return retlen;
			
			buf = m_buffers[m_index];
			rem = buf.remaining();
		}
		
		if (len > 0) {
			buf.get(b, off, len);
			rem -= len;
			retlen += len;
			
			if (rem == 0)
				m_index++;
		}
		

		return retlen;
	}

 }