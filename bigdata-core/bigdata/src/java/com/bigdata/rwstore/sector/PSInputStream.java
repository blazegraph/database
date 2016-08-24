package com.bigdata.rwstore.sector;

import java.io.*;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

/************************************************************************
 * PSInputStream
 * 
 * Unlike the original PSInputStream this does not incrementally read
 * from the store but rather immediate maps the ByteBuffers to the
 * in-memory storage.
 **/
public class PSInputStream extends InputStream {
	
	/**
     * Logger.
     */
    final private static Logger log = Logger.getLogger(PSInputStream.class);
	
	final ByteBuffer[] m_buffers;
	
	int m_index = 0;
	int m_cursor = 0;
	
	public PSInputStream(IMemoryManager mm, long addr) {
		int sze = mm.allocationSize(addr);
		
		if (sze > SectorAllocator.BLOB_SIZE) {
			int buffers = 1 + (sze-1)/SectorAllocator.BLOB_SIZE;
			m_buffers = new ByteBuffer[buffers];
			// blob header
			ByteBuffer[] hdr = mm.get((addr & 0xFFFFFFFF00000000L) + ((buffers+1)*4));
			
			/*
			 * The header can itself be a blob array of buffers
			 */
			int rem = sze;
			int hdrsze = hdr[0].getInt();
			if (hdrsze != buffers) 
				throw new IllegalStateException("Check blob header assumptions: hdrsize(" 
						+ hdrsze + ") != buffers(" + buffers + ")");
			
			
			/*
			 * Calculate the data remaining in all header buffers
			 */
			{
				int totalRemaining = 0;
				for (ByteBuffer hbuf : hdr) {
					totalRemaining += hbuf.remaining();
				}
				if (totalRemaining != (buffers*4))
					throw new IllegalStateException("Check blob header assumptions, remaining: " 
							+ totalRemaining + " for " + buffers + " buffers");
			}
			
			int index = 0;
			int hdrIndex = 0;
			while (rem > SectorAllocator.BLOB_SIZE) {
				long ba = hdr[hdrIndex].getInt();
				ba <<= 32;
				ByteBuffer[] block = mm.get(ba + SectorAllocator.BLOB_SIZE);
				m_buffers[index++] = block[0];
				
				rem -= SectorAllocator.BLOB_SIZE;
				
				if (hdr[hdrIndex].remaining() == 0) {
					hdrIndex++; // next header buffer
				}
			}
			if (rem > 0) {
				long ba = hdr[hdrIndex].getInt();
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