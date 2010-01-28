/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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

package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.bigdata.counters.CounterSet;
import com.bigdata.rawstore.IUpdateStore;
import com.bigdata.rwstore.RWStore;

/**
 * The hook that accesses the RWStore to provide read/write services as opposed to the
 * WORM characteristics of the DiskOnlyStrategy
 * 
 * @author mgc
 */
public class RWStrategy extends AbstractBufferStrategy implements
IDiskBasedStrategy, IUpdateStore {

	File m_file = null;
	
	RWStore m_store = null;
	
	RWStrategy(final long maximumExtent, final FileMetadata fileMetadata) {

        super(fileMetadata.extent, maximumExtent, fileMetadata.offsetBits,
                fileMetadata.nextOffset, fileMetadata.bufferMode,
                fileMetadata.readOnly);

        m_file = fileMetadata.file;
        
        m_store = new RWStore(m_file, false); // not read-only for now
	}

	RWStrategy(File file) {

        super(0L, 200*1000*1000, 32,
                8 * 1000, BufferMode.DiskRW,
                false);

        m_file = file;
        
        m_store = new RWStore(m_file, false); // not read-only for now
	}

	@Override
	public FileChannel getChannel() {
		return m_store.getChannel();
	}

	@Override
	public File getFile() {
		return m_file;
	}

	@Override
	public int getHeaderSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public RandomAccessFile getRandomAccessFile() {
		return m_store.getRandomAccessFile();
	}

	@Override
	public CounterSet getCounters() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getExtent() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getUserExtent() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ByteBuffer readRootBlock(boolean rootBlock0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long transferTo(RandomAccessFile out) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void truncate(long extent) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeRootBlock(IRootBlockView rootBlock,
			ForceEnum forceOnCommitEnum) {
		rootBlock.asReadOnlyBuffer();
		
	}

	@Override
	public void deleteResources() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void force(boolean metadata) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isFullyBuffered() {
		return false;
	}

	@Override
	public boolean isStable() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ByteBuffer read(long addr) {
		int rwaddr = decodeAddr(addr);
		int sze = decodeSize(addr);
		
		byte buf[] = new byte[sze];		
		m_store.getData(rwaddr, buf);
		
		return ByteBuffer.wrap(buf);
	}

	@Override
	public long write(ByteBuffer data) {
        final int nbytes = data.remaining();
		long rwaddr = m_store.alloc(data.array(), nbytes);
		
		return 0;
	}

	public long allocate(int nbytes) {
		return encodeAddr(m_store.alloc(nbytes), nbytes);
	}

	private long encodeAddr(long alloc, int nbytes) {
		alloc <<= 32;
		alloc += nbytes;
		
		return alloc;
	}
	private int decodeAddr(long addr) {
		addr >>= 32;
		
		return (int) addr;
	}
	private int decodeSize(long addr) {
		return (int) (addr & 0xFFFFFFFF);
	}

	@Override
	public void update(long addr, int off, ByteBuffer data) {
		// m_store.
	}

	@Override
	public void delete(long addr) {
		m_store.free(addr);
	}

}
