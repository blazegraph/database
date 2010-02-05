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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.UUID;

import com.bigdata.counters.CounterSet;
import com.bigdata.mdi.IResourceMetadata;
import com.bigdata.rawstore.AbstractRawStore;
import com.bigdata.rawstore.IAddressManager;
import com.bigdata.rawstore.IUpdateStore;
import com.bigdata.rwstore.RWStore;

/**
 * The hook that accesses the RWStore to provide read/write services as opposed to the
 * WORM characteristics of the DiskOnlyStrategy
 * 
 * @author mgc
 */
public class RWStrategy extends AbstractRawStore implements
IDiskBasedStrategy, IUpdateStore, IBufferStrategy, IAddressManager {

	File m_file = null;
	
	RWStore m_store = null;
	
//	RWStrategy(final long maximumExtent, final FileMetadata fileMetadata) {
//
////        super(fileMetadata.extent, maximumExtent, fileMetadata.offsetBits,
////                fileMetadata.nextOffset, fileMetadata.bufferMode,
////                fileMetadata.readOnly);
//
//        m_file = fileMetadata.file;
//        
//        m_store = new RWStore(m_file, false); // not read-only for now
//	}

	RWStrategy(File file) {

//        super(0L, 200*1000*1000, 32,
//                8 * 1000, BufferMode.DiskRW,
//                false);

        m_file = file;
        
        m_store = new RWStore(m_file, false); // not read-only for now
	}

	public FileChannel getChannel() {
		return m_store.getChannel();
	}

	public File getFile() {
		return m_file;
	}

	public int getHeaderSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	public RandomAccessFile getRandomAccessFile() {
		return m_store.getRandomAccessFile();
	}

	public CounterSet getCounters() {
		// TODO Auto-generated method stub
		return null;
	}

	public long getExtent() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getUserExtent() {
		// TODO Auto-generated method stub
		return 0;
	}

	public ByteBuffer readRootBlock(boolean rootBlock0) {
		// TODO Auto-generated method stub
		return null;
	}

	public long transferTo(RandomAccessFile out) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	public void truncate(long extent) {
		// TODO Auto-generated method stub
		
	}

	public void writeRootBlock(IRootBlockView rootBlock,
			ForceEnum forceOnCommitEnum) {
		rootBlock.asReadOnlyBuffer();
		
	}

	public void deleteResources() {
		// TODO Auto-generated method stub
		
	}

	public void force(boolean metadata) {
		// TODO Auto-generated method stub
		
	}

	public boolean isFullyBuffered() {
		return false;
	}

	public boolean isStable() {
		// TODO Auto-generated method stub
		return false;
	}

	public ByteBuffer read(long addr) {
		int rwaddr = decodeAddr(addr);
		int sze = decodeSize(addr);
		
		byte buf[] = new byte[sze];		
		m_store.getData(rwaddr, buf);
		
		return ByteBuffer.wrap(buf);
	}

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

	public void update(long addr, int off, ByteBuffer data) {
		// m_store.
	}

	public void delete(long addr) {
		m_store.free(addr);
	}

    public IAddressManager getAddressManager() {
        // TODO Auto-generated method stub
        return null;
    }

    public void closeForWrites() {
        // TODO Auto-generated method stub
        
    }

    public BufferMode getBufferMode() {
        // TODO Auto-generated method stub
        return null;
    }

    public long getInitialExtent() {
        // TODO Auto-generated method stub
        return 0;
    }

    public long getMaximumExtent() {
        // TODO Auto-generated method stub
        return 0;
    }

    public long getNextOffset() {
        // TODO Auto-generated method stub
        return 0;
    }

    public void close() {
        // TODO Auto-generated method stub
        
    }

    public void destroy() {
        // TODO Auto-generated method stub
        
    }

    public IResourceMetadata getResourceMetadata() {
        // TODO Auto-generated method stub
        return null;
    }

    public UUID getUUID() {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean isOpen() {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean isReadOnly() {
        // TODO Auto-generated method stub
        return false;
    }

    public long size() {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getByteCount(long addr) {
        // TODO Auto-generated method stub
        return 0;
    }

    public long getOffset(long addr) {
        // TODO Auto-generated method stub
        return 0;
    }

//    public void packAddr(DataOutput out, long addr) throws IOException {
//        // TODO Auto-generated method stub
//        
//    }

    public long toAddr(int nbytes, long offset) {
        // TODO Auto-generated method stub
        return 0;
    }

    public String toString(long addr) {
        // TODO Auto-generated method stub
        return null;
    }

//    public long unpackAddr(DataInput in) throws IOException {
//        // TODO Auto-generated method stub
//        return 0;
//    }

}
